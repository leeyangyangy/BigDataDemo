package xyz.leeyangy.spc.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * 登录尝试限制服务单元测试。
 *
 * <p>重点验证 P0 修复: fail-closed 行为 (Redis 异常时假定已锁定, 防止暴力破解绕过)。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("登录尝试限制服务测试 (fail-closed)")
class LoginAttemptServiceImplTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private LoginAttemptServiceImpl loginAttemptService;

    private static final int MAX_ATTEMPTS = 5;
    private static final int WINDOW_SECONDS = 300;
    private static final int LOCKOUT_SECONDS = 1800;

    @BeforeEach
    void setUp() {
        // @Value 字段不会被 @InjectMocks 注入, 手动设置
        ReflectionTestUtils.setField(loginAttemptService, "maxAttempts", MAX_ATTEMPTS);
        ReflectionTestUtils.setField(loginAttemptService, "windowSeconds", WINDOW_SECONDS);
        ReflectionTestUtils.setField(loginAttemptService, "lockoutSeconds", LOCKOUT_SECONDS);
    }

    // ==================== getRemainingLockSeconds ====================

    @Test
    @DisplayName("Redis 异常时返回 lockoutSeconds (fail-closed, 不放行)")
    void getRemainingLockSeconds_RedisException_ReturnsLockoutSeconds() {
        when(redisTemplate.getExpire(anyString(), eq(TimeUnit.SECONDS)))
                .thenThrow(new RuntimeException("Redis 连接断开"));

        Long result = loginAttemptService.getRemainingLockSeconds("s12345");

        assertNotNull(result, "fail-closed: Redis 异常时不应返回 null (null=未锁定)");
        assertEquals(LOCKOUT_SECONDS, result, "fail-closed: 应返回完整锁定时长");
    }

    @Test
    @DisplayName("TTL=0 (已过期未清理) 返回 null (未锁定)")
    void getRemainingLockSeconds_TTLZero_ReturnsNull() {
        when(redisTemplate.getExpire(anyString(), eq(TimeUnit.SECONDS))).thenReturn(0L);

        Long result = loginAttemptService.getRemainingLockSeconds("s12345");

        assertNull(result, "TTL=0 表示已过期, 应返回 null (未锁定)");
    }

    @Test
    @DisplayName("TTL=-2 (key 不存在) 返回 null (未锁定)")
    void getRemainingLockSeconds_KeyNotExists_ReturnsNull() {
        when(redisTemplate.getExpire(anyString(), eq(TimeUnit.SECONDS))).thenReturn(-2L);

        Long result = loginAttemptService.getRemainingLockSeconds("s12345");

        assertNull(result, "key 不存在时应返回 null (未锁定)");
    }

    @Test
    @DisplayName("TTL>0 返回剩余锁定秒数")
    void getRemainingLockSeconds_TTLPositive_ReturnsTTL() {
        when(redisTemplate.getExpire(anyString(), eq(TimeUnit.SECONDS))).thenReturn(900L);

        Long result = loginAttemptService.getRemainingLockSeconds("s12345");

        assertNotNull(result);
        assertEquals(900L, result);
    }

    @Test
    @DisplayName("空工号返回 null")
    void getRemainingLockSeconds_EmptyEmpNo_ReturnsNull() {
        Long result = loginAttemptService.getRemainingLockSeconds("");

        assertNull(result);
        verifyNoInteractions(redisTemplate);
    }

    @Test
    @DisplayName("null 工号返回 null")
    void getRemainingLockSeconds_NullEmpNo_ReturnsNull() {
        Long result = loginAttemptService.getRemainingLockSeconds(null);

        assertNull(result);
        verifyNoInteractions(redisTemplate);
    }

    // ==================== recordFailedAttempt ====================

    @Test
    @DisplayName("Redis 异常时返回 maxAttempts (fail-closed, 触发锁定检查)")
    void recordFailedAttempt_RedisException_ReturnsMaxAttempts() {
        when(redisTemplate.opsForValue()).thenThrow(new RuntimeException("Redis 连接断开"));

        long result = loginAttemptService.recordFailedAttempt("s12345", "192.168.1.1");

        assertEquals(MAX_ATTEMPTS, result, "fail-closed: Redis 异常时应返回 maxAttempts 触发锁定");
    }

    @Test
    @DisplayName("达到阈值时触发锁定 (写入 lockKey)")
    void recordFailedAttempt_ReachedThreshold_TriggersLock() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment(anyString())).thenReturn((long) MAX_ATTEMPTS);

        long result = loginAttemptService.recordFailedAttempt("s12345", "192.168.1.1");

        assertEquals(MAX_ATTEMPTS, result);
        // 验证锁 key 被写入
        verify(redisTemplate.opsForValue())
                .set(eq("cockpit:login:lock:s12345"), eq("1"), eq((long) LOCKOUT_SECONDS), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("未达阈值时返回当前失败次数, 不触发锁定")
    void recordFailedAttempt_BelowThreshold_DoesNotTriggerLock() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment(anyString())).thenReturn(2L);

        long result = loginAttemptService.recordFailedAttempt("s12345", "192.168.1.1");

        assertEquals(2L, result);
        // 验证锁 key 未被写入
        verify(redisTemplate.opsForValue(), never())
                .set(anyString(), anyString(), anyLong(), any(TimeUnit.class));
    }

    @Test
    @DisplayName("首次失败设置失败计数 TTL")
    void recordFailedAttempt_FirstFailure_SetsTTL() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment(anyString())).thenReturn(1L);

        loginAttemptService.recordFailedAttempt("s12345", "192.168.1.1");

        verify(redisTemplate).expire(eq("cockpit:login:fail:s12345"), eq((long) WINDOW_SECONDS), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("空工号返回 0, 不操作 Redis")
    void recordFailedAttempt_EmptyEmpNo_ReturnsZero() {
        long result = loginAttemptService.recordFailedAttempt("", "192.168.1.1");

        assertEquals(0, result);
        verifyNoInteractions(redisTemplate);
    }

    // ==================== recordSuccess ====================

    @Test
    @DisplayName("登录成功时清除失败计数和 IP 记录")
    void recordSuccess_ClearsFailCountAndIp() {
        loginAttemptService.recordSuccess("s12345");

        verify(redisTemplate).delete("cockpit:login:fail:s12345");
        verify(redisTemplate).delete("cockpit:login:fail:ip:s12345");
        // 不应清除 lockKey (锁定由 TTL 自动过期)
        verify(redisTemplate, never()).delete("cockpit:login:lock:s12345");
    }

    @Test
    @DisplayName("recordSuccess Redis 异常时不抛出 (不影响登录流程)")
    void recordSuccess_RedisException_DoesNotThrow() {
        doThrow(new RuntimeException("Redis 断开"))
                .when(redisTemplate).delete(anyString());

        assertDoesNotThrow(() -> loginAttemptService.recordSuccess("s12345"));
    }
}
