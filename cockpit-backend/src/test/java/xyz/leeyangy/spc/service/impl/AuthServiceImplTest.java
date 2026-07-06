package xyz.leeyangy.spc.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;
import xyz.leeyangy.spc.common.JwtUtil;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.LoginAttemptService;
import xyz.leeyangy.spc.service.SysUserService;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 认证服务单元测试。
 *
 * <p>覆盖 login / wechatLogin 两条主链路: 账号锁定、用户不存在、密码错误、
 * 登录成功、密码过期提醒、企业微信登录等场景。</p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("认证服务测试")
class AuthServiceImplTest {

    @Mock
    private SysUserService sysUserService;

    @Mock
    private PasswordEncoder passwordEncoder;

    @Mock
    private JwtUtil jwtUtil;

    @Mock
    private LoginAttemptService loginAttemptService;

    @InjectMocks
    private AuthServiceImpl authService;

    private static final int PASSWORD_MAX_AGE_DAYS = 90;
    private static final String TEST_IP = "192.168.1.1";

    @BeforeEach
    void setUp() {
        // @Value 字段不会被 @InjectMocks 注入, 手动设置
        ReflectionTestUtils.setField(authService, "passwordMaxAgeDays", PASSWORD_MAX_AGE_DAYS);
    }

    // ==================== login ====================

    @Test
    @DisplayName("login: 账号已锁定时返回错误, 且不记录失败尝试")
    void login_LockedAccount_ShouldReturnError() {
        String empNo = "EMP001";
        String password = "any-password";
        // 账号锁定检查先于用户查询, getByEmpNo 不会被调用 (Mockito 默认返回 null)
        when(loginAttemptService.getRemainingLockSeconds(empNo)).thenReturn(600L);

        Map<String, Object> result = authService.login(empNo, password, TEST_IP);

        assertTrue(result.containsKey("error"), "已锁定账号应返回 error");
        assertTrue(result.get("error").toString().contains("锁定"), "错误信息应提示账号已锁定");
        verify(loginAttemptService, never()).recordFailedAttempt(anyString(), anyString());
        verify(sysUserService, never()).getByEmpNo(anyString());
    }

    @Test
    @DisplayName("login: 用户不存在时记录失败尝试并返回错误")
    void login_UserNotFound_ShouldRecordFailedAttempt() {
        String empNo = "EMP001";
        String password = "any-password";
        when(loginAttemptService.getRemainingLockSeconds(empNo)).thenReturn(null);
        when(sysUserService.getByEmpNo(empNo)).thenReturn(null);

        Map<String, Object> result = authService.login(empNo, password, TEST_IP);

        assertTrue(result.containsKey("error"), "用户不存在应返回 error");
        verify(loginAttemptService, times(1)).recordFailedAttempt(empNo, TEST_IP);
    }

    @Test
    @DisplayName("login: 密码错误时记录失败尝试并返回错误")
    void login_WrongPassword_ShouldRecordFailedAttempt() {
        String empNo = "EMP001";
        String password = "wrong-password";
        SysUser user = new SysUser();
        user.setId(1L);
        user.setEmpNo(empNo);
        user.setUsername("testuser");
        user.setRole("admin");
        user.setStatus(1);
        user.setPassword("encoded-hash");
        when(loginAttemptService.getRemainingLockSeconds(empNo)).thenReturn(null);
        when(sysUserService.getByEmpNo(empNo)).thenReturn(user);
        when(passwordEncoder.matches(password, "encoded-hash")).thenReturn(false);

        Map<String, Object> result = authService.login(empNo, password, TEST_IP);

        assertTrue(result.containsKey("error"), "密码错误应返回 error");
        verify(loginAttemptService, times(1)).recordFailedAttempt(empNo, TEST_IP);
    }

    @Test
    @DisplayName("login: 登录成功返回 token 及用户信息, 并清空失败计数, 密码未过期")
    void login_Success_ShouldReturnTokenAndClearFailures() {
        String empNo = "EMP001";
        String password = "correct-password";
        SysUser user = new SysUser();
        user.setId(1L);
        user.setEmpNo(empNo);
        user.setUsername("testuser");
        user.setRole("admin");
        user.setStatus(1);
        user.setPassword("encoded-hash");
        user.setPasswordUpdatedAt(LocalDateTime.now());
        when(loginAttemptService.getRemainingLockSeconds(empNo)).thenReturn(null);
        when(sysUserService.getByEmpNo(empNo)).thenReturn(user);
        when(passwordEncoder.matches(password, "encoded-hash")).thenReturn(true);
        when(jwtUtil.generateToken(1L, empNo, "testuser", "admin")).thenReturn("test-token");

        Map<String, Object> result = authService.login(empNo, password, TEST_IP);

        assertEquals("test-token", result.get("token"));
        assertEquals(1L, result.get("userId"));
        assertEquals(empNo, result.get("empNo"));
        assertEquals("testuser", result.get("username"));
        assertEquals("admin", result.get("role"));
        assertFalse(result.containsKey("passwordExpired"), "密码未过期时不应包含 passwordExpired");
        verify(loginAttemptService, times(1)).recordSuccess(empNo);
    }

    @Test
    @DisplayName("login: 密码超过 90 天未更换时返回 passwordExpired 标记和提示信息")
    void login_PasswordExpired_ShouldReturnPasswordExpiredFlag() {
        String empNo = "EMP001";
        String password = "correct-password";
        SysUser user = new SysUser();
        user.setId(1L);
        user.setEmpNo(empNo);
        user.setUsername("testuser");
        user.setRole("admin");
        user.setStatus(1);
        user.setPassword("encoded-hash");
        user.setPasswordUpdatedAt(LocalDateTime.now().minusDays(100));
        when(loginAttemptService.getRemainingLockSeconds(empNo)).thenReturn(null);
        when(sysUserService.getByEmpNo(empNo)).thenReturn(user);
        when(passwordEncoder.matches(password, "encoded-hash")).thenReturn(true);
        when(jwtUtil.generateToken(1L, empNo, "testuser", "admin")).thenReturn("test-token");

        Map<String, Object> result = authService.login(empNo, password, TEST_IP);

        assertEquals(true, result.get("passwordExpired"), "密码已过期应返回 passwordExpired=true");
        assertTrue(result.containsKey("passwordExpiredMsg"), "应包含 passwordExpiredMsg 提示");
        assertNotNull(result.get("passwordExpiredMsg"));
        verify(loginAttemptService, times(1)).recordSuccess(empNo);
    }

    @Test
    @DisplayName("login: passwordUpdatedAt 为 null 时视为密码已过期")
    void login_PasswordUpdatedAtNull_ShouldReturnPasswordExpiredFlag() {
        String empNo = "EMP001";
        String password = "correct-password";
        SysUser user = new SysUser();
        user.setId(1L);
        user.setEmpNo(empNo);
        user.setUsername("testuser");
        user.setRole("admin");
        user.setStatus(1);
        user.setPassword("encoded-hash");
        user.setPasswordUpdatedAt(null);
        when(loginAttemptService.getRemainingLockSeconds(empNo)).thenReturn(null);
        when(sysUserService.getByEmpNo(empNo)).thenReturn(user);
        when(passwordEncoder.matches(password, "encoded-hash")).thenReturn(true);
        when(jwtUtil.generateToken(1L, empNo, "testuser", "admin")).thenReturn("test-token");

        Map<String, Object> result = authService.login(empNo, password, TEST_IP);

        assertEquals(true, result.get("passwordExpired"), "passwordUpdatedAt 为 null 应视为已过期");
        assertTrue(result.containsKey("passwordExpiredMsg"));
        verify(loginAttemptService, times(1)).recordSuccess(empNo);
    }

    // ==================== wechatLogin ====================

    @Test
    @DisplayName("wechatLogin: code 为空时返回错误")
    void wechatLogin_CodeEmpty_ShouldReturnError() {
        String code = "";
        Map<String, Object> userInfo = new HashMap<>();

        Map<String, Object> result = authService.wechatLogin(code, userInfo, TEST_IP);

        assertTrue(result.containsKey("error"), "code 为空应返回 error");
        verifyNoInteractions(sysUserService);
        verifyNoInteractions(loginAttemptService);
    }

    @Test
    @DisplayName("wechatLogin: 已存在用户被锁定时返回错误, 且不记录失败尝试")
    void wechatLogin_LockedUser_ShouldReturnError() {
        String code = "testcode";
        Map<String, Object> userInfo = new HashMap<>();
        SysUser user = new SysUser();
        user.setEmpNo("EMP001");
        when(sysUserService.getByWecomUserId(code)).thenReturn(user);
        when(loginAttemptService.getRemainingLockSeconds("EMP001")).thenReturn(600L);

        Map<String, Object> result = authService.wechatLogin(code, userInfo, TEST_IP);

        assertTrue(result.containsKey("error"), "已锁定用户应返回 error");
        verify(loginAttemptService, never()).recordFailedAttempt(anyString(), anyString());
    }

    @Test
    @DisplayName("wechatLogin: 不存在用户被锁定时返回错误")
    void wechatLogin_LockedNonExistentUser_ShouldReturnError() {
        String code = "testcode";
        Map<String, Object> userInfo = new HashMap<>();
        when(sysUserService.getByWecomUserId(code)).thenReturn(null);
        when(loginAttemptService.getRemainingLockSeconds("wecom:testcode")).thenReturn(300L);

        Map<String, Object> result = authService.wechatLogin(code, userInfo, TEST_IP);

        assertTrue(result.containsKey("error"), "已锁定 (不存在用户) 应返回 error");
        verify(loginAttemptService, never()).recordFailedAttempt(anyString(), anyString());
    }

    @Test
    @DisplayName("wechatLogin: 用户不存在时记录失败尝试 (lockKey=wecom:code)")
    void wechatLogin_UserNotFound_ShouldRecordFailedAttempt() {
        String code = "testcode";
        Map<String, Object> userInfo = new HashMap<>();
        when(sysUserService.getByWecomUserId(code)).thenReturn(null);
        when(loginAttemptService.getRemainingLockSeconds("wecom:testcode")).thenReturn(null);

        Map<String, Object> result = authService.wechatLogin(code, userInfo, TEST_IP);

        assertTrue(result.containsKey("error"), "用户不存在应返回 error");
        verify(loginAttemptService, times(1)).recordFailedAttempt("wecom:testcode", TEST_IP);
    }

    @Test
    @DisplayName("wechatLogin: 账号已停用时记录失败尝试 (lockKey=empNo)")
    void wechatLogin_DisabledUser_ShouldRecordFailedAttempt() {
        String code = "testcode";
        Map<String, Object> userInfo = new HashMap<>();
        SysUser user = new SysUser();
        user.setEmpNo("EMP001");
        user.setStatus(0);
        when(sysUserService.getByWecomUserId(code)).thenReturn(user);
        when(loginAttemptService.getRemainingLockSeconds("EMP001")).thenReturn(null);

        Map<String, Object> result = authService.wechatLogin(code, userInfo, TEST_IP);

        assertTrue(result.containsKey("error"), "账号已停用应返回 error");
        verify(loginAttemptService, times(1)).recordFailedAttempt("EMP001", TEST_IP);
    }

    @Test
    @DisplayName("wechatLogin: 登录成功记录成功并返回 token")
    void wechatLogin_Success_ShouldRecordSuccessAndReturnToken() {
        String code = "testcode";
        Map<String, Object> userInfo = new HashMap<>();
        SysUser user = new SysUser();
        user.setId(1L);
        user.setEmpNo("EMP001");
        user.setUsername("testuser");
        user.setRole("admin");
        user.setStatus(1);
        when(sysUserService.getByWecomUserId(code)).thenReturn(user);
        when(loginAttemptService.getRemainingLockSeconds("EMP001")).thenReturn(null);
        when(jwtUtil.generateToken(1L, "EMP001", "testuser", "admin")).thenReturn("test-token");

        Map<String, Object> result = authService.wechatLogin(code, userInfo, TEST_IP);

        assertEquals("test-token", result.get("token"));
        assertEquals(1L, result.get("userId"));
        assertEquals("EMP001", result.get("empNo"));
        assertEquals("testuser", result.get("username"));
        assertEquals("admin", result.get("role"));
        verify(loginAttemptService, times(1)).recordSuccess("EMP001");
    }
}
