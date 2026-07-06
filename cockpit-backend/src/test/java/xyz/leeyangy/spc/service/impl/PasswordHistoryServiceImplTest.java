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
import xyz.leeyangy.spc.entity.PasswordHistory;
import xyz.leeyangy.spc.mapper.PasswordHistoryMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * 密码历史服务单元测试。
 *
 * <p>重点验证 P0 修复: fail-closed 行为 (异常时假定密码已重用, 拒绝变更)。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("密码历史服务测试 (fail-closed)")
class PasswordHistoryServiceImplTest {

    @Mock
    private PasswordHistoryMapper passwordHistoryMapper;

    @Mock
    private PasswordEncoder passwordEncoder;

    @InjectMocks
    private PasswordHistoryServiceImpl passwordHistoryService;

    private static final int HISTORY_COUNT = 5;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(passwordHistoryService, "historyCount", HISTORY_COUNT);
    }

    // ==================== isPasswordReused ====================

    @Test
    @DisplayName("Mapper 异常时返回 true (fail-closed, 拒绝密码变更)")
    void isPasswordReused_MapperException_ReturnsTrue() {
        when(passwordHistoryMapper.selectRecentByUserId(anyLong(), anyInt()))
                .thenThrow(new RuntimeException("数据库连接断开"));

        boolean result = passwordHistoryService.isPasswordReused(1L, "NewPassword@123");

        assertTrue(result, "fail-closed: 异常时应返回 true (假定已重用, 拒绝变更)");
    }

    @Test
    @DisplayName("密码与历史记录匹配时返回 true")
    void isPasswordReused_PasswordMatches_ReturnsTrue() {
        PasswordHistory ph = new PasswordHistory();
        ph.setPasswordHash("hashed_old_password");

        when(passwordHistoryMapper.selectRecentByUserId(1L, HISTORY_COUNT))
                .thenReturn(Collections.singletonList(ph));
        when(passwordEncoder.matches("SamePassword@123", "hashed_old_password"))
                .thenReturn(true);

        boolean result = passwordHistoryService.isPasswordReused(1L, "SamePassword@123");

        assertTrue(result);
    }

    @Test
    @DisplayName("密码与历史记录不匹配时返回 false")
    void isPasswordReused_PasswordDoesNotMatch_ReturnsFalse() {
        PasswordHistory ph = new PasswordHistory();
        ph.setPasswordHash("hashed_old_password");

        when(passwordHistoryMapper.selectRecentByUserId(1L, HISTORY_COUNT))
                .thenReturn(Collections.singletonList(ph));
        when(passwordEncoder.matches("DifferentPassword@456", "hashed_old_password"))
                .thenReturn(false);

        boolean result = passwordHistoryService.isPasswordReused(1L, "DifferentPassword@456");

        assertFalse(result);
    }

    @Test
    @DisplayName("密码历史为空时返回 false")
    void isPasswordReused_EmptyHistory_ReturnsFalse() {
        when(passwordHistoryMapper.selectRecentByUserId(1L, HISTORY_COUNT))
                .thenReturn(Collections.emptyList());

        boolean result = passwordHistoryService.isPasswordReused(1L, "AnyPassword@123");

        assertFalse(result);
    }

    @Test
    @DisplayName("密码历史返回 null 时返回 false")
    void isPasswordReused_NullHistory_ReturnsFalse() {
        when(passwordHistoryMapper.selectRecentByUserId(1L, HISTORY_COUNT))
                .thenReturn(null);

        boolean result = passwordHistoryService.isPasswordReused(1L, "AnyPassword@123");

        assertFalse(result);
    }

    @Test
    @DisplayName("多条历史记录中有一条匹配时返回 true")
    void isPasswordReused_MultipleRecordsOneMatch_ReturnsTrue() {
        PasswordHistory ph1 = new PasswordHistory();
        ph1.setPasswordHash("hash1");
        PasswordHistory ph2 = new PasswordHistory();
        ph2.setPasswordHash("hash2");
        PasswordHistory ph3 = new PasswordHistory();
        ph3.setPasswordHash("hash3");

        when(passwordHistoryMapper.selectRecentByUserId(1L, HISTORY_COUNT))
                .thenReturn(Arrays.asList(ph1, ph2, ph3));
        when(passwordEncoder.matches("Password@123", "hash1")).thenReturn(false);
        when(passwordEncoder.matches("Password@123", "hash2")).thenReturn(true);

        boolean result = passwordHistoryService.isPasswordReused(1L, "Password@123");

        assertTrue(result);
        // 第三条不应被检查 (短路)
        verify(passwordEncoder, never()).matches("Password@123", "hash3");
    }

    @Test
    @DisplayName("历史记录中有 null 哈希时跳过, 不抛 NPE")
    void isPasswordReused_NullHashInHistory_SkipsSafely() {
        PasswordHistory ph1 = new PasswordHistory();
        ph1.setPasswordHash(null);
        PasswordHistory ph2 = new PasswordHistory();
        ph2.setPasswordHash("hash2");

        when(passwordHistoryMapper.selectRecentByUserId(1L, HISTORY_COUNT))
                .thenReturn(Arrays.asList(ph1, ph2));
        when(passwordEncoder.matches("Password@123", "hash2")).thenReturn(false);

        boolean result = passwordHistoryService.isPasswordReused(1L, "Password@123");

        assertFalse(result);
    }

    @Test
    @DisplayName("null userId 返回 false")
    void isPasswordReused_NullUserId_ReturnsFalse() {
        boolean result = passwordHistoryService.isPasswordReused(null, "Password@123");

        assertFalse(result);
        verifyNoInteractions(passwordHistoryMapper);
    }

    @Test
    @DisplayName("空密码返回 false")
    void isPasswordReused_EmptyPassword_ReturnsFalse() {
        boolean result = passwordHistoryService.isPasswordReused(1L, "");

        assertFalse(result);
        verifyNoInteractions(passwordHistoryMapper);
    }

    // ==================== recordPasswordChange ====================

    @Test
    @DisplayName("记录密码变更成功")
    void recordPasswordChange_Success() {
        when(passwordHistoryMapper.insert(any(PasswordHistory.class))).thenReturn(1);
        when(passwordHistoryMapper.deleteOldRecords(1L, HISTORY_COUNT)).thenReturn(0);

        passwordHistoryService.recordPasswordChange(1L, "new_hashed_password");

        verify(passwordHistoryMapper).insert(any(PasswordHistory.class));
        verify(passwordHistoryMapper).deleteOldRecords(1L, HISTORY_COUNT);
    }

    @Test
    @DisplayName("记录密码变更异常时不抛出 (不影响密码更新)")
    void recordPasswordChange_Exception_DoesNotThrow() {
        when(passwordHistoryMapper.insert(any(PasswordHistory.class)))
                .thenThrow(new RuntimeException("DB 断开"));

        assertDoesNotThrow(() -> passwordHistoryService.recordPasswordChange(1L, "new_hashed_password"));
    }
}
