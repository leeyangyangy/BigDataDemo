package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.dto.UserCreateDTO;
import xyz.leeyangy.spc.dto.UserUpdateDTO;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.PasswordHistoryService;
import xyz.leeyangy.spc.service.SysUserService;
import xyz.leeyangy.spc.service.SysUserWorkshopService;
import xyz.leeyangy.spc.service.TokenBlacklistService;
import xyz.leeyangy.spc.vo.SysUserVO;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * SysUserController 单元测试。
 *
 * <p>重点验证 P2 修复的密码变更安全逻辑:
 * <ul>
 *     <li>创建用户时设置 passwordUpdatedAt</li>
 *     <li>更新密码时检查密码历史、记录变更、踢出会话</li>
 *     <li>密码重用时拒绝变更</li>
 *     <li>未变更密码时不触发密码历史检查与踢出</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("SysUserController 单元测试 (P2 密码变更安全逻辑)")
class SysUserControllerTest {

    @Mock
    private SysUserService sysUserService;

    @Mock
    private PasswordEncoder passwordEncoder;

    @Mock
    private SysUserWorkshopService sysUserWorkshopService;

    @Mock
    private TokenBlacklistService tokenBlacklistService;

    @Mock
    private PasswordHistoryService passwordHistoryService;

    @InjectMocks
    private SysUserController sysUserController;

    // ==================== create ====================

    @Test
    @DisplayName("create: 有效密码时保存用户并设置 passwordUpdatedAt")
    void create_WithValidPassword_ShouldSetPasswordUpdatedAt() {
        // given
        UserCreateDTO req = new UserCreateDTO();
        req.setEmpNo("EMP001");
        req.setUsername("张三");
        req.setPassword("Strong@123");

        when(sysUserService.count(any())).thenReturn(0L);
        when(passwordEncoder.encode("Strong@123")).thenReturn("encoded-hash");

        // when
        R<SysUserVO> result = sysUserController.create(req);

        // then
        assertEquals(StatusCode.SUCCESS, result.getCode(), "应返回 R.ok");

        ArgumentCaptor<SysUser> captor = ArgumentCaptor.forClass(SysUser.class);
        verify(sysUserService).save(captor.capture());
        SysUser savedUser = captor.getValue();
        assertNotNull(savedUser.getPasswordUpdatedAt(), "passwordUpdatedAt 不应为 null");
        assertEquals("encoded-hash", savedUser.getPassword(), "密码应为编码后的哈希值");
        assertEquals("EMP001", savedUser.getEmpNo());
        assertEquals("张三", savedUser.getUsername());
    }

    // ==================== update ====================

    @Test
    @DisplayName("update: 包含新密码时检查历史、更新密码、记录历史并踢出会话")
    void update_WithPassword_ShouldCheckHistoryAndUpdatePasswordUpdatedAt() {
        // given
        Long userId = 1L;
        UserUpdateDTO req = new UserUpdateDTO();
        req.setPassword("NewPass@123");

        SysUser existUser = new SysUser();
        existUser.setId(userId);
        existUser.setEmpNo("EMP001");
        existUser.setUsername("张三");

        when(sysUserService.getById(userId)).thenReturn(existUser);
        when(passwordHistoryService.isPasswordReused(userId, "NewPass@123")).thenReturn(false);
        when(passwordEncoder.encode("NewPass@123")).thenReturn("new-hash");
        when(sysUserWorkshopService.getWorkshopIds(any())).thenReturn(Collections.emptyList());
        when(sysUserWorkshopService.getPrimaryWorkshopId(any())).thenReturn(null);
        when(sysUserWorkshopService.getTestStationIds(any())).thenReturn(Collections.emptyList());

        // when
        R<SysUserVO> result = sysUserController.update(userId, req);

        // then
        assertEquals(StatusCode.SUCCESS, result.getCode(), "应返回 R.ok");
        verify(sysUserService).update(any(LambdaUpdateWrapper.class));
        verify(passwordHistoryService).recordPasswordChange(userId, "new-hash");
        verify(tokenBlacklistService).kickUser(userId);
    }

    @Test
    @DisplayName("update: 密码与历史重复时返回错误, 不更新密码也不踢出会话")
    void update_WithReusedPassword_ShouldReturnError() {
        // given
        Long userId = 1L;
        UserUpdateDTO req = new UserUpdateDTO();
        req.setPassword("Reused@123");

        SysUser existUser = new SysUser();
        existUser.setId(userId);
        existUser.setEmpNo("EMP001");

        when(sysUserService.getById(userId)).thenReturn(existUser);
        when(passwordHistoryService.isPasswordReused(userId, "Reused@123")).thenReturn(true);

        // when
        R<SysUserVO> result = sysUserController.update(userId, req);

        // then
        assertEquals(StatusCode.FAIL, result.getCode(), "应返回 R.fail");
        verify(sysUserService, never()).update(any(LambdaUpdateWrapper.class));
        verify(tokenBlacklistService, never()).kickUser(any());
        verify(passwordHistoryService, never()).recordPasswordChange(any(), any());
    }

    @Test
    @DisplayName("update: 不含密码时不检查历史、不踢出会话, 仅更新基本信息")
    void update_WithoutPassword_ShouldNotCheckHistoryOrKickUser() {
        // given
        Long userId = 1L;
        UserUpdateDTO req = new UserUpdateDTO();
        req.setUsername("李四");

        SysUser existUser = new SysUser();
        existUser.setId(userId);
        existUser.setEmpNo("EMP001");
        existUser.setUsername("张三");

        when(sysUserService.getById(userId)).thenReturn(existUser);
        when(sysUserWorkshopService.getWorkshopIds(any())).thenReturn(Collections.emptyList());
        when(sysUserWorkshopService.getPrimaryWorkshopId(any())).thenReturn(null);
        when(sysUserWorkshopService.getTestStationIds(any())).thenReturn(Collections.emptyList());

        // when
        R<SysUserVO> result = sysUserController.update(userId, req);

        // then
        assertEquals(StatusCode.SUCCESS, result.getCode(), "应返回 R.ok");
        verify(passwordHistoryService, never()).isPasswordReused(any(), any());
        verify(tokenBlacklistService, never()).kickUser(any());
        verify(sysUserService).update(any(LambdaUpdateWrapper.class));
    }
}
