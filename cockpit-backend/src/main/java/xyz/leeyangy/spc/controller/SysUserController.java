package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.common.StatusMsg;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.common.constants.RoleConstants;
import xyz.leeyangy.spc.dto.UserCreateDTO;
import xyz.leeyangy.spc.dto.UserStatusDTO;
import xyz.leeyangy.spc.dto.UserUpdateDTO;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.PasswordHistoryService;
import xyz.leeyangy.spc.service.SysUserService;
import xyz.leeyangy.spc.service.SysUserWorkshopService;
import xyz.leeyangy.spc.service.TokenBlacklistService;
import xyz.leeyangy.spc.vo.SysUserVO;

import javax.validation.Valid;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/admin/user")
@RequiredArgsConstructor
public class SysUserController {

    private final SysUserService sysUserService;
    private final PasswordEncoder passwordEncoder;
    private final SysUserWorkshopService sysUserWorkshopService;
    private final TokenBlacklistService tokenBlacklistService;
    private final PasswordHistoryService passwordHistoryService;

    @GetMapping("/page")
    public R<Page<SysUserVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String role,
            @RequestParam(required = false) Integer status) {

        Page<SysUser> page = new Page<>(current, size);
        LambdaQueryWrapper<SysUser> wrapper = new LambdaQueryWrapper<SysUser>()
                .like(keyword != null && !keyword.isEmpty(), SysUser::getEmpNo, keyword)
                .or()
                .like(keyword != null && !keyword.isEmpty(), SysUser::getUsername, keyword)
                .or()
                .like(keyword != null && !keyword.isEmpty(), SysUser::getPhone, keyword)
                .eq(role != null && !role.isEmpty(), SysUser::getRole, role)
                .eq(status != null, SysUser::getStatus, status)
                .orderByDesc(SysUser::getCreatedAt);

        Page<SysUserVO> result = PageConvert.convert(sysUserService.page(page, wrapper), SysUserVO::from);
        return R.ok(result);
    }

    @GetMapping("/{id}")
    public R<SysUserVO> getById(@PathVariable Long id) {
        SysUser user = sysUserService.getById(id);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, StatusMsg.USER_NOT_FOUND);
        }
        SysUserVO vo = SysUserVO.from(user);
        fillWorkshopBindings(vo);
        return R.ok(vo);
    }

    /** 填充用户的多车间/测试站绑定信息 */
    private void fillWorkshopBindings(SysUserVO vo) {
        if (vo == null || vo.getId() == null) return;
        vo.setWorkshopIds(sysUserWorkshopService.getWorkshopIds(vo.getId()));
        vo.setPrimaryWorkshopId(sysUserWorkshopService.getPrimaryWorkshopId(vo.getId()));
        vo.setTestStationIds(sysUserWorkshopService.getTestStationIds(vo.getId()));
    }

    @OperationLog(module = "USER", action = "CREATE", targetType = "SysUser",
            content = "'创建用户: ' + #result.data.empNo + ' - ' + #result.data.username + ' (角色:' + #result.data.role + ')'",
            targetId = "#result.data.id")
    @PostMapping
    public R<SysUserVO> create(@Valid @RequestBody UserCreateDTO req) {
        if (req.getEmpNo() == null || req.getEmpNo().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, StatusMsg.EMP_NO_REQUIRED);
        }
        if (req.getUsername() == null || req.getUsername().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, StatusMsg.NAME_REQUIRED);
        }
        if (req.getPassword() == null || req.getPassword().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, StatusMsg.PASSWORD_REQUIRED);
        }

        Long count = sysUserService.count(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getEmpNo, req.getEmpNo())
                .eq(SysUser::getDeleted, 0));
        if (count > 0) {
            return R.fail(StatusCode.CONFLICT, StatusMsg.EMP_NO_EXISTS);
        }

        SysUser user = new SysUser();
        user.setEmpNo(req.getEmpNo().trim());
        user.setUsername(req.getUsername().trim());
        user.setPassword(passwordEncoder.encode(req.getPassword()));
        user.setPasswordUpdatedAt(LocalDateTime.now());
        user.setEmail(req.getEmail());
        user.setPhone(req.getPhone());
        user.setRole(req.getRole() != null ? req.getRole() : RoleConstants.OPERATOR);
        user.setStatus(req.getStatus() != null ? req.getStatus() : 1);

        sysUserService.save(user);

        log.info("[Admin] 创建用户: empNo={} username={}", user.getEmpNo(), user.getUsername());
        SysUserVO vo = SysUserVO.from(user);
        fillWorkshopBindings(vo);
        return R.ok(StatusMsg.CREATE_SUCCESS, vo);
    }

    @OperationLog(module = "USER", action = "UPDATE", targetType = "SysUser",
            content = "'更新用户 id=' + #id",
            targetId = "#id")
    @PutMapping("/{id}")
    public R<SysUserVO> update(@PathVariable Long id, @Valid @RequestBody UserUpdateDTO req) {
        SysUser existUser = sysUserService.getById(id);
        if (existUser == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, StatusMsg.USER_NOT_FOUND);
        }

        // 密码变更前置检查: 密码历史防重用 (等保三级)
        boolean passwordChanged = false;
        String newHash = null;
        if (req.getPassword() != null && !req.getPassword().trim().isEmpty()) {
            if (passwordHistoryService.isPasswordReused(id, req.getPassword())) {
                return R.fail(StatusMsg.PASSWORD_REUSED);
            }
            newHash = passwordEncoder.encode(req.getPassword());
            passwordChanged = true;
        }

        LambdaUpdateWrapper<SysUser> wrapper = new LambdaUpdateWrapper<SysUser>()
                .eq(SysUser::getId, id);

        if (req.getUsername() != null) {
            wrapper.set(SysUser::getUsername, req.getUsername().trim());
        }
        if (req.getEmail() != null) {
            wrapper.set(SysUser::getEmail, req.getEmail());
        }
        if (req.getPhone() != null) {
            wrapper.set(SysUser::getPhone, req.getPhone());
        }
        if (req.getRole() != null) {
            wrapper.set(SysUser::getRole, req.getRole());
        }
        if (req.getStatus() != null) {
            wrapper.set(SysUser::getStatus, req.getStatus());
        }
        if (passwordChanged) {
            wrapper.set(SysUser::getPassword, newHash);
            wrapper.set(SysUser::getPasswordUpdatedAt, LocalDateTime.now());
        }

        // 仅在有字段需要更新时才执行 UPDATE (避免空 SET 子句触发 SQL 语法错误)
        if (req.getUsername() != null || req.getEmail() != null || req.getPhone() != null
                || req.getRole() != null || req.getStatus() != null || passwordChanged) {
            sysUserService.update(wrapper);
        }

        // 密码变更后处理: 记录历史 + 踢出会话 (等保三级: 密码防重用 + 会话管理)
        if (passwordChanged) {
            passwordHistoryService.recordPasswordChange(id, newHash);
            tokenBlacklistService.kickUser(id);
            log.info("[Admin] 管理员重置用户密码, 已踢出会话并记录历史: id={} empNo={}", id, existUser.getEmpNo());
        }

        log.info("[Admin] 更新用户: id={} empNo={}", id, existUser.getEmpNo());

        SysUser updated = sysUserService.getById(id);
        SysUserVO vo = SysUserVO.from(updated);
        fillWorkshopBindings(vo);
        return R.ok(StatusMsg.UPDATE_SUCCESS, vo);
    }

    @OperationLog(module = "USER", action = "STATUS_CHANGE", targetType = "SysUser",
            content = "'用户状态变更 id=' + #id",
            targetId = "#id")
    @PutMapping("/{id}/status")
    public R<Void> toggleStatus(@PathVariable Long id, @Valid @RequestBody UserStatusDTO req) {
        SysUser user = sysUserService.getById(id);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, StatusMsg.USER_NOT_FOUND);
        }
        Integer newStatus = req.getStatus();
        if (newStatus == null) {
            newStatus = user.getStatus() == 1 ? 0 : 1;
        }
        String statusDesc = newStatus == 1 ? "启用" : "停用";
        user.setStatus(newStatus);
        sysUserService.updateById(user);
        // 停用用户时立即踢出其所有Token, 强制下线
        if (newStatus == 0) {
            tokenBlacklistService.kickUser(id);
        }
        log.info("[Admin] 用户状态变更: id={} status={}", id, newStatus);
        return R.ok(null);
    }

    @OperationLog(module = "USER", action = "DELETE", targetType = "SysUser",
            content = "'删除用户 id=' + #id",
            targetId = "#id")
    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        SysUser user = sysUserService.getById(id);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, StatusMsg.USER_NOT_FOUND);
        }
        sysUserService.update(new LambdaUpdateWrapper<SysUser>()
                .eq(SysUser::getId, id)
                .set(SysUser::getDeleted, 1));
        log.info("[Admin] 删除用户: id={} empNo={}", id, user.getEmpNo());
        return R.ok(null);
    }
}
