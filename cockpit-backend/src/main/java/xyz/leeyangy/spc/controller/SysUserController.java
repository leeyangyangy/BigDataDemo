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
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.common.constants.RoleConstants;
import xyz.leeyangy.spc.dto.UserCreateDTO;
import xyz.leeyangy.spc.dto.UserStatusDTO;
import xyz.leeyangy.spc.dto.UserUpdateDTO;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.SysUserService;
import xyz.leeyangy.spc.service.SysUserWorkshopService;
import xyz.leeyangy.spc.vo.SysUserVO;

import javax.validation.Valid;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/admin/user")
@RequiredArgsConstructor
public class SysUserController {

    private final SysUserService sysUserService;
    private final PasswordEncoder passwordEncoder;
    private final SysUserWorkshopService sysUserWorkshopService;

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
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
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
            return R.fail(StatusCode.PARAM_REQUIRED, "工号不能为空");
        }
        if (req.getUsername() == null || req.getUsername().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "姓名不能为空");
        }
        if (req.getPassword() == null || req.getPassword().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "密码不能为空");
        }

        Long count = sysUserService.count(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getEmpNo, req.getEmpNo())
                .eq(SysUser::getDeleted, 0));
        if (count > 0) {
            return R.fail(StatusCode.CONFLICT, "工号已存在");
        }

        SysUser user = new SysUser();
        user.setEmpNo(req.getEmpNo().trim());
        user.setUsername(req.getUsername().trim());
        user.setPassword(passwordEncoder.encode(req.getPassword()));
        user.setEmail(req.getEmail());
        user.setPhone(req.getPhone());
        user.setRole(req.getRole() != null ? req.getRole() : RoleConstants.OPERATOR);
        user.setStatus(req.getStatus() != null ? req.getStatus() : 1);

        sysUserService.save(user);

        log.info("[Admin] 创建用户: empNo={} username={}", user.getEmpNo(), user.getUsername());
        SysUserVO vo = SysUserVO.from(user);
        fillWorkshopBindings(vo);
        return R.ok("创建成功", vo);
    }

    @OperationLog(module = "USER", action = "UPDATE", targetType = "SysUser",
            content = "'更新用户 id=' + #id",
            targetId = "#id")
    @PutMapping("/{id}")
    public R<SysUserVO> update(@PathVariable Long id, @Valid @RequestBody UserUpdateDTO req) {
        SysUser existUser = sysUserService.getById(id);
        if (existUser == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
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
        if (req.getPassword() != null && !req.getPassword().trim().isEmpty()) {
            wrapper.set(SysUser::getPassword, passwordEncoder.encode(req.getPassword()));
        }

        // 仅在有字段需要更新时才执行 UPDATE (避免空 SET 子句触发 SQL 语法错误)
        if (req.getUsername() != null || req.getEmail() != null || req.getPhone() != null
                || req.getRole() != null || req.getStatus() != null
                || (req.getPassword() != null && !req.getPassword().trim().isEmpty())) {
            sysUserService.update(wrapper);
        }

        log.info("[Admin] 更新用户: id={} empNo={}", id, existUser.getEmpNo());

        SysUser updated = sysUserService.getById(id);
        SysUserVO vo = SysUserVO.from(updated);
        fillWorkshopBindings(vo);
        return R.ok("更新成功", vo);
    }

    @OperationLog(module = "USER", action = "STATUS_CHANGE", targetType = "SysUser",
            content = "'用户状态变更 id=' + #id",
            targetId = "#id")
    @PutMapping("/{id}/status")
    public R<Void> toggleStatus(@PathVariable Long id, @Valid @RequestBody UserStatusDTO req) {
        SysUser user = sysUserService.getById(id);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        }
        Integer newStatus = req.getStatus();
        if (newStatus == null) {
            newStatus = user.getStatus() == 1 ? 0 : 1;
        }
        String statusDesc = newStatus == 1 ? "启用" : "停用";
        user.setStatus(newStatus);
        sysUserService.updateById(user);
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
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        }
        sysUserService.update(new LambdaUpdateWrapper<SysUser>()
                .eq(SysUser::getId, id)
                .set(SysUser::getDeleted, 1));
        log.info("[Admin] 删除用户: id={} empNo={}", id, user.getEmpNo());
        return R.ok(null);
    }
}
