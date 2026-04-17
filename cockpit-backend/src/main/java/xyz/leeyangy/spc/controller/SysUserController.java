package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.SysUserService;

@Slf4j
@RestController
@RequestMapping("/api/admin/user")
@RequiredArgsConstructor
public class SysUserController {

    private final SysUserService sysUserService;
    private final PasswordEncoder passwordEncoder;

    @GetMapping("/page")
    public R<Page<SysUser>> page(
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

        return R.ok(sysUserService.page(page, wrapper));
    }

    @GetMapping("/{id}")
    public R<SysUser> getById(@PathVariable Long id) {
        SysUser user = sysUserService.getById(id);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        }
        user.setPassword(null);
        return R.ok(user);
    }

    @PostMapping
    public R<SysUser> create(@RequestBody UserCreateRequest req) {
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
        user.setRole(req.getRole() != null ? req.getRole() : "OPERATOR");
        user.setWorkshopId(req.getWorkshopId());
        user.setStatus(req.getStatus() != null ? req.getStatus() : 1);

        sysUserService.save(user);
        user.setPassword(null);
        log.info("[Admin] 创建用户: empNo={} username={}", user.getEmpNo(), user.getUsername());
        return R.ok("创建成功", user);
    }

    @PutMapping("/{id}")
    public R<SysUser> update(@PathVariable Long id, @RequestBody UserUpdateRequest req) {
        SysUser existUser = sysUserService.getById(id);
        if (existUser == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        }

        if (req.getUsername() != null) {
            existUser.setUsername(req.getUsername().trim());
        }
        if (req.getEmail() != null) {
            existUser.setEmail(req.getEmail());
        }
        if (req.getPhone() != null) {
            existUser.setPhone(req.getPhone());
        }
        if (req.getRole() != null) {
            existUser.setRole(req.getRole());
        }
        if (req.getWorkshopId() != null) {
            existUser.setWorkshopId(req.getWorkshopId());
        }
        if (req.getStatus() != null) {
            existUser.setStatus(req.getStatus());
        }
        if (req.getPassword() != null && !req.getPassword().trim().isEmpty()) {
            existUser.setPassword(passwordEncoder.encode(req.getPassword()));
        }

        sysUserService.updateById(existUser);
        existUser.setPassword(null);
        log.info("[Admin] 更新用户: id={} empNo={}", id, existUser.getEmpNo());
        return R.ok("更新成功", existUser);
    }

    @PutMapping("/{id}/status")
    public R<Void> toggleStatus(@PathVariable Long id, @RequestBody StatusRequest req) {
        SysUser user = sysUserService.getById(id);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        }
        Integer newStatus = req.getStatus();
        if (newStatus == null) {
            newStatus = user.getStatus() == 1 ? 0 : 1;
        }
        user.setStatus(newStatus);
        sysUserService.updateById(user);
        log.info("[Admin] 用户状态变更: id={} status={}", id, newStatus);
        return R.ok(null);
    }

    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        SysUser user = sysUserService.getById(id);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        }
        user.setDeleted(1);
        sysUserService.updateById(user);
        log.info("[Admin] 删除用户: id={} empNo={}", id, user.getEmpNo());
        return R.ok(null);
    }

    @Data
    public static class UserCreateRequest {
        private String empNo;
        private String username;
        private String password;
        private String email;
        private String phone;
        private String role;
        private Long workshopId;
        private Integer status;
    }

    @Data
    public static class UserUpdateRequest {
        private String username;
        private String password;
        private String email;
        private String phone;
        private String role;
        private Long workshopId;
        private Integer status;
    }

    @Data
    public static class StatusRequest {
        private Integer status;
    }
}
