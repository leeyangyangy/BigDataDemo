package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.JwtUtil;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.OperationLogService;
import xyz.leeyangy.spc.service.SysUserService;
import xyz.leeyangy.spc.service.TokenBlacklistService;
import xyz.leeyangy.spc.service.WeComService;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final SysUserService sysUserService;
    private final JwtUtil jwtUtil;
    private final PasswordEncoder passwordEncoder;
    private final WeComService weComService;
    private final TokenBlacklistService blacklistService;
    private final OperationLogService operationLogService;

    @PostMapping("/login")
    public R<Map<String, Object>> login(@RequestBody LoginRequest request, HttpServletRequest httpRequest) {
        if (request.getEmpNo() == null || request.getEmpNo().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "工号不能为空");
        }
        if (request.getPassword() == null || request.getPassword().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "密码不能为空");
        }

        SysUser user = sysUserService.getByEmpNo(request.getEmpNo());
        if (user == null) {
            operationLogService.record("USER", "LOGIN", "FAIL", httpRequest, null, request.getEmpNo());
            return R.fail(StatusCode.AUTH_LOGIN_FAILED, "工号不存在或账号已停用");
        }

        if (user.getStatus() != null && user.getStatus() == 0) {
            operationLogService.record("USER", "LOGIN", null, null, "账号已停用", "FAIL", null, 0, httpRequest, user.getId(), user.getUsername());
            return R.fail(StatusCode.AUTH_ACCOUNT_DISABLED, "账号已停用，请联系管理员");
        }

        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            operationLogService.record("USER", "LOGIN", null, null, null, "FAIL", "密码错误", 0, httpRequest, user.getId(), user.getUsername());
            return R.fail(StatusCode.AUTH_LOGIN_FAILED, "密码错误，请重新输入");
        }

        String token = jwtUtil.generateToken(user.getId(), user.getEmpNo(), user.getUsername(), user.getRole());

        String ip = getClientIp(httpRequest);
        sysUserService.updateLoginInfo(user.getId(), ip);

        Map<String, Object> result = new HashMap<>();
        result.put("token", token);
        result.put("userId", user.getId());
        result.put("empNo", user.getEmpNo());
        result.put("username", user.getUsername());
        result.put("role", user.getRole());
        result.put("email", user.getEmail());
        result.put("phone", user.getPhone());
        result.put("workshopId", user.getWorkshopId());

        log.info("[Auth] 用户登录成功: empNo={} username={} ip={}", user.getEmpNo(), user.getUsername(), ip);
        operationLogService.record("USER", "LOGIN", "SUCCESS", httpRequest, user.getId(), user.getUsername());
        return R.ok("登录成功", result);
    }

    @GetMapping("/info")
    public R<Map<String, Object>> getUserInfo(@RequestAttribute("userId") Long userId) {
        SysUser user = sysUserService.getById(userId);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在或已被删除");
        }

        Map<String, Object> result = new HashMap<>();
        result.put("userId", user.getId());
        result.put("empNo", user.getEmpNo());
        result.put("username", user.getUsername());
        result.put("role", user.getRole());
        result.put("email", user.getEmail());
        result.put("phone", user.getPhone());
        result.put("avatar", user.getAvatar());
        result.put("workshopId", user.getWorkshopId());
        return R.ok(result);
    }

    @PostMapping("/logout")
    public R<Void> logout(HttpServletRequest request) {
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7);
            blacklistService.blacklistToken(token);
            log.info("[Auth] 用户已登出, Token已加入黑名单");
            operationLogService.record("USER", "LOGOUT", "SUCCESS", request, null, null);
        }
        return R.ok(null);
    }

    @PostMapping("/change-password")
    public R<Void> changePassword(@RequestBody ChangePasswordRequest request,
                                  @RequestAttribute("userId") Long userId) {
        if (request.getOldPassword() == null || request.getOldPassword().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "原密码不能为空");
        }
        if (request.getNewPassword() == null || request.getNewPassword().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "新密码不能为空");
        }
        if (request.getNewPassword().length() < 6) {
            return R.fail(StatusCode.PARAM_REQUIRED, "新密码长度不能少于6位");
        }

        SysUser user = sysUserService.getById(userId);
        if (user == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        }

        if (!passwordEncoder.matches(request.getOldPassword(), user.getPassword())) {
            return R.fail(StatusCode.AUTH_LOGIN_FAILED, "原密码错误");
        }

        sysUserService.update(new LambdaUpdateWrapper<SysUser>()
                .eq(SysUser::getId, userId)
                .set(SysUser::getPassword, passwordEncoder.encode(request.getNewPassword())));

        blacklistService.kickUser(userId);
        log.info("[Auth] 用户{}修改密码成功, 所有Token已失效", userId);
        return R.ok();
    }

    @GetMapping("/wecom/config")
    public R<Map<String, Object>> getWeComConfig() {
        if (!weComService.isEnabled()) {
            return R.fail("企业微信登录未启用");
        }
        Map<String, Object> config = new HashMap<>();
        config.put("corpId", weComService.getCorpId());
        config.put("agentId", weComService.getAgentId());
        config.put("redirectUri", "/wecom/callback");
        return R.ok(config);
    }

    @PostMapping("/wecom/callback")
    public R<Map<String, Object>> weComCallback(@RequestBody WeComCallbackRequest request, HttpServletRequest httpRequest) {
        if (!weComService.isEnabled()) {
            return R.fail("企业微信登录未启用");
        }
        if (request.getCode() == null || request.getCode().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "扫码code不能为空");
        }

        Map<String, Object> wecomUserInfo;
        try {
            wecomUserInfo = weComService.getUserInfoByCode(request.getCode());
        } catch (Exception e) {
            return R.fail(e.getMessage());
        }

        String wecomUserId = (String) wecomUserInfo.get("UserId");

        SysUser existingUser = sysUserService.getOne(
                new LambdaQueryWrapper<SysUser>().eq(SysUser::getWecomUserId, wecomUserId).eq(SysUser::getDeleted, 0));

        Map<String, Object> result = new HashMap<>();
        result.put("wecomUserId", wecomUserId);

        if (existingUser != null) {
            if (existingUser.getStatus() != null && existingUser.getStatus() == 0) {
                return R.fail(StatusCode.AUTH_ACCOUNT_DISABLED, "该账号已停用，请联系管理员");
            }
            String token = jwtUtil.generateToken(existingUser.getId(), existingUser.getEmpNo(), existingUser.getUsername(), existingUser.getRole());

            String ip = getClientIp(httpRequest);
            sysUserService.updateLoginInfo(existingUser.getId(), ip);

            result.put("bound", true);
            result.put("token", token);
            result.put("userId", existingUser.getId());
            result.put("empNo", existingUser.getEmpNo());
            result.put("username", existingUser.getUsername());
            result.put("role", existingUser.getRole());
            result.put("email", existingUser.getEmail());
            result.put("phone", existingUser.getPhone());
            result.put("workshopId", existingUser.getWorkshopId());

            log.info("[WeCom] 企业微信登录成功: wecomUserId={} empNo={}", wecomUserId, existingUser.getEmpNo());
            return R.ok("登录成功", result);
        }

        result.put("bound", false);
        log.info("[WeCom] 企业微信用户待绑定: wecomUserId={}", wecomUserId);
        return R.ok("BIND_REQUIRED", result);
    }

    @PostMapping("/wecom/bind")
    public R<Map<String, Object>> weComBind(@RequestBody WeComBindRequest request, HttpServletRequest httpRequest) {
        if (!weComService.isEnabled()) {
            return R.fail("企业微信登录未启用");
        }
        if (request.getWecomUserId() == null || request.getWecomUserId().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "企业微信用户标识不能为空");
        }
        if (request.getEmpNo() == null || request.getEmpNo().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "工号不能为空");
        }

        SysUser user = sysUserService.getByEmpNo(request.getEmpNo().trim());
        if (user == null) {
            return R.fail(StatusCode.AUTH_LOGIN_FAILED, "工号不存在，请联系管理员创建账号");
        }

        if (user.getStatus() != null && user.getStatus() == 0) {
            return R.fail(StatusCode.AUTH_ACCOUNT_DISABLED, "该账号已停用，请联系管理员");
        }

        SysUser alreadyBound = sysUserService.getOne(
                new LambdaQueryWrapper<SysUser>().eq(SysUser::getWecomUserId, request.getWecomUserId()).ne(SysUser::getId, user.getId()).eq(SysUser::getDeleted, 0));
        if (alreadyBound != null) {
            return R.fail("该企业微信账号已绑定其他工号: " + alreadyBound.getEmpNo());
        }

        if (user.getWecomUserId() != null && !user.getWecomUserId().equals(request.getWecomUserId())) {
            log.warn("[WeCom] 重新绑定企业微信: empNo={} oldWecom={} newWecom={}", user.getEmpNo(), user.getWecomUserId(), request.getWecomUserId());
        }

        sysUserService.update(new LambdaUpdateWrapper<SysUser>()
                .eq(SysUser::getId, user.getId())
                .set(SysUser::getWecomUserId, request.getWecomUserId()));

        String token = jwtUtil.generateToken(user.getId(), user.getEmpNo(), user.getUsername(), user.getRole());

        String ip = getClientIp(httpRequest);
        sysUserService.updateLoginInfo(user.getId(), ip);

        Map<String, Object> result = new HashMap<>();
        result.put("token", token);
        result.put("userId", user.getId());
        result.put("empNo", user.getEmpNo());
        result.put("username", user.getUsername());
        result.put("role", user.getRole());
        result.put("email", user.getEmail());
        result.put("phone", user.getPhone());
        result.put("workshopId", user.getWorkshopId());

        log.info("[WeCom] 企业微信绑定并登录成功: wecomUserId={} empNo={}", request.getWecomUserId(), user.getEmpNo());
        return R.ok("绑定成功", result);
    }

    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }

    @Data
    public static class LoginRequest {
        private String empNo;
        private String password;
    }

    @Data
    public static class WeComCallbackRequest {
        private String code;
    }

    @Data
    public static class WeComBindRequest {
        private String wecomUserId;
        private String empNo;
    }

    @Data
    public static class ChangePasswordRequest {
        private String oldPassword;
        private String newPassword;
    }
}
