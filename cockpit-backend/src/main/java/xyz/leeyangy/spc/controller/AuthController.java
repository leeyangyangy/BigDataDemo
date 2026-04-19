package xyz.leeyangy.spc.controller;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.JwtUtil;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.SysUserService;

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
            return R.fail(StatusCode.AUTH_LOGIN_FAILED, "工号不存在或账号已停用");
        }

        if (user.getStatus() != null && user.getStatus() == 0) {
            return R.fail(StatusCode.AUTH_ACCOUNT_DISABLED, "账号已停用，请联系管理员");
        }

        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
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
    public R<Void> logout() {
        return R.ok(null);
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
}
