package xyz.leeyangy.spc.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.IpUtil;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.ChangePasswordDTO;
import xyz.leeyangy.spc.dto.LoginDTO;
import xyz.leeyangy.spc.dto.WechatLoginDTO;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.AuthService;
import xyz.leeyangy.spc.service.SysUserService;
import xyz.leeyangy.spc.service.TokenBlacklistService;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;
    private final SysUserService sysUserService;
    private final PasswordEncoder passwordEncoder;
    private final TokenBlacklistService tokenBlacklistService;

    @OperationLog(module = "AUTH", action = "LOGIN", targetType = "SysUser",
            content = "'登录: empNo=' + #request.empNo + (#result.success ? '' : ' reason=' + #result.msg)",
            targetId = "#result.data != null ? #result.data['userId'] : null")
    @PostMapping("/login")
    public R<Map<String, Object>> login(@Valid @RequestBody LoginDTO request, HttpServletRequest httpRequest) {
        Map<String, Object> result = authService.login(request.getEmpNo(), request.getPassword(), IpUtil.getClientIp(httpRequest));
        if (result.containsKey("error")) {
            return R.fail((String) result.get("error"));
        }
        return R.ok(result);
    }

    @OperationLog(module = "AUTH", action = "CHANGE_PASSWORD", targetType = "SysUser",
            content = "'修改密码: userId=' + #userId",
            targetId = "#userId")
    @PostMapping("/change-password")
    public R<Void> changePassword(@Valid @RequestBody ChangePasswordDTO req,
                                  @RequestAttribute Long userId) {
        SysUser user = sysUserService.getById(userId);
        if (user == null) {
            return R.fail("用户不存在");
        }
        if (!passwordEncoder.matches(req.getOldPassword(), user.getPassword())) {
            return R.fail("原密码错误");
        }
        sysUserService.updatePassword(userId, req.getNewPassword());
        tokenBlacklistService.kickUser(userId);
        log.info("[Auth] 用户修改密码: id={} empNo={}", userId, user.getEmpNo());
        return R.ok(null);
    }

    @OperationLog(module = "AUTH", action = "LOGOUT", targetType = "SysUser",
            content = "'登出: userId=' + #userId",
            targetId = "#userId")
    @PostMapping("/logout")
    public R<Void> logout(@RequestAttribute Long userId, HttpServletRequest httpRequest) {
        String authHeader = httpRequest.getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            tokenBlacklistService.blacklistToken(authHeader.substring(7));
        }
        log.info("[Auth] 用户登出: id={}", userId);
        return R.ok(null);
    }

    @OperationLog(module = "AUTH", action = "WECHAT_LOGIN", targetType = "SysUser",
            content = "'企业微信登录: code=' + #request.code + (#result.success ? '' : ' reason=' + #result.msg)",
            targetId = "#result.data != null ? #result.data['userId'] : null")
    @PostMapping("/wechat-login")
    public R<Map<String, Object>> wechatLogin(@Valid @RequestBody WechatLoginDTO request, HttpServletRequest httpRequest) {
        Map<String, Object> result = authService.wechatLogin(request.getCode(), request.getUserInfo(), IpUtil.getClientIp(httpRequest));
        if (result.containsKey("error")) {
            return R.fail((String) result.get("error"));
        }
        return R.ok(result);
    }
}
