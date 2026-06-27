package xyz.leeyangy.spc.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.common.JwtUtil;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.AuthService;
import xyz.leeyangy.spc.service.SysUserService;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {

    private final SysUserService sysUserService;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;

    @Override
    public Map<String, Object> login(String empNo, String password, String ip) {
        Map<String, Object> result = new HashMap<>();

        if (empNo == null || empNo.trim().isEmpty()) {
            result.put("error", "工号不能为空");
            return result;
        }

        SysUser user = sysUserService.getByEmpNo(empNo.trim());
        if (user == null) {
            log.warn("[Auth] 登录失败: 用户不存在 - empNo={}", empNo);
            result.put("error", "用户不存在或密码错误");
            return result;
        }

        if (password == null || password.trim().isEmpty()) {
            result.put("error", "密码不能为空");
            return result;
        }

        if (!passwordEncoder.matches(password, user.getPassword())) {
            log.warn("[Auth] 登录失败: 密码错误 - empNo={}", empNo);
            result.put("error", "用户不存在或密码错误");
            return result;
        }

        if (user.getStatus() != 1) {
            log.warn("[Auth] 登录失败: 账号已停用 - empNo={}", empNo);
            result.put("error", "账号已停用，请联系管理员");
            return result;
        }

        String token = jwtUtil.generateToken(user.getId(), user.getEmpNo(), user.getUsername(), user.getRole());

        result.put("token", token);
        result.put("userId", user.getId());
        result.put("empNo", user.getEmpNo());
        result.put("username", user.getUsername());
        result.put("role", user.getRole());

        sysUserService.updateLoginInfo(user.getId(), ip);
        log.info("[Auth] 登录成功: empNo={} username={} role={}", user.getEmpNo(), user.getUsername(), user.getRole());
        return result;
    }

    @Override
    public Map<String, Object> wechatLogin(String code, Map<String, Object> userInfo, String ip) {
        Map<String, Object> result = new HashMap<>();

        if (code == null || code.trim().isEmpty()) {
            log.warn("[Auth] 企业微信登录失败: code为空");
            result.put("error", "企业微信授权码无效");
            return result;
        }

        SysUser user = sysUserService.getByWecomUserId(code);
        if (user == null) {
            log.warn("[Auth] 企业微信登录失败: 未绑定企业微信用户 - wecomUserId={}", code);
            result.put("error", "未找到关联的账号，请先联系管理员绑定企业微信");
            return result;
        }

        if (user.getStatus() != 1) {
            log.warn("[Auth] 企业微信登录失败: 账号已停用 - wecomUserId={}", code);
            result.put("error", "账号已停用，请联系管理员");
            return result;
        }

        String token = jwtUtil.generateToken(user.getId(), user.getEmpNo(), user.getUsername(), user.getRole());

        result.put("token", token);
        result.put("userId", user.getId());
        result.put("empNo", user.getEmpNo());
        result.put("username", user.getUsername());
        result.put("role", user.getRole());

        sysUserService.updateLoginInfo(user.getId(), ip);
        log.info("[Auth] 企业微信登录成功: empNo={} username={}", user.getEmpNo(), user.getUsername());
        return result;
    }
}