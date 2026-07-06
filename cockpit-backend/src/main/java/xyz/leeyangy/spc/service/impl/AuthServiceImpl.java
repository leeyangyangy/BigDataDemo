package xyz.leeyangy.spc.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.common.JwtUtil;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.AuthService;
import xyz.leeyangy.spc.service.LoginAttemptService;
import xyz.leeyangy.spc.service.SysUserService;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {

    private final SysUserService sysUserService;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final LoginAttemptService loginAttemptService;

    /** 密码最长使用天数 (等保三级: 定期更换, 默认 90 天) */
    @Value("${cockpit.security.password.max-age-days:90}")
    private int passwordMaxAgeDays;

    @Override
    public Map<String, Object> login(String empNo, String password, String ip) {
        Map<String, Object> result = new HashMap<>();

        if (empNo == null || empNo.trim().isEmpty()) {
            result.put("error", "工号不能为空");
            return result;
        }

        // 1. 账号锁定检查 (等保三级: 限制非法登录次数)
        Long remainingLock = loginAttemptService.getRemainingLockSeconds(empNo);
        if (remainingLock != null) {
            log.warn("[Auth] 登录拒绝: 账号已锁定 - empNo={} 剩余秒数={}", empNo, remainingLock);
            result.put("error", "账号已被锁定, 请 " + (remainingLock / 60 + 1) + " 分钟后再试");
            return result;
        }

        SysUser user = sysUserService.getByEmpNo(empNo.trim());
        if (user == null) {
            log.warn("[Auth] 登录失败: 用户不存在 - empNo={}", empNo);
            // 仍记录失败次数, 防止通过返回信息差异进行用户枚举
            loginAttemptService.recordFailedAttempt(empNo, ip);
            result.put("error", "用户不存在或密码错误");
            return result;
        }

        if (password == null || password.trim().isEmpty()) {
            result.put("error", "密码不能为空");
            return result;
        }

        if (!passwordEncoder.matches(password, user.getPassword())) {
            log.warn("[Auth] 登录失败: 密码错误 - empNo={}", empNo);
            loginAttemptService.recordFailedAttempt(empNo, ip);
            result.put("error", "用户不存在或密码错误");
            return result;
        }

        if (user.getStatus() != 1) {
            log.warn("[Auth] 登录失败: 账号已停用 - empNo={}", empNo);
            result.put("error", "账号已停用，请联系管理员");
            return result;
        }

        // 2. 登录成功: 清空失败计数
        loginAttemptService.recordSuccess(empNo);

        // 3. 密码过期检查 (等保三级: 定期更换, 非阻断式提醒)
        LocalDateTime passwordUpdatedAt = user.getPasswordUpdatedAt();
        boolean passwordExpired = false;
        if (passwordUpdatedAt == null) {
            // 老用户未记录密码修改时间, 视为已过期
            passwordExpired = true;
        } else {
            long daysSinceUpdate = ChronoUnit.DAYS.between(passwordUpdatedAt, LocalDateTime.now());
            if (daysSinceUpdate >= passwordMaxAgeDays) {
                passwordExpired = true;
            }
        }
        if (passwordExpired) {
            log.warn("[SECURITY_ALERT] 密码已过期提醒 - empNo={} lastChange={} maxAgeDays={}",
                    empNo, passwordUpdatedAt, passwordMaxAgeDays);
            result.put("passwordExpired", true);
            result.put("passwordExpiredMsg", "密码已超过 " + passwordMaxAgeDays + " 天未更换, 请尽快修改密码");
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

        // 锁定键策略: 用户存在用 empNo, 不存在用 wecom 前缀 + code, 防止暴力枚举 wecomUserId
        String lockKey = (user != null) ? user.getEmpNo() : "wecom:" + code;

        // 1. 账号锁定检查 (等保三级: 限制非法登录次数, 与 login 方法保持一致)
        Long remainingLock = loginAttemptService.getRemainingLockSeconds(lockKey);
        if (remainingLock != null) {
            log.warn("[Auth] 企业微信登录拒绝: 已锁定 - lockKey={} 剩余秒数={}", lockKey, remainingLock);
            result.put("error", "账号已被锁定, 请 " + (remainingLock / 60 + 1) + " 分钟后再试");
            return result;
        }

        if (user == null) {
            log.warn("[Auth] 企业微信登录失败: 未绑定企业微信用户 - wecomUserId={}", code);
            loginAttemptService.recordFailedAttempt(lockKey, ip);
            result.put("error", "未找到关联的账号，请先联系管理员绑定企业微信");
            return result;
        }

        if (user.getStatus() != 1) {
            log.warn("[Auth] 企业微信登录失败: 账号已停用 - wecomUserId={}", code);
            loginAttemptService.recordFailedAttempt(lockKey, ip);
            result.put("error", "账号已停用，请联系管理员");
            return result;
        }

        // 2. 登录成功: 清空失败计数
        loginAttemptService.recordSuccess(lockKey);

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