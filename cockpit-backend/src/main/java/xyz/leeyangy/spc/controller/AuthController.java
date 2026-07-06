package xyz.leeyangy.spc.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.AESUtil;
import xyz.leeyangy.spc.common.CryptoKeyService;
import xyz.leeyangy.spc.common.IpUtil;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.RsaKeyHolder;
import xyz.leeyangy.spc.common.StatusMsg;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.ChangePasswordDTO;
import xyz.leeyangy.spc.dto.LoginDTO;
import xyz.leeyangy.spc.dto.WechatLoginDTO;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.service.AuthService;
import xyz.leeyangy.spc.service.PasswordHistoryService;
import xyz.leeyangy.spc.service.SysUserService;
import xyz.leeyangy.spc.service.SysUserWorkshopService;
import xyz.leeyangy.spc.service.TokenBlacklistService;
import xyz.leeyangy.spc.service.WorkshopService;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.HashMap;
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
    private final RsaKeyHolder rsaKeyHolder;
    private final CryptoKeyService cryptoKeyService;
    private final AESUtil aesUtil;
    private final SysUserWorkshopService sysUserWorkshopService;
    private final WorkshopService workshopService;
    private final PasswordHistoryService passwordHistoryService;

    /**
     * 下发 RSA 公钥 (Base64 编码, X.509 SubjectPublicKeyInfo)。
     * 前端启动或登录前拉取, 用于加密随机生成的 AES key/iv 上送。
     * 此接口不需要鉴权。
     */
    @GetMapping("/public-key")
    public R<Map<String, Object>> getPublicKey() {
        Map<String, Object> data = new HashMap<>();
        data.put("publicKey", rsaKeyHolder.getPublicKeyBase64());
        data.put("fingerprint", rsaKeyHolder.getFingerprint());
        data.put("encryptionEnabled", aesUtil.isEncryptionEnabled());
        return R.ok(data);
    }

    /**
     * 接收前端经 RSA 加密的 AES key/iv, 解密后存 Redis 绑定 userId。
     * 调用此接口需携带登录后返回的 JWT。
     */
    @PostMapping("/key-exchange")
    public R<Void> keyExchange(@RequestBody Map<String, String> body,
                              @RequestAttribute Long userId) {
        String encKey = body.get("encKey");
        String encIv = body.get("encIv");
        if (encKey == null || encIv == null) {
            return R.fail(StatusMsg.KEY_EXCHANGE_PARAMS_REQUIRED);
        }
        boolean ok = cryptoKeyService.storeKey(userId, encKey, encIv);
        return ok ? R.ok(null) : R.fail(StatusMsg.KEY_EXCHANGE_FAILED);
    }

    /**
     * 获取当前登录用户信息 (含绑定的车间/测试站列表)。
     * 前端据此决定可查询哪些车间的数据。
     */
    @GetMapping("/info")
    public R<Map<String, Object>> info(@RequestAttribute Long userId,
                                       @RequestAttribute String role) {
        SysUser user = sysUserService.getById(userId);
        if (user == null) {
            return R.fail(StatusMsg.USER_NOT_FOUND);
        }
        Map<String, Object> data = new HashMap<>();
        data.put("id", user.getId());
        data.put("empNo", user.getEmpNo());
        data.put("username", user.getUsername());
        data.put("email", user.getEmail());
        data.put("phone", user.getPhone());
        data.put("avatar", user.getAvatar());
        data.put("role", user.getRole());
        data.put("workshopId", user.getWorkshopId());
        data.put("wecomUserId", user.getWecomUserId());
        data.put("status", user.getStatus());

        // 绑定的车间ID列表 + 主车间
        java.util.List<Long> workshopIds = sysUserWorkshopService.getWorkshopIds(userId);
        data.put("workshopIds", workshopIds);
        data.put("primaryWorkshopId", sysUserWorkshopService.getPrimaryWorkshopId(userId));
        // 绑定的测试站ID列表
        data.put("testStationIds", sysUserWorkshopService.getTestStationIds(userId));

        // 关联车间详情 (前端展示车间名用)
        java.util.List<Map<String, Object>> workshops = new java.util.ArrayList<>();
        for (Long wid : workshopIds) {
            Workshop w = workshopService.getById(wid);
            if (w != null) {
                Map<String, Object> wm = new HashMap<>();
                wm.put("id", w.getId());
                wm.put("workshopCode", w.getWorkshopCode());
                wm.put("workshopName", w.getWorkshopName());
                wm.put("workshopType", w.getWorkshopType());
                workshops.add(wm);
            }
        }
        data.put("workshops", workshops);

        // 测试站详情
        java.util.List<Long> stationIds = sysUserWorkshopService.getTestStationIds(userId);
        java.util.List<Map<String, Object>> stations = new java.util.ArrayList<>();
        for (Long sid : stationIds) {
            Workshop s = workshopService.getById(sid);
            if (s != null) {
                Map<String, Object> sm = new HashMap<>();
                sm.put("id", s.getId());
                sm.put("workshopCode", s.getWorkshopCode());
                sm.put("workshopName", s.getWorkshopName());
                stations.add(sm);
            }
        }
        data.put("testStations", stations);

        return R.ok(data);
    }

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
            return R.fail(StatusMsg.USER_NOT_FOUND);
        }
        if (!passwordEncoder.matches(req.getOldPassword(), user.getPassword())) {
            return R.fail(StatusMsg.ORIGINAL_PASSWORD_ERROR);
        }
        // 等保三级: 密码防重用 (新密码不能与最近 N 次密码相同)
        if (passwordHistoryService.isPasswordReused(userId, req.getNewPassword())) {
            return R.fail(StatusMsg.PASSWORD_REUSED);
        }
        // 同时检查新密码不能与当前密码相同
        if (passwordEncoder.matches(req.getNewPassword(), user.getPassword())) {
            return R.fail(StatusMsg.PASSWORD_SAME_AS_CURRENT);
        }

        String newHash = passwordEncoder.encode(req.getNewPassword());
        sysUserService.updatePassword(userId, req.getNewPassword());
        // 记录密码历史 (在密码更新成功后)
        passwordHistoryService.recordPasswordChange(userId, newHash);
        // 踢出该用户所有会话, 强制重新登录
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
        // 清除该用户的 AES KV, 下次登录需重新 key-exchange (KV 轮换)
        cryptoKeyService.removeKey(userId);
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
