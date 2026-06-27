package xyz.leeyangy.spc.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import javax.crypto.Cipher;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * AES/CBC/PKCS5 加解密工具。
 *
 * 改造说明:
 *   旧版本从配置文件读取静态 key/iv, 全站共享一份密钥且会进 Git 仓库, 不安全。
 *   新版本从 {@link CryptoKeyService} 按 userId 取 KV —— 每用户独立,
 *   登录时由前端随机生成经 RSA 加密上传, 服务端只在内存中暂存。
 *
 * 兼容策略:
 *   - dev/development/local profile 一律不加密, 直接返回原文
 *   - prod profile 且 spc.security.encryption.enabled=true 才启用
 *   - 启用后若 Redis 中没有该用户的 KV (尚未 key-exchange), 也不加密, 避免登录死循环
 */
@Component
public class AESUtil {

    @Value("${spc.security.encryption.enabled:false}")
    private boolean encryptionEnabledConfig;

    @Value("${spc.security.encryption.exclude-paths:}")
    private List<String> excludePaths;

    private final Environment environment;
    private final CryptoKeyService cryptoKeyService;

    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";

    public AESUtil(Environment environment, CryptoKeyService cryptoKeyService) {
        this.environment = environment;
        this.cryptoKeyService = cryptoKeyService;
    }

    public boolean isEncryptionEnabled() {
        // 通过 Spring Environment 获取激活的 profile,
        // 而不是 System.getProperty("spring.profiles.active") ——
        // 后者只能拿到 JVM -D 参数, 在 Docker 中通过环境变量
        // SPRING_PROFILES_ACTIVE 激活 profile 时拿不到, 导致加密永远不会启用。
        boolean devProfile = Arrays.stream(environment.getActiveProfiles())
                .anyMatch(p -> "dev".equals(p) || "development".equals(p) || "local".equals(p));
        if (devProfile) {
            return false;
        }
        return encryptionEnabledConfig;
    }

    public boolean isPathExcluded(String path) {
        if (excludePaths == null || excludePaths.isEmpty()) {
            return false;
        }
        for (String excluded : excludePaths) {
            if (path != null && (path.equals(excluded) || path.startsWith(excluded.replace("/**", "")))) {
                return true;
            }
            if (excluded.endsWith("/**") && path != null && path.startsWith(excluded.substring(0, excluded.length() - 3))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 用 userId 对应的 KV 加密。
     * 未启用或 KV 不存在时返回原文, 由调用方决定是否跳过。
     */
    public String encrypt(String data, Long userId) {
        if (!isEncryptionEnabled() || data == null || data.isEmpty()) {
            return data;
        }
        CryptoKeyService.KeyIv kv = cryptoKeyService.getKeyIv(userId);
        if (kv == null) {
            return data;
        }
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, kv.key, kv.iv);
            byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new RuntimeException("AES加密失败", e);
        }
    }

    /**
     * 用 userId 对应的 KV 解密。
     */
    public String decrypt(String encryptedData, Long userId) {
        if (!isEncryptionEnabled() || encryptedData == null || encryptedData.isEmpty()) {
            return encryptedData;
        }
        CryptoKeyService.KeyIv kv = cryptoKeyService.getKeyIv(userId);
        if (kv == null) {
            return encryptedData;
        }
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, kv.key, kv.iv);
            byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(encryptedData));
            return new String(decrypted, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("AES解密失败", e);
        }
    }

    public static class EncryptedRequest {
        private Boolean encrypted;
        private String data;

        public Boolean getEncrypted() { return encrypted; }
        public void setEncrypted(Boolean encrypted) { this.encrypted = encrypted; }
        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }
}
