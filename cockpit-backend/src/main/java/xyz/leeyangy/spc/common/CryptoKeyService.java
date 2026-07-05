package xyz.leeyangy.spc.common;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * 管理 Redis 中每用户独立的 AES 密钥/IV。
 *
 * Key exchange 流程:
 *   1. 前端生成随机 16 字节 AES key + 16 字节 IV
 *   2. 用后端下发的 RSA 公钥加密后调用 /api/auth/key-exchange
 *   3. 后端用 RSA 私钥解密, 存 Redis: enc:kv:{userId} -> "base64(key):base64(iv)"
 *   4. DecryptRequestFilter / EncryptResponseAdvice 通过 userId 从这里取 KV
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CryptoKeyService {

    private final StringRedisTemplate redisTemplate;
    private final RsaKeyHolder rsaKeyHolder;

    @Value("${cockpit.jwt.expiration:86400000}")
    private long jwtExpirationMs;

    private static final String KEY_PREFIX = "enc:kv:";
    private static final int AES_KEY_SIZE = 16;   // 128 bit
    private static final int AES_IV_SIZE = 16;    // 128 bit

    /**
     * 存储前端通过 RSA 加密上传的 AES key/iv。
     *
     * @param userId        用户 ID
     * @param encKeyBase64  RSA 加密后的 AES key (Base64)
     * @param encIvBase64   RSA 加密后的 AES iv  (Base64)
     * @return true=存储成功
     */
    public boolean storeKey(Long userId, String encKeyBase64, String encIvBase64) {
        try {
            // 前端流程: 16 字节随机数 -> Base64 字符串 -> RSA 加密 -> Base64 字符串上送
            // 后端需要: Base64 解码拿 RSA 密文 -> RSA 解密拿 Base64 字符串的字节 -> Base64 解码拿原始 16 字节
            byte[] rsaDecryptedKey = rsaDecrypt(Base64.getDecoder().decode(encKeyBase64));
            byte[] rsaDecryptedIv = rsaDecrypt(Base64.getDecoder().decode(encIvBase64));
            byte[] keyBytes = Base64.getDecoder().decode(new String(rsaDecryptedKey, StandardCharsets.UTF_8));
            byte[] ivBytes = Base64.getDecoder().decode(new String(rsaDecryptedIv, StandardCharsets.UTF_8));

            if (keyBytes.length != AES_KEY_SIZE || ivBytes.length != AES_IV_SIZE) {
                log.warn("[Crypto] key-exchange 长度异常: key={}B iv={}B (期望 16/16)",
                        keyBytes.length, ivBytes.length);
                return false;
            }

            String value = Base64.getEncoder().encodeToString(keyBytes)
                    + ":" + Base64.getEncoder().encodeToString(ivBytes);
            String redisKey = KEY_PREFIX + userId;
            long ttl = jwtExpirationMs + 60000L;
            redisTemplate.opsForValue().set(redisKey, value, ttl, TimeUnit.MILLISECONDS);
            log.info("[Crypto] 用户{}的 AES KV 已存储, TTL={}ms", userId, ttl);
            return true;
        } catch (Exception e) {
            log.error("[Crypto] key-exchange 失败: userId={} err={}", userId, e.getMessage());
            return false;
        }
    }

    /**
     * 获取 AES 加解密所需的 key/iv spec。
     * 未启用或不存在时返回 null, 调用方按未加密处理。
     */
    public KeyIv getKeyIv(Long userId) {
        if (userId == null) return null;
        String value = redisTemplate.opsForValue().get(KEY_PREFIX + userId);
        if (value == null) return null;

        String[] parts = value.split(":");
        if (parts.length != 2) return null;

        byte[] keyBytes = Base64.getDecoder().decode(parts[0]);
        byte[] ivBytes = Base64.getDecoder().decode(parts[1]);

        return new KeyIv(
                new SecretKeySpec(keyBytes, "AES"),
                new IvParameterSpec(ivBytes)
        );
    }

    /**
     * 生成一对随机 KV (仅供测试或回退使用)。
     */
    public KeyIv generateRandom() {
        SecureRandom sr = new SecureRandom();
        byte[] key = new byte[AES_KEY_SIZE];
        byte[] iv = new byte[AES_IV_SIZE];
        sr.nextBytes(key);
        sr.nextBytes(iv);
        return new KeyIv(
                new SecretKeySpec(key, "AES"),
                new IvParameterSpec(iv)
        );
    }

    public void removeKey(Long userId) {
        if (userId == null) return;
        redisTemplate.delete(KEY_PREFIX + userId);
    }

    private byte[] rsaDecrypt(byte[] encrypted) throws Exception {
        Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        cipher.init(Cipher.DECRYPT_MODE, rsaKeyHolder.getPrivateKey());
        return cipher.doFinal(encrypted);
    }

    public static class KeyIv {
        public final SecretKeySpec key;
        public final IvParameterSpec iv;

        public KeyIv(SecretKeySpec key, IvParameterSpec iv) {
            this.key = key;
            this.iv = iv;
        }
    }
}
