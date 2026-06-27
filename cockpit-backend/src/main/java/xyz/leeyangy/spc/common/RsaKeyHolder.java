package xyz.leeyangy.spc.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;

/**
 * RSA 密钥对持有者。
 *
 * 启动时生成 2048 位 RSA 密钥对, 私钥常驻内存不入仓库, 公钥通过 /api/auth/public-key 下发给前端。
 * 每次重启指纹变化, 旧 KV 自然失效, 前端会重新发起 key-exchange。
 */
@Slf4j
@Component
public class RsaKeyHolder {

    private static final int KEY_SIZE = 2048;

    private KeyPair keyPair;
    private String publicKeyBase64;
    private String fingerprint;

    @PostConstruct
    public void init() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(KEY_SIZE);
        this.keyPair = generator.generateKeyPair();

        PublicKey pub = keyPair.getPublic();
        PrivateKey priv = keyPair.getPrivate();

        this.publicKeyBase64 = Base64.getEncoder().encodeToString(pub.getEncoded());

        // 指纹取公钥 SHA-256 前 8 字节, 转成 hex 用于启动日志辨识
        byte[] sha = java.security.MessageDigest.getInstance("SHA-256").digest(pub.getEncoded());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i > 0) sb.append(':');
            sb.append(String.format("%02X%02X", sha[i * 2], sha[i * 2 + 1]));
        }
        this.fingerprint = sb.toString();

        log.info("[Crypto] RSA 密钥对已生成 | 公钥指纹: {} | 公钥长度: {} bits",
                fingerprint, pub.getEncoded().length * 8);
    }

    public PublicKey getPublicKey() {
        return keyPair.getPublic();
    }

    public PrivateKey getPrivateKey() {
        return keyPair.getPrivate();
    }

    public String getPublicKeyBase64() {
        return publicKeyBase64;
    }

    public String getFingerprint() {
        return fingerprint;
    }
}
