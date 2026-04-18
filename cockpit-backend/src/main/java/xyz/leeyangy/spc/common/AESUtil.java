package xyz.leeyangy.spc.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

@Component
public class AESUtil {

    @Value("${spc.security.encryption.enabled:false}")
    private boolean encryptionEnabledConfig;

    @Value("${spc.security.encryption.key:SPC@2026#Secure!Key}")
    private String aesKey;

    @Value("${spc.security.encryption.iv:SPC2026SecureIV16}")
    private String aesIv;

    @Value("${spc.security.encryption.exclude-paths:}")
    private List<String> excludePaths;

    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";

    public boolean isEncryptionEnabled() {
        String profile = System.getProperty("spring.profiles.active", "dev");
        if ("dev".equals(profile) || "development".equals(profile) || "local".equals(profile)) {
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

    public String encrypt(String data) {
        if (!isEncryptionEnabled() || data == null || data.isEmpty()) {
            return data;
        }
        try {
            SecretKeySpec keySpec = new SecretKeySpec(aesKey.getBytes(StandardCharsets.UTF_8), "AES");
            IvParameterSpec ivSpec = new IvParameterSpec(aesIv.getBytes(StandardCharsets.UTF_8));
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
            byte[] encrypted = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new RuntimeException("AES加密失败", e);
        }
    }

    public String decrypt(String encryptedData) {
        if (!isEncryptionEnabled() || encryptedData == null || encryptedData.isEmpty()) {
            return encryptedData;
        }
        try {
            SecretKeySpec keySpec = new SecretKeySpec(aesKey.getBytes(StandardCharsets.UTF_8), "AES");
            IvParameterSpec ivSpec = new IvParameterSpec(aesIv.getBytes(StandardCharsets.UTF_8));
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
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
