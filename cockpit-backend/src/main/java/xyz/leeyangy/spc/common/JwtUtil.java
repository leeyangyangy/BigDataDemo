package xyz.leeyangy.spc.common;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class JwtUtil {

    /**
     * JWT 密钥。生产环境必须通过环境变量 SPC_JWT_SECRET 注入, 长度 >= 32 字节。
     * 开发环境若未配置, 默认启用随机密钥, 不再使用静态默认值。
     */
    @Value("${cockpit.jwt.secret:}")
    private String secret;

    @Value("${cockpit.jwt.expiration:28800000}")
    private long expiration;

    /**
     * 是否在启动时生成随机密钥。
     * - 生产环境 (prod profile) 强制为 true
     * - 开发环境默认 true, 避免使用静态密钥
     */
    @Value("${cockpit.jwt.random-secret-on-startup:true}")
    private boolean randomSecretOnStartup;

    private SecretKey key;

    @PostConstruct
    public void init() {
        byte[] keyBytes;
        boolean useRandom = randomSecretOnStartup
                || secret == null
                || secret.trim().isEmpty()
                || secret.getBytes(StandardCharsets.UTF_8).length < 32;

        if (useRandom) {
            SecureRandom sr = new SecureRandom();
            keyBytes = new byte[32];  // 256 bit, 满足 HS256 要求
            sr.nextBytes(keyBytes);
            if (!randomSecretOnStartup) {
                log.warn("[JWT] 配置的 secret 为空或长度<32字节, 已自动启用随机密钥 ({} bytes)", keyBytes.length);
            } else {
                log.info("[JWT] 已生成随机密钥 ({} bytes), 重启后所有Token将失效", keyBytes.length);
            }
        } else {
            keyBytes = secret.getBytes(StandardCharsets.UTF_8);
            log.info("[JWT] 使用配置的静态密钥 ({} bytes)", keyBytes.length);
        }
        this.key = Keys.hmacShaKeyFor(keyBytes);
    }

    public String generateToken(Long userId, String empNo, String username, String role) {
        Map<String, Object> claims = new HashMap<>();
        claims.put("userId", userId);
        claims.put("empNo", empNo);
        claims.put("username", username);
        claims.put("role", role);

        return Jwts.builder()
                .setClaims(claims)
                .setSubject(empNo)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + expiration))
                .signWith(key, SignatureAlgorithm.HS256)
                .compact();
    }

    public Claims parseToken(String token) {
        return Jwts.parserBuilder()
                .setSigningKey(key)
                .build()
                .parseClaimsJws(token)
                .getBody();
    }

    public boolean validateToken(String token) {
        try {
            parseToken(token);
            return true;
        } catch (Exception e) {
            log.warn("[JWT] Token验证失败: {}", e.getMessage());
            return false;
        }
    }

    public Long getUserId(String token) {
        Claims claims = parseToken(token);
        return claims.get("userId", Long.class);
    }

    public String getEmpNo(String token) {
        Claims claims = parseToken(token);
        return claims.getSubject();
    }

    public String getRole(String token) {
        Claims claims = parseToken(token);
        return claims.get("role", String.class);
    }

    public String getUsername(String token) {
        Claims claims = parseToken(token);
        return claims.get("username", String.class);
    }
}
