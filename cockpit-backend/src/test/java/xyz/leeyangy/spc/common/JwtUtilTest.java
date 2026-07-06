package xyz.leeyangy.spc.common;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import javax.crypto.SecretKey;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JwtUtil 单元测试。
 *
 * <p>使用静态密钥 (randomSecretOnStartup=false) 以保证测试可重复性。
 * 通过 ReflectionTestUtils 注入 @Value 字段后手动调用 init() 完成 SecretKey 初始化。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("JwtUtil 单元测试")
class JwtUtilTest {

    private static final String TEST_SECRET = "test-secret-key-for-jwt-signing-32bytes!";
    private static final long TEST_EXPIRATION = 28800000L;

    private JwtUtil jwtUtil;

    @BeforeEach
    void setUp() {
        jwtUtil = new JwtUtil();
        ReflectionTestUtils.setField(jwtUtil, "secret", TEST_SECRET);
        ReflectionTestUtils.setField(jwtUtil, "expiration", TEST_EXPIRATION);
        ReflectionTestUtils.setField(jwtUtil, "randomSecretOnStartup", false);
        jwtUtil.init();
    }

    @Test
    @DisplayName("generateToken 生成的 token 解析后 issuer=cockpit, audience=cockpit-web")
    void generateToken_ShouldContainIssuerAndAudience() {
        String token = jwtUtil.generateToken(1L, "EMP001", "lee", "ADMIN");

        Claims claims = jwtUtil.parseToken(token);

        assertNotNull(claims);
        assertEquals("cockpit", claims.getIssuer());
        assertEquals("cockpit-web", claims.getAudience());
    }

    @Test
    @DisplayName("parseToken 解析正常 token 成功")
    void parseToken_WithValidToken_ShouldSucceed() {
        String token = jwtUtil.generateToken(1L, "EMP001", "lee", "ADMIN");

        Claims claims = jwtUtil.parseToken(token);

        assertNotNull(claims);
        assertEquals("EMP001", claims.getSubject());
    }

    @Test
    @DisplayName("validateToken 校验正常 token 返回 true")
    void validateToken_WithValidToken_ShouldReturnTrue() {
        String token = jwtUtil.generateToken(1L, "EMP001", "lee", "ADMIN");

        assertTrue(jwtUtil.validateToken(token));
    }

    @Test
    @DisplayName("validateToken 校验非法 token 返回 false")
    void validateToken_WithInvalidToken_ShouldReturnFalse() {
        assertFalse(jwtUtil.validateToken("invalid.token.string"));
    }

    @Test
    @DisplayName("parseToken 解析无 issuer 的 token 应抛出异常")
    void parseToken_WithoutIssuer_ShouldFail() {
        SecretKey key = (SecretKey) ReflectionTestUtils.getField(jwtUtil, "key");
        assertNotNull(key, "init() 后 key 字段不应为 null");

        String token = Jwts.builder()
                .setSubject("EMP001")
                .setAudience("cockpit-web")
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + TEST_EXPIRATION))
                .signWith(key, SignatureAlgorithm.HS256)
                .compact();

        assertThrows(Exception.class, () -> jwtUtil.parseToken(token));
    }

    @Test
    @DisplayName("parseToken 解析错误 issuer 的 token 应抛出异常")
    void parseToken_WithWrongIssuer_ShouldFail() {
        SecretKey key = (SecretKey) ReflectionTestUtils.getField(jwtUtil, "key");
        assertNotNull(key, "init() 后 key 字段不应为 null");

        String token = Jwts.builder()
                .setSubject("EMP001")
                .setIssuer("wrong-issuer")
                .setAudience("cockpit-web")
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + TEST_EXPIRATION))
                .signWith(key, SignatureAlgorithm.HS256)
                .compact();

        assertThrows(Exception.class, () -> jwtUtil.parseToken(token));
    }

    @Test
    @DisplayName("parseToken 解析无 audience 的 token 应抛出异常")
    void parseToken_WithoutAudience_ShouldFail() {
        SecretKey key = (SecretKey) ReflectionTestUtils.getField(jwtUtil, "key");
        assertNotNull(key, "init() 后 key 字段不应为 null");

        String token = Jwts.builder()
                .setSubject("EMP001")
                .setIssuer("cockpit")
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + TEST_EXPIRATION))
                .signWith(key, SignatureAlgorithm.HS256)
                .compact();

        assertThrows(Exception.class, () -> jwtUtil.parseToken(token));
    }

    @Test
    @DisplayName("getUserId / getEmpNo / getRole / getUsername 返回正确的 Claims 值")
    void getUserId_GetEmpNo_GetRole_GetUsername_ShouldReturnCorrectClaims() {
        String token = jwtUtil.generateToken(100L, "EMP002", "zhangsan", "OPERATOR");

        assertEquals(100L, jwtUtil.getUserId(token));
        assertEquals("EMP002", jwtUtil.getEmpNo(token));
        assertEquals("OPERATOR", jwtUtil.getRole(token));
        assertEquals("zhangsan", jwtUtil.getUsername(token));
    }
}
