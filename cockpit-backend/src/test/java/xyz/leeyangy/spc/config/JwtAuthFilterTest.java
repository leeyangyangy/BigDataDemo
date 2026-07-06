package xyz.leeyangy.spc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import xyz.leeyangy.spc.common.JwtUtil;
import xyz.leeyangy.spc.service.TokenBlacklistService;

import javax.servlet.http.HttpServletRequest;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JWT 认证过滤器单元测试。
 *
 * <p>重点验证 P0 修复: /actuator 路径不再被 shouldNotFilter 放行。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("JWT 认证过滤器测试 (/actuator 不再放行)")
class JwtAuthFilterTest {

    @Mock
    private JwtUtil jwtUtil;

    @Mock
    private TokenBlacklistService blacklistService;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private JwtAuthFilter jwtAuthFilter;

    @Test
    @DisplayName("/actuator 不再被 shouldNotFilter 放行 (P0 修复)")
    void shouldNotFilter_Actuator_ReturnsFalse() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator");
        request.setRequestURI("/actuator");

        boolean result = jwtAuthFilter.shouldNotFilter(request);

        assertFalse(result, "/actuator 应该经过 JWT 过滤器鉴权, 不再被放行");
    }

    @Test
    @DisplayName("/actuator/health 不再被放行")
    void shouldNotFilter_ActuatorHealth_ReturnsFalse() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/health");
        request.setRequestURI("/actuator/health");

        boolean result = jwtAuthFilter.shouldNotFilter(request);

        assertFalse(result);
    }

    @Test
    @DisplayName("/api/auth/login 仍被放行 (无需鉴权)")
    void shouldNotFilter_Login_ReturnsTrue() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/auth/login");
        request.setRequestURI("/api/auth/login");

        boolean result = jwtAuthFilter.shouldNotFilter(request);

        assertTrue(result);
    }

    @Test
    @DisplayName("/api/auth/wechat-login 仍被放行")
    void shouldNotFilter_WechatLogin_ReturnsTrue() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/auth/wechat-login");
        request.setRequestURI("/api/auth/wechat-login");

        boolean result = jwtAuthFilter.shouldNotFilter(request);

        assertTrue(result);
    }

    @Test
    @DisplayName("/api/auth/public-key 仍被放行")
    void shouldNotFilter_PublicKey_ReturnsTrue() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/auth/public-key");
        request.setRequestURI("/api/auth/public-key");

        boolean result = jwtAuthFilter.shouldNotFilter(request);

        assertTrue(result);
    }

    @Test
    @DisplayName("/error 仍被放行")
    void shouldNotFilter_Error_ReturnsTrue() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/error");
        request.setRequestURI("/error");

        boolean result = jwtAuthFilter.shouldNotFilter(request);

        assertTrue(result);
    }

    @Test
    @DisplayName("/api/spc/data/page 不被放行 (需要鉴权)")
    void shouldNotFilter_ApiEndpoint_ReturnsFalse() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/spc/data/page");
        request.setRequestURI("/api/spc/data/page");

        boolean result = jwtAuthFilter.shouldNotFilter(request);

        assertFalse(result);
    }
}
