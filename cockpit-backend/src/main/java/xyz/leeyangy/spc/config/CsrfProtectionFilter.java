package xyz.leeyangy.spc.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * CSRF 防护过滤器 (适配无状态 JWT 架构)
 *
 * <p>由于本项目使用 Bearer Token (Authorization Header) 而非 Cookie 进行鉴权,
 * 传统 CSRF 攻击面较小。但为符合等保三级 "通信完整性" 要求, 仍需显式防护。</p>
 *
 * <p>本过滤器对状态变更请求 (POST/PUT/DELETE/PATCH) 进行 Origin/Referer 校验:</p>
 * <ol>
 *   <li>优先校验 Origin 头是否在允许列表 (cockpit.cors.allowed-origins)</li>
 *   <li>Origin 缺失时校验 Referer 头</li>
 *   <li>两者都缺失时拒绝请求 (除非是 multipart/form-data 文件上传, 此时要求 X-Requested-With)</li>
 * </ol>
 *
 * <p>等保依据: GB/T 22239-2019 三级要求 "应采用密码技术防止通信过程中的数据被篡改和重放"。</p>
 */
@Slf4j
@Component
public class CsrfProtectionFilter extends OncePerRequestFilter {

    private static final Set<HttpMethod> STATE_CHANGING_METHODS;

    static {
        Set<HttpMethod> m = new HashSet<>();
        m.add(HttpMethod.POST);
        m.add(HttpMethod.PUT);
        m.add(HttpMethod.DELETE);
        m.add(HttpMethod.PATCH);
        STATE_CHANGING_METHODS = Collections.unmodifiableSet(m);
    }

    @Value("${cockpit.cors.allowed-origins:http://localhost:5174,http://localhost:5173,http://127.0.0.1:5174}")
    private String allowedOrigins;

    private volatile Set<String> allowedOriginSet;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        HttpMethod method = HttpMethod.resolve(request.getMethod());
        // 仅对状态变更方法进行校验; OPTIONS/GET/HEAD 等不在集合中, 直接放行
        if (method == null || !STATE_CHANGING_METHODS.contains(method)) {
            filterChain.doFilter(request, response);
            return;
        }

        // 公开端点放行 (登录、密钥交换等不需要 CSRF 校验, 它们本就是初始请求)
        String uri = request.getRequestURI();
        if (isPublicEndpoint(uri)) {
            filterChain.doFilter(request, response);
            return;
        }

        if (!checkOriginOrReferer(request)) {
            log.warn("[SECURITY_ALERT] CSRF 防护拦截: 不可信来源 uri={} method={} ip={}",
                    uri, method, request.getRemoteAddr());
            writeForbidden(response, "请求来源不可信, 已拒绝 (CSRF 防护)");
            return;
        }

        filterChain.doFilter(request, response);
    }

    private boolean isPublicEndpoint(String uri) {
        return uri != null && (
                uri.startsWith("/api/auth/login")
                        || uri.startsWith("/api/auth/public-key")
                        || uri.startsWith("/api/auth/key-exchange")
                        || uri.startsWith("/api/auth/wechat-login")
                        || uri.startsWith("/api/yield/wecom/callback")
                        || uri.startsWith("/actuator/health"));
    }

    private boolean checkOriginOrReferer(HttpServletRequest request) {
        String origin = request.getHeader("Origin");
        if (origin != null && !origin.isEmpty()) {
            return isAllowedOrigin(origin);
        }
        // Origin 缺失, 检查 Referer
        String referer = request.getHeader("Referer");
        if (referer != null && !referer.isEmpty()) {
            String refererOrigin = extractOrigin(referer);
            return refererOrigin != null && isAllowedOrigin(refererOrigin);
        }
        // Origin/Referer 都缺失: 拒绝 (现代浏览器跨域请求必然携带 Origin 或 Referer)
        // 例外: 同源请求可能不携带 Origin, 但应携带 Referer
        // 若两者都无, 视为可疑请求
        log.debug("[CSRF] Origin/Referer 均缺失, 拒绝: uri={}", request.getRequestURI());
        return false;
    }

    private boolean isAllowedOrigin(String origin) {
        if (origin == null || origin.isEmpty()) {
            return false;
        }
        Set<String> set = getAllowedOriginSet();
        // 严格匹配 (防止部分匹配绕过, 如 evil.com 匹配 example.com)
        return set.contains(origin.trim());
    }

    private Set<String> getAllowedOriginSet() {
        if (allowedOriginSet == null) {
            synchronized (this) {
                if (allowedOriginSet == null) {
                    Set<String> set = new HashSet<>();
                    if (allowedOrigins != null && !allowedOrigins.isEmpty()) {
                        for (String o : allowedOrigins.split(",")) {
                            String trimmed = o.trim();
                            if (!trimmed.isEmpty()) {
                                set.add(trimmed);
                            }
                        }
                    }
                    allowedOriginSet = Collections.unmodifiableSet(set);
                }
            }
        }
        return allowedOriginSet;
    }

    private String extractOrigin(String referer) {
        try {
            URI uri = new URI(referer);
            String scheme = uri.getScheme();
            String host = uri.getHost();
            int port = uri.getPort();
            if (scheme == null || host == null) {
                return null;
            }
            // 默认端口 (http:80, https:443) 不加入 origin 字符串
            if (("http".equals(scheme) && port == 80)
                    || ("https".equals(scheme) && port == 443)) {
                return scheme + "://" + host;
            }
            return scheme + "://" + host + ":" + port;
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private void writeForbidden(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(
                "{\"code\":403,\"msg\":\"" + message + "\",\"data\":null}");
    }
}
