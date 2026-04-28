package xyz.leeyangy.spc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import xyz.leeyangy.spc.common.IpUtil;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class RateLimitFilter extends OncePerRequestFilter {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spc.rate-limit.login.max-attempts:5}")
    private int loginMaxAttempts;

    @Value("${spc.rate-limit.login.window-seconds:60}")
    private int loginWindowSeconds;

    @Value("${spc.rate-limit.api.max-requests:200}")
    private int apiMaxRequests;

    @Value("${spc.rate-limit.api.window-seconds:60}")
    private int apiWindowSeconds;

    public RateLimitFilter(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String uri = request.getRequestURI();
        String clientIp = IpUtil.getClientIp(request);

        if (uri.contains("/auth/login") || uri.contains("/wecom/callback")) {
            if (isRateLimited("ratelimit:login:" + clientIp, loginMaxAttempts, loginWindowSeconds)) {
                log.warn("[RateLimit] 登录接口触发限流: ip={} URI={}", clientIp, uri);
                writeLimitExceeded(response, "登录尝试过于频繁，请" + loginWindowSeconds + "秒后重试");
                return;
            }
        } else if (uri.startsWith("/api/")) {
            if (isRateLimited("ratelimit:api:" + clientIp, apiMaxRequests, apiWindowSeconds)) {
                log.warn("[RateLimit] API接口触发限流: ip={} URI={}", clientIp, uri);
                writeLimitExceeded(response, "请求过于频繁，请稍后再试");
                return;
            }
        }

        filterChain.doFilter(request, response);
    }

    private boolean isRateLimited(String key, int maxAttempts, int windowSeconds) {
        Long count = redisTemplate.opsForValue().increment(key);
        if (count != null && count == 1) {
            redisTemplate.expire(key, windowSeconds, TimeUnit.SECONDS);
        }
        return count != null && count > maxAttempts;
    }

    private void writeLimitExceeded(HttpServletResponse response, String message) throws IOException {
        response.setStatus(429);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(objectMapper.writeValueAsString(
                java.util.Map.of("code", 429, "msg", message, "data", null)));
    }
}
