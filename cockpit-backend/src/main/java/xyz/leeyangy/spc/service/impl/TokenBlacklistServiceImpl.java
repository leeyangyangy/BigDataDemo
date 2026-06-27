package xyz.leeyangy.spc.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.service.TokenBlacklistService;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class TokenBlacklistServiceImpl implements TokenBlacklistService {

    private final StringRedisTemplate redisTemplate;

    @Value("${spc.jwt.expiration:86400000}")
    private long jwtExpirationMs;

    private static final String BLACKLIST_PREFIX = "spc:token:blacklist:";
    private static final String USER_KICK_PREFIX = "spc:user:kicked:";

    @Override
    public void blacklistToken(String token) {
        if (token == null || token.isEmpty()) return;
        String key = BLACKLIST_PREFIX + token;
        long ttl = jwtExpirationMs + 60000L;
        redisTemplate.opsForValue().set(key, "1", ttl, TimeUnit.MILLISECONDS);
        log.debug("[Blacklist] Token已加入黑名单, TTL={}ms", ttl);
    }

    @Override
    public boolean isBlacklisted(String token) {
        if (token == null || token.isEmpty()) return false;
        String key = BLACKLIST_PREFIX + token;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    @Override
    public void kickUser(Long userId) {
        if (userId == null) return;
        String key = USER_KICK_PREFIX + userId;
        long ttl = jwtExpirationMs + 60000L;
        redisTemplate.opsForValue().set(key, "1", ttl, TimeUnit.MILLISECONDS);
        log.info("[Blacklist] 用户{}的所有Token已被踢出", userId);
    }

    @Override
    public boolean isUserKicked(Long userId) {
        if (userId == null) return false;
        String key = USER_KICK_PREFIX + userId;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    @Override
    public void clearUserKick(Long userId) {
        if (userId == null) return;
        String key = USER_KICK_PREFIX + userId;
        redisTemplate.delete(key);
    }
}
