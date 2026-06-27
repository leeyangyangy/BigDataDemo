package xyz.leeyangy.spc.service;

/**
 * Token 黑名单 Service 接口（基于 Redis）
 */
public interface TokenBlacklistService {

    void blacklistToken(String token);

    boolean isBlacklisted(String token);

    void kickUser(Long userId);

    boolean isUserKicked(Long userId);

    void clearUserKick(Long userId);
}
