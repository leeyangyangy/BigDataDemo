package xyz.leeyangy.spc.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.service.LoginAttemptService;

import java.util.concurrent.TimeUnit;

/**
 * 登录尝试限制服务实现 (基于 Redis)。
 *
 * <p>Redis Key 设计:</p>
 * <ul>
 *   <li>失败计数: {@code spc:login:fail:{empNo}} (TTL = window-seconds)</li>
 *   <li>锁定标记: {@code spc:login:lock:{empNo}} (TTL = lockout-seconds)</li>
 *   <li>IP 关联: {@code spc:login:fail:ip:{empNo}} 记录最近失败 IP</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LoginAttemptServiceImpl implements LoginAttemptService {

    private static final String FAIL_COUNT_KEY_PREFIX = "cockpit:login:fail:";
    private static final String LOCK_KEY_PREFIX = "cockpit:login:lock:";
    private static final String FAIL_IP_KEY_PREFIX = "cockpit:login:fail:ip:";

    private final StringRedisTemplate redisTemplate;

    @Value("${cockpit.rate-limit.login.max-attempts:5}")
    private int maxAttempts;

    @Value("${cockpit.rate-limit.login.window-seconds:300}")
    private int windowSeconds;

    @Value("${cockpit.rate-limit.login.lockout-seconds:1800}")
    private int lockoutSeconds;

    @Override
    public Long getRemainingLockSeconds(String empNo) {
        if (empNo == null || empNo.isEmpty()) {
            return null;
        }
        try {
            Long ttl = redisTemplate.getExpire(LOCK_KEY_PREFIX + empNo, TimeUnit.SECONDS);
            // Redis 返回 -2 表示 key 不存在, -1 表示无 TTL
            if (ttl == null || ttl < 0) {
                return null;
            }
            return ttl;
        } catch (Exception e) {
            log.warn("[LoginAttempt] 读取锁定状态失败(降级为未锁定): empNo={} cause={}", empNo, e.getMessage());
            return null;
        }
    }

    @Override
    public long recordFailedAttempt(String empNo, String ip) {
        if (empNo == null || empNo.isEmpty()) {
            return 0;
        }
        try {
            String failKey = FAIL_COUNT_KEY_PREFIX + empNo;
            Long count = redisTemplate.opsForValue().increment(failKey);
            if (count != null && count == 1) {
                redisTemplate.expire(failKey, windowSeconds, TimeUnit.SECONDS);
            }
            // 记录最近失败 IP (用于审计)
            if (ip != null && !ip.isEmpty()) {
                String ipKey = FAIL_IP_KEY_PREFIX + empNo;
                redisTemplate.opsForValue().set(ipKey, ip, windowSeconds, TimeUnit.SECONDS);
            }

            long currentFailures = count == null ? 0 : count;

            // 达到阈值: 锁定账号
            if (currentFailures >= maxAttempts) {
                String lockKey = LOCK_KEY_PREFIX + empNo;
                redisTemplate.opsForValue().set(lockKey, "1", lockoutSeconds, TimeUnit.SECONDS);
                log.error("[LoginAttempt] 账号已被锁定: empNo={} failures={} lockoutSec={} ip={}",
                        empNo, currentFailures, lockoutSeconds, ip);
                // 安全告警: 等保要求记录安全事件
                log.error("[SECURITY_ALERT] 连续登录失败触发账号锁定 - empNo={} ip={} failures={} threshold={} lockoutSec={}",
                        empNo, ip, currentFailures, maxAttempts, lockoutSeconds);
            } else {
                log.warn("[LoginAttempt] 登录失败: empNo={} failures={}/{} ip={}",
                        empNo, currentFailures, maxAttempts, ip);
            }
            return currentFailures;
        } catch (Exception e) {
            log.warn("[LoginAttempt] 记录失败次数异常(不影响登录流程): empNo={} cause={}", empNo, e.getMessage());
            return 0;
        }
    }

    @Override
    public void recordSuccess(String empNo) {
        if (empNo == null || empNo.isEmpty()) {
            return;
        }
        try {
            String failKey = FAIL_COUNT_KEY_PREFIX + empNo;
            String ipKey = FAIL_IP_KEY_PREFIX + empNo;
            redisTemplate.delete(failKey);
            redisTemplate.delete(ipKey);
            // 不清除 lockKey: 即使登录成功, 锁定期间仍拒绝访问 (防止暴力破解后立即成功)
            // 锁定由 TTL 自动过期
        } catch (Exception e) {
            log.warn("[LoginAttempt] 清空失败计数异常(不影响登录流程): empNo={} cause={}", empNo, e.getMessage());
        }
    }
}
