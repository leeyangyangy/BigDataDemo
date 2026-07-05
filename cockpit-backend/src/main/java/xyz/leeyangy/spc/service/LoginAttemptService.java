package xyz.leeyangy.spc.service;

/**
 * 登录尝试限制服务
 *
 * <p>基于 Redis 实现, 支持:</p>
 * <ul>
 *   <li>账号 + IP 双维度失败次数跟踪</li>
 *   <li>达到阈值后账号锁定 (默认 30 分钟)</li>
 *   <li>登录成功后清空计数</li>
 *   <li>达到告警阈值时记录安全告警日志</li>
 * </ul>
 *
 * <p>符合等保三级 "身份鉴别" 要求 (GB/T 22239-2019):
 * "应具有登录失败处理功能, 配置并启用结束会话、限制非法登录次数和登录连接超时自动退出等措施"</p>
 */
public interface LoginAttemptService {

    /**
     * 检查账号是否被锁定。
     *
     * @param empNo 工号
     * @return 若被锁定, 返回剩余锁定秒数; 否则返回 null
     */
    Long getRemainingLockSeconds(String empNo);

    /**
     * 记录一次登录失败, 并在达到阈值时锁定账号。
     *
     * @param empNo 工号
     * @param ip    客户端 IP
     * @return 当前失败次数
     */
    long recordFailedAttempt(String empNo, String ip);

    /**
     * 登录成功后清空失败计数。
     *
     * @param empNo 工号
     */
    void recordSuccess(String empNo);

    /**
     * 判断账号是否被锁定。
     */
    default boolean isLocked(String empNo) {
        return getRemainingLockSeconds(empNo) != null;
    }
}
