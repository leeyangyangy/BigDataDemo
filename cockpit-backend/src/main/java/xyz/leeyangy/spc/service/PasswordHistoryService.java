package xyz.leeyangy.spc.service;

/**
 * 密码历史服务
 *
 * <p>实现等保三级密码防重用要求: 新密码不能与最近 N 次使用过的密码相同。</p>
 */
public interface PasswordHistoryService {

    /**
     * 检查新密码是否与历史密码重复。
     *
     * @param userId      用户 ID
     * @param newPassword 新密码明文
     * @return true 表示密码已使用过 (拒绝), false 表示可以使用
     */
    boolean isPasswordReused(Long userId, String newPassword);

    /**
     * 记录密码变更历史。
     *
     * <p>应在密码更新成功后调用, 同时清理过期记录。</p>
     *
     * @param userId          用户 ID
     * @param newPasswordHash 新密码哈希 (BCrypt)
     */
    void recordPasswordChange(Long userId, String newPasswordHash);
}
