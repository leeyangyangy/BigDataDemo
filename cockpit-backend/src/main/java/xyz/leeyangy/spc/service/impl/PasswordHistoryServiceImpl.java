package xyz.leeyangy.spc.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import xyz.leeyangy.spc.entity.PasswordHistory;
import xyz.leeyangy.spc.mapper.PasswordHistoryMapper;
import xyz.leeyangy.spc.service.PasswordHistoryService;

import java.util.List;

/**
 * 密码历史服务实现
 *
 * <p>默认保留最近 5 次密码记录用于比对, 防止密码重用。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PasswordHistoryServiceImpl implements PasswordHistoryService {

    private final PasswordHistoryMapper passwordHistoryMapper;
    private final PasswordEncoder passwordEncoder;

    /** 保留密码历史数量 (等保三级要求通常为 5) */
    @Value("${cockpit.security.password.history-count:5}")
    private int historyCount;

    @Override
    public boolean isPasswordReused(Long userId, String newPassword) {
        if (userId == null || newPassword == null || newPassword.isEmpty()) {
            return false;
        }
        try {
            List<PasswordHistory> history = passwordHistoryMapper.selectRecentByUserId(userId, historyCount);
            if (history == null || history.isEmpty()) {
                return false;
            }
            for (PasswordHistory ph : history) {
                if (ph.getPasswordHash() != null
                        && passwordEncoder.matches(newPassword, ph.getPasswordHash())) {
                    log.warn("[PasswordHistory] 密码重用被拒绝: userId={}", userId);
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            log.warn("[PasswordHistory] 检查密码历史异常(降级为允许, 但记录日志): userId={} cause={}",
                    userId, e.getMessage());
            return false;
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordPasswordChange(Long userId, String newPasswordHash) {
        if (userId == null || newPasswordHash == null || newPasswordHash.isEmpty()) {
            return;
        }
        try {
            PasswordHistory ph = new PasswordHistory();
            ph.setUserId(userId);
            ph.setPasswordHash(newPasswordHash);
            passwordHistoryMapper.insert(ph);

            // 清理超出保留数量的旧记录
            int deleted = passwordHistoryMapper.deleteOldRecords(userId, historyCount);
            if (deleted > 0) {
                log.debug("[PasswordHistory] 清理旧密码历史: userId={} deleted={}", userId, deleted);
            }
        } catch (Exception e) {
            log.error("[PasswordHistory] 记录密码历史失败(不影响密码更新): userId={} cause={}",
                    userId, e.getMessage());
        }
    }
}
