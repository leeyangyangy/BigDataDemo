-- ============================================================
-- 密码历史记录表 (等保三级 - 密码防重用)
-- ============================================================

CREATE TABLE IF NOT EXISTS `sys_password_history` (
    `id`             BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `user_id`        BIGINT       NOT NULL COMMENT '用户ID',
    `password_hash`  VARCHAR(100) NOT NULL COMMENT '密码哈希 (BCrypt)',
    `created_by`     BIGINT       DEFAULT NULL COMMENT '创建人',
    `created_at`     DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_by`     BIGINT       DEFAULT NULL COMMENT '更新人',
    `updated_at`     DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `deleted`        TINYINT      DEFAULT 0 COMMENT '逻辑删除: 0未删除 1已删除',
    PRIMARY KEY (`id`),
    KEY `idx_user_id_created_at` (`user_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='密码历史记录表';

-- ============================================================
-- PasswordHistoryMapper.xml
-- ============================================================
