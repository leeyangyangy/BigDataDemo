-- =====================================================================
-- 阶段 1 + 2 + 3 数据库迁移
-- 1. 用户-车间/测试站 关联表 sys_user_workshop
-- 2. 数据迁移: sys_user.workshop_id → sys_user_workshop
-- 3. 种子数据: 测试车间类型记录
--
-- 注: 良率权限已简化为基于绑定车间判断, 不再使用 sys_user_yield_grant 白名单表
-- =====================================================================

-- ----------------------------
-- 1. sys_user_workshop 用户-车间/测试站 关联表
-- ----------------------------
CREATE TABLE IF NOT EXISTS `sys_user_workshop` (
    `id`           BIGINT NOT NULL AUTO_INCREMENT,
    `user_id`      BIGINT NOT NULL COMMENT '用户ID',
    `workshop_id`  BIGINT NOT NULL COMMENT '车间/测试站ID (spc_workshop.id)',
    `bind_type`    VARCHAR(16) NOT NULL DEFAULT 'WORKSHOP' COMMENT '绑定类型: WORKSHOP / TEST_STATION',
    `is_primary`   TINYINT NOT NULL DEFAULT 0 COMMENT '是否主车间 (仅 WORKSHOP 类型有意义)',
    `created_by`   BIGINT NULL,
    `created_at`   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`   BIGINT NULL,
    `updated_at`   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`      INT NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_user_workshop_type` (`user_id`, `workshop_id`, `bind_type`),
    KEY `idx_user_id` (`user_id`),
    KEY `idx_workshop_id` (`workshop_id`),
    KEY `idx_bind_type` (`bind_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户-车间/测试站 关联表';

-- ----------------------------
-- 2. 数据迁移: sys_user.workshop_id → sys_user_workshop (WORKSHOP, is_primary=1)
--    仅迁移 deleted=0 且 workshop_id 不为空的用户
-- ----------------------------
INSERT INTO `sys_user_workshop` (`user_id`, `workshop_id`, `bind_type`, `is_primary`, `created_at`)
SELECT `id`, `workshop_id`, 'WORKSHOP', 1, NOW()
FROM `sys_user`
WHERE `workshop_id` IS NOT NULL
  AND `deleted` = 0
  AND NOT EXISTS (
      SELECT 1 FROM `sys_user_workshop` usw
      WHERE usw.`user_id` = `sys_user`.`id`
        AND usw.`workshop_id` = `sys_user`.`workshop_id`
        AND usw.`bind_type` = 'WORKSHOP'
  );

-- ----------------------------
-- 3. 种子数据: 测试车间类型记录 (如果不存在)
--    如果 spc_workshop 已有测试车间类型数据, 此处会跳过
-- ----------------------------
INSERT INTO `spc_workshop` (`workshop_code`, `workshop_name`, `workshop_type`, `description`, `status`, `sort_order`, `created_at`)
SELECT 'TEST-001', '一号测试站', '测试车间', '默认测试车间种子数据', 1, 100, NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM `spc_workshop` WHERE `workshop_type` = '测试车间' AND `deleted` = 0 LIMIT 1
);
