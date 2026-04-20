-- ============================================================
-- 迁移: 工序-参数绑定表 (N:M 复用模型)
-- 执行时间: 2026-04-21
-- 用途: 一个工序可绑定多个参数，一个参数可被多个工序复用
-- ============================================================

-- -----------------------------------------------------------
-- Step 1: 建表 (如已存在则跳过)
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS `spc_process_param` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    `process_id`     BIGINT       NOT NULL COMMENT '工序ID',
    `param_id`       BIGINT       NOT NULL COMMENT '参数ID',
    `sort_order`     INT          NOT NULL DEFAULT 0 COMMENT '排序(数字越小越靠前)',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_process_param` (`process_id`, `param_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='工序-参数绑定';

-- -----------------------------------------------------------
-- Step 2: 兼容处理
-- -----------------------------------------------------------
DELETE FROM `spc_process_param` WHERE `deleted` = 1;

-- -----------------------------------------------------------
-- Step 3: 数据迁移 (从 param.process_id 字段迁移到关联表)
-- 注意: 此处不自动全量迁移，由管理员在后台手动绑定
-- -----------------------------------------------------------
-- INSERT IGNORE INTO `spc_process_param` (`process_id`, `param_id`, `sort_order`)
-- SELECT p.process_id, p.id, 0
-- FROM spc_param p
-- WHERE p.deleted = 0 AND p.process_id IS NOT NULL;
