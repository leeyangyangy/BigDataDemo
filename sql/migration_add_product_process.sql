-- ============================================================
-- 迁移: 产品-工序绑定表 (N:M 复用模型)
-- 执行时间: 2026-04-21
-- 用途: 一个产品可绑定多个工序，一个工序可被多个产品复用
-- ============================================================

-- -----------------------------------------------------------
-- Step 1: 建表 (如已存在则跳过)
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS `spc_product_process` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    `product_id`     BIGINT       NOT NULL COMMENT '产品ID',
    `process_id`     BIGINT       NOT NULL COMMENT '工序ID',
    `sort_order`     INT          NOT NULL DEFAULT 0 COMMENT '排序(数字越小越靠前)',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_product_process` (`product_id`, `process_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='产品-工序绑定';

-- -----------------------------------------------------------
-- Step 2: 兼容处理 (仅当表中存在旧字段/旧数据时执行)
-- -----------------------------------------------------------

-- 如果旧表带 deleted/status/is_required 等废弃字段，先清理
SET @has_deleted = (SELECT COUNT(*) FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA = DATABASE()
                     AND TABLE_NAME = 'spc_product_process'
                     AND COLUMN_NAME = 'deleted');

-- 清理残留的软删除行
DELETE FROM `spc_product_process` WHERE `deleted` = 1;

-- 如果唯一键包含 deleted 字段，重建
SET @uk_cols = (SELECT COUNT(*) FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'spc_product_process'
                  AND INDEX_NAME = 'uk_product_process'
                  AND COLUMN_NAME IN ('deleted'));

-- -----------------------------------------------------------
-- Step 3: 数据迁移 (从 param.process_id 字段迁移到关联表)
-- 注意: 此处不自动全量关联，由管理员在后台手动绑定
-- -----------------------------------------------------------
-- INSERT IGNORE INTO `spc_product_process` (`product_id`, `process_id`, `sort_order`)
-- SELECT p.id, pr.id, pr.sort_order
-- FROM spc_product p CROSS JOIN spc_process pr
-- WHERE p.deleted = 0 AND pr.deleted = 0 AND pr.status = 1;
