-- ============================================================
-- SPC统计结果表字段修复
-- 执行时间: 2026-04-18
-- 说明: 添加SPC过程能力分析相关的新增字段
-- ============================================================

ALTER TABLE `spc_stat_result`
    ADD COLUMN `pass_rate`         DECIMAL(6,2)  NULL DEFAULT NULL COMMENT '合格率(%)' AFTER `calc_cl`,
    ADD COLUMN `pass_count`        INT           NULL DEFAULT NULL COMMENT '合格数',
    ADD COLUMN `fail_count`        INT           NULL DEFAULT NULL COMMENT '不合格数',
    ADD COLUMN `normality_w`       DECIMAL(10,6) NULL DEFAULT NULL COMMENT 'Shapiro-Wilk W统计量',
    ADD COLUMN `normality_p_value` DECIMAL(10,6) NULL DEFAULT NULL COMMENT '正态性p值',
    ADD COLUMN `is_normal`         TINYINT(1)    NULL DEFAULT NULL COMMENT '是否正态分布(1=是)';
