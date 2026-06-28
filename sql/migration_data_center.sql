-- =====================================================================
-- 数据中心改造迁移脚本
-- 1. spc_workshop 新增 data_center_visible 字段 (标记车间是否进入数据中心)
-- 2. 新建 sys_workshop_component 表 (车间-数据组件关联)
--
-- 设计说明:
--   - 车间标记 data_center_visible=1 后, 才能被纳入数据中心车间选择器
--   - 用户可见车间 = (用户绑定车间) ∩ (data_center_visible=1 的车间)
--   - 车间关联的组件由 sys_workshop_component 表维护, 前端按列表动态渲染
--   - SPC tab 保持独立, 不受数据中心改造影响
-- =====================================================================

-- ----------------------------
-- 1. spc_workshop 新增 data_center_visible 字段
-- ----------------------------
ALTER TABLE `spc_workshop`
  ADD COLUMN `data_center_visible` TINYINT NOT NULL DEFAULT 0
  COMMENT '是否在数据中心可见 (0=否, 1=是)'
  AFTER `workshop_type`;

-- ----------------------------
-- 2. sys_workshop_component 车间-数据组件关联表
-- ----------------------------
CREATE TABLE IF NOT EXISTS `sys_workshop_component` (
    `id`            BIGINT NOT NULL AUTO_INCREMENT,
    `workshop_id`   BIGINT NOT NULL COMMENT '车间ID (spc_workshop.id, 须 data_center_visible=1)',
    `component_key` VARCHAR(64) NOT NULL COMMENT '组件标识 (前端注册表 key, 如 yield_dashboard)',
    `sort_order`    INT NOT NULL DEFAULT 0 COMMENT '显示顺序 (升序)',
    `enabled`       TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用 (0=禁用, 1=启用)',
    `created_by`    BIGINT NULL,
    `created_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`    BIGINT NULL,
    `updated_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`       INT NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_workshop_component` (`workshop_id`, `component_key`),
    KEY `idx_workshop_id` (`workshop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='车间-数据组件关联表';

-- ----------------------------
-- 3. (可选) 为现有标记为数据中心的车间默认关联 yield_dashboard 组件
--    若已存在关联记录, 此处会跳过
-- ----------------------------
-- INSERT INTO `sys_workshop_component` (`workshop_id`, `component_key`, `sort_order`, `enabled`, `created_at`)
-- SELECT `id`, 'yield_dashboard', 0, 1, NOW()
-- FROM `spc_workshop`
-- WHERE `data_center_visible` = 1 AND `deleted` = 0
--   AND NOT EXISTS (
--       SELECT 1 FROM `sys_workshop_component` swc
--       WHERE swc.`workshop_id` = `spc_workshop`.`id`
--         AND swc.`component_key` = 'yield_dashboard'
--   );
