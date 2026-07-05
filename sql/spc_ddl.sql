-- ============================================================
-- SPC 过程控制管理系统 - 数据库设计 (v2)
-- ============================================================
--
-- 目录:
--   Part 1  基础建模 (车间 / 产线 / 设备)
--   Part 2  产品与工序 (产品 / 工序)
--   Part 3  参数体系 (参数定义 / 标准版本化)
--   Part 4  数据采集 (批次 / 采集数据 / 统计结果)
--   Part 5  报警与审计 (报警 / 变更日志 / 操作日志)
--   Part 6  索引优化
--   Part 7  初始种子数据
-- ============================================================


-- ============================================================
-- Part 1: 基础建模 - 车间 / 产线 / 设备
-- ============================================================

CREATE DATABASE IF NOT EXISTS `spc_db`
    DEFAULT CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE `spc_db`;

-- -----------------------------------------------------------
-- 1.1 车间 (Workshop)
-- -----------------------------------------------------------

DROP TABLE IF EXISTS `spc_alert`;
DROP TABLE IF EXISTS `spc_standard_change_log`;
DROP TABLE IF EXISTS `spc_operation_log`;
DROP TABLE IF EXISTS `spc_stat_result`;
DROP TABLE IF EXISTS `spc_data`;
DROP TABLE IF EXISTS `spc_batch`;
DROP TABLE IF EXISTS `spc_param_version`;
DROP TABLE IF EXISTS `spc_param`;
DROP TABLE IF EXISTS `spc_product`;
DROP TABLE IF EXISTS `spc_process`;
DROP TABLE IF EXISTS `spc_equipment`;
DROP TABLE IF EXISTS `spc_production_line`;
DROP TABLE IF EXISTS `spc_workshop`;

CREATE TABLE `spc_workshop` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `workshop_code`   VARCHAR(64)  NOT NULL COMMENT '车间编码',
    `workshop_name`   VARCHAR(128) NOT NULL COMMENT '车间名称',
    `workshop_type`   VARCHAR(32)  NULL     DEFAULT NULL COMMENT '车间类型: 生产车间/质检车间/包装车间',
    `description`     VARCHAR(512) NULL     DEFAULT NULL,
    `status`          TINYINT      NOT NULL DEFAULT 1 COMMENT '1启用 0停用',
    `sort_order`      INT          NULL     DEFAULT 0,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_workshop_code` (`workshop_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='车间';

-- -----------------------------------------------------------
-- 1.2 产线 (Production Line)
-- -----------------------------------------------------------

CREATE TABLE `spc_production_line` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `workshop_id`     BIGINT       NOT NULL COMMENT '所属车间ID',
    `line_code`       VARCHAR(64)  NOT NULL COMMENT '产线编码',
    `line_name`       VARCHAR(128) NOT NULL COMMENT '产线名称',
    `description`     VARCHAR(512) NULL     DEFAULT NULL,
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `sort_order`      INT          NULL     DEFAULT 0,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_line_code` (`line_code`),
    KEY `idx_workshop_id` (`workshop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='产线';

-- -----------------------------------------------------------
-- 1.3 设备 (Equipment)
--     支持绑定到工序(process_id)，供填报数据时选择
-- -----------------------------------------------------------

CREATE TABLE `spc_equipment` (
    `id`              BIGINT        NOT NULL AUTO_INCREMENT,
    `line_id`         BIGINT        NULL     DEFAULT NULL COMMENT '产线ID',
    `process_id`      BIGINT        NULL     DEFAULT NULL COMMENT '所属工序ID(设备绑定到工序)',
    `equip_code`      VARCHAR(64)   NOT NULL COMMENT '设备编码',
    `equip_name`      VARCHAR(128)  NOT NULL COMMENT '设备名称',
    `equip_model`     VARCHAR(128)  NULL     DEFAULT NULL COMMENT '设备型号',
    `equip_type`      VARCHAR(32)   NULL     DEFAULT NULL COMMENT '设备类型: 检测设备/生产设备/辅助设备',
    `location`        VARCHAR(128)  NULL     DEFAULT NULL COMMENT '位置',
    `status`          VARCHAR(16)   NOT NULL DEFAULT '正常' COMMENT '状态: 正常/维修中/停用/报废',
    `remark`          VARCHAR(256)  NULL     DEFAULT NULL COMMENT '备注',
    `created_by`      BIGINT        NULL     DEFAULT NULL,
    `created_at`      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT        NULL     DEFAULT NULL,
    `updated_at`      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_equip_code` (`equip_code`),
    KEY `idx_line_id` (`line_id`),
    KEY `idx_process_id` (`process_id`),
    KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='设备';


-- ============================================================
-- Part 2: 产品与工序
-- ============================================================

-- -----------------------------------------------------------
-- 2.1 产品 (Product)
-- -----------------------------------------------------------

CREATE TABLE `spc_product` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `product_code`    VARCHAR(64)  NOT NULL COMMENT '产品编码',
    `product_name`    VARCHAR(128) NOT NULL COMMENT '产品名称',
    `product_type`    VARCHAR(32)  NULL     DEFAULT NULL COMMENT '产品类型',
    `specification`   VARCHAR(256) NULL     DEFAULT NULL COMMENT '规格说明',
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_product_code` (`product_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='产品';

-- -----------------------------------------------------------
-- 2.2 工序 (Process)
--     支持绑定到车间(workshop_id)，下拉框选择
-- -----------------------------------------------------------

CREATE TABLE `spc_process` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `process_code`    VARCHAR(64)  NOT NULL COMMENT '工序编码',
    `process_name`    VARCHAR(128) NOT NULL COMMENT '工序名称',
    `process_type`    VARCHAR(32)  NULL     DEFAULT NULL COMMENT '工序类型: 前道/后道/测试/包装',
    `workshop_id`     BIGINT       NULL     DEFAULT NULL COMMENT '所属车间ID',
    `description`     VARCHAR(512) NULL     DEFAULT NULL,
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `sort_order`      INT          NULL     DEFAULT 0,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_process_code` (`process_code`),
    KEY `idx_workshop_id` (`workshop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='工序';


-- ============================================================
-- Part 3: 参数体系 - 定义 + 标准版本化
-- ============================================================

-- -----------------------------------------------------------
-- 3.1 检测参数定义 (Param)
-- -----------------------------------------------------------

CREATE TABLE `spc_param` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `param_code`      VARCHAR(64)  NOT NULL COMMENT '参数编码',
    `param_name`      VARCHAR(128) NOT NULL COMMENT '参数名称',
    `param_type`      VARCHAR(16)  NOT NULL DEFAULT 'DIMENSION' COMMENT '参数类型: DIMENSION/ELECTRICAL/VISUAL/WEIGHT',
    `unit`            VARCHAR(32)  NULL     DEFAULT NULL COMMENT '测量单位',
    `data_type`       VARCHAR(16)  NOT NULL DEFAULT 'CONTINUOUS' COMMENT '数据类型: CONTINUOUS连续型/DISCRETE离散型/COUNT计数型',
    `decimal_places`  INT          NULL     DEFAULT 4 COMMENT '小数位数',
    `process_id`      BIGINT       NOT NULL COMMENT '所属工序ID',
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_param_code` (`param_code`),
    KEY `idx_process_id` (`process_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='检测参数定义';

-- -----------------------------------------------------------
-- 3.2 参数标准版本 (ParamVersion) - 核心表
--     支持标准变更版本化，每次上下限变更创建新版本
--     规则: 同一参数同一产品下只能有1个启用版本(status=1, is_current=1)
-- -----------------------------------------------------------

CREATE TABLE `spc_param_version` (
    `id`                BIGINT        NOT NULL AUTO_INCREMENT,
    `param_id`          BIGINT        NOT NULL COMMENT '参数ID',
    `product_id`        BIGINT        NOT NULL COMMENT '产品ID(0表示通用)',
    `version_no`        INT           NOT NULL DEFAULT 1 COMMENT '版本号(递增)',
    `usl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '规格上限 USL',
    `lsl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '规格下限 LSL',
    `target`            DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '目标值 Target',
    `ucl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '控制上限 UCL',
    `lcl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '控制下限 LCL',
    `cl`                DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '中心线 CL',
    `sigma_width`       DECIMAL(5,2)  NULL    DEFAULT 3.00 COMMENT '控制限宽度(sigma倍数)',
    `subgroup_size`     INT           NULL    DEFAULT 5 COMMENT '子组大小',
    `chart_type`        VARCHAR(16)   NULL    DEFAULT 'I_MR' COMMENT '控制图: I_MR/XBAR_R/XBAR_S/P/NP/C/U',
    `calc_method`       VARCHAR(16)   NULL    DEFAULT 'AUTO' COMMENT '计算方式: AUTO/MANUAL',
    `effective_from`    DATETIME      NOT NULL COMMENT '生效时间',
    `effective_to`      DATETIME      NULL    DEFAULT NULL COMMENT '失效时间(NULL=当前有效)',
    `is_current`        TINYINT       NOT NULL DEFAULT 1 COMMENT '1当前版本 0历史版本',
    `change_reason`     VARCHAR(512)  NULL    DEFAULT NULL COMMENT '变更原因',
    `change_type`       VARCHAR(32)   NULL    DEFAULT NULL COMMENT 'NEW/LIMIT_ADJUST/CHART_TYPE_CHANGE',
    `prev_version_id`   BIGINT        NULL    DEFAULT NULL COMMENT '前一版本ID',
    `status`            TINYINT       NOT NULL DEFAULT 1 COMMENT '1启用 0停用',
    `created_by`        BIGINT        NULL    DEFAULT NULL,
    `created_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`        BIGINT        NULL    DEFAULT NULL,
    `updated_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`           TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_param_product_version` (`param_id`, `product_id`, `version_no`),
    KEY `idx_param_id` (`param_id`),
    KEY `idx_product_id` (`product_id`),
    KEY `idx_is_current` (`is_current`),
    KEY `idx_status` (`status`),
    KEY `idx_effective` (`effective_from`, `effective_to`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='参数标准版本(标准变更版本化)';


-- ============================================================
-- Part 4: 数据采集 - 批次 / 采集数据 / 统计结果
-- ============================================================

-- -----------------------------------------------------------
-- 4.1 批次 (Batch)
-- -----------------------------------------------------------

CREATE TABLE `spc_batch` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `batch_code`      VARCHAR(64)  NOT NULL COMMENT '批次号',
    `product_id`      BIGINT       NOT NULL COMMENT '产品ID',
    `process_id`      BIGINT       NOT NULL COMMENT '工序ID',
    `workshop_id`     BIGINT       NULL     DEFAULT NULL,
    `line_id`         BIGINT       NULL     DEFAULT NULL,
    `lot_size`        INT          NULL     DEFAULT NULL COMMENT '批次数量',
    `batch_status`    VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE/COMPLETED/ON_HOLD/SCRAPPED',
    `start_time`      DATETIME     NULL     DEFAULT NULL,
    `end_time`        DATETIME     NULL     DEFAULT NULL,
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_batch_code` (`batch_code`),
    KEY `idx_product_id` (`product_id`),
    KEY `idx_process_id` (`process_id`),
    KEY `idx_batch_status` (`batch_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='批次';

-- -----------------------------------------------------------
-- 4.2 SPC采集数据 (Data) - 高并发写入核心表
--     param_version_id 绑定采集时刻生效的标准版本:
--       - 数据上传时自动解析当前生效版本并绑定
--       - 版本切换后新数据自动关联到新版本
--       - 历史数据归属不变(属于当时生效的版本)
--     建议按月分表: spc_data_202601, spc_data_202602 ...
-- -----------------------------------------------------------

CREATE TABLE `spc_data` (
    `id`                BIGINT        NOT NULL AUTO_INCREMENT,
    `param_version_id`  BIGINT        NOT NULL COMMENT '采集时生效的标准版本ID(版本切换后新数据自动关联新版本)',
    `batch_id`          VARCHAR(64)   NULL     DEFAULT NULL COMMENT '批次号(可留空)',
    `product_id`        BIGINT        NOT NULL COMMENT '产品ID',
    `process_id`        BIGINT        NOT NULL COMMENT '工序ID',
    `param_id`          BIGINT        NOT NULL COMMENT '参数ID',
    `equipment_id`      BIGINT        NULL     DEFAULT NULL COMMENT '设备ID',
    `workstation_no`    VARCHAR(32)   NULL     DEFAULT NULL COMMENT '工位号',
    `measured_value`    DECIMAL(16,6) NOT NULL COMMENT '测量值(计数图存不合格数/缺陷数)',
    `sample_size`       INT           NULL     DEFAULT NULL COMMENT '样本量(计数图P/NP/U图用,连续型为空)',
    `subgroup_idx`      INT           NULL     DEFAULT NULL COMMENT '子组内序号',
    `subgroup_size`     INT           NULL     DEFAULT 1 COMMENT '子组大小',
    `subgroup_seq`      BIGINT        NULL     DEFAULT NULL COMMENT '子组序号(全局递增)',
    `deviation`         DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '偏差(测量值-目标值)',
    `is_ooc`            TINYINT       NULL     DEFAULT 0 COMMENT '超出控制限: 0否 1是',
    `is_oos`            TINYINT       NULL     DEFAULT 0 COMMENT '超出规格限: 0否 1是',
    `sigma_level`       DECIMAL(8,4)  NULL     DEFAULT NULL COMMENT '所在sigma层级',
    `zone`              INT           NULL     DEFAULT NULL COMMENT '控制图区域(1:A区 2:B区 3:C区)',
    `spc_flags`         VARCHAR(128)  NULL     DEFAULT NULL COMMENT 'SPC标记(Nelson规则编号)',
    `data_source`       VARCHAR(32)   NULL     DEFAULT 'MANUAL' COMMENT '数据来源: MANUAL/AUTO/IMPORT',
    `collect_time`      DATETIME      NOT NULL COMMENT '采集时间',
    `fill_time`         DATETIME      NULL     DEFAULT NULL COMMENT '填写时间(不填则为当前时间)',
    `data_quality`      TINYINT       NULL     DEFAULT 1 COMMENT '数据质量: 1正常 0异常',
    `trace_id`          VARCHAR(64)   NULL     DEFAULT NULL COMMENT '链路追踪ID',
    `msg_id`            VARCHAR(64)   NULL     DEFAULT NULL COMMENT '消息幂等ID',
    `source_system`     VARCHAR(32)   NULL     DEFAULT NULL COMMENT '来源系统',
    `created_by`        BIGINT        NULL     DEFAULT NULL,
    `created_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`        BIGINT        NULL     DEFAULT NULL,
    `updated_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`           TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_msg_id` (`msg_id`),
    KEY `idx_param_version_id` (`param_version_id`),
    KEY `idx_batch_id` (`batch_id`),
    KEY `idx_product_param` (`product_id`, `param_id`),
    KEY `idx_collect_time` (`collect_time`),
    KEY `idx_equipment_id` (`equipment_id`),
    KEY `idx_is_ooc` (`is_ooc`),
    KEY `idx_is_oos` (`is_oos`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='SPC采集数据(建议按月分表)';

-- -----------------------------------------------------------
-- 4.3 SPC统计结果 (StatResult) - 按版本隔离
--     统计数据与版本一一对应:
--       - VERSION_CREATE: 创建新版本时自动计算
--       - VERSION_SWITCH: 切换生效版本时重新计算
--       - VERSION_UPDATE: 更新规格限后重新计算
--       - MANUAL:        手动触发
--       - AUTO:          控制图查询时自动补偿(兜底)
-- -----------------------------------------------------------

CREATE TABLE `spc_stat_result` (
    `id`                BIGINT        NOT NULL AUTO_INCREMENT,
    `param_version_id`  BIGINT        NOT NULL COMMENT '参数标准版本ID',
    `batch_id`          BIGINT        NULL     DEFAULT NULL COMMENT '批次ID(NULL=跨批次统计)',
    `product_id`        BIGINT        NOT NULL,
    `process_id`        BIGINT        NOT NULL,
    `param_id`          BIGINT        NOT NULL,
    `stat_type`         VARCHAR(16)   NOT NULL DEFAULT 'XBAR_R' COMMENT '统计类型',
    `sample_count`      INT           NOT NULL DEFAULT 0 COMMENT '样本数',
    `mean_value`        DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '均值',
    `std_dev`           DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '标准差',
    `range_value`       DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '极差',
    `cp`                DECIMAL(8,4)  NULL     DEFAULT NULL,
    `cpk`               DECIMAL(8,4)  NULL     DEFAULT NULL,
    `pp`                DECIMAL(8,4)  NULL     DEFAULT NULL,
    `ppk`               DECIMAL(8,4)  NULL     DEFAULT NULL,
    `calc_ucl`          DECIMAL(16,6) NULL     DEFAULT NULL,
    `calc_lcl`          DECIMAL(16,6) NULL     DEFAULT NULL,
    `calc_cl`           DECIMAL(16,6) NULL     DEFAULT NULL,
    `pass_rate`         DECIMAL(6,2)  NULL     DEFAULT NULL COMMENT '合格率(%)',
    `pass_count`        INT           NULL     DEFAULT NULL COMMENT '合格数',
    `fail_count`        INT           NULL     DEFAULT NULL COMMENT '不合格数',
    `normality_w`       DECIMAL(10,6) NULL     DEFAULT NULL COMMENT 'Shapiro-Wilk W统计量',
    `normality_p_value` DECIMAL(10,6) NULL     DEFAULT NULL COMMENT '正态性p值',
    `is_normal`         TINYINT(1)    NULL     DEFAULT NULL COMMENT '是否正态分布(1=是)',
    `trigger_source`    VARCHAR(32)   NULL     DEFAULT NULL COMMENT '统计触发来源: VERSION_CREATE/VERSION_SWITCH/VERSION_UPDATE/MANUAL/AUTO',
    `stat_time`         DATETIME      NOT NULL COMMENT '统计时间点',
    `period_start`      DATETIME      NULL     DEFAULT NULL,
    `period_end`        DATETIME      NULL     DEFAULT NULL,
    `status`            TINYINT       NOT NULL DEFAULT 1,
    `created_by`        BIGINT        NULL     DEFAULT NULL,
    `created_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`        BIGINT        NULL     DEFAULT NULL,
    `updated_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`           TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY `idx_param_version_id` (`param_version_id`),
    KEY `idx_product_param` (`product_id`, `param_id`),
    KEY `idx_batch_id` (`batch_id`),
    KEY `idx_stat_time` (`stat_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='SPC统计结果(按版本隔离)';


-- ============================================================
-- Part 5: 报警与审计
-- ============================================================

-- -----------------------------------------------------------
-- 5.1 报警记录 (Alert)
-- -----------------------------------------------------------

CREATE TABLE `spc_alert` (
    `id`                BIGINT        NOT NULL AUTO_INCREMENT,
    `alert_code`        VARCHAR(64)   NOT NULL COMMENT '报警编号',
    `param_version_id`  BIGINT        NOT NULL COMMENT '参数标准版本ID',
    `batch_id`          BIGINT        NULL     DEFAULT NULL,
    `process_id`        BIGINT        NULL     DEFAULT NULL,
    `param_id`          BIGINT        NULL     DEFAULT NULL,
    `product_id`        BIGINT        NULL     DEFAULT NULL,
    `data_id`           BIGINT        NULL     DEFAULT NULL COMMENT '触发数据ID',
    `alert_level`       INT           NOT NULL DEFAULT 1 COMMENT '级别: 0信息 1警告 2严重 3致命',
    `alert_type`        VARCHAR(16)   NOT NULL COMMENT 'OOC/OOS/NELSON/TREND/CAP_LOW/ANOMALY',
    `rule_name`         VARCHAR(64)   NULL     DEFAULT NULL,
    `rule_number`       VARCHAR(16)   NULL     DEFAULT NULL COMMENT 'Nelson规则编号',
    `measured_value`    DECIMAL(16,6) NULL     DEFAULT NULL,
    `ucl`               DECIMAL(16,6) NULL     DEFAULT NULL,
    `lcl`               DECIMAL(16,6) NULL     DEFAULT NULL,
    `usl`               DECIMAL(16,6) NULL     DEFAULT NULL,
    `lsl`               DECIMAL(16,6) NULL     DEFAULT NULL,
    `cl`                DECIMAL(16,6) NULL     DEFAULT NULL,
    `deviation`         DECIMAL(16,6) NULL     DEFAULT NULL,
    `sigma_level`       DECIMAL(8,4)  NULL     DEFAULT NULL,
    `severity`          VARCHAR(16)   NULL     DEFAULT 'WARNING',
    `message`           VARCHAR(512)  NULL     DEFAULT NULL,
    `detail_json`       TEXT          NULL     DEFAULT NULL,
    `status`            VARCHAR(16)   NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING/ACK/RESOLVED/SUPPRESSED/CLOSED',
    `acknowledged_by`   BIGINT        NULL     DEFAULT NULL,
    `acknowledged_at`   DATETIME      NULL     DEFAULT NULL,
    `resolve_remark`    VARCHAR(512)  NULL     DEFAULT NULL,
    `source_channel`    VARCHAR(32)   NULL     DEFAULT NULL,
    `mq_message_id`     VARCHAR(64)   NULL     DEFAULT NULL,
    `ws_pushed_at`      BIGINT        NULL     DEFAULT NULL COMMENT 'WebSocket推送时间戳',
    `alert_time`        DATETIME      NOT NULL COMMENT '报警时间',
    `created_by`        BIGINT        NULL     DEFAULT NULL,
    `created_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`        BIGINT        NULL     DEFAULT NULL,
    `updated_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`           TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY `idx_param_version_id` (`param_version_id`),
    KEY `idx_alert_type` (`alert_type`),
    KEY `idx_status` (`status`),
    KEY `idx_alert_time` (`alert_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='SPC报警记录';

-- -----------------------------------------------------------
-- 5.2 标准变更日志 (StandardChangeLog)
-- -----------------------------------------------------------

CREATE TABLE `spc_standard_change_log` (
    `id`                  BIGINT        NOT NULL AUTO_INCREMENT,
    `param_id`            BIGINT        NOT NULL COMMENT '参数ID',
    `product_id`          BIGINT        NULL     DEFAULT NULL COMMENT '产品ID',
    `old_version_id`      BIGINT        NULL     DEFAULT NULL COMMENT '旧版本ID',
    `new_version_id`      BIGINT        NULL     DEFAULT NULL COMMENT '新版本ID',
    `old_version_no`      INT           NULL     DEFAULT NULL COMMENT '旧版本号',
    `new_version_no`      INT           NULL     DEFAULT NULL COMMENT '新版本号',
    `change_type`         VARCHAR(32)   NOT NULL COMMENT '变更类型: NEW/LIMIT_ADJUST/CHART_TYPE_CHANGE/VERSION_SWITCH/VERSION_UPDATE/VERSION_DISABLE',
    `change_reason`       VARCHAR(512)  NULL     DEFAULT NULL COMMENT '变更原因',
    `old_usl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `new_usl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `old_lsl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `new_lsl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `old_target`          DECIMAL(16,6) NULL     DEFAULT NULL,
    `new_target`          DECIMAL(16,6) NULL     DEFAULT NULL,
    `old_ucl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `new_ucl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `old_lcl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `new_lcl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `regenerate_spc`      TINYINT       NOT NULL DEFAULT 0 COMMENT '是否触发SPC重算: 0否 1是',
    `regenerate_status`   VARCHAR(16)   NULL     DEFAULT NULL COMMENT '重算状态: PENDING/RUNNING/COMPLETED/FAILED',
    `regenerate_started_at` DATETIME    NULL     DEFAULT NULL,
    `regenerate_finished_at` DATETIME    NULL     DEFAULT NULL,
    `affected_data_count`  BIGINT        NULL     DEFAULT NULL COMMENT '影响数据条数',
    `status`              TINYINT       NOT NULL DEFAULT 1 COMMENT '1有效 0删除',
    `created_by`          BIGINT        NULL     DEFAULT NULL,
    `created_at`          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`          BIGINT        NULL     DEFAULT NULL,
    `updated_at`          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`             TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY `idx_param_id` (`param_id`),
    KEY `idx_product_id` (`product_id`),
    KEY `idx_new_version_id` (`new_version_id`),
    KEY `idx_change_type` (`change_type`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='标准变更日志';

-- -----------------------------------------------------------
-- 5.3 操作日志 (OperationLog)
-- -----------------------------------------------------------

CREATE TABLE `spc_operation_log` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT,
    `module`        VARCHAR(32)  NOT NULL COMMENT '模块: USER/EQUIPMENT/PROCESS/PARAM/DATA/ALERT',
    `action`        VARCHAR(32)  NOT NULL COMMENT '操作: CREATE/UPDATE/DELETE/LOGIN/LOGOUT',
    `target_id`     BIGINT       NULL     DEFAULT NULL COMMENT '操作对象ID',
    `target_type`   VARCHAR(32)  NULL     DEFAULT NULL COMMENT '对象类型',
    `content`       TEXT         NULL     DEFAULT NULL COMMENT '操作内容(JSON)',
    `ip_address`    VARCHAR(64)  NULL     DEFAULT NULL,
    `user_agent`    VARCHAR(512) NULL     DEFAULT NULL,
    `operator_id`   BIGINT       NULL     DEFAULT NULL,
    `operator_name` VARCHAR(64)  NULL     DEFAULT NULL,
    `result`        VARCHAR(16)  NOT NULL DEFAULT 'SUCCESS' COMMENT 'SUCCESS/FAIL',
    `error_msg`     VARCHAR(512) NULL     DEFAULT NULL,
    `duration_ms`   INT          NULL     DEFAULT NULL COMMENT '耗时(ms)',
    `created_at`    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `idx_module_action` (`module`, `action`),
    KEY `idx_operator_id` (`operator_id`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='操作日志';


-- ============================================================
-- Part 6: 外键约束 (可选，按需启用)
-- ============================================================

-- ALTER TABLE `spc_production_line` ADD CONSTRAINT `fk_line_workshop`
--     FOREIGN KEY (`workshop_id`) REFERENCES `spc_workshop`(`id`);

-- ALTER TABLE `spc_equipment` ADD CONSTRAINT `fk_equip_line`
--     FOREIGN KEY (`line_id`) REFERENCES `spc_production_line`(`id`);

-- ALTER TABLE `spc_equipment` ADD CONSTRAINT `fk_equip_process`
--     FOREIGN KEY (`process_id`) REFERENCES `spc_process`(`id`);

-- ALTER TABLE `spc_process` ADD CONSTRAINT `fk_process_workshop`
--     FOREIGN KEY (`workshop_id`) REFERENCES `spc_workshop`(`id`);

-- ALTER TABLE `spc_param` ADD CONSTRAINT `fk_param_process`
--     FOREIGN KEY (`process_id`) REFERENCES `spc_process`(`id`);

-- ALTER TABLE `spc_param_version` ADD CONSTRAINT `fk_pv_param`
--     FOREIGN KEY (`param_id`) REFERENCES `spc_param`(`id`);

-- ALTER TABLE `spc_param_version` ADD CONSTRAINT `fk_pv_product`
--     FOREIGN KEY (`product_id`) REFERENCES `spc_product`(`id`);

-- ALTER TABLE `spc_data` ADD CONSTRAINT `fk_data_param_version`
--     FOREIGN KEY (`param_version_id`) REFERENCES `spc_param_version`(`id`);


-- ============================================================
-- Part 7: 初始种子数据
-- ============================================================

INSERT INTO `spc_workshop` (`workshop_code`, `workshop_name`, `workshop_type`, `description`, `status`, `sort_order`) VALUES
('WS001', '一号生产车间', '生产车间', '主生产线', 1, 1),
('WS002', '二号质检车间', '质检车间', '质量检测', 1, 2),
('WS003', '包装车间', '包装车间', '成品包装', 1, 3);

INSERT INTO `spc_product` (`product_code`, `product_name`, `product_type`, `specification`, `status`) VALUES
('P001', '激光器芯片A型', '光通信', '1550nm波段', 1),
('P002', '探测器芯片B型', '光通信', '1310nm波段', 1);

INSERT INTO `spc_process` (`process_code`, `process_name`, `process_type`, `workshop_id`, `description`, `status`, `sort_order`) VALUES
('P001', '芯片贴装', '前道', 1, 'Die Attach工艺', 1, 1),
('P002', '引线键合', '后道', 1, 'Wire Bonding工艺', 1, 2),
('P003', '共晶焊接', '前道', 1, 'Eutectic Bonding', 1, 3);

INSERT INTO `spc_equipment` (`equip_code`, `equip_name`, `equip_type`, `equip_model`, `process_id`, `location`, `status`, `remark`) VALUES
('EQ001', '高精度贴片机', '生产设备', 'ASM AD830', 1, '一号车间-A区', '正常', '主力设备'),
('EQ002', '自动键合机', '生产设备', 'K&S 8028', 2, '一号车间-B区', '正常', NULL),
('EQ003', '共晶焊台', '生产设备', 'PULSAR 300', 3, '一号车间-C区', '正常', NULL);


-- ============================================================
-- Part 8: 数据修复脚本 (仅用于已有数据库升级，新部署可忽略)
-- ============================================================

UPDATE spc_param_version SET deleted = 0 WHERE deleted IS NULL;
UPDATE spc_param_version SET status = 1 WHERE status = 0 AND is_current = 1;
UPDATE spc_param_version SET status = 0 WHERE status = 1 AND is_current != 1;

-- 如果表已存在旧结构，需要重建或 ALTER：
ALTER TABLE `spc_standard_change_log`
    ADD COLUMN `product_id`          BIGINT        NULL DEFAULT NULL COMMENT '产品ID' AFTER `param_id`,
    CHANGE COLUMN `param_version_id` `new_version_id` BIGINT NULL DEFAULT NULL COMMENT '新版本ID',
    CHANGE COLUMN `prev_version_id`  `old_version_id` BIGINT NULL DEFAULT NULL COMMENT '旧版本ID',
    ADD COLUMN `old_version_no`      INT           NULL DEFAULT NULL COMMENT '旧版本号' AFTER `new_version_id`,
    ADD COLUMN `new_version_no`      INT           NULL DEFAULT NULL COMMENT '新版本号' AFTER `old_version_no`,
    MODIFY COLUMN `change_type`      VARCHAR(32)   NOT NULL COMMENT '变更类型: NEW/LIMIT_ADJUST/CHART_TYPE_CHANGE/VERSION_SWITCH/VERSION_UPDATE/VERSION_DISABLE',
    ADD COLUMN `old_ucl`             DECIMAL(16,6) NULL DEFAULT NULL AFTER `new_target`,
    ADD COLUMN `new_ucl`             DECIMAL(16,6) NULL DEFAULT NULL,
    ADD COLUMN `old_lcl`             DECIMAL(16,6) NULL DEFAULT NULL,
    ADD COLUMN `new_lcl`             DECIMAL(16,6) NULL DEFAULT NULL,
    ADD COLUMN `regenerate_spc`      TINYINT       NOT NULL DEFAULT 0 COMMENT '是否触发SPC重算' AFTER `new_lcl`,
    ADD COLUMN `regenerate_status`   VARCHAR(16)   NULL DEFAULT NULL COMMENT '重算状态',
    ADD COLUMN `regenerate_started_at` DATETIME    NULL DEFAULT NULL,
    ADD COLUMN `regenerate_finished_at` DATETIME    NULL DEFAULT NULL,
    ADD COLUMN `affected_data_count`  BIGINT        NULL DEFAULT NULL COMMENT '影响数据条数',
    ADD COLUMN `status`              TINYINT       NOT NULL DEFAULT 1 COMMENT '1有效 0删除',
    ADD COLUMN `created_by`          BIGINT        NULL DEFAULT NULL,
    ADD COLUMN `updated_by`          BIGINT        NULL DEFAULT NULL,
    ADD COLUMN `updated_at`          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    ADD COLUMN `deleted`             TINYINT       NOT NULL DEFAULT 0,
    DROP COLUMN `is_auto_created`,
    DROP COLUMN `operator_id`,
    DROP COLUMN `operator_name`,
    ADD KEY `idx_product_id` (`product_id`),
    ADD KEY `idx_new_version_id` (`new_version_id`);