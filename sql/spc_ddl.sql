-- ============================================================
-- SPC 过程控制管理系统 - 数据库设计
-- 核心特性：标准变更版本化 / 多车间多工序多参数 / 高并发写入
-- ============================================================

-- -----------------------------------------------------------
-- 1. 组织建模：车间 / 产线 / 设备
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_workshop` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `workshop_code`   VARCHAR(64)  NOT NULL COMMENT '车间编码',
    `workshop_name`   VARCHAR(128) NOT NULL COMMENT '车间名称',
    `workshop_type`   VARCHAR(32)  NULL     DEFAULT NULL COMMENT '车间类型: FT封测/光刻/镀膜',
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='车间';

CREATE TABLE IF NOT EXISTS `spc_production_line` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `workshop_id`     BIGINT       NOT NULL COMMENT '车间ID',
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='产线';

CREATE TABLE IF NOT EXISTS `spc_equipment` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `line_id`         BIGINT       NULL     DEFAULT NULL COMMENT '产线ID',
    `equip_code`      VARCHAR(64)  NOT NULL COMMENT '设备编码',
    `equip_name`      VARCHAR(128) NOT NULL COMMENT '设备名称',
    `equip_model`     VARCHAR(128) NULL     DEFAULT NULL COMMENT '设备型号',
    `equip_type`      VARCHAR(32)  NULL     DEFAULT NULL COMMENT '设备类型',
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_equip_code` (`equip_code`),
    KEY `idx_line_id` (`line_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='设备';

-- -----------------------------------------------------------
-- 2. 产品建模：产品 / 工序 / 参数
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_product` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='产品';

CREATE TABLE IF NOT EXISTS `spc_process` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `process_code`    VARCHAR(64)  NOT NULL COMMENT '工序编码',
    `process_name`    VARCHAR(128) NOT NULL COMMENT '工序名称',
    `process_type`    VARCHAR(32)  NULL     DEFAULT NULL COMMENT '工序类型: FT封测/光刻/镀膜',
    `workshop_id`     BIGINT       NULL     DEFAULT NULL COMMENT '所属车间',
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='工序';

-- -----------------------------------------------------------
-- 3. 参数配置（核心：支持标准变更版本化）
--    当产品上下限变更时，创建新版本记录，旧版本标记为历史
--    SPC数据关联到 param_version_id，变更后新数据写入新版本
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_param` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `param_code`      VARCHAR(64)  NOT NULL COMMENT '参数编码',
    `param_name`      VARCHAR(128) NOT NULL COMMENT '参数名称',
    `param_type`      VARCHAR(16)  NOT NULL DEFAULT 'DIMENSION' COMMENT '参数类型: DIMENSION尺寸/ELECTRICAL电气/VISUAL外观/WEIGHT重量',
    `unit`            VARCHAR(32)  NULL     DEFAULT NULL COMMENT '单位',
    `data_type`       VARCHAR(16)  NOT NULL DEFAULT 'DECIMAL' COMMENT '数据类型: DECIMAL/INTEGER/TEXT',
    `decimal_places`  INT          NULL     DEFAULT 4 COMMENT '小数位数',
    `process_id`      BIGINT       NOT NULL COMMENT '所属工序',
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_param_code` (`param_code`),
    KEY `idx_process_id` (`process_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='检测参数定义';

-- -----------------------------------------------------------
-- 4. 参数标准版本（核心表：标准变更版本化）
--    每次上下限变更 → 新增一条记录，version递增
--    SPC数据通过 param_version_id 关联到具体版本
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_param_version` (
    `id`                BIGINT       NOT NULL AUTO_INCREMENT,
    `param_id`          BIGINT       NOT NULL COMMENT '参数ID',
    `product_id`        BIGINT       NOT NULL COMMENT '产品ID',
    `version_no`        INT          NOT NULL DEFAULT 1 COMMENT '版本号',
    `usl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '规格上限(USL)',
    `lsl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '规格下限(LSL)',
    `target`            DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '目标值(Target)',
    `ucl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '控制上限(UCL)',
    `lcl`               DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '控制下限(LCL)',
    `cl`                DECIMAL(16,6) NULL    DEFAULT NULL COMMENT '中心线(CL)',
    `sigma_width`       DECIMAL(5,2)  NULL    DEFAULT 3.00 COMMENT '控制限宽度(几倍sigma)',
    `subgroup_size`     INT           NULL    DEFAULT 1 COMMENT '子组大小',
    `chart_type`        VARCHAR(16)   NULL    DEFAULT 'XBAR_R' COMMENT '控制图类型: XBAR_R/XBAR_S/I_MR/P/C/U',
    `calc_method`       VARCHAR(16)   NULL    DEFAULT 'AUTO' COMMENT '计算方式: AUTO自动/MANUAL手动',
    `effective_from`    DATETIME      NOT NULL COMMENT '生效时间',
    `effective_to`      DATETIME      NULL    DEFAULT NULL COMMENT '失效时间(NULL=当前有效)',
    `is_current`        TINYINT       NOT NULL DEFAULT 1 COMMENT '1当前版本 0历史版本',
    `change_reason`     VARCHAR(512)  NULL    DEFAULT NULL COMMENT '变更原因',
    `change_type`       VARCHAR(32)   NULL    DEFAULT NULL COMMENT '变更类型: NEW新建/LIMIT_ADJUST限值调整/CHART_TYPE_CHANGE图表类型变更',
    `prev_version_id`   BIGINT        NULL    DEFAULT NULL COMMENT '前一版本ID',
    `status`            TINYINT       NOT NULL DEFAULT 1,
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
    KEY `idx_effective` (`effective_from`, `effective_to`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='参数标准版本(支持标准变更版本化)';

-- -----------------------------------------------------------
-- 5. 批次管理
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_batch` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='批次';

-- -----------------------------------------------------------
-- 6. SPC采集数据（核心表：高并发写入）
--    通过 param_version_id 关联到具体标准版本
--    变更标准后，新数据写入新版本ID，旧数据保持不变
--    按月分表：spc_data_202601, spc_data_202602, ...
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_data` (
    `id`                BIGINT        NOT NULL AUTO_INCREMENT,
    `param_version_id`  BIGINT        NOT NULL COMMENT '参数标准版本ID(关联spc_param_version)',
    `batch_id`          VARCHAR(64)    NULL     DEFAULT NULL COMMENT '批次号(可留空)',
    `product_id`        BIGINT        NOT NULL COMMENT '产品ID',
    `process_id`        BIGINT        NOT NULL COMMENT '工序ID',
    `param_id`          BIGINT        NOT NULL COMMENT '参数ID',
    `equipment_id`      BIGINT        NULL     DEFAULT NULL COMMENT '设备ID',
    `workstation_no`    VARCHAR(32)   NULL     DEFAULT NULL COMMENT '工位号',
    `measured_value`    DECIMAL(16,6) NOT NULL COMMENT '测量值',
    `subgroup_idx`      INT           NULL     DEFAULT NULL COMMENT '子组内序号',
    `subgroup_size`     INT           NULL     DEFAULT 1 COMMENT '子组大小',
    `subgroup_seq`      BIGINT        NULL     DEFAULT NULL COMMENT '子组序号(全局递增)',
    `deviation`         DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '偏差(测量值-目标值)',
    `is_ooc`            TINYINT       NULL     DEFAULT 0 COMMENT '是否超出控制限: 0否 1是',
    `is_oos`            TINYINT       NULL     DEFAULT 0 COMMENT '是否超出规格限: 0否 1是',
    `sigma_level`       DECIMAL(8,4)  NULL     DEFAULT NULL COMMENT '所在sigma层级',
    `zone`              INT           NULL     DEFAULT NULL COMMENT '控制图区域(1:A区 2:B区 3:C区)',
    `spc_flags`         VARCHAR(128)  NULL     DEFAULT NULL COMMENT 'SPC标记(违反的Nelson规则编号,逗号分隔)',
    `data_source`       VARCHAR(32)   NULL     DEFAULT 'MANUAL' COMMENT '数据来源: MANUAL/AUTO/IMPORT',
    `collect_time`      DATETIME      NOT NULL COMMENT '采集时间',
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='SPC采集数据(按月分表)';

-- -----------------------------------------------------------
-- 7. SPC统计结果（按版本隔离）
--    标准变更后，旧版本统计结果保留，新版本重新计算
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_stat_result` (
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
    `cp`                DECIMAL(8,4)  NULL     DEFAULT NULL COMMENT 'Cp',
    `cpk`               DECIMAL(8,4)  NULL     DEFAULT NULL COMMENT 'Cpk',
    `pp`                DECIMAL(8,4)  NULL     DEFAULT NULL COMMENT 'Pp',
    `ppk`               DECIMAL(8,4)  NULL     DEFAULT NULL COMMENT 'Ppk',
    `calc_ucl`          DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '计算控制上限',
    `calc_lcl`          DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '计算控制下限',
    `calc_cl`           DECIMAL(16,6) NULL     DEFAULT NULL COMMENT '计算中心线',
    `stat_time`         DATETIME      NOT NULL COMMENT '统计时间点',
    `period_start`      DATETIME      NULL     DEFAULT NULL COMMENT '统计区间开始',
    `period_end`        DATETIME      NULL     DEFAULT NULL COMMENT '统计区间结束',
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='SPC统计结果(按版本隔离)';

-- -----------------------------------------------------------
-- 8. 报警记录
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_alert` (
    `id`              BIGINT        NOT NULL AUTO_INCREMENT,
    `alert_code`      VARCHAR(64)   NOT NULL COMMENT '报警编号',
    `param_version_id` BIGINT       NOT NULL COMMENT '参数标准版本ID',
    `batch_id`        BIGINT        NULL     DEFAULT NULL,
    `process_id`      BIGINT        NULL     DEFAULT NULL,
    `param_id`        BIGINT        NULL     DEFAULT NULL,
    `product_id`      BIGINT        NULL     DEFAULT NULL,
    `data_id`         BIGINT        NULL     DEFAULT NULL COMMENT '触发数据ID',
    `alert_level`     INT           NOT NULL DEFAULT 1 COMMENT '报警级别: 0信息 1警告 2严重 3致命',
    `alert_type`      VARCHAR(16)   NOT NULL COMMENT 'OOC/OOS/NELSON/TREND/CAP_LOW/ANOMALY',
    `rule_name`       VARCHAR(64)   NULL     DEFAULT NULL,
    `rule_number`     VARCHAR(16)   NULL     DEFAULT NULL COMMENT 'Nelson规则编号',
    `measured_value`  DECIMAL(16,6) NULL     DEFAULT NULL,
    `ucl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `lcl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `usl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `lsl`             DECIMAL(16,6) NULL     DEFAULT NULL,
    `cl`              DECIMAL(16,6) NULL     DEFAULT NULL,
    `deviation`       DECIMAL(16,6) NULL     DEFAULT NULL,
    `sigma_level`     DECIMAL(8,4)  NULL     DEFAULT NULL,
    `severity`        VARCHAR(16)   NULL     DEFAULT 'WARNING',
    `message`         VARCHAR(512)  NULL     DEFAULT NULL,
    `detail_json`     TEXT          NULL     DEFAULT NULL,
    `status`          VARCHAR(16)   NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING/ACK/RESOLVED/SUPPRESSED/CLOSED',
    `acknowledged_by` BIGINT        NULL     DEFAULT NULL,
    `acknowledged_at` DATETIME      NULL     DEFAULT NULL,
    `resolve_remark`  VARCHAR(512)  NULL     DEFAULT NULL,
    `source_channel`  VARCHAR(32)   NULL     DEFAULT NULL,
    `mq_message_id`   VARCHAR(64)   NULL     DEFAULT NULL,
    `ws_pushed_at`    BIGINT        NULL     DEFAULT NULL COMMENT 'WebSocket推送时间戳',
    `alert_time`      DATETIME      NOT NULL COMMENT '报警时间',
    `created_by`      BIGINT        NULL     DEFAULT NULL,
    `created_at`      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT        NULL     DEFAULT NULL,
    `updated_at`      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_alert_code` (`alert_code`),
    KEY `idx_param_version_id` (`param_version_id`),
    KEY `idx_batch_id` (`batch_id`),
    KEY `idx_product_id` (`product_id`),
    KEY `idx_status` (`status`),
    KEY `idx_alert_level` (`alert_level`),
    KEY `idx_alert_time` (`alert_time`),
    KEY `idx_alert_type` (`alert_type`),
    KEY `idx_data_id` (`data_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='SPC报警记录';

-- -----------------------------------------------------------
-- 9. 标准变更记录（审计追踪）
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `spc_standard_change_log` (
    `id`                BIGINT       NOT NULL AUTO_INCREMENT,
    `param_id`          BIGINT       NOT NULL,
    `product_id`        BIGINT       NOT NULL,
    `old_version_id`    BIGINT       NULL     DEFAULT NULL COMMENT '旧版本ID',
    `new_version_id`    BIGINT       NOT NULL COMMENT '新版本ID',
    `old_version_no`    INT          NULL     DEFAULT NULL,
    `new_version_no`    INT          NOT NULL,
    `change_type`       VARCHAR(32)  NOT NULL COMMENT '变更类型',
    `change_reason`     VARCHAR(512) NULL     DEFAULT NULL,
    `old_usl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `new_usl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `old_lsl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `new_lsl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `old_target`        DECIMAL(16,6) NULL    DEFAULT NULL,
    `new_target`        DECIMAL(16,6) NULL    DEFAULT NULL,
    `old_ucl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `new_ucl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `old_lcl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `new_lcl`           DECIMAL(16,6) NULL    DEFAULT NULL,
    `regenerate_spc`    TINYINT      NOT NULL DEFAULT 1 COMMENT '是否重新生成SPC: 1是 0否',
    `regenerate_status` VARCHAR(16)  NULL     DEFAULT NULL COMMENT 'PENDING/RUNNING/COMPLETED/FAILED',
    `regenerate_started_at` DATETIME NULL     DEFAULT NULL,
    `regenerate_finished_at` DATETIME NULL    DEFAULT NULL,
    `affected_data_count` BIGINT    NULL     DEFAULT NULL COMMENT '受影响数据条数',
    `status`            TINYINT      NOT NULL DEFAULT 1,
    `created_by`        BIGINT       NULL     DEFAULT NULL,
    `created_at`        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`        BIGINT       NULL     DEFAULT NULL,
    `updated_at`        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`           TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY `idx_param_product` (`param_id`, `product_id`),
    KEY `idx_old_version` (`old_version_id`),
    KEY `idx_new_version` (`new_version_id`),
    KEY `idx_regenerate_status` (`regenerate_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='标准变更记录(审计追踪)';

-- -----------------------------------------------------------
-- 10. 用户表（工号登录 + 邮箱 + 手机号）
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS `sys_user` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `emp_no`          VARCHAR(64)  NOT NULL COMMENT '工号(登录账号)',
    `username`        VARCHAR(128) NOT NULL COMMENT '用户姓名',
    `password`        VARCHAR(256) NOT NULL COMMENT '密码(BCrypt加密)',
    `email`           VARCHAR(128) NULL     DEFAULT NULL COMMENT '邮箱',
    `phone`           VARCHAR(32)  NULL     DEFAULT NULL COMMENT '手机号',
    `avatar`          VARCHAR(512) NULL     DEFAULT NULL COMMENT '头像URL',
    `role`            VARCHAR(32)  NOT NULL DEFAULT 'OPERATOR' COMMENT '角色: ADMIN/ENGINEER/OPERATOR/VIEWER',
    `workshop_id`     BIGINT       NULL     DEFAULT NULL COMMENT '所属车间ID',
    `status`          TINYINT      NOT NULL DEFAULT 1 COMMENT '1启用 0停用',
    `last_login_at`   DATETIME     NULL     DEFAULT NULL COMMENT '最后登录时间',
    `last_login_ip`   VARCHAR(64)  NULL     DEFAULT NULL COMMENT '最后登录IP',
    `created_by`      BIGINT       NULL     DEFAULT NULL,
    `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_by`      BIGINT       NULL     DEFAULT NULL,
    `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `deleted`         TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_emp_no` (`emp_no`),
    KEY `idx_email` (`email`),
    KEY `idx_phone` (`phone`),
    KEY `idx_role` (`role`),
    KEY `idx_workshop_id` (`workshop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='系统用户(工号登录)';

-- 初始管理员账号 (密码: admin123, BCrypt加密)
INSERT INTO `sys_user` (`emp_no`, `username`, `password`, `email`, `phone`, `role`)
VALUES ('ADMIN001', '系统管理员', '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iKTVKIUi', 'admin@spc.com', '13800000001', 'ADMIN');

-- -----------------------------------------------------------
-- spc_data 表增加填写时间字段
-- -----------------------------------------------------------

ALTER TABLE `spc_data` ADD COLUMN `fill_time` DATETIME NULL DEFAULT NULL COMMENT '填写时间(不填则为当前时间)' AFTER `collect_time`;

-- 为 spc_equipment 表补充管理功能所需的扩展列
ALTER TABLE spc_equipment 
  ADD COLUMN process_id BIGINT NULL DEFAULT NULL COMMENT '所属工序ID' AFTER line_id,
  ADD COLUMN location VARCHAR(128) NULL DEFAULT NULL COMMENT '位置' AFTER process_id,
  ADD COLUMN remark VARCHAR(256) NULL DEFAULT NULL COMMENT '备注' AFTER location;

-- 可选：修改 status 列类型以支持中文状态值 (DDL原为 TINYINT)
ALTER TABLE spc_equipment MODIFY COLUMN status VARCHAR(16) NOT NULL DEFAULT '正常' COMMENT '状态: 正常/维修中/停用/报废';

ALTER TABLE spc_data MODIFY COLUMN batch_id VARCHAR(64) NULL DEFAULT NULL COMMENT '批次号(可留空)';