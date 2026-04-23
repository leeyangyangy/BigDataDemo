ALTER TABLE sys_user ADD COLUMN wecom_user_id VARCHAR(64) DEFAULT NULL COMMENT '企业微信用户ID' AFTER workshop_id;
CREATE INDEX idx_sys_user_wecom_userid ON sys_user(wecom_user_id);


ALTER TABLE `spc_stat_result`
    ADD COLUMN `trigger_source` VARCHAR(32) NULL DEFAULT NULL
        COMMENT '统计触发来源: VERSION_CREATE/VERSION_SWITCH/VERSION_UPDATE/MANUAL/AUTO';