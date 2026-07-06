-- ============================================================
-- sys_user 增加密码最后修改时间字段 (等保三级 - 密码定期更换)
--
-- 等保三级 GB/T 22239-2019 要求: 应提示用户定期更换密码。
-- 本字段用于记录密码最后修改时间, 登录时检查是否超过 90 天未更换。
-- 现有用户的 password_updated_at 初始化为 created_at (账号创建时间),
-- 即账号创建超过 90 天的用户, 下次登录时将收到密码过期提醒。
-- ============================================================

ALTER TABLE `sys_user` ADD COLUMN `password_updated_at` DATETIME DEFAULT NULL COMMENT '密码最后修改时间 (等保三级: 密码定期更换)' AFTER `password`;

-- 将现有用户的密码修改时间初始化为账号创建时间
UPDATE `sys_user` SET `password_updated_at` = `created_at` WHERE `password_updated_at` IS NULL;
