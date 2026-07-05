-- 迁移脚本：为 spc_data 增加 sample_size 字段(计数型图 P/NP/U 图样本量)
-- 计数型图中 measured_value 存不合格数/缺陷数，sample_size 存样本量/检查单位数
-- 连续型数据该字段为空

ALTER TABLE spc_data ADD COLUMN sample_size INT NULL DEFAULT NULL COMMENT '样本量(计数图P/NP/U图用,连续型为空)' AFTER measured_value;
