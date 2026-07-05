-- 迁移脚本：统一 spc_param.data_type 字段为英文枚举 CONTINUOUS/DISCRETE/COUNT
-- 旧值可能为：''(空)、'连续型'/'连续'、'离散型'/'离散'、'计数型'/'计数'、
--            'continuous'/'Continuous'、'discrete'/'Discrete'、'count'/'Count'、
--            以及 DDL 旧定义的 'DECIMAL'/'INTEGER'/'TEXT'
-- 执行前请备份 spc_param 表

-- CONTINUOUS: 含 DDL 旧默认值 DECIMAL(按连续型数值处理) 与 TEXT(默认归入连续型)
UPDATE spc_param SET data_type = 'CONTINUOUS'
 WHERE data_type IS NULL OR data_type = ''
    OR data_type = '连续型' OR data_type = '连续'
    OR data_type = 'continuous' OR data_type = 'Continuous'
    OR data_type = 'DECIMAL' OR data_type = 'TEXT';
-- DISCRETE: 含 DDL 旧值 INTEGER
UPDATE spc_param SET data_type = 'DISCRETE'
 WHERE data_type = '离散型' OR data_type = '离散'
    OR data_type = 'discrete' OR data_type = 'Discrete'
    OR data_type = 'INTEGER';
UPDATE spc_param SET data_type = 'COUNT'
 WHERE data_type = '计数型' OR data_type = '计数'
    OR data_type = 'count' OR data_type = 'Count';

-- 修正字段注释（保持 DECIMAL/INTEGER/TEXT 的存储类型说明移除，统一为统计分类）
ALTER TABLE spc_param MODIFY COLUMN data_type VARCHAR(16) NOT NULL DEFAULT 'CONTINUOUS' COMMENT '数据类型: CONTINUOUS连续型/DISCRETE离散型/COUNT计数型';
