package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.SpcRuleConstants;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.service.SpcStatService;
import xyz.leeyangy.spc.service.SpcRuleEngine;
import xyz.leeyangy.spc.service.calculator.SpcCalculator;
import xyz.leeyangy.spc.entity.SpcAlert;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/spc/chart")
@RequiredArgsConstructor
public class SpcChartController {

    private final SpcDataService spcDataService;
    private final SpcStatService spcStatService;
    private final ParamVersionService paramVersionService;
    private final SpcRuleEngine spcRuleEngine;
    private final SpcCalculator spcCalculator;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    /** 图表缓存 key 前缀 */
    private static final String CHART_CACHE_PREFIX = "cockpit:chart:";
    /** 图表缓存 TTL */
    private static final Duration CHART_CACHE_TTL = Duration.ofMinutes(5);

    @OperationLog(module = "SPC_CHART", action = "QUERY_CONTROL", targetType = "ParamVersion",
            content = "'查询控制图: paramId=' + #paramId + ' productId=' + #productId + ' dataPoints=' + #result.data['totalPoints']")
    @GetMapping("/control")
    public R<Map<String, Object>> getControlChart(
            @RequestParam Long paramId,
            @RequestParam Long productId,
            @RequestParam(required = false) String batchId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "100") Integer limit) {

        // cache-aside: 查询优先读缓存, 命中直接返回; 数据写入/删除时由 SpcDataServiceImpl.clearListCache() 清除
        String cacheKey = buildChartCacheKey("control", paramId, productId, batchId, limit, startTime, endTime);
        Map<String, Object> cached = readChartCache(cacheKey);
        if (cached != null) {
            return R.ok(cached);
        }

        ParamVersion version = paramVersionService.getCurrentVersion(paramId, productId);
        if (version == null) {
            // 未配置版本时不缓存, 避免后续配置完成后仍返回空结果
            Map<String, Object> emptyResult = new LinkedHashMap<>();
            emptyResult.put("paramId", paramId);
            emptyResult.put("productId", productId);
            emptyResult.put("versionNo", 0);
            emptyResult.put("timeSeries", new ArrayList<>());
            emptyResult.put("values", new ArrayList<>());
            emptyResult.put("zones", new ArrayList<>());
            emptyResult.put("oocFlags", new ArrayList<>());
            emptyResult.put("limits", new LinkedHashMap<>());
            emptyResult.put("totalPoints", 0);
            emptyResult.put("message", "该参数尚未配置标准版本，请先在后台管理中设置上下限");
            return R.ok(emptyResult);
        }

        List<SpcData> dataList = spcDataService.listRecentData(version.getId(), limit, startTime, endTime);
        // listRecentData 按 collect_time DESC 返回（最新在前），反转成正序（旧→新）以便控制图按时间方向渲染
        Collections.reverse(dataList);

        List<String> timeSeries = new ArrayList<>();
        List<BigDecimal> values = new ArrayList<>();
        List<Integer> zones = new ArrayList<>();
        List<Integer> oocFlags = new ArrayList<>();
        List<Integer> sampleSizes = new ArrayList<>();

        for (SpcData d : dataList) {
            timeSeries.add(d.getCollectTime().toString());
            values.add(d.getMeasuredValue());
            zones.add(d.getZone() != null ? d.getZone() : 0);
            oocFlags.add(d.getIsOoc() != null ? d.getIsOoc() : 0);
            sampleSizes.add(d.getSampleSize());
        }

        // 基于当前查询数据集(受 limit/startTime/endTime 影响)实时计算统计量, 不读历史快照, 不落库
        // 这样前端展示的 mean/stdDev/Cp/Cpk 等指标随用户选择的数据范围动态变化
        SpcStatResult stat = !dataList.isEmpty()
                ? spcCalculator.computeStatistics(dataList, version, version.getId(), batchId, "QUERY")
                : null;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("paramVersionId", version.getId());
        result.put("paramId", paramId);
        result.put("productId", productId);
        result.put("versionNo", version.getVersionNo());
        result.put("chartType", version.getChartType() != null ? version.getChartType() : "I_MR");
        result.put("subgroupSize", version.getSubgroupSize() != null ? version.getSubgroupSize() : 1);
        result.put("timeSeries", timeSeries);
        result.put("values", values);
        result.put("zones", zones);
        result.put("oocFlags", oocFlags);
        result.put("sampleSizes", sampleSizes);

        Map<String, Object> limits = new LinkedHashMap<>();
        limits.put("ucl", version.getUcl() != null ? version.getUcl() :
                (stat != null ? stat.getCalcUcl() : null));
        limits.put("lcl", version.getLcl() != null ? version.getLcl() :
                (stat != null ? stat.getCalcLcl() : null));
        limits.put("cl", version.getCl() != null ? version.getCl() :
                (stat != null ? stat.getCalcCl() : null));
        limits.put("usl", version.getUsl());
        limits.put("lsl", version.getLsl());
        limits.put("target", version.getTarget());
        result.put("limits", limits);

        if (stat != null) {
            Map<String, Object> capability = new LinkedHashMap<>();
            capability.put("cp", stat.getCp());
            capability.put("cpk", stat.getCpk());
            capability.put("pp", stat.getPp());
            capability.put("ppk", stat.getPpk());
            capability.put("cpm", stat.getCpm());
            capability.put("mean", stat.getMeanValue());
            capability.put("stdDev", stat.getStdDev());
            capability.put("stdDevOverall", stat.getStdDevOverall());
            capability.put("min", stat.getMinValue());
            capability.put("max", stat.getMaxValue());
            capability.put("median", stat.getMedian());
            capability.put("range", stat.getRangeValue());
            capability.put("sampleCount", stat.getSampleCount());
            result.put("capability", capability);
            result.put("passRate", stat.getPassRate());
            result.put("passCount", stat.getPassCount());
            result.put("failCount", stat.getFailCount());
            result.put("normalityW", stat.getNormalityW());
            result.put("normalityPValue", stat.getNormalityPValue());
            result.put("isNormal", stat.getIsNormal());
        }

        result.put("totalPoints", dataList.size());
        writeChartCache(cacheKey, result);
        return R.ok(result);
    }

    @GetMapping("/data")
    public R<Map<String, Object>> getDataByEquipment(
            @RequestParam Long paramId,
            @RequestParam(required = false) Long equipmentId,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "100") Integer limit,
            HttpServletRequest request) {

        // cache-aside: 查询优先读缓存, 命中直接返回; 数据写入/删除时由 SpcDataServiceImpl.clearListCache() 清除
        String cacheKey = buildChartCacheKey("data", paramId, equipmentId, productId, limit, startTime, endTime);
        Map<String, Object> cached = readChartCache(cacheKey);
        if (cached != null) {
            return R.ok(cached);
        }

        Page<SpcData> pageResult = spcDataService.pageByCondition(
                new Page<>(1, limit), null, null, productId, paramId, startTime, endTime);

        List<SpcData> allData = pageResult.getRecords();
        if (equipmentId != null) {
            allData = allData.stream()
                    .filter(d -> equipmentId.equals(d.getEquipmentId()))
                    .collect(Collectors.toList());
        }
        // pageByCondition 按 collect_time DESC 返回(最新在前)，反转成正序(旧→新)以便控制图按时间方向渲染
        // 与 /control 端点保持一致
        Collections.reverse(allData);

        List<String> timeSeries = new ArrayList<>();
        List<BigDecimal> values = new ArrayList<>();
        List<Integer> zones = new ArrayList<>();
        List<Integer> oocFlags = new ArrayList<>();
        List<Integer> sampleSizes = new ArrayList<>();
        for (SpcData d : allData) {
            if (d.getCollectTime() != null && d.getMeasuredValue() != null) {
                timeSeries.add(d.getCollectTime().toString());
                values.add(d.getMeasuredValue());
                zones.add(d.getZone() != null ? d.getZone() : 0);
                oocFlags.add(d.getIsOoc() != null ? d.getIsOoc() : 0);
                sampleSizes.add(d.getSampleSize());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("paramId", paramId);
        result.put("equipmentId", equipmentId);
        result.put("timeSeries", timeSeries);
        result.put("values", values);
        result.put("zones", zones);
        result.put("oocFlags", oocFlags);
        result.put("sampleSizes", sampleSizes);
        result.put("totalPoints", allData.size());

        ParamVersion version = (productId != null) ? paramVersionService.getCurrentVersion(paramId, productId) : null;
        SpcStatResult stat = null;
        if (version != null) {
            // 基于当前查询数据集(受 limit/startTime/endTime/equipmentId 影响)实时计算统计量, 不读历史快照
            stat = !allData.isEmpty()
                    ? spcCalculator.computeStatistics(allData, version, version.getId(), null, "QUERY")
                    : null;
            Map<String, Object> limits = new LinkedHashMap<>();
            // 与 /control 端点一致: version 限为空时回退 stat 计算限(计数型图控制限由算法计算)
            limits.put("usl", version.getUsl());
            limits.put("lsl", version.getLsl());
            limits.put("target", version.getTarget());
            limits.put("ucl", version.getUcl() != null ? version.getUcl()
                    : (stat != null ? stat.getCalcUcl() : null));
            limits.put("lcl", version.getLcl() != null ? version.getLcl()
                    : (stat != null ? stat.getCalcLcl() : null));
            limits.put("cl", version.getCl() != null ? version.getCl()
                    : (stat != null ? stat.getCalcCl() : version.getTarget()));
            result.put("limits", limits);
            result.put("paramVersionId", version.getId());
            result.put("versionNo", version.getVersionNo());
            result.put("chartType", version.getChartType() != null ? version.getChartType() : "I_MR");
            result.put("subgroupSize", version.getSubgroupSize() != null ? version.getSubgroupSize() : 1);
            if (stat != null && !allData.isEmpty()) {
                Map<String, Object> capability = new LinkedHashMap<>();
                capability.put("cp", stat.getCp());
                capability.put("cpk", stat.getCpk());
                capability.put("pp", stat.getPp());
                capability.put("ppk", stat.getPpk());
                capability.put("cpm", stat.getCpm());
                capability.put("mean", stat.getMeanValue());
                capability.put("stdDev", stat.getStdDev());
                capability.put("stdDevOverall", stat.getStdDevOverall());
                capability.put("min", stat.getMinValue());
                capability.put("max", stat.getMaxValue());
                capability.put("median", stat.getMedian());
                capability.put("range", stat.getRangeValue());
                capability.put("sampleCount", stat.getSampleCount());
                result.put("capability", capability);
                result.put("passRate", stat.getPassRate());
                result.put("passCount", stat.getPassCount());
                result.put("failCount", stat.getFailCount());
                result.put("normalityW", stat.getNormalityW());
                result.put("normalityPValue", stat.getNormalityPValue());
                result.put("isNormal", stat.getIsNormal());
            }
        } else {
            Map<String, Object> emptyLimits = new LinkedHashMap<>();
            emptyLimits.put("usl", null);
            emptyLimits.put("lsl", null);
            emptyLimits.put("target", null);
            emptyLimits.put("ucl", null);
            emptyLimits.put("lcl", null);
            emptyLimits.put("cl", null);
            result.put("limits", emptyLimits);
            result.put("paramVersionId", null);
            result.put("versionNo", 0);
        }

        writeChartCache(cacheKey, result);
        return R.ok(result);
    }

    @OperationLog(module = "SPC_CHART", action = "DETECT_ALERTS", targetType = "ParamVersion",
            content = "'异常检测: paramId=' + #paramId + ' productId=' + #productId + ' dataPoints=' + #result.data.size() + (#ruleIds != null ? ' rules=' + #ruleIds : '')")
    @GetMapping("/alerts")
    public R<List<Map<String, Object>>> getAlerts(
            @RequestParam Long paramId,
            @RequestParam Long productId,
            @RequestParam(required = false) Long equipmentId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "100") Integer limit,
            @RequestParam(required = false) String ruleIds) {

        ParamVersion version = paramVersionService.getCurrentVersion(paramId, productId);
        if (version == null) {
            return R.ok(new ArrayList<>());
        }

        List<SpcData> dataList = spcDataService.listRecentData(version.getId(), limit, startTime, endTime);

        if (equipmentId != null) {
            dataList = dataList.stream()
                    .filter(d -> equipmentId.equals(d.getEquipmentId()))
                    .collect(Collectors.toList());
        }

        Set<Integer> enabledRuleSet = null;
        if (ruleIds != null && !ruleIds.isBlank()) {
            final String rawRuleIds = ruleIds;
            Set<String> invalidTokens = new HashSet<>();
            Set<Integer> outOfRangeIds = new HashSet<>();
            enabledRuleSet = Arrays.stream(rawRuleIds.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(s -> {
                        try {
                            int parsed = Integer.parseInt(s);
                            if (!SpcRuleConstants.isValidRuleId(parsed)) {
                                outOfRangeIds.add(parsed);
                                return null;
                            }
                            return parsed;
                        } catch (NumberFormatException e) {
                            invalidTokens.add(s);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            if (!invalidTokens.isEmpty() || !outOfRangeIds.isEmpty()) {
                log.warn("[ChartController] ruleIds参数'{}'包含无效项: 非数字={}, 超范围{}, 有效范围: {}。实际启用规则: {}",
                        rawRuleIds, invalidTokens.isEmpty() ? "无" : invalidTokens,
                        outOfRangeIds.isEmpty() ? "无" : outOfRangeIds,
                        SpcRuleConstants.formatValidRange(),
                        enabledRuleSet.isEmpty() ? "(全量检测)" : enabledRuleSet);
            }
            if (enabledRuleSet.isEmpty() && (!invalidTokens.isEmpty() || !outOfRangeIds.isEmpty())) {
                log.warn("[ChartController] ruleIds参数'{}'所有项均无效, 将执行全量检测", rawRuleIds);
                enabledRuleSet = null;
            }
        }

        List<SpcAlert> alerts = spcRuleEngine.detectRules(dataList, version, enabledRuleSet);

        List<Map<String, Object>> result = new ArrayList<>();
        for (int i = 0; i < alerts.size(); i++) {
            SpcAlert alert = alerts.get(i);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("ruleId", extractRuleId(alert.getRuleNumber(), i + 1));
            item.put("ruleCode", alert.getRuleNumber());
            item.put("ruleName", alert.getRuleName());
            item.put("value", alert.getMeasuredValue() != null ? alert.getMeasuredValue().toString() : "");
            item.put("time", alert.getAlertTime() != null ? alert.getAlertTime().toString() : "");
            item.put("index", i);
            item.put("message", alert.getMessage());
            item.put("severity", getSeverityLevel(alert.getRuleNumber()));
            result.add(item);
        }

        return R.ok(result);
    }

    /** 从规则代码（如 "N1"）提取数字 ID，失败时回退到 fallback 值 */
    private int extractRuleId(String ruleCode, int fallback) {
        if (ruleCode != null && ruleCode.startsWith("N")) {
            try {
                return Integer.parseInt(ruleCode.substring(1));
            } catch (NumberFormatException e) {
                return fallback;
            }
        }
        return fallback;
    }

    private String getSeverityLevel(String ruleCode) {
        if (SpcRuleEngine.RULE_1_BEYOND_3SIGMA.equals(ruleCode)) return "critical";
        if (SpcRuleEngine.RULE_2_NINE_ONE_SIDE.equals(ruleCode)) return "major";
        if (SpcRuleEngine.RULE_3_SIX_TREND.equals(ruleCode)) return "major";
        if (SpcRuleEngine.RULE_4_FOURTEEN_ALTERNATE.equals(ruleCode)) return "warning";
        if (SpcRuleEngine.RULE_5_TWO_OF_THREE_2SIGMA.equals(ruleCode)) return "warning";
        if (SpcRuleEngine.RULE_6_FOUR_OF_FIVE_1SIGMA.equals(ruleCode)) return "warning";
        if (SpcRuleEngine.RULE_7_FIFTEEN_IN_1SIGMA.equals(ruleCode)) return "info";
        if (SpcRuleEngine.RULE_8_EIGHT_OUTSIDE_1SIGMA.equals(ruleCode)) return "info";
        return "warning";
    }

    /**
     * 构建图表缓存 key
     * 格式: cockpit:chart:{endpoint}:{part1}:{part2}:...
     * null 值统一编码为 "null" 以避免 key 冲突
     */
    private String buildChartCacheKey(String endpoint, Object... parts) {
        StringBuilder sb = new StringBuilder(CHART_CACHE_PREFIX).append(endpoint).append(":");
        for (int i = 0; i < parts.length; i++) {
            sb.append(parts[i] != null ? parts[i].toString() : "null");
            if (i < parts.length - 1) sb.append(":");
        }
        return sb.toString();
    }

    /**
     * 读取图表缓存
     * 命中返回反序列化后的 Map, 未命中或异常返回 null (降级到 DB 查询)
     */
    private Map<String, Object> readChartCache(String key) {
        try {
            String json = redisTemplate.opsForValue().get(key);
            if (json == null || json.isEmpty()) {
                return null;
            }
            return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.warn("[Cache] 读取图表缓存失败 key={}, msg={}", key, e.getMessage());
            return null;
        }
    }

    /**
     * 写入图表缓存
     * 异常仅记录日志, 不影响主流程返回结果 (5 分钟 TTL, 由 SpcDataServiceImpl.clearListCache 主动失效)
     */
    private void writeChartCache(String key, Map<String, Object> result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            redisTemplate.opsForValue().set(key, json, CHART_CACHE_TTL);
        } catch (Exception e) {
            log.warn("[Cache] 写入图表缓存失败 key={}, msg={}", key, e.getMessage());
        }
    }
}
