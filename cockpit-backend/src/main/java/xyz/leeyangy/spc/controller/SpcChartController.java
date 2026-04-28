package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.SpcRuleConstants;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.service.SpcStatService;
import xyz.leeyangy.spc.service.SpcRuleEngine;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.math.BigDecimal;
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

    @GetMapping("/control")
    public R<Map<String, Object>> getControlChart(
            @RequestParam Long paramId,
            @RequestParam Long productId,
            @RequestParam(required = false) String batchId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "100") Integer limit) {

        ParamVersion version = paramVersionService.getCurrentVersion(paramId, productId);
        if (version == null) {
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

        List<String> timeSeries = new ArrayList<>();
        List<BigDecimal> values = new ArrayList<>();
        List<Integer> zones = new ArrayList<>();
        List<Integer> oocFlags = new ArrayList<>();

        for (SpcData d : dataList) {
            timeSeries.add(d.getCollectTime().toString());
            values.add(d.getMeasuredValue());
            zones.add(d.getZone() != null ? d.getZone() : 0);
            oocFlags.add(d.getIsOoc() != null ? d.getIsOoc() : 0);
        }

        SpcStatResult stat = spcStatService.getLatestStat(version.getId(), batchId);
        if (stat == null && !dataList.isEmpty()) {
            stat = spcStatService.calculateAndSave(version.getId(), batchId, "AUTO");
        }

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
            capability.put("mean", stat.getMeanValue());
            capability.put("stdDev", stat.getStdDev());
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
        return R.ok(result);
    }

    @GetMapping("/data")
    public R<Map<String, Object>> getDataByEquipment(
            @RequestParam Long paramId,
            @RequestParam(required = false) Long equipmentId,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "100") Integer limit) {

        Page<SpcData> pageResult = spcDataService.pageByCondition(
                new Page<>(1, limit), null, null, productId, paramId, startTime, endTime);

        List<SpcData> allData = pageResult.getRecords();
        if (equipmentId != null) {
            allData = allData.stream()
                    .filter(d -> equipmentId.equals(d.getEquipmentId()))
                    .collect(Collectors.toList());
        }

        List<String> timeSeries = new ArrayList<>();
        List<BigDecimal> values = new ArrayList<>();
        List<Integer> zones = new ArrayList<>();
        List<Integer> oocFlags = new ArrayList<>();
        for (SpcData d : allData) {
            if (d.getCollectTime() != null && d.getMeasuredValue() != null) {
                timeSeries.add(d.getCollectTime().toString());
                values.add(d.getMeasuredValue());
                zones.add(d.getZone() != null ? d.getZone() : 0);
                oocFlags.add(d.getIsOoc() != null ? d.getIsOoc() : 0);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("paramId", paramId);
        result.put("equipmentId", equipmentId);
        result.put("timeSeries", timeSeries);
        result.put("values", values);
        result.put("zones", zones);
        result.put("oocFlags", oocFlags);
        result.put("totalPoints", allData.size());

        ParamVersion version = (productId != null) ? paramVersionService.getCurrentVersion(paramId, productId) : null;
        if (version != null) {
            Map<String, Object> limits = new LinkedHashMap<>();
            limits.put("usl", version.getUsl());
            limits.put("lsl", version.getLsl());
            limits.put("target", version.getTarget());
            limits.put("ucl", version.getUcl());
            limits.put("lcl", version.getLcl());
            limits.put("cl", version.getCl() != null ? version.getCl() : version.getTarget());
            result.put("limits", limits);
            result.put("paramVersionId", version.getId());
            result.put("versionNo", version.getVersionNo());
            result.put("chartType", version.getChartType() != null ? version.getChartType() : "I_MR");
            result.put("subgroupSize", version.getSubgroupSize() != null ? version.getSubgroupSize() : 1);

            SpcStatResult stat = spcStatService.getLatestStat(version.getId(), null);
            if (stat == null && !allData.isEmpty()) {
                stat = spcStatService.calculateAndSave(version.getId(), null, "AUTO");
            }
            if (stat != null && !allData.isEmpty()) {
                Map<String, Object> capability = new LinkedHashMap<>();
                capability.put("cp", stat.getCp());
                capability.put("cpk", stat.getCpk());
                capability.put("pp", stat.getPp());
                capability.put("ppk", stat.getPpk());
                capability.put("mean", stat.getMeanValue());
                capability.put("stdDev", stat.getStdDev());
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

        return R.ok(result);
    }

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
            item.put("ruleId", i + 1);
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

    private String getSeverityLevel(String ruleCode) {
        if (SpcRuleEngine.RULE_1_BEYOND_3SIGMA.equals(ruleCode)) return "critical";
        if (SpcRuleEngine.RULE_2_NINE_ONE_SIDE.equals(ruleCode)) return "major";
        if (SpcRuleEngine.RULE_3_SIX_TREND.equals(ruleCode)) return "major";
        if (SpcRuleEngine.RULE_5_TWO_OF_THREE_2SIGMA.equals(ruleCode)) return "warning";
        if (SpcRuleEngine.RULE_6_FOUR_OF_FIVE_1SIGMA.equals(ruleCode)) return "warning";
        return "warning";
    }
}
