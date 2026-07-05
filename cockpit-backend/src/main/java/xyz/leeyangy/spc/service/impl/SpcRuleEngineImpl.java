package xyz.leeyangy.spc.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.common.SpcRuleConstants;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.service.SpcAlertService;
import xyz.leeyangy.spc.service.SpcRuleEngine;
import xyz.leeyangy.spc.service.calculator.SpcCalculator;
import xyz.leeyangy.spc.service.rule.SpcRule;
import xyz.leeyangy.spc.service.rule.SpcRuleContext;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * SPC 判异规则引擎实现
 *
 * <p>基于策略模式：注入 {@link SpcRule} 列表，按规则序号顺序执行检测。
 * 新增规则只需新增 {@code SpcRule} 实现类，无需修改本类（开闭原则）。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SpcRuleEngineImpl implements SpcRuleEngine {

    private final SpcAlertService spcAlertService;
    private final List<SpcRule> rules;
    private final SpcCalculator spcCalculator;

    @Override
    public List<SpcAlert> detectRules(List<SpcData> dataList, ParamVersion version) {
        return detectRules(dataList, version, null);
    }

    @Override
    public List<SpcAlert> detectRules(List<SpcData> dataList, ParamVersion version, Set<Integer> enabledRuleIds) {
        List<SpcAlert> alerts = new ArrayList<>();
        if (dataList == null || dataList.size() < 2 || version == null) return alerts;

        Set<Integer> validRuleIds = validateAndFilterRuleIds(enabledRuleIds);
        boolean runAll = validRuleIds == null;

        String chartType = version.getChartType() != null
                ? version.getChartType().replace("-", "_").replace(" ", "").toUpperCase() : "";
        boolean isCountChart = "P".equals(chartType) || "NP".equals(chartType)
                || "C".equals(chartType) || "U".equals(chartType);

        BigDecimal cl = version.getCl();
        BigDecimal ucl = version.getUcl();
        BigDecimal lcl = version.getLcl();

        // 计数图(P/NP/C/U): version 手动限为空时, 用 SpcCalculator 动态计算控制限
        // 计数图控制限由 p̄/c̄/ū 等算法得出, 通常不存于 ParamVersion 而由 SpcCalculator 实时计算
        if (isCountChart && cl == null && ucl == null && lcl == null) {
            try {
                SpcStatResult stat = spcCalculator.computeStatistics(
                        dataList, version, version.getId(), null, "RULE_ENGINE");
                if (stat != null) {
                    cl = stat.getCalcCl();
                    ucl = stat.getCalcUcl();
                    lcl = stat.getCalcLcl();
                }
            } catch (Exception e) {
                log.warn("[RuleEngine] 计数图动态控制限计算失败, 跳过判异: paramVersionId={} err={}",
                        version.getId(), e.getMessage());
                return alerts;
            }
        }

        // CL 必须存在；UCL 和 LCL 至少存在一个（支持单边控制限）
        if (cl == null || (ucl == null && lcl == null)) return alerts;

        // sigma 由存在的控制限推导：UCL=CL+3σ 或 LCL=CL-3σ
        BigDecimal sigma;
        String sided;
        if (ucl != null && lcl != null) {
            sigma = ucl.subtract(cl).divide(BigDecimal.valueOf(3), 10, RoundingMode.HALF_UP);
            sided = "BOTH";
        } else if (ucl != null) {
            sigma = ucl.subtract(cl).divide(BigDecimal.valueOf(3), 10, RoundingMode.HALF_UP);
            sided = "UPPER";
        } else {
            sigma = cl.subtract(lcl).divide(BigDecimal.valueOf(3), 10, RoundingMode.HALF_UP);
            sided = "LOWER";
        }

        double[] values = new double[dataList.size()];
        boolean isRateChart = "P".equals(chartType) || "U".equals(chartType);
        for (int i = 0; i < dataList.size(); i++) {
            SpcData d = dataList.get(i);
            // P/U 计数图描点值为比率(不合格率/单位缺陷数)，需用 measuredValue/sampleSize 与比率控制限比较
            if (isRateChart && d.getSampleSize() != null && d.getSampleSize() > 0) {
                values[i] = d.getMeasuredValue().doubleValue() / d.getSampleSize();
            } else {
                values[i] = d.getMeasuredValue().doubleValue();
            }
        }

        SpcRuleContext ctx = SpcRuleContext.builder()
                .dataList(dataList)
                .values(values)
                .version(version)
                .cl(cl)
                .ucl(ucl)
                .lcl(lcl)
                .oneSigma(sigma)
                .twoSigma(sigma.multiply(BigDecimal.valueOf(2)))
                .threeSigma(sigma.multiply(BigDecimal.valueOf(3)))
                .sided(sided)
                .build();

        Set<Integer> flaggedIndices = new HashSet<>();

        rules.stream()
                .sorted(Comparator.comparingInt(SpcRule::ruleId))
                .filter(rule -> runAll || validRuleIds.contains(rule.ruleId()))
                .forEach(rule -> rule.check(ctx, flaggedIndices, alerts));

        return alerts;
    }

    private Set<Integer> validateAndFilterRuleIds(Set<Integer> rawRuleIds) {
        if (rawRuleIds == null) {
            log.debug("[RuleEngine] enabledRuleIds=null, 执行全量检测");
            return null;
        }
        if (rawRuleIds.isEmpty()) {
            log.info("[RuleEngine] enabledRuleIds为空集, 执行全量检测");
            return null;
        }
        if (rawRuleIds.contains(SpcRuleConstants.SELECT_ALL_FLAG)) {
            log.info("[RuleEngine] 检测到规则ID=0(全选标记), 将执行全部{}条规则", SpcRuleConstants.TOTAL_RULES);
            return null;
        }

        Set<Integer> invalidIds = new TreeSet<>();
        Set<Integer> validIds = new LinkedHashSet<>();
        for (Integer id : rawRuleIds) {
            if (SpcRuleConstants.isValidRuleId(id)) {
                validIds.add(id);
            } else {
                invalidIds.add(id);
            }
        }

        if (!invalidIds.isEmpty()) {
            log.warn("[RuleEngine] 检测到无效规则ID: {}, 有效范围: {}, 已自动过滤。实际启用规则: [{}]",
                    invalidIds, SpcRuleConstants.formatValidRange(),
                    validIds.isEmpty() ? "(无,将全量检测)" : String.join(",", validIds.stream().map(String::valueOf).toArray(String[]::new)));
        }
        if (validIds.isEmpty()) {
            log.warn("[RuleEngine] 所有传入的规则ID均无效({}), 将执行全量检测", rawRuleIds);
            return null;
        }
        return validIds;
    }

    @Override
    public void saveAndLogAlerts(List<SpcData> dataList, ParamVersion version) {
        try {
            List<SpcAlert> alerts = detectRules(dataList, version);
            if (!alerts.isEmpty()) {
                spcAlertService.saveBatch(alerts);
                log.info("[RuleEngine] 检测到 {} 条判异报警, paramVersionId={}, 规则分布: {}",
                        alerts.size(), version.getId(),
                        alerts.stream().map(SpcAlert::getRuleNumber).collect(Collectors.toList()).toString());
            }
        } catch (Exception e) {
            log.error("[RuleEngine] 判异检测执行失败, paramVersionId={}", version.getId(), e);
        }
    }
}
