package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.entity.SpcData;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpcRuleEngine {

    private final SpcAlertService spcAlertService;

    public static final String RULE_1_BEYOND_3SIGMA = "N1";
    public static final String RULE_2_NINE_ONE_SIDE = "N2";
    public static final String RULE_3_SIX_TREND = "N3";
    public static final String RULE_4_FOURTEEN_ALTERNATE = "N4";
    public static final String RULE_5_TWO_OF_THREE_2SIGMA = "N5";
    public static final String RULE_6_FOUR_OF_FIVE_1SIGMA = "N6";
    public static final String RULE_7_FIFTEEN_IN_1SIGMA = "N7";
    public static final String RULE_8_EIGHT_OUTSIDE_1SIGMA = "N8";

    private static final Map<String, String> RULE_NAMES = new LinkedHashMap<>();
    static {
        RULE_NAMES.put(RULE_1_BEYOND_3SIGMA, "超出3σ控制限");
        RULE_NAMES.put(RULE_2_NINE_ONE_SIDE, "连续9点在中心线同侧");
        RULE_NAMES.put(RULE_3_SIX_TREND, "连续6点单调递增/递减");
        RULE_NAMES.put(RULE_4_FOURTEEN_ALTERNATE, "连续14点上下交替");
        RULE_NAMES.put(RULE_5_TWO_OF_THREE_2SIGMA, "连续3点中有2点超出2σ");
        RULE_NAMES.put(RULE_6_FOUR_OF_FIVE_1SIGMA, "连续5点中有4点超出1σ");
        RULE_NAMES.put(RULE_7_FIFTEEN_IN_1SIGMA, "连续15点在1σ内(分层)");
        RULE_NAMES.put(RULE_8_EIGHT_OUTSIDE_1SIGMA, "连续8点在1σ外(分层)");
    }

    public List<SpcAlert> detectRules(List<SpcData> dataList, ParamVersion version) {
        List<SpcAlert> alerts = new ArrayList<>();
        if (dataList == null || dataList.size() < 2 || version == null) return alerts;

        BigDecimal cl = version.getCl();
        BigDecimal ucl = version.getUcl();
        BigDecimal lcl = version.getLcl();

        if (cl == null || ucl == null || lcl == null) return alerts;

        BigDecimal sigma = ucl.subtract(cl).divide(BigDecimal.valueOf(3), 10, RoundingMode.HALF_UP);
        BigDecimal oneSigma = sigma;
        BigDecimal twoSigma = sigma.multiply(BigDecimal.valueOf(2));
        BigDecimal threeSigma = sigma.multiply(BigDecimal.valueOf(3));

        double[] values = new double[dataList.size()];
        for (int i = 0; i < dataList.size(); i++) {
            values[i] = dataList.get(i).getMeasuredValue().doubleValue();
        }

        Set<Integer> flaggedIndices = new HashSet<>();

        checkRule1(dataList, values, ucl, lcl, threeSigma, version, flaggedIndices, alerts);
        checkRule2(dataList, values, cl, version, flaggedIndices, alerts);
        checkRule3(dataList, values, cl, version, flaggedIndices, alerts);
        checkRule4(dataList, values, cl, version, flaggedIndices, alerts);
        checkRule5(dataList, values, cl, oneSigma, twoSigma, version, flaggedIndices, alerts);
        checkRule6(dataList, values, cl, oneSigma, version, flaggedIndices, alerts);
        checkRule7(dataList, values, cl, oneSigma, version, flaggedIndices, alerts);
        checkRule8(dataList, values, cl, oneSigma, version, flaggedIndices, alerts);

        return alerts;
    }

    private void checkRule1(List<SpcData> dataList, double[] values,
                             BigDecimal ucl, BigDecimal lcl, BigDecimal threeSigma,
                             ParamVersion version, Set<Integer> flagged, List<SpcAlert> alerts) {
        for (int i = 0; i < values.length; i++) {
            if (!flagged.contains(i)) {
                double v = values[i];
                if (v > ucl.doubleValue() + threeSigma.doubleValue() * 0.001 ||
                    v < lcl.doubleValue() - threeSigma.doubleValue() * 0.001) {
                    SpcData d = dataList.get(i);
                    alerts.add(createAlert(d, version, RULE_1_BEYOND_3SIGMA,
                            "数据点超出3σ控制限: 值=" + v +
                            (v > ucl.doubleValue() ? " > UCL" : " < LCL")));
                    flagged.add(i);
                }
            }
        }
    }

    private void checkRule2(List<SpcData> dataList, double[] values,
                             BigDecimal cl, ParamVersion version,
                             Set<Integer> flagged, List<SpcAlert> alerts) {
        int n = values.length;
        if (n < 9) return;
        double centerLine = cl.doubleValue();

        for (int end = 9; end <= n; end++) {
            boolean allAbove = true;
            boolean allBelow = true;
            for (int i = end - 9; i < end && (allAbove || allBelow); i++) {
                if (values[i] >= centerLine) allBelow = false;
                if (values[i] <= centerLine) allAbove = false;
            }
            if (allAbove || allBelow) {
                int triggerIdx = end - 1;
                if (!flagged.contains(triggerIdx)) {
                    SpcData d = dataList.get(triggerIdx);
                    alerts.add(createAlert(d, version, RULE_2_NINE_ONE_SIDE,
                            "连续9点在中心线" + (allAbove ? "上方" : "下方")));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }

    private void checkRule3(List<SpcData> dataList, double[] values,
                             BigDecimal cl, ParamVersion version,
                             Set<Integer> flagged, List<SpcAlert> alerts) {
        int n = values.length;
        if (n < 6) return;

        for (int start = 0; start <= n - 6; start++) {
            boolean increasing = true;
            boolean decreasing = true;
            for (int i = start; i < start + 5; i++) {
                if (values[i + 1] <= values[i]) increasing = false;
                if (values[i + 1] >= values[i]) decreasing = false;
            }
            if (increasing || decreasing) {
                int triggerIdx = start + 5;
                if (!flagged.contains(triggerIdx)) {
                    SpcData d = dataList.get(triggerIdx);
                    alerts.add(createAlert(d, version, RULE_3_SIX_TREND,
                            "连续6点" + (increasing ? "递增" : "递减") + "趋势"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }

    private void checkRule4(List<SpcData> dataList, double[] values,
                             BigDecimal cl, ParamVersion version,
                             Set<Integer> flagged, List<SpcAlert> alerts) {
        int n = values.length;
        if (n < 14) return;

        for (int start = 0; start <= n - 14; start++) {
            boolean alternating = true;
            for (int i = start; i < start + 13; i++) {
                double currentDiff = values[i + 1] - values[i];
                if (i > start) {
                    double prevDiff = values[i] - values[i - 1];
                    if (currentDiff * prevDiff >= 0) {
                        alternating = false;
                        break;
                    }
                }
            }
            if (alternating) {
                int triggerIdx = start + 13;
                if (!flagged.contains(triggerIdx)) {
                    SpcData d = dataList.get(triggerIdx);
                    alerts.add(createAlert(d, version, RULE_4_FOURTEEN_ALTERNATE,
                            "连续14点上下交替震荡"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }

    private void checkRule5(List<SpcData> dataList, double[] values,
                             BigDecimal cl, BigDecimal oneSigma, BigDecimal twoSigma,
                             ParamVersion version, Set<Integer> flagged, List<SpcAlert> alerts) {
        int n = values.length;
        if (n < 3) return;
        double centerLine = cl.doubleValue();
        double upper2sigma = centerLine + twoSigma.doubleValue();
        double lower2sigma = centerLine - twoSigma.doubleValue();

        for (int end = 3; end <= n; end++) {
            int countAbove2S = 0;
            int countBelow2S = 0;
            int lastAboveIdx = -1;
            int lastBelowIdx = -1;
            for (int i = end - 3; i < end; i++) {
                if (values[i] > upper2sigma) { countAbove2S++; lastAboveIdx = i; }
                if (values[i] < lower2sigma) { countBelow2S++; lastBelowIdx = i; }
            }
            if (countAbove2S >= 2 || countBelow2S >= 2) {
                int triggerIdx = (countAbove2S >= 2) ? lastAboveIdx : lastBelowIdx;
                if (triggerIdx >= 0 && !flagged.contains(triggerIdx)) {
                    SpcData d = dataList.get(triggerIdx);
                    alerts.add(createAlert(d, version, RULE_5_TWO_OF_THREE_2SIGMA,
                            "连续3点中有2点超出2σ范围"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }

    private void checkRule6(List<SpcData> dataList, double[] values,
                             BigDecimal cl, BigDecimal oneSigma,
                             ParamVersion version, Set<Integer> flagged, List<SpcAlert> alerts) {
        int n = values.length;
        if (n < 5) return;
        double centerLine = cl.doubleValue();
        double upper1sigma = centerLine + oneSigma.doubleValue();
        double lower1sigma = centerLine - oneSigma.doubleValue();

        for (int end = 5; end <= n; end++) {
            int countAbove1S = 0;
            int countBelow1S = 0;
            int lastAboveIdx = -1;
            int lastBelowIdx = -1;
            for (int i = end - 5; i < end; i++) {
                if (values[i] > upper1sigma) { countAbove1S++; lastAboveIdx = i; }
                if (values[i] < lower1sigma) { countBelow1S++; lastBelowIdx = i; }
            }
            if (countAbove1S >= 4 || countBelow1S >= 4) {
                int triggerIdx = (countAbove1S >= 4) ? lastAboveIdx : lastBelowIdx;
                if (triggerIdx >= 0 && !flagged.contains(triggerIdx)) {
                    SpcData d = dataList.get(triggerIdx);
                    alerts.add(createAlert(d, version, RULE_6_FOUR_OF_FIVE_1SIGMA,
                            "连续5点中有4点超出1σ范围"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }

    private void checkRule7(List<SpcData> dataList, double[] values,
                             BigDecimal cl, BigDecimal oneSigma,
                             ParamVersion version, Set<Integer> flagged, List<SpcAlert> alerts) {
        int n = values.length;
        if (n < 15) return;
        double centerLine = cl.doubleValue();
        double upper1sigma = centerLine + oneSigma.doubleValue();
        double lower1sigma = centerLine - oneSigma.doubleValue();

        for (int end = 15; end <= n; end++) {
            boolean allWithin = true;
            for (int i = end - 15; i < end; i++) {
                if (values[i] > upper1sigma || values[i] < lower1sigma) {
                    allWithin = false;
                    break;
                }
            }
            if (allWithin) {
                int triggerIdx = end - 1;
                if (!flagged.contains(triggerIdx)) {
                    SpcData d = dataList.get(triggerIdx);
                    alerts.add(createAlert(d, version, RULE_7_FIFTEEN_IN_1SIGMA,
                            "连续15点落在1σ范围内(可能存在分层)"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }

    private void checkRule8(List<SpcData> dataList, double[] values,
                             BigDecimal cl, BigDecimal oneSigma,
                             ParamVersion version, Set<Integer> flagged, List<SpcAlert> alerts) {
        int n = values.length;
        if (n < 8) return;
        double centerLine = cl.doubleValue();
        double upper1sigma = centerLine + oneSigma.doubleValue();
        double lower1sigma = centerLine - oneSigma.doubleValue();

        for (int end = 8; end <= n; end++) {
            boolean allOutside = true;
            for (int i = end - 8; i < end; i++) {
                if (values[i] >= lower1sigma && values[i] <= upper1sigma) {
                    allOutside = false;
                    break;
                }
            }
            if (allOutside) {
                int triggerIdx = end - 1;
                if (!flagged.contains(triggerIdx)) {
                    SpcData d = dataList.get(triggerIdx);
                    alerts.add(createAlert(d, version, RULE_8_EIGHT_OUTSIDE_1SIGMA,
                            "连续8点都在1σ范围之外(可能存在混合来源)"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }

    private SpcAlert createAlert(SpcData data, ParamVersion version, String ruleCode, String message) {
        SpcAlert alert = new SpcAlert();
        alert.setAlertCode("SPC-" + ruleCode + "-" + System.currentTimeMillis());
        alert.setParamVersionId(version.getId());
        alert.setBatchId(data.getBatchId());
        alert.setProcessId(data.getProcessId());
        alert.setParamId(data.getParamId());
        alert.setProductId(data.getProductId());
        alert.setDataId(data.getId());
        alert.setRuleNumber(ruleCode);
        alert.setRuleName(RULE_NAMES.getOrDefault(ruleCode, "未知规则"));
        alert.setMeasuredValue(data.getMeasuredValue());
        alert.setUcl(version.getUcl());
        alert.setLcl(version.getLcl());
        alert.setUsl(version.getUsl());
        alert.setLsl(version.getLsl());
        alert.setCl(version.getCl());
        alert.setDeviation(data.getDeviation());
        alert.setSigmaLevel(data.getSigmaLevel());
        alert.setMessage(message);
        alert.setDetailJson("{\"value\":" + data.getMeasuredValue()
                + ",\"collectTime\":\"" + data.getCollectTime() + "\"}");
        alert.setStatus("ACTIVE");
        alert.setAlertLevel(ruleCode.equals(RULE_1_BEYOND_3SIGMA) ? 3 :
                        ruleCode.equals(RULE_2_NINE_ONE_SIDE) || ruleCode.equals(RULE_3_SIX_TREND) ? 2 : 1);
        alert.setAlertType("RULE_VIOLATION");
        alert.setSeverity(alert.getAlertLevel() == 3 ? "CRITICAL" :
                        alert.getAlertLevel() == 2 ? "WARNING" : "INFO");
        alert.setAlertTime(LocalDateTime.now());
        return alert;
    }

    public void saveAndLogAlerts(List<SpcData> dataList, ParamVersion version) {
        try {
            List<SpcAlert> alerts = detectRules(dataList, version);
            if (!alerts.isEmpty()) {
                spcAlertService.saveBatch(alerts);
                log.info("[RuleEngine] 检测到 {} 条判异报警, paramVersionId={}, 规则分布: {}",
                        alerts.size(), version.getId(),
                        alerts.stream().map(a -> a.getRuleNumber()).toString());
            }
        } catch (Exception e) {
            log.error("[RuleEngine] 判异检测执行失败, paramVersionId={}", version.getId(), e);
        }
    }
}
