package xyz.leeyangy.spc.vo;

import lombok.Data;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * SPC 分析报告数据载体
 *
 * <p>对标 SPC 统计过程控制分析报告 PDF 的内容结构：
 * 基本信息 / 统计信息 / 控制限 / 过程能力 / 异常分析 / 图表截图。</p>
 */
@Data
public class SpcReportData {

    /** 报告生成时间 */
    private LocalDateTime generateTime;

    /** 基本信息区块 */
    private BasicInfo basicInfo;

    /** 统计信息区块 */
    private Statistics statistics;

    /** 控制限区块 */
    private ControlLimits controlLimits;

    /** 过程能力区块 */
    private ProcessCapability capability;

    /** 异常分析区块 */
    private AnomalyAnalysis anomaly;

    /** 图表截图(base64),由前端 ECharts 渲染后上送 */
    private Charts charts;

    /** 原始数据(用于附录或调试) */
    private List<SpcData> rawData;

    @Data
    public static class BasicInfo {
        /** 控制图类型名称 */
        private String chartTypeName;
        /** 子组容量 */
        private Integer subgroupSize;
        /** 子组数量 */
        private Integer subgroupCount;
        /** 样本总数 */
        private Integer sampleCount;
        /** 过程状态: 受控/失控 */
        private String processStatus;
        /** 测量系统评定 */
        private String measurementAssessment;
    }

    @Data
    public static class Statistics {
        /** 均值 μ */
        private BigDecimal mean;
        /** 标准差 σ(组内) */
        private BigDecimal stdDevWithin;
        /** 标准差 σ(整体) */
        private BigDecimal stdDevOverall;
        /** 最小值 */
        private BigDecimal min;
        /** 最大值 */
        private BigDecimal max;
        /** 中位数 */
        private BigDecimal median;
        /** 极差 */
        private BigDecimal range;
        /** 正态性检验 W 统计量 */
        private BigDecimal normalityW;
        /** 正态性检验 p 值 */
        private BigDecimal normalityPValue;
        /** 是否正态 */
        private Boolean isNormal;
    }

    @Data
    public static class ControlLimits {
        /** 上控制限 UCL */
        private BigDecimal ucl;
        /** 中心线 CL */
        private BigDecimal cl;
        /** 下控制限 LCL */
        private BigDecimal lcl;
        /** 上规格限 USL */
        private BigDecimal usl;
        /** 下规格限 LSL */
        private BigDecimal lsl;
        /** 目标值 Target */
        private BigDecimal target;
    }

    @Data
    public static class ProcessCapability {
        /** Cp */
        private BigDecimal cp;
        /** Cpk */
        private BigDecimal cpk;
        /** Cpm */
        private BigDecimal cpm;
        /** Pp */
        private BigDecimal pp;
        /** Ppk */
        private BigDecimal ppk;
        /** 合格率% */
        private BigDecimal passRate;
        /** 合格数 */
        private Integer passCount;
        /** 不合格数 */
        private Integer failCount;
        /** 综合评价 */
        private String evaluation;
    }

    @Data
    public static class AnomalyAnalysis {
        /** 是否检测到异常 */
        private boolean hasAnomaly;
        /** 异常点数量 */
        private int anomalyPointCount;
        /** 异常详情(规则代码 + 描述) */
        private List<SpcAlert> alerts;
        /** 总结性描述 */
        private String summary;
    }

    @Data
    public static class Charts {
        /** 单值移动极差控制图 base64(含 data:image/png;base64, 前缀) */
        private String controlChartImr;
        /** X-bar 控制图 base64 */
        private String controlChartXbar;
        /** R 控制图 base64 */
        private String controlChartR;
        /** 过程能力直方图 base64 */
        private String capabilityHistogram;
        /** 正态概率图 base64 */
        private String normalProbabilityPlot;
        /** 趋势图 base64 */
        private String trendChart;
    }

    /** 由 ParamVersion + SpcStatResult + alerts + 图表 构造完整报告数据 */
    public static SpcReportData build(ParamVersion version, SpcStatResult stat,
                                       List<SpcData> dataList, List<SpcAlert> alerts,
                                       Charts charts) {
        SpcReportData data = new SpcReportData();
        data.setGenerateTime(LocalDateTime.now());
        data.setRawData(dataList);

        // === 基本信息 ===
        BasicInfo basic = new BasicInfo();
        String chartType = version.getChartType() != null ? version.getChartType() : "I_MR";
        basic.setChartTypeName(chartTypeToName(chartType));
        int subgroupSize = version.getSubgroupSize() != null && version.getSubgroupSize() > 0
                ? version.getSubgroupSize() : 1;
        basic.setSubgroupSize(subgroupSize);
        basic.setSubgroupCount(subgroupSize > 0 ? dataList.size() / subgroupSize : 0);
        basic.setSampleCount(dataList.size());
        boolean hasAnomaly = alerts != null && !alerts.isEmpty();
        basic.setProcessStatus(hasAnomaly ? "失控" : "受控");
        basic.setMeasurementAssessment(hasAnomaly ? "过程失控,需排查" : "过程受控");
        data.setBasicInfo(basic);

        // === 统计信息 ===
        Statistics s = new Statistics();
        s.setMean(stat.getMeanValue());
        s.setStdDevWithin(stat.getStdDev());
        // stdDevOverall 不在 SpcStatResult 中,从原始数据重算
        s.setStdDevOverall(computeOverallSigma(dataList));
        s.setMin(dataList.stream().map(SpcData::getMeasuredValue).min(BigDecimal::compareTo).orElse(null));
        s.setMax(dataList.stream().map(SpcData::getMeasuredValue).max(BigDecimal::compareTo).orElse(null));
        s.setMedian(computeMedian(dataList));
        s.setRange(stat.getRangeValue());
        s.setNormalityW(stat.getNormalityW());
        s.setNormalityPValue(stat.getNormalityPValue());
        s.setIsNormal(stat.getIsNormal());
        data.setStatistics(s);

        // === 控制限 ===
        ControlLimits cl = new ControlLimits();
        cl.setUcl(stat.getCalcUcl());
        cl.setCl(stat.getCalcCl());
        cl.setLcl(stat.getCalcLcl());
        cl.setUsl(version.getUsl());
        cl.setLsl(version.getLsl());
        cl.setTarget(version.getTarget());
        data.setControlLimits(cl);

        // === 过程能力 ===
        ProcessCapability cap = new ProcessCapability();
        cap.setCp(stat.getCp());
        cap.setCpk(stat.getCpk());
        cap.setCpm(computeCpm(version, stat));
        cap.setPp(stat.getPp());
        cap.setPpk(stat.getPpk());
        cap.setPassRate(stat.getPassRate());
        cap.setPassCount(stat.getPassCount());
        cap.setFailCount(stat.getFailCount());
        cap.setEvaluation(evaluateCpk(stat.getCpk()));
        data.setCapability(cap);

        // === 异常分析 ===
        AnomalyAnalysis anomaly = new AnomalyAnalysis();
        anomaly.setHasAnomaly(hasAnomaly);
        anomaly.setAnomalyPointCount(alerts == null ? 0 : alerts.size());
        anomaly.setAlerts(alerts);
        anomaly.setSummary(hasAnomaly
                ? "检测到 " + alerts.size() + " 处异常,过程处于失控状态,需排查原因。"
                : "未检测到异常点或规则违规,过程处于良好的统计控制状态。");
        data.setAnomaly(anomaly);

        // === 图表 ===
        data.setCharts(charts);

        return data;
    }

    /** Cpm = (USL - LSL) / (6 × √(σ² + (μ - T)²)) */
    private static BigDecimal computeCpm(ParamVersion version, SpcStatResult stat) {
        if (version.getUsl() == null || version.getLsl() == null || version.getTarget() == null
                || stat.getMeanValue() == null || stat.getStdDev() == null) {
            return null;
        }
        BigDecimal sigma = stat.getStdDev();
        if (sigma.compareTo(BigDecimal.ZERO) <= 0) return null;
        BigDecimal mean = stat.getMeanValue();
        BigDecimal target = version.getTarget();
        BigDecimal variance = sigma.multiply(sigma);
        BigDecimal meanDevSq = mean.subtract(target).pow(2);
        BigDecimal denom = sqrt(variance.add(meanDevSq), 10).multiply(BigDecimal.valueOf(6));
        if (denom.compareTo(BigDecimal.ZERO) <= 0) return null;
        return version.getUsl().subtract(version.getLsl()).divide(denom, 4, BigDecimal.ROUND_HALF_UP);
    }

    /** 整体标准差 σ_overall(样本方差,除以 n-1) */
    private static BigDecimal computeOverallSigma(List<SpcData> dataList) {
        if (dataList.size() < 2) return BigDecimal.ZERO;
        BigDecimal sum = BigDecimal.ZERO, sumSq = BigDecimal.ZERO;
        for (SpcData d : dataList) {
            BigDecimal v = d.getMeasuredValue();
            sum = sum.add(v);
            sumSq = sumSq.add(v.multiply(v));
        }
        BigDecimal mean = sum.divide(BigDecimal.valueOf(dataList.size()), 10, BigDecimal.ROUND_HALF_UP);
        BigDecimal variance = sumSq.subtract(mean.multiply(sum))
                .divide(BigDecimal.valueOf(dataList.size() - 1), 10, BigDecimal.ROUND_HALF_UP);
        return variance.compareTo(BigDecimal.ZERO) > 0 ? sqrt(variance, 10) : BigDecimal.ZERO;
    }

    /** 中位数 */
    private static BigDecimal computeMedian(List<SpcData> dataList) {
        if (dataList.isEmpty()) return null;
        List<BigDecimal> sorted = new java.util.ArrayList<>();
        for (SpcData d : dataList) sorted.add(d.getMeasuredValue());
        sorted.sort(BigDecimal::compareTo);
        int n = sorted.size();
        return n % 2 == 1
                ? sorted.get(n / 2)
                : sorted.get(n / 2 - 1).add(sorted.get(n / 2))
                        .divide(BigDecimal.valueOf(2), 4, BigDecimal.ROUND_HALF_UP);
    }

    /** Cpk 评价标准: ≥1.67优秀, 1.33-1.67良好, 1.00-1.33合格, <1.00不足 */
    private static String evaluateCpk(BigDecimal cpk) {
        if (cpk == null) return "无法评价";
        double v = cpk.doubleValue();
        if (v >= 1.67) return "优秀";
        if (v >= 1.33) return "良好";
        if (v >= 1.00) return "合格";
        return "不足";
    }

    private static String chartTypeToName(String chartType) {
        if (chartType == null) return "单值移动极差图";
        switch (chartType.replace("-", "_").replace(" ", "").toUpperCase()) {
            case "I_MR":
            case "IMR":
                return "单值移动极差图";
            case "XBAR_R":
            case "XBARR":
                return "X-bar & R 图";
            case "XBAR_S":
            case "XBARS":
                return "X-bar & S 图";
            case "P":
                return "P 图(不合格率)";
            default:
                return chartType;
        }
    }

    private static BigDecimal sqrt(BigDecimal value, int scale) {
        BigDecimal x0 = BigDecimal.ZERO;
        BigDecimal x1 = BigDecimal.valueOf(Math.sqrt(value.doubleValue()));
        while (!x0.equals(x1)) {
            x0 = x1;
            x1 = value.divide(x0, scale, BigDecimal.ROUND_HALF_UP)
                    .add(x0).divide(BigDecimal.valueOf(2), scale, BigDecimal.ROUND_HALF_UP);
        }
        return x1;
    }
}
