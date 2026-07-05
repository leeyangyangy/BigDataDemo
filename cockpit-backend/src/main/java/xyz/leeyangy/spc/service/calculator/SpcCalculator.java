package xyz.leeyangy.spc.service.calculator;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.List;

/**
 * SPC 统计算法计算器
 *
 * <p>封装纯数学计算逻辑（均值、标准差、过程能力指数 Cp/Cpk/Pp/Ppk、
 * Shapiro-Wilk 正态性检验、控制限计算等），与 {@code SpcStatServiceImpl}
 * 的业务编排分离，遵循单一职责原则。</p>
 */
@Component
public class SpcCalculator {

    /**
     * 计算完整统计指标并填充到 SpcStatResult。
     *
     * <p>算法规范：
     * <ul>
     *   <li>整体标准差 σ_overall：样本方差（除以 n-1），用于 Pp/Ppk</li>
     *   <li>组内标准差 σ_within：I-MR 用 MR̄/d₂，Xbar-R 用 R̄/d₂，用于 Cp/Cpk 和控制限</li>
     *   <li>控制限：I-MR 用 mean ± k×σ_within，Xbar-R 用 grandMean ± A₂×R̄</li>
     *   <li>stdDev 字段存 σ_within（控制图标准差）</li>
     * </ul>
     *
     * @param dataList        样本数据（非空）
     * @param version         参数标准版本（含 USL/LSL/sigmaWidth/chartType/subgroupSize 等）
     * @param paramVersionId  参数版本ID
     * @param batchId         批次ID（可空）
     * @param triggerSource   触发来源
     */
    public SpcStatResult computeStatistics(List<SpcData> dataList, ParamVersion version,
                                           Long paramVersionId, String batchId, String triggerSource) {
        SpcStatResult result = new SpcStatResult();
        result.setParamVersionId(paramVersionId);
        result.setBatchId(batchId);
        result.setProductId(version.getProductId());
        result.setProcessId(dataList.get(0).getProcessId());
        result.setParamId(version.getParamId());
        result.setStatType(version.getChartType() != null ? version.getChartType() : "I_MR");
        result.setSampleCount(dataList.size());
        result.setStatTime(LocalDateTime.now());
        result.setTriggerSource(triggerSource);

        // 计数型图(P/NP/C/U)走独立算法分支，不适用计量值的均值/σ/Cpk 逻辑
        int n = dataList.size();
        if (isCountChart(version.getChartType())) {
            return computeCountStatistics(dataList, version, result, n);
        }

        BigDecimal sum = BigDecimal.ZERO;
        BigDecimal sumSq = BigDecimal.ZERO;
        BigDecimal minVal = null;
        BigDecimal maxVal = null;

        for (SpcData d : dataList) {
            BigDecimal v = d.getMeasuredValue();
            sum = sum.add(v);
            sumSq = sumSq.add(v.multiply(v));
            if (minVal == null || v.compareTo(minVal) < 0) minVal = v;
            if (maxVal == null || v.compareTo(maxVal) > 0) maxVal = v;
        }

        BigDecimal mean = sum.divide(BigDecimal.valueOf(n), 10, RoundingMode.HALF_UP);
        BigDecimal range = maxVal != null ? maxVal.subtract(minVal) : BigDecimal.ZERO;

        // 整体标准差 σ_overall（样本方差，除以 n-1）—— 用于 Pp/Ppk
        BigDecimal varianceOverall = n > 1
                ? sumSq.subtract(mean.multiply(sum)).divide(BigDecimal.valueOf(n - 1), 10, RoundingMode.HALF_UP)
                : BigDecimal.ZERO;
        BigDecimal stdDevOverall = varianceOverall.compareTo(BigDecimal.ZERO) > 0
                ? sqrt(varianceOverall, 10) : BigDecimal.ZERO;

        // 组内标准差 σ_within 和控制限 —— 用于 Cp/Cpk 和控制图
        int subgroupSize = version.getSubgroupSize() != null && version.getSubgroupSize() > 0
                ? version.getSubgroupSize() : 5;
        BigDecimal sigmaWidth = version.getSigmaWidth() != null ? version.getSigmaWidth() : BigDecimal.valueOf(3);
        BigDecimal stdDevWithin = computeWithinSigma(dataList, mean, version.getChartType(), subgroupSize);
        BigDecimal[] controlLimits = computeControlLimits(dataList, mean, stdDevWithin,
                version.getChartType(), subgroupSize, sigmaWidth);

        result.setMeanValue(mean);
        result.setStdDev(stdDevWithin);
        result.setRangeValue(range);
        result.setCalcCl(controlLimits[0]);
        result.setCalcUcl(controlLimits[1]);
        result.setCalcLcl(controlLimits[2]);

        BigDecimal usl = version.getUsl();
        BigDecimal lsl = version.getLsl();
        boolean hasUsl = usl != null;
        boolean hasLsl = lsl != null;

        // 规格限存在性决定能力指数计算方式：
        //   双边(USL&LSL): Cp=(USL-LSL)/6σ, Cpk=min(Cpu,Cpl), Pp/Ppk 同理
        //   单边上限(仅USL): Cpk=Cpu=(USL-μ)/3σ, Cp/Pp 不计算(置null, 符合AIAG SPC手册)
        //   单边下限(仅LSL): Cpk=Cpl=(μ-LSL)/3σ, Cp/Pp 不计算
        if (hasUsl || hasLsl) {
            // Cp/Cpk 用 σ_within（组内标准差）
            if (stdDevWithin.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal threeSigmaWithin = stdDevWithin.multiply(BigDecimal.valueOf(3));
                if (hasUsl && hasLsl) {
                    BigDecimal cpu = usl.subtract(mean).divide(threeSigmaWithin, 4, RoundingMode.HALF_UP);
                    BigDecimal cpl = mean.subtract(lsl).divide(threeSigmaWithin, 4, RoundingMode.HALF_UP);
                    result.setCp(usl.subtract(lsl).divide(stdDevWithin.multiply(BigDecimal.valueOf(6)), 4, RoundingMode.HALF_UP));
                    result.setCpk(cpu.min(cpl));
                } else if (hasUsl) {
                    result.setCpk(usl.subtract(mean).divide(threeSigmaWithin, 4, RoundingMode.HALF_UP));
                } else {
                    result.setCpk(mean.subtract(lsl).divide(threeSigmaWithin, 4, RoundingMode.HALF_UP));
                }
            }

            // Pp/Ppk 用 σ_overall（整体标准差）
            if (stdDevOverall.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal threeSigmaOverall = stdDevOverall.multiply(BigDecimal.valueOf(3));
                if (hasUsl && hasLsl) {
                    BigDecimal ppu = usl.subtract(mean).divide(threeSigmaOverall, 4, RoundingMode.HALF_UP);
                    BigDecimal ppl = mean.subtract(lsl).divide(threeSigmaOverall, 4, RoundingMode.HALF_UP);
                    result.setPp(usl.subtract(lsl).divide(stdDevOverall.multiply(BigDecimal.valueOf(6)), 4, RoundingMode.HALF_UP));
                    result.setPpk(ppu.min(ppl));
                } else if (hasUsl) {
                    result.setPpk(usl.subtract(mean).divide(threeSigmaOverall, 4, RoundingMode.HALF_UP));
                } else {
                    result.setPpk(mean.subtract(lsl).divide(threeSigmaOverall, 4, RoundingMode.HALF_UP));
                }
            }

            // 合格率按实际存在的规格限判定（单边时只判一侧）
            int passCnt = 0;
            for (SpcData d : dataList) {
                BigDecimal v = d.getMeasuredValue();
                boolean pass = true;
                if (hasUsl && v.compareTo(usl) > 0) pass = false;
                if (hasLsl && v.compareTo(lsl) < 0) pass = false;
                if (pass) passCnt++;
            }
            result.setPassCount(passCnt);
            result.setFailCount(n - passCnt);
            result.setPassRate(BigDecimal.valueOf(passCnt * 100)
                    .divide(BigDecimal.valueOf(n), 2, RoundingMode.HALF_UP));
        }

        if (n >= 3 && n <= 5000) {
            double[] sortedValues = dataList.stream()
                    .mapToDouble(d -> d.getMeasuredValue().doubleValue())
                    .sorted()
                    .toArray();
            double[] wResult = shapiroWilkTest(sortedValues);
            result.setNormalityW(BigDecimal.valueOf(wResult[0]).setScale(6, RoundingMode.HALF_UP));
            result.setNormalityPValue(BigDecimal.valueOf(wResult[1]).setScale(6, RoundingMode.HALF_UP));
            result.setIsNormal(wResult[1] >= 0.05);
        }

        return result;
    }

    /**
     * 计算组内标准差 σ_within
     *
     * <p>I-MR 图：σ = MR̄ / d₂（d₂=1.128，等效子组大小=2）
     * <br>Xbar-R 图：σ = R̄ / d₂（d₂ 根据 subgroupSize 查表）
     * <br>子组不足或系数不可用时回退到 I-MR 算法
     */
    private BigDecimal computeWithinSigma(List<SpcData> dataList, BigDecimal mean,
                                           String chartType, int subgroupSize) {
        if (isXbarChart(chartType) && ControlChartConstants.isValidN(subgroupSize)) {
            int n = dataList.size();
            int numSubgroups = n / subgroupSize;
            if (numSubgroups >= 2) {
                BigDecimal avgRange = computeAverageRange(dataList, subgroupSize, numSubgroups);
                if (avgRange.compareTo(BigDecimal.ZERO) > 0) {
                    return avgRange.divide(ControlChartConstants.d2(subgroupSize), 10, RoundingMode.HALF_UP);
                }
            }
        }
        // I-MR 算法或回退
        return computeImrSigma(dataList);
    }

    /**
     * 计算控制限 [CL, UCL, LCL]
     *
     * <p>I-MR 图：CL=mean, UCL/LCL = mean ± sigmaWidth × σ_within
     * <br>Xbar-R 图：CL=grandMean, UCL/LCL = grandMean ± A₂ × R̄
     */
    private BigDecimal[] computeControlLimits(List<SpcData> dataList, BigDecimal mean,
                                               BigDecimal stdDevWithin, String chartType,
                                               int subgroupSize, BigDecimal sigmaWidth) {
        if (isXbarChart(chartType) && ControlChartConstants.isValidN(subgroupSize)) {
            int n = dataList.size();
            int numSubgroups = n / subgroupSize;
            if (numSubgroups >= 2) {
                BigDecimal avgRange = computeAverageRange(dataList, subgroupSize, numSubgroups);
                BigDecimal grandMean = computeGrandMean(dataList, subgroupSize, numSubgroups, mean);
                BigDecimal A2 = ControlChartConstants.A2(subgroupSize);
                BigDecimal xbarUcl = grandMean.add(A2.multiply(avgRange));
                BigDecimal xbarLcl = grandMean.subtract(A2.multiply(avgRange));
                return new BigDecimal[]{grandMean, xbarUcl, xbarLcl};
            }
        }
        // I-MR 算法或回退
        return new BigDecimal[]{
                mean,
                mean.add(sigmaWidth.multiply(stdDevWithin)),
                mean.subtract(sigmaWidth.multiply(stdDevWithin))
        };
    }

    /** I-MR 组内标准差：MR̄ / d₂ */
    private BigDecimal computeImrSigma(List<SpcData> dataList) {
        int n = dataList.size();
        if (n < 2) return BigDecimal.ZERO;
        BigDecimal mrSum = BigDecimal.ZERO;
        for (int i = 1; i < n; i++) {
            BigDecimal mr = dataList.get(i).getMeasuredValue()
                    .subtract(dataList.get(i - 1).getMeasuredValue()).abs();
            mrSum = mrSum.add(mr);
        }
        BigDecimal mrBar = mrSum.divide(BigDecimal.valueOf(n - 1), 10, RoundingMode.HALF_UP);
        return mrBar.divide(ControlChartConstants.IMR_D2, 10, RoundingMode.HALF_UP);
    }

    /** Xbar-R 平均极差 R̄ */
    private BigDecimal computeAverageRange(List<SpcData> dataList, int subgroupSize, int numSubgroups) {
        BigDecimal rangeSum = BigDecimal.ZERO;
        for (int i = 0; i < numSubgroups; i++) {
            int start = i * subgroupSize;
            BigDecimal sgMin = null, sgMax = null;
            for (int j = start; j < start + subgroupSize && j < dataList.size(); j++) {
                BigDecimal v = dataList.get(j).getMeasuredValue();
                if (sgMin == null || v.compareTo(sgMin) < 0) sgMin = v;
                if (sgMax == null || v.compareTo(sgMax) > 0) sgMax = v;
            }
            if (sgMin != null && sgMax != null) {
                rangeSum = rangeSum.add(sgMax.subtract(sgMin));
            }
        }
        return rangeSum.divide(BigDecimal.valueOf(numSubgroups), 10, RoundingMode.HALF_UP);
    }

    /** Xbar-R 总均值 X̄̄ */
    private BigDecimal computeGrandMean(List<SpcData> dataList, int subgroupSize,
                                         int numSubgroups, BigDecimal fallbackMean) {
        BigDecimal grandMeanSum = BigDecimal.ZERO;
        int validGroups = 0;
        for (int i = 0; i < numSubgroups; i++) {
            int start = i * subgroupSize;
            BigDecimal sgSum = BigDecimal.ZERO;
            int count = 0;
            for (int j = start; j < start + subgroupSize && j < dataList.size(); j++) {
                sgSum = sgSum.add(dataList.get(j).getMeasuredValue());
                count++;
            }
            if (count > 0) {
                grandMeanSum = grandMeanSum.add(sgSum.divide(BigDecimal.valueOf(count), 10, RoundingMode.HALF_UP));
                validGroups++;
            }
        }
        return validGroups > 0
                ? grandMeanSum.divide(BigDecimal.valueOf(validGroups), 10, RoundingMode.HALF_UP)
                : fallbackMean;
    }

    /** 判断是否为 Xbar 系列控制图（Xbar-R 或 Xbar-S） */
    private boolean isXbarChart(String chartType) {
        if (chartType == null) return false;
        String upper = chartType.replace("-", "_").replace(" ", "").toUpperCase();
        return "XBAR_R".equals(upper) || "XBARR".equals(upper)
                || "XBAR_S".equals(upper) || "XBARS".equals(upper);
    }

    /** 判断是否为计数型图(P/NP/C/U) */
    private boolean isCountChart(String chartType) {
        if (chartType == null) return false;
        String upper = chartType.replace("-", "_").replace(" ", "").toUpperCase();
        return "P".equals(upper) || "NP".equals(upper) || "C".equals(upper) || "U".equals(upper);
    }

    /**
     * 计数型图(P/NP/C/U)统计计算
     *
     * <p>数据约定：measuredValue 存不合格数(P/NP)或缺陷数(C/U)，sampleSize 存样本量/检查单位数。
     * <br>P图: CL=p̄=Σd/Σn, σ=√(p̄(1-p̄)/n̄), 描点值= d_i/n_i
     * <br>NP图: CL=np̄=p̄×n̄, σ=√(np̄(1-p̄)), 描点值= d_i
     * <br>C图: CL=c̄=Σd/k, σ=√c̄, 描点值= d_i
     * <br>U图: CL=ū=Σd/Σn, σ=√(ū/n̄), 描点值= d_i/n_i
     * <p>计数型数据不计算 Cp/Cpk/Pp/Ppk(属性数据能力用 DPMO/σ水平)，不做正态性检验。</p>
     */
    private SpcStatResult computeCountStatistics(List<SpcData> dataList, ParamVersion version,
                                                   SpcStatResult result, int n) {
        String chartType = version.getChartType().replace("-", "_").replace(" ", "").toUpperCase();

        BigDecimal sumCount = BigDecimal.ZERO;
        BigDecimal sumSample = BigDecimal.ZERO;
        for (SpcData d : dataList) {
            sumCount = sumCount.add(d.getMeasuredValue());
            int sz = d.getSampleSize() != null ? d.getSampleSize() : 1;
            sumSample = sumSample.add(BigDecimal.valueOf(sz));
        }
        BigDecimal nBar = sumSample.divide(BigDecimal.valueOf(n), 10, RoundingMode.HALF_UP);
        if (nBar.compareTo(BigDecimal.ZERO) == 0) nBar = BigDecimal.ONE;
        // 防御 sumSample=0 导致 P/NP/U 图除零(数据入口应已校验 sampleSize>0，此处兜底)
        if (sumSample.compareTo(BigDecimal.ZERO) == 0) sumSample = BigDecimal.ONE;

        BigDecimal cl;
        BigDecimal sigma;

        switch (chartType) {
            case "P": {
                cl = sumCount.divide(sumSample, 10, RoundingMode.HALF_UP);
                double p = cl.doubleValue();
                double sig = p > 0 && p < 1 ? Math.sqrt(p * (1 - p) / nBar.doubleValue()) : 0;
                sigma = BigDecimal.valueOf(sig);
                break;
            }
            case "NP": {
                BigDecimal pBar = sumCount.divide(sumSample, 10, RoundingMode.HALF_UP);
                cl = pBar.multiply(nBar);
                // 与 P 图守卫一致: pBar 超出 (0,1) 时 σ=0，避免 Math.sqrt 负数产生 NaN
                double sig = (cl.doubleValue() > 0 && pBar.doubleValue() > 0 && pBar.doubleValue() < 1)
                        ? Math.sqrt(cl.doubleValue() * (1 - pBar.doubleValue())) : 0;
                sigma = BigDecimal.valueOf(sig);
                break;
            }
            case "C": {
                cl = sumCount.divide(BigDecimal.valueOf(n), 10, RoundingMode.HALF_UP);
                double sig = cl.doubleValue() > 0 ? Math.sqrt(cl.doubleValue()) : 0;
                sigma = BigDecimal.valueOf(sig);
                break;
            }
            case "U": {
                cl = sumCount.divide(sumSample, 10, RoundingMode.HALF_UP);
                double sig = cl.doubleValue() > 0 ? Math.sqrt(cl.doubleValue() / nBar.doubleValue()) : 0;
                sigma = BigDecimal.valueOf(sig);
                break;
            }
            default:
                return result;
        }

        BigDecimal sigmaWidth = version.getSigmaWidth() != null ? version.getSigmaWidth() : BigDecimal.valueOf(3);
        BigDecimal ucl = cl.add(sigmaWidth.multiply(sigma));
        BigDecimal lcl = cl.subtract(sigmaWidth.multiply(sigma));
        if (lcl.compareTo(BigDecimal.ZERO) < 0) lcl = BigDecimal.ZERO;

        // rangeValue: 描点值极差
        BigDecimal minPlot = null, maxPlot = null;
        for (SpcData d : dataList) {
            BigDecimal v = plottedCountValue(d, chartType);
            if (minPlot == null || v.compareTo(minPlot) < 0) minPlot = v;
            if (maxPlot == null || v.compareTo(maxPlot) > 0) maxPlot = v;
        }
        BigDecimal range = minPlot != null ? maxPlot.subtract(minPlot) : BigDecimal.ZERO;

        result.setMeanValue(cl);
        result.setStdDev(sigma);
        result.setRangeValue(range);
        result.setCalcCl(cl);
        result.setCalcUcl(ucl);
        result.setCalcLcl(lcl);

        // 合格率: 仅在版本设置规格限时按描点值判定
        BigDecimal usl = version.getUsl();
        BigDecimal lsl = version.getLsl();
        if (usl != null || lsl != null) {
            int passCnt = 0;
            for (SpcData d : dataList) {
                BigDecimal v = plottedCountValue(d, chartType);
                boolean pass = true;
                if (usl != null && v.compareTo(usl) > 0) pass = false;
                if (lsl != null && v.compareTo(lsl) < 0) pass = false;
                if (pass) passCnt++;
            }
            result.setPassCount(passCnt);
            result.setFailCount(n - passCnt);
            result.setPassRate(BigDecimal.valueOf(passCnt * 100)
                    .divide(BigDecimal.valueOf(n), 2, RoundingMode.HALF_UP));
        }
        // 计数型数据不做正态性检验、不计算 Cp/Cpk/Pp/Ppk
        return result;
    }

    /** 计数型图描点值：P/U 图为比率(d/n)，NP/C 图为原始计数(d) */
    private BigDecimal plottedCountValue(SpcData d, String chartType) {
        if ("P".equals(chartType) || "U".equals(chartType)) {
            // 防御 sampleSize<=0 导致除零(数据入口应已校验，此处兜底)
            int sz = (d.getSampleSize() != null && d.getSampleSize() > 0) ? d.getSampleSize() : 1;
            return d.getMeasuredValue().divide(BigDecimal.valueOf(sz), 10, RoundingMode.HALF_UP);
        }
        return d.getMeasuredValue();
    }

    private BigDecimal sqrt(BigDecimal value, int scale) {
        BigDecimal x0 = BigDecimal.ZERO;
        BigDecimal x1 = BigDecimal.valueOf(Math.sqrt(value.doubleValue()));
        while (!x0.equals(x1)) {
            x0 = x1;
            x1 = value.divide(x0, scale, RoundingMode.HALF_UP)
                    .add(x0).divide(BigDecimal.valueOf(2), scale, RoundingMode.HALF_UP);
        }
        return x1;
    }

    private double[] shapiroWilkTest(double[] x) {
        int n = x.length;
        if (n < 3) return new double[]{0, 0};

        double mean = 0;
        for (double v : x) mean += v;
        mean /= n;

        double s2 = 0;
        for (double v : x) s2 += (v - mean) * (v - mean);

        double[] a = shapiroWilkCoefficients(n);
        if (a == null) {
            double w = 1.0 / (1.0 + 0.75 / n + 2.25 / (n * n));
            return new double[]{w, 0.5};
        }

        double sumA = 0;
        for (int i = 0; i < n; i++) sumA += a[i] * a[i];

        double m = 0;
        for (int i = 0; i < n; i++) m += a[i] * x[i];
        m *= m;

        double w = m / s2;
        if (w > 1.0) w = 1.0;
        if (w < 0.0001) w = 0.0001;

        double pValue = shapiroWilkPValue(w, n);
        return new double[]{w, pValue};
    }

    private static final double[][] SHAPIRO_WILK_A_TABLE = {
            null,
            null,
            null,
            {0.7071067811865475},
            {0.6872188953997624, 0.1676924778912456},
            {0.6646434411056668, 0.241349118861299},
            {0.6450690432879613, 0.3034891148189654},
            {0.6278533828863957, 0.3544523649872666},
            {0.6122050387836837, 0.3962155126852013},
            {0.5979850009974934, 0.4309509561995179},
            {0.5847929421583794, 0.460090057770680},
            {0.5724067068469746, 0.484735552561603},
            {0.560669780015921, 0.505720338179587},
            {0.549475161811932, 0.523707088248224},
            {0.538726914255663, 0.539263423290254}
    };

    private double[] shapiroWilkCoefficients(int n) {
        if (n >= 3 && n <= 16 && SHAPIRO_WILK_A_TABLE[n] != null) {
            return SHAPIRO_WILK_A_TABLE[n].clone();
        }
        if (n > 5000 || n < 3) return null;

        double m[] = new double[n];
        double[] a = new double[n];
        for (int i = 1; i <= n; i++) {
            m[i - 1] = normalQuantile((i - 0.375) / (n + 0.25));
        }
        double sumM2 = 0;
        for (double v : m) sumM2 += v * v;
        double normFactor = Math.sqrt(sumM2);
        for (int i = 0; i < n; i++) {
            a[i] = m[i] / normFactor;
        }
        return a;
    }

    private double normalQuantile(double p) {
        if (p <= 0) return -10;
        if (p >= 1) return 10;
        if (p > 0.5) return -normalQuantile(1 - p);

        double a[] = {-3.969683028665376e+01, 2.209460984245205e+02,
                -2.759285104469687e+02, 1.383577518672690e+02,
                -3.066479806614716e+01, 2.506628277459239e+00};
        double b[] = {-5.447609879822406e+01, 1.615858368580409e+02,
                -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01};
        double c[] = {-7.784894002430293e-03, -3.223964580411365e-01,
                -2.400758277161838e+00, -2.549732539343734e+00,
                4.374664141464968e+00, 2.938163982698783e+00};
        double d[] = {7.784695709041462e-03, 3.224671290700398e-01,
                2.445134137142996e+00, 3.754408661907416e+00};

        double pLow = 0.02425, pHigh = 1 - pLow;
        double q, r;
        if (p < pLow) {
            q = Math.sqrt(-2 * Math.log(p));
            return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) /
                    ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);
        } else if (p <= pHigh) {
            q = p - 0.5;
            r = q * q;
            return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5]) * q /
                    (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1);
        } else {
            q = Math.sqrt(-2 * Math.log(1 - p));
            return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) /
                    ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);
        }
    }

    private double shapiroWilkPValue(double w, int n) {
        if (n < 3) return 1.0;
        double pi = 6.0 / (n + 1.0);
        double u = (1.0 - w) * Math.sqrt(n);
        double mu = -1.2725 + 1.0521 * (Math.log(pi)) - 0.266 * pi;
        double sigma = 1.0308 - 0.26758 * pi;
        double z = (u - mu) / sigma;
        return normalCdf(z);
    }

    private double normalCdf(double z) {
        return 0.5 * (1.0 + erf(z / Math.sqrt(2.0)));
    }

    private double erf(double z) {
        double t = 1.0 / (1.0 + 0.5 * Math.abs(z));
        double poly = t * Math.exp(-z * z - 1.26551223 +
                t * (1.00002368 + t * (0.37409196 + t * (0.09678418 +
                        t * (0.18628008 + t * (0.27886807 + t * (-1.13520398 +
                                t * (1.48851587 + t * (-0.82215223 +
                                        t * 0.17087277)))))))));
        return z >= 0 ? 1 - poly : poly - 1;
    }
}
