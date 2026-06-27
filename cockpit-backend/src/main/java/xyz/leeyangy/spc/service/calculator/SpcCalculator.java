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
     * @param dataList        样本数据（非空）
     * @param version         参数标准版本（含 USL/LSL/sigmaWidth 等）
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

        int n = dataList.size();
        BigDecimal mean = sum.divide(BigDecimal.valueOf(n), 10, RoundingMode.HALF_UP);
        BigDecimal variance = sumSq.divide(BigDecimal.valueOf(n), 10, RoundingMode.HALF_UP)
                .subtract(mean.multiply(mean));
        BigDecimal stdDev = variance.compareTo(BigDecimal.ZERO) > 0
                ? sqrt(variance, 10) : BigDecimal.ZERO;
        BigDecimal range = maxVal != null ? maxVal.subtract(minVal) : BigDecimal.ZERO;

        result.setMeanValue(mean);
        result.setStdDev(stdDev);
        result.setRangeValue(range);

        BigDecimal usl = version.getUsl();
        BigDecimal lsl = version.getLsl();

        if (usl != null && lsl != null && stdDev.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal specRange = usl.subtract(lsl);
            BigDecimal sixSigma = stdDev.multiply(BigDecimal.valueOf(6));

            result.setCp(specRange.divide(sixSigma, 4, RoundingMode.HALF_UP));

            BigDecimal cpu = usl.subtract(mean).divide(stdDev.multiply(BigDecimal.valueOf(3)), 4, RoundingMode.HALF_UP);
            BigDecimal cpl = mean.subtract(lsl).divide(stdDev.multiply(BigDecimal.valueOf(3)), 4, RoundingMode.HALF_UP);
            result.setCpk(cpu.min(cpl));

            BigDecimal ppVal = specRange.divide(sixSigma, 4, RoundingMode.HALF_UP);
            result.setPp(ppVal);

            BigDecimal ppu = usl.subtract(mean).divide(stdDev.multiply(BigDecimal.valueOf(3)), 4, RoundingMode.HALF_UP);
            BigDecimal ppl = mean.subtract(lsl).divide(stdDev.multiply(BigDecimal.valueOf(3)), 4, RoundingMode.HALF_UP);
            result.setPpk(ppu.min(ppl));

            int passCnt = 0;
            for (SpcData d : dataList) {
                BigDecimal v = d.getMeasuredValue();
                if (v.compareTo(lsl) >= 0 && v.compareTo(usl) <= 0) passCnt++;
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

        BigDecimal sigmaWidth = version.getSigmaWidth() != null ? version.getSigmaWidth() : BigDecimal.valueOf(3);
        result.setCalcCl(mean);
        result.setCalcUcl(mean.add(sigmaWidth.multiply(stdDev)));
        result.setCalcLcl(mean.subtract(sigmaWidth.multiply(stdDev)));

        return result;
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
