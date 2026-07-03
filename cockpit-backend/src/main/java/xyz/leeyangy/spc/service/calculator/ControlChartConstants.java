package xyz.leeyangy.spc.service.calculator;

import java.math.BigDecimal;

/**
 * SPC 控制图系数表
 *
 * <p>提供 I-MR 和 Xbar-R 控制图的标准统计系数（基于子组大小 n）。
 * 系数来源：ASQC/ANSI 标准控制图系数表 (n=2..10)。</p>
 *
 * <table>
 *   <tr><th>n</th><th>A2</th><th>D3</th><th>D4</th><th>d2</th></tr>
 *   <tr><td>2</td><td>1.880</td><td>0</td><td>3.267</td><td>1.128</td></tr>
 *   <tr><td>3</td><td>1.023</td><td>0</td><td>2.574</td><td>1.693</td></tr>
 *   <tr><td>4</td><td>0.729</td><td>0</td><td>2.282</td><td>2.059</td></tr>
 *   <tr><td>5</td><td>0.577</td><td>0</td><td>2.114</td><td>2.326</td></tr>
 *   <tr><td>6</td><td>0.483</td><td>0</td><td>2.004</td><td>2.534</td></tr>
 *   <tr><td>7</td><td>0.419</td><td>0</td><td>1.924</td><td>2.704</td></tr>
 *   <tr><td>8</td><td>0.373</td><td>0</td><td>1.864</td><td>2.847</td></tr>
 *   <tr><td>9</td><td>0.337</td><td>0</td><td>1.816</td><td>2.970</td></tr>
 *   <tr><td>10</td><td>0.308</td><td>0</td><td>1.777</td><td>3.078</td></tr>
 * </table>
 */
public final class ControlChartConstants {

    private ControlChartConstants() {}

    /** I-MR 图的等效子组大小（移动极差 = 相邻两点之差，等效 n=2） */
    public static final int IMR_EQUIVALENT_N = 2;

    /** I-MR 图 d2 系数（n=2） */
    public static final BigDecimal IMR_D2 = BigDecimal.valueOf(1.128);

    /** I-MR 图 MR 上控制限系数 D4（n=2） */
    public static final BigDecimal IMR_D4 = BigDecimal.valueOf(3.267);

    // ---- 系数数组，下标 = subgroupSize ----
    private static final BigDecimal[] A2 = {
            null, null,
            bd("1.880"), bd("1.023"), bd("0.729"), bd("0.577"),
            bd("0.483"), bd("0.419"), bd("0.373"), bd("0.337"), bd("0.308")
    };

    private static final BigDecimal[] D3 = {
            null, null,
            bd("0"), bd("0"), bd("0"), bd("0"),
            bd("0"), bd("0.076"), bd("0.136"), bd("0.184"), bd("0.223")
    };

    private static final BigDecimal[] D4 = {
            null, null,
            bd("3.267"), bd("2.574"), bd("2.282"), bd("2.114"),
            bd("2.004"), bd("1.924"), bd("1.864"), bd("1.816"), bd("1.777")
    };

    private static final BigDecimal[] D2 = {
            null, null,
            bd("1.128"), bd("1.693"), bd("2.059"), bd("2.326"),
            bd("2.534"), bd("2.704"), bd("2.847"), bd("2.970"), bd("3.078")
    };

    private static final int MIN_N = 2;
    private static final int MAX_N = 10;

    /** Xbar-R 均值图系数 A2 */
    public static BigDecimal A2(int subgroupSize) {
        validateN(subgroupSize);
        return A2[subgroupSize];
    }

    /** R 图下控制限系数 D3 */
    public static BigDecimal D3(int subgroupSize) {
        validateN(subgroupSize);
        return D3[subgroupSize];
    }

    /** R 图上控制限系数 D4 */
    public static BigDecimal D4(int subgroupSize) {
        validateN(subgroupSize);
        return D4[subgroupSize];
    }

    /** 标准差估计系数 d2（σ = R̄ / d2） */
    public static BigDecimal d2(int subgroupSize) {
        validateN(subgroupSize);
        return D2[subgroupSize];
    }

    /** 判断子组大小是否在有效范围内 [2, 10] */
    public static boolean isValidN(int subgroupSize) {
        return subgroupSize >= MIN_N && subgroupSize <= MAX_N;
    }

    private static void validateN(int subgroupSize) {
        if (!isValidN(subgroupSize)) {
            throw new IllegalArgumentException(
                    "子组大小必须在 " + MIN_N + "~" + MAX_N + " 之间, 实际: " + subgroupSize);
        }
    }

    private static BigDecimal bd(String val) {
        return new BigDecimal(val);
    }
}
