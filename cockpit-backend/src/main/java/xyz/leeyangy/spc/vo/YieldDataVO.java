package xyz.leeyangy.spc.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import xyz.leeyangy.spc.entity.YieldRate;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 良率大屏数据响应
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class YieldDataVO implements Serializable {

    /**
     * 当前良率列表
     */
    private List<YieldRate> currentYieldRates;

    /**
     * 历史数据：key = "产品-片号-分等"，value = 时间序列
     */
    private Map<String, List<YieldHistoryPoint>> historicalData;

    /**
     * 历史趋势点
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class YieldHistoryPoint implements Serializable {
        private String timestamp;
        private Double yieldRate;
    }
}
