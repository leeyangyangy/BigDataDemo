package xyz.leeyangy.spc.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 良率数据记录（老化前/老化后计算结果）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class YieldRate implements Serializable {

    private String workshop;
    private String productName;
    private String productCode;
    private String binRank;
    private Integer beforeTotal;
    private Integer afterCount;
    private Double yieldRate;
}
