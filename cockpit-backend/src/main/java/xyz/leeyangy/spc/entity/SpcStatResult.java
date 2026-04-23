package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("spc_stat_result")
public class SpcStatResult extends BaseEntity {

    private Long paramVersionId;
    private String batchId;
    private Long productId;
    private Long processId;
    private Long paramId;
    private String statType;
    private Integer sampleCount;
    private BigDecimal meanValue;
    private BigDecimal stdDev;
    private BigDecimal rangeValue;
    private BigDecimal cp;
    private BigDecimal cpk;
    private BigDecimal pp;
    private BigDecimal ppk;
    private BigDecimal calcUcl;
    private BigDecimal calcLcl;
    private BigDecimal calcCl;
    private BigDecimal passRate;
    private Integer passCount;
    private Integer failCount;
    private BigDecimal normalityW;
    private BigDecimal normalityPValue;
    private Boolean isNormal;
    private String triggerSource;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime statTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime periodStart;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime periodEnd;

    private Integer status;
}
