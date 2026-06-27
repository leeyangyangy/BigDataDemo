package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.SpcStatResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class SpcStatResultVO {

    private Long id;
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

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static SpcStatResultVO from(SpcStatResult entity) {
        if (entity == null) {
            return null;
        }
        SpcStatResultVO vo = new SpcStatResultVO();
        vo.setId(entity.getId());
        vo.setParamVersionId(entity.getParamVersionId());
        vo.setBatchId(entity.getBatchId());
        vo.setProductId(entity.getProductId());
        vo.setProcessId(entity.getProcessId());
        vo.setParamId(entity.getParamId());
        vo.setStatType(entity.getStatType());
        vo.setSampleCount(entity.getSampleCount());
        vo.setMeanValue(entity.getMeanValue());
        vo.setStdDev(entity.getStdDev());
        vo.setRangeValue(entity.getRangeValue());
        vo.setCp(entity.getCp());
        vo.setCpk(entity.getCpk());
        vo.setPp(entity.getPp());
        vo.setPpk(entity.getPpk());
        vo.setCalcUcl(entity.getCalcUcl());
        vo.setCalcLcl(entity.getCalcLcl());
        vo.setCalcCl(entity.getCalcCl());
        vo.setPassRate(entity.getPassRate());
        vo.setPassCount(entity.getPassCount());
        vo.setFailCount(entity.getFailCount());
        vo.setNormalityW(entity.getNormalityW());
        vo.setNormalityPValue(entity.getNormalityPValue());
        vo.setIsNormal(entity.getIsNormal());
        vo.setTriggerSource(entity.getTriggerSource());
        vo.setStatTime(entity.getStatTime());
        vo.setPeriodStart(entity.getPeriodStart());
        vo.setPeriodEnd(entity.getPeriodEnd());
        vo.setStatus(entity.getStatus());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
