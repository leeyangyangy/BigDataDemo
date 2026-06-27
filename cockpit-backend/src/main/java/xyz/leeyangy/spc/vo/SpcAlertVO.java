package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class SpcAlertVO {

    private Long id;
    private String alertCode;
    private Long paramVersionId;
    private String batchId;
    private Long processId;
    private Long paramId;
    private Long productId;
    private Long dataId;
    private Integer alertLevel;
    private String alertType;
    private String ruleName;
    private String ruleNumber;
    private BigDecimal measuredValue;
    private BigDecimal ucl;
    private BigDecimal lcl;
    private BigDecimal usl;
    private BigDecimal lsl;
    private BigDecimal cl;
    private BigDecimal deviation;
    private BigDecimal sigmaLevel;
    private String severity;
    private String message;
    private String detailJson;
    private String status;
    private Long acknowledgedBy;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime acknowledgedAt;

    private Long confirmedBy;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime confirmedAt;

    private String handleResult;
    private String handleRemark;
    private Long handledBy;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime handledAt;

    private String resolveRemark;
    private String sourceChannel;
    private String mqMessageId;
    private Long wsPushedAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime alertTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static SpcAlertVO from(SpcAlert entity) {
        if (entity == null) {
            return null;
        }
        SpcAlertVO vo = new SpcAlertVO();
        vo.setId(entity.getId());
        vo.setAlertCode(entity.getAlertCode());
        vo.setParamVersionId(entity.getParamVersionId());
        vo.setBatchId(entity.getBatchId());
        vo.setProcessId(entity.getProcessId());
        vo.setParamId(entity.getParamId());
        vo.setProductId(entity.getProductId());
        vo.setDataId(entity.getDataId());
        vo.setAlertLevel(entity.getAlertLevel());
        vo.setAlertType(entity.getAlertType());
        vo.setRuleName(entity.getRuleName());
        vo.setRuleNumber(entity.getRuleNumber());
        vo.setMeasuredValue(entity.getMeasuredValue());
        vo.setUcl(entity.getUcl());
        vo.setLcl(entity.getLcl());
        vo.setUsl(entity.getUsl());
        vo.setLsl(entity.getLsl());
        vo.setCl(entity.getCl());
        vo.setDeviation(entity.getDeviation());
        vo.setSigmaLevel(entity.getSigmaLevel());
        vo.setSeverity(entity.getSeverity());
        vo.setMessage(entity.getMessage());
        vo.setDetailJson(entity.getDetailJson());
        vo.setStatus(entity.getStatus());
        vo.setAcknowledgedBy(entity.getAcknowledgedBy());
        vo.setAcknowledgedAt(entity.getAcknowledgedAt());
        vo.setConfirmedBy(entity.getConfirmedBy());
        vo.setConfirmedAt(entity.getConfirmedAt());
        vo.setHandleResult(entity.getHandleResult());
        vo.setHandleRemark(entity.getHandleRemark());
        vo.setHandledBy(entity.getHandledBy());
        vo.setHandledAt(entity.getHandledAt());
        vo.setResolveRemark(entity.getResolveRemark());
        vo.setSourceChannel(entity.getSourceChannel());
        vo.setMqMessageId(entity.getMqMessageId());
        vo.setWsPushedAt(entity.getWsPushedAt());
        vo.setAlertTime(entity.getAlertTime());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
