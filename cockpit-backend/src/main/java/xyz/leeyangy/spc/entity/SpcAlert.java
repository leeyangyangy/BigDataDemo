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
@TableName("spc_alert")
public class SpcAlert extends BaseEntity {

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
}
