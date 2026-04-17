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
@TableName("spc_standard_change_log")
public class StandardChangeLog extends BaseEntity {

    private Long paramId;
    private Long productId;
    private Long oldVersionId;
    private Long newVersionId;
    private Integer oldVersionNo;
    private Integer newVersionNo;
    private String changeType;
    private String changeReason;
    private BigDecimal oldUsl;
    private BigDecimal newUsl;
    private BigDecimal oldLsl;
    private BigDecimal newLsl;
    private BigDecimal oldTarget;
    private BigDecimal newTarget;
    private BigDecimal oldUcl;
    private BigDecimal newUcl;
    private BigDecimal oldLcl;
    private BigDecimal newLcl;
    private Integer regenerateSpc;
    private String regenerateStatus;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime regenerateStartedAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime regenerateFinishedAt;

    private Long affectedDataCount;
    private Integer status;
}
