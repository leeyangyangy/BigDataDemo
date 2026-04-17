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
@TableName("spc_param_version")
public class ParamVersion extends BaseEntity {

    private Long paramId;
    private Long productId;
    private Integer versionNo;
    private BigDecimal usl;
    private BigDecimal lsl;
    private BigDecimal target;
    private BigDecimal ucl;
    private BigDecimal lcl;
    private BigDecimal cl;
    private BigDecimal sigmaWidth;
    private Integer subgroupSize;
    private String chartType;
    private String calcMethod;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime effectiveFrom;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime effectiveTo;

    private Integer isCurrent;
    private String changeReason;
    private String changeType;
    private Long prevVersionId;
    private Integer status;
}
