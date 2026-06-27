package xyz.leeyangy.spc.dto;

import lombok.Data;
import java.math.BigDecimal;

/**
 * 更新标准版本请求 DTO（字段全部可选）
 */
@Data
public class ParamVersionUpdateDTO {

    /** 规格上限 */
    private BigDecimal usl;

    /** 规格下限 */
    private BigDecimal lsl;

    /** 目标值 */
    private BigDecimal target;

    /** 控制上限 */
    private BigDecimal ucl;

    /** 控制下限 */
    private BigDecimal lcl;

    /** 中心线 */
    private BigDecimal cl;

    /** 控制图类型 */
    private String chartType;

    /** 状态 */
    private Integer status;

    /** 子组大小 */
    private Integer subgroupSize;

    /** 变更原因 */
    private String changeReason;
}
