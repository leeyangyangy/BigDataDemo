package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

/**
 * 创建标准版本请求 DTO
 */
@Data
public class ParamVersionCreateDTO {

    /** 参数ID */
    @NotNull(message = "参数ID不能为空")
    private Long paramId;

    /** 产品ID（可选，未指定时表示通用版本） */
    private Long productId;

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

    /** 变更原因 */
    private String changeReason;

    /** 变更类型 */
    private String changeType;

    /** 子组大小 */
    private Integer subgroupSize;
}
