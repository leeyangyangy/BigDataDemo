package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;

/**
 * 创建工艺参数请求 DTO
 */
@Data
public class ParamCreateDTO {

    /** 工艺参数编码 */
    @NotBlank(message = "工艺参数编码不能为空")
    private String paramCode;

    /** 工艺参数名称 */
    @NotBlank(message = "工艺参数名称不能为空")
    private String paramName;

    /** 工序ID */
    private Long processId;

    /** 单位 */
    private String unit;

    /** 数据类型 */
    private String dataType;

    /** 状态 */
    private Integer status;
}
