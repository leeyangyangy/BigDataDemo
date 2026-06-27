package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 更新工艺参数请求 DTO（字段全部可选）
 */
@Data
public class ParamUpdateDTO {

    /** 工艺参数编码 */
    private String paramCode;

    /** 工艺参数名称 */
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
