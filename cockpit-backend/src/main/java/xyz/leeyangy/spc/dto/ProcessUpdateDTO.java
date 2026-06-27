package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 更新工序请求 DTO（字段全部可选）
 */
@Data
public class ProcessUpdateDTO {

    /** 工序名称 */
    private String processName;

    /** 工序类型 */
    private String processType;

    /** 工序描述 */
    private String description;

    /** 车间ID */
    private Long workshopId;

    /** 状态 */
    private Integer status;

    /** 排序 */
    private Integer sortOrder;
}
