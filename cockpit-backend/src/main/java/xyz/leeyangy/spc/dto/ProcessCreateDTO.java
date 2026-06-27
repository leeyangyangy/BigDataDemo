package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;

/**
 * 创建工序请求 DTO
 */
@Data
public class ProcessCreateDTO {

    /** 工序编码 */
    @NotBlank(message = "工序编码不能为空")
    private String processCode;

    /** 工序名称 */
    @NotBlank(message = "工序名称不能为空")
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
