package xyz.leeyangy.spc.dto;

import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * 创建车间请求 DTO
 *
 * <p>排除 id / deleted / createdBy / createdAt / updatedBy / updatedAt 等审计字段。
 */
@Data
public class WorkshopCreateDTO {

    /** 车间编码 */
    @NotBlank(message = "车间编码不能为空")
    private String workshopCode;

    /** 车间名称 */
    @NotBlank(message = "车间名称不能为空")
    private String workshopName;

    /** 车间类型 */
    private String workshopType;

    /** 是否在数据中心可见 (0=否, 1=是) */
    private Integer dataCenterVisible;

    /** 描述 */
    private String description;

    /** 状态 */
    private Integer status;

    /** 排序 */
    private Integer sortOrder;
}
