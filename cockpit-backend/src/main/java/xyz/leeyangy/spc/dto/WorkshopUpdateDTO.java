package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 更新车间请求 DTO（字段全部可选，仅更新传入的字段）
 *
 * <p>排除 id / deleted / createdBy / createdAt / updatedBy / updatedAt 等审计字段，
 * 且不允许修改 workshopCode (业务编码创建后不可变)。
 */
@Data
public class WorkshopUpdateDTO {

    /** 车间名称 */
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
