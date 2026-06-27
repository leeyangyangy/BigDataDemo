package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 更新产品请求 DTO（字段全部可选）
 */
@Data
public class ProductUpdateDTO {

    /** 产品名称 */
    private String productName;

    /** 产品类型 */
    private String productType;

    /** 规格 */
    private String specification;

    /** 状态 */
    private Integer status;
}
