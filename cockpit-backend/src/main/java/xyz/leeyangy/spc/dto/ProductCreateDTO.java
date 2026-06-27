package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;

/**
 * 创建产品请求 DTO
 */
@Data
public class ProductCreateDTO {

    /** 产品编码 */
    @NotBlank(message = "产品编码不能为空")
    private String productCode;

    /** 产品名称 */
    @NotBlank(message = "产品名称不能为空")
    private String productName;

    /** 产品类型 */
    private String productType;

    /** 规格 */
    private String specification;

    /** 状态 */
    private Integer status;
}
