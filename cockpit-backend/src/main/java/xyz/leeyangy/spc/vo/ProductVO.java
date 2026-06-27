package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.Product;

import java.time.LocalDateTime;

@Data
public class ProductVO {

    private Long id;
    private String productCode;
    private String productName;
    private String productType;
    private String specification;
    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static ProductVO from(Product entity) {
        if (entity == null) {
            return null;
        }
        ProductVO vo = new ProductVO();
        vo.setId(entity.getId());
        vo.setProductCode(entity.getProductCode());
        vo.setProductName(entity.getProductName());
        vo.setProductType(entity.getProductType());
        vo.setSpecification(entity.getSpecification());
        vo.setStatus(entity.getStatus());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
