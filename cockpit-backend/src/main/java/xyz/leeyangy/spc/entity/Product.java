package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("spc_product")
public class Product extends BaseEntity {

    private String productCode;
    private String productName;
    private String productType;
    private String specification;
    private Integer status;
}
