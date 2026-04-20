package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("spc_product_process")
public class ProductProcess {

    @TableId(type = IdType.AUTO)
    private Long id;
    private Long productId;
    private Long processId;
    private Integer sortOrder;
}
