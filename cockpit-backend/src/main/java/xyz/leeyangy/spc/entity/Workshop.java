package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("spc_workshop")
public class Workshop extends BaseEntity {

    private String workshopCode;
    private String workshopName;
    private String workshopType;
    /** 是否在数据中心可见 (0=否, 1=是) */
    private Integer dataCenterVisible;
    private String description;
    private Integer status;
    private Integer sortOrder;
}
