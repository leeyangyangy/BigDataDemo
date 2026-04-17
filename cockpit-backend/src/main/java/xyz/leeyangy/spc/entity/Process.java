package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("spc_process")
public class Process extends BaseEntity {

    private String processCode;
    private String processName;
    private String processType;
    private Long workshopId;
    private String description;
    private Integer status;
    private Integer sortOrder;
}
