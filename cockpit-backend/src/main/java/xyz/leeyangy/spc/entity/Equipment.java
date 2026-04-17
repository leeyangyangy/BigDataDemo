package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("spc_equipment")
public class Equipment extends BaseEntity {

    private String equipCode;
    private String equipName;
    private String equipModel;
    private String equipType;
    private Long lineId;

    @TableField("process_id")
    private Long processId;

    @TableField("location")
    private String location;

    private String status;

    @TableField("remark")
    private String remark;
}
