package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("spc_param")
public class Param extends BaseEntity {

    private String paramCode;
    private String paramName;
    private String paramType;
    private String unit;
    private String dataType;
    private Integer decimalPlaces;
    private Long processId;
    private Integer status;
}
