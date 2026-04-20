package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("spc_process_param")
public class ProcessParam {

    @TableId(type = IdType.AUTO)
    private Long id;
    private Long processId;
    private Long paramId;
    private Integer sortOrder;
}
