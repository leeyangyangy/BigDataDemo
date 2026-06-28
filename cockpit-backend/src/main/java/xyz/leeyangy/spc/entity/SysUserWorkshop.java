package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

import java.time.LocalDateTime;

/**
 * 用户-车间/测试站 关联表。
 *
 * bind_type:
 *   WORKSHOP     - 工作车间 (用户管理多个车间, is_primary=1 表示主车间)
 *   TEST_STATION - 测试站 (workshop_id 指向 spc_workshop 中 workshop_type='测试车间' 的记录)
 *
 * 设计说明: 测试站与车间都是 spc_workshop 表的记录, 通过 workshop_type 区分。
 * 用户可同时绑定多个车间和多个测试站, 同一记录唯一约束 (user_id, workshop_id, bind_type)。
 */
@Data
@EqualsAndHashCode(callSuper = true)
@TableName("sys_user_workshop")
public class SysUserWorkshop extends BaseEntity {

    /** 用户ID */
    private Long userId;

    /** 车间/测试站ID (spc_workshop.id) */
    private Long workshopId;

    /** 绑定类型: WORKSHOP / TEST_STATION */
    private String bindType;

    /** 是否主车间 (仅 WORKSHOP 类型有意义; 0=否, 1=是) */
    private Integer isPrimary;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;
}
