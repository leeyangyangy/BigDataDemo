package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

import java.time.LocalDateTime;

/**
 * 车间-数据组件 关联表。
 *
 * 用于数据中心动态渲染:
 *   - workshop_id 必须为 spc_workshop 中 data_center_visible=1 的车间
 *   - component_key 对应前端注册表中的组件标识 (如 yield_dashboard)
 *   - enabled=0 时该关联不生效, 数据中心不渲染该组件
 *   - sort_order 升序排列, 控制组件在数据中心页面内的显示顺序
 *
 * SPC tab 保持独立, 不受此表影响。
 */
@Data
@EqualsAndHashCode(callSuper = true)
@TableName("sys_workshop_component")
public class SysWorkshopComponent extends BaseEntity {

    /** 车间ID (spc_workshop.id, 须 data_center_visible=1) */
    private Long workshopId;

    /** 组件标识 (前端注册表 key, 如 yield_dashboard) */
    private String componentKey;

    /** 显示顺序 (升序, 默认 0) */
    private Integer sortOrder;

    /** 是否启用 (0=禁用, 1=启用) */
    private Integer enabled;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;
}
