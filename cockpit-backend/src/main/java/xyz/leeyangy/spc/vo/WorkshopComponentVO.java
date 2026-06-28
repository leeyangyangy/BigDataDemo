package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 车间-数据组件 关联 VO。
 */
@Data
public class WorkshopComponentVO {

    private Long id;
    private Long workshopId;
    private String componentKey;
    private Integer sortOrder;
    private Integer enabled;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    public static WorkshopComponentVO from(xyz.leeyangy.spc.entity.SysWorkshopComponent entity) {
        if (entity == null) return null;
        WorkshopComponentVO vo = new WorkshopComponentVO();
        vo.setId(entity.getId());
        vo.setWorkshopId(entity.getWorkshopId());
        vo.setComponentKey(entity.getComponentKey());
        vo.setSortOrder(entity.getSortOrder());
        vo.setEnabled(entity.getEnabled());
        vo.setCreatedAt(entity.getCreatedAt());
        return vo;
    }
}
