package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.Workshop;

import java.time.LocalDateTime;

@Data
public class WorkshopVO {

    private Long id;
    private String workshopCode;
    private String workshopName;
    private String workshopType;
    /** 是否在数据中心可见 (0=否, 1=是) */
    private Integer dataCenterVisible;
    private String description;
    private Integer status;
    private Integer sortOrder;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static WorkshopVO from(Workshop entity) {
        if (entity == null) {
            return null;
        }
        WorkshopVO vo = new WorkshopVO();
        vo.setId(entity.getId());
        vo.setWorkshopCode(entity.getWorkshopCode());
        vo.setWorkshopName(entity.getWorkshopName());
        vo.setWorkshopType(entity.getWorkshopType());
        vo.setDataCenterVisible(entity.getDataCenterVisible());
        vo.setDescription(entity.getDescription());
        vo.setStatus(entity.getStatus());
        vo.setSortOrder(entity.getSortOrder());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
