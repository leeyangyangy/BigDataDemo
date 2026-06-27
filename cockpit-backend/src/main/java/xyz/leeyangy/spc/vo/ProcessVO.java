package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.Process;

import java.time.LocalDateTime;

@Data
public class ProcessVO {

    private Long id;
    private String processCode;
    private String processName;
    private String processType;
    private Long workshopId;
    private String description;
    private Integer status;
    private Integer sortOrder;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static ProcessVO from(Process entity) {
        if (entity == null) {
            return null;
        }
        ProcessVO vo = new ProcessVO();
        vo.setId(entity.getId());
        vo.setProcessCode(entity.getProcessCode());
        vo.setProcessName(entity.getProcessName());
        vo.setProcessType(entity.getProcessType());
        vo.setWorkshopId(entity.getWorkshopId());
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
