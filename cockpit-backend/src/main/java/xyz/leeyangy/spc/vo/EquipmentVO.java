package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.Equipment;

import java.time.LocalDateTime;

@Data
public class EquipmentVO {

    private Long id;
    private String equipCode;
    private String equipName;
    private String equipModel;
    private String equipType;
    private Long lineId;
    private Long processId;
    private String location;
    private String status;
    private String remark;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static EquipmentVO from(Equipment entity) {
        if (entity == null) {
            return null;
        }
        EquipmentVO vo = new EquipmentVO();
        vo.setId(entity.getId());
        vo.setEquipCode(entity.getEquipCode());
        vo.setEquipName(entity.getEquipName());
        vo.setEquipModel(entity.getEquipModel());
        vo.setEquipType(entity.getEquipType());
        vo.setLineId(entity.getLineId());
        vo.setProcessId(entity.getProcessId());
        vo.setLocation(entity.getLocation());
        vo.setStatus(entity.getStatus());
        vo.setRemark(entity.getRemark());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
