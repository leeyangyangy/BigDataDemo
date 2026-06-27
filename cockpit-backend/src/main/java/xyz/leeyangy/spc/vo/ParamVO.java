package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.Param;

import java.time.LocalDateTime;

@Data
public class ParamVO {

    private Long id;
    private String paramCode;
    private String paramName;
    private String paramType;
    private String unit;
    private String dataType;
    private Integer decimalPlaces;
    private Long processId;
    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static ParamVO from(Param entity) {
        if (entity == null) {
            return null;
        }
        ParamVO vo = new ParamVO();
        vo.setId(entity.getId());
        vo.setParamCode(entity.getParamCode());
        vo.setParamName(entity.getParamName());
        vo.setParamType(entity.getParamType());
        vo.setUnit(entity.getUnit());
        vo.setDataType(entity.getDataType());
        vo.setDecimalPlaces(entity.getDecimalPlaces());
        vo.setProcessId(entity.getProcessId());
        vo.setStatus(entity.getStatus());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
