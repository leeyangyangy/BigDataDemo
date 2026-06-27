package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;

/**
 * 创建设备请求 DTO
 */
@Data
public class EquipmentCreateDTO {

    /** 设备编码 */
    @NotBlank(message = "设备编码不能为空")
    private String equipCode;

    /** 设备名称 */
    @NotBlank(message = "设备名称不能为空")
    private String equipName;

    /** 设备类型 */
    private String equipType;

    /** 设备型号 */
    private String equipModel;

    /** 产线ID */
    private Long lineId;

    /** 工序ID */
    private Long processId;

    /** 安装位置 */
    private String location;

    /** 状态 */
    private String status;

    /** 备注 */
    private String remark;
}
