package xyz.leeyangy.spc.dto;

import lombok.Data;

/**
 * 更新设备请求 DTO（字段全部可选）
 */
@Data
public class EquipmentUpdateDTO {

    /** 设备名称 */
    private String equipName;

    /** 设备类型 */
    private String equipType;

    /** 设备型号 */
    private String equipModel;

    /** 产线ID */
    private Long lineId;

    /** 工序ID */
    private Long processId;

    /** 是否清空工序绑定 */
    private Boolean clearProcessId;

    /** 安装位置 */
    private String location;

    /** 状态 */
    private String status;

    /** 备注 */
    private String remark;
}
