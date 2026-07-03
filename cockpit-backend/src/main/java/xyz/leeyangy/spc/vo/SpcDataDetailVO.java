package xyz.leeyangy.spc.vo;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * SPC 数据详情 VO
 *
 * <p>在 SpcDataVO 基础上携带关联实体的编码/名称/单位等展示字段，
 * 由 SpcDataMapper 多表联查直接填充，避免前端二次查询。
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class SpcDataDetailVO extends SpcDataVO {

    /** 产品编码 */
    private String productCode;

    /** 产品名称 */
    private String productName;

    /** 工序编码 */
    private String processCode;

    /** 工序名称 */
    private String processName;

    /** 参数编码 */
    private String paramCode;

    /** 参数名称 */
    private String paramName;

    /** 参数单位 */
    private String paramUnit;

    /** 设备编码 */
    private String equipCode;

    /** 设备名称 */
    private String equipName;

    /** 参数版本号 */
    private Integer versionNo;

    /** 图表类型（来自参数版本） */
    private String chartType;

    /** 目标值（来自参数版本） */
    private java.math.BigDecimal target;

    /** 规格上限（来自参数版本） */
    private java.math.BigDecimal usl;

    /** 规格下限（来自参数版本） */
    private java.math.BigDecimal lsl;

    /** 控制上限（来自参数版本） */
    private java.math.BigDecimal ucl;

    /** 控制下限（来自参数版本） */
    private java.math.BigDecimal lcl;
}
