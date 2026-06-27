package xyz.leeyangy.spc.vo;

import lombok.Data;

/**
 * 参数简单信息视图对象
 * 
 * <p>用于向前端返回工序关联的参数基本信息。
 * 只包含参数的核心标识信息，用于下拉选择等场景。</p>
 * 
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>GET /api/process/{processId}/params 响应体</li>
 *   <li>工序详情页面展示参数列表（简化版）</li>
 *   <li>参数选择器数据源</li>
 * </ul>
 * 
 * @author SPC System
 * @version 1.0.0
 * @since 2026-04-29
 */
@Data
public class ParamSimpleVO {

    /**
     * 参数ID
     * 
     * <p>参数的主键标识，用于前端进行后续操作
     * （如查看参数详细配置、查看SPC控制图等）。</p>
     */
    private Long id;

    /**
     * 参数ID
     *
     * <p>参数的主键标识，用于前端进行后续操作
     * （如查看参数详细配置、查看SPC控制图等）。</p>
     */
    private Long paramId;

    /**
     * 参数编码
     * 
     * <p>参数的唯一编码，通常与工艺文件或标准规范中的编号一致。
     * 例如："TEMP-001"、"PRESSURE-A"等。</p>
     */
    private String paramCode;

    /**
     * 参数名称
     * 
     * <p>参数的中文名称，用于界面显示。
     * 例如："温度A区压力"、"产品直径"等。</p>
     */
    private String paramName;
}
