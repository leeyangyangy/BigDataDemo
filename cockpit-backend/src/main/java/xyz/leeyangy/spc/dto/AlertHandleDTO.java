package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

/**
 * 报警处理请求DTO
 * 
 * <p>用于接收前端提交的SPC报警处理请求。
 * 包含处理结果和处理备注信息，用于记录对异常情况的处置。</p>
 * 
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>PUT /api/spc/alert/{id}/handle</li>
 *   <li>SPC报警管理页面的处理操作</li>
 * </ul>
 * 
 * <h3>验证规则：</h3>
 * <ul>
 *   <li>handleResult: 必填，不能为空字符串，最大长度100字符</li>
 *   <li>handleRemark: 可选，最大长度500字符（用于详细说明处理过程）</li>
 * </ul>
 * 
 * @author SPC System
 * @version 1.0.0
 * @since 2026-04-29
 */
@Data
public class AlertHandleDTO {

    /**
     * 处理结果
     * 
     * <p>必填字段，描述对报警的处理结论。
     * 例如："已调整设备参数"、"已更换原材料"、"误报"等。</p>
     */
    @NotBlank(message = "处理结果不能为空")
    @Size(max = 100, message = "处理结果长度不能超过100个字符")
    private String handleResult;

    /**
     * 处理备注
     * 
     * <p>可选字段，用于提供更详细的处理说明。
     * 可以包含原因分析、采取的措施、预防措施等信息。</p>
     */
    @Size(max = 500, message = "处理备注长度不能超过500个字符")
    private String handleRemark;
}
