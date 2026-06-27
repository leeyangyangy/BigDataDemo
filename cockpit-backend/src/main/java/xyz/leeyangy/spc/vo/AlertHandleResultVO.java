package xyz.leeyangy.spc.vo;

import lombok.Data;
import java.time.LocalDateTime;

/**
 * 报警处理结果视图对象
 * 
 * <p>用于向前端返回SPC报警处理操作的结果信息。
 * 包含处理状态确认和时间戳，用于前端更新UI状态。</p>
 * 
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>PUT /api/spc/alert/{id}/handle 响应体</li>
 *   <li>报警处理成功后的结果反馈</li>
 * </ul>
 * 
 * @author SPC System
 * @version 1.0.0
 * @since 2026-04-29
 */
@Data
public class AlertHandleResultVO {

    /**
     * 报警记录ID
     * 
     * <p>被处理的报警记录主键，用于前端定位和关联。</p>
     */
    private Long alertId;

    /**
     * 是否已处理成功
     * 
     * <p>标识处理操作是否执行成功。
     * true表示处理完成，false表示处理失败（通常不会出现）。</p>
     */
    private Boolean handled;

    /**
     * 处理时间
     * 
     * <p>处理操作的服务器时间戳，格式为 yyyy-MM-dd HH:mm:ss。
     * 用于界面显示处理时间和审计追踪。</p>
     */
    private LocalDateTime handleTime;
}
