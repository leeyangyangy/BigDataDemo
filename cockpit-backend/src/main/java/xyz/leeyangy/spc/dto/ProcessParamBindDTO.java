package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;

/**
 * 工序参数绑定请求DTO
 * 
 * <p>用于接收前端提交的工序与参数批量绑定请求。
 * 支持一次绑定多个参数到指定工序，用于配置SPC监控的参数列表。</p>
 * 
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>POST /api/process/{processId}/params/bind</li>
 *   <li>工序管理页面中的参数关联配置</li>
 * </ul>
 * 
 * <h3>验证规则：</h3>
 * <ul>
 *   <li>items: 不能为空，至少需要包含1个参数</li>
 *   <li>paramId: 每个参数的ID不能为空</li>
 * </ul>
 * 
 * @author SPC System
 * @version 1.0.0
 * @since 2026-04-29
 */
@Data
public class ProcessParamBindDTO {

    /**
     * 要绑定的参数列表
     * 
     * <p>列表中的每个元素代表一个要绑定的参数，
     * 绑定顺序由列表索引决定（也可通过sortOrder显式指定）。</p>
     */
    @NotNull(message = "绑定的参数列表不能为空")
    @Size(min = 1, message = "至少需要绑定一个参数")
    private List<@Valid ParamBindItem> items;

    /**
     * 参数绑定项
     * 
     * <p>表示单个参数的绑定信息，包含参数ID等必要字段。</p>
     */
    @Data
    public static class ParamBindItem {

        /**
         * 参数ID
         * 
         * <p>关联的参数主键，必须存在于spc_param表中。</p>
         */
        @NotNull(message = "参数ID不能为空")
        private Long paramId;
    }
}
