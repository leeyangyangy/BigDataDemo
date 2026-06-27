package xyz.leeyangy.spc.dto;

import lombok.Data;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;

/**
 * 产品工序绑定请求DTO
 * 
 * <p>用于接收前端提交的产品与工序批量绑定请求。
 * 支持一次绑定多个工序到指定产品，并指定排序顺序。</p>
 * 
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>POST /api/product/{productId}/processes/bind</li>
 *   <li>产品管理页面中的工序关联配置</li>
 * </ul>
 * 
 * <h3>验证规则：</h3>
 * <ul>
 *   <li>items: 不能为空，至少需要包含1个工序</li>
 *   <li>processId: 每个工序的ID不能为空</li>
 * </ul>
 * 
 * @author SPC System
 * @version 1.0.0
 * @since 2026-04-29
 */
@Data
public class ProductProcessBindDTO {

    /**
     * 要绑定的工序列表
     * 
     * <p>列表中的每个元素代表一个要绑定的工序，
     * 绑定顺序由列表索引决定（也可通过sortOrder显式指定）。</p>
     */
    @NotNull(message = "绑定的工序列表不能为空")
    @Size(min = 1, message = "至少需要绑定一个工序")
    private List<@Valid ProcessBindItem> items;

    /**
     * 工序绑定项
     * 
     * <p>表示单个工序的绑定信息，包含工序ID等必要字段。</p>
     */
    @Data
    public static class ProcessBindItem {

        /**
         * 工序ID
         * 
         * <p>关联的工序主键，必须存在于spc_process表中。</p>
         */
        @NotNull(message = "工序ID不能为空")
        private Long processId;
    }
}
