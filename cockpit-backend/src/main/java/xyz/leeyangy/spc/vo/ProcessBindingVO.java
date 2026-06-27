package xyz.leeyangy.spc.vo;

import lombok.Data;

/**
 * 工序绑定信息视图对象
 * 
 * <p>用于向前端返回产品关联的工序绑定信息。
 * 隐藏了数据库内部字段（如id、createdAt等），只暴露必要的业务信息。</p>
 * 
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>GET /api/product/{productId}/processes 响应体</li>
 *   <li>产品详情页面展示关联工序列表</li>
 * </ul>
 * 
 * @author SPC System
 * @version 1.0.0
 * @since 2026-04-29
 */
@Data
public class ProcessBindingVO {

    /**
     * 工序ID
     * 
     * <p>关联工序的主键标识，用于前端进行后续操作（如查看工序详情、配置参数等）。</p>
     */
    private Long processId;

    /**
     * 排序顺序
     * 
     * <p>该工序在产品工艺流程中的显示顺序。
     * 数值越小越靠前，从0开始编号。</p>
     */
    private Integer sortOrder;
}
