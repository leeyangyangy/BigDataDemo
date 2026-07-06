package xyz.leeyangy.spc.dto;

import lombok.Data;

import java.util.List;

/**
 * 用户-车间重新绑定请求 DTO (全量替换)
 *
 * <p>替换原 {@code Map<String, Object>} 入参，提供类型安全的字段绑定与校验。
 *
 * <p>字段说明:
 * <ul>
 *   <li>workshopIds: 绑定的车间ID列表 (可为空列表表示解绑全部车间)</li>
 *   <li>primaryWorkshopId: 主车间ID (可为 null；非空时必须包含在 workshopIds 中)</li>
 *   <li>testStationIds: 绑定的测试站ID列表 (可为空列表表示解绑全部测试站)</li>
 * </ul>
 */
@Data
public class UserWorkshopRebindDTO {

    /** 车间ID列表 (全量替换，空列表表示解绑全部) */
    private List<Long> workshopIds;

    /** 主车间ID (可选，非空时必须属于 workshopIds) */
    private Long primaryWorkshopId;

    /** 测试站ID列表 (全量替换，空列表表示解绑全部) */
    private List<Long> testStationIds;
}
