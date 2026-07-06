package xyz.leeyangy.spc.common.util;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.common.exception.BusinessStateException;
import xyz.leeyangy.spc.service.SysUserWorkshopService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 车间访问权限工具。
 *
 * <p>基于 {@link SysUserWorkshopService} 的用户-车间/测试站绑定关系，
 * 解析当前用户可访问的车间 ID 集合，用于在 SPC 数据/图表查询中做车间可见性过滤，
 * 修复 IDOR（不安全直接对象引用）漏洞。
 *
 * <p>语义约定：
 * <ul>
 *   <li>{@code null} 返回值表示"不限制"（ADMIN 角色由调用方判断后传入）</li>
 *   <li>空集合表示"用户已绑定但无任何车间" → 查询应返回空结果</li>
 *   <li>非空集合表示"仅允许访问这些车间对应的数据"</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class WorkshopAccessHelper {

    private final SysUserWorkshopService sysUserWorkshopService;

    /**
     * 查询用户可访问的车间 ID 集合（生产车间 + 测试站绑定）。
     *
     * <p>不判断角色：ADMIN 是否跳过由调用方决定（ADMIN 传入 null 表示不限制）。
     *
     * @param userId 用户ID
     * @return 车间ID集合（永不为 null，无绑定时返回空集）
     */
    public Set<Long> getAccessibleWorkshopIds(Long userId) {
        if (userId == null) {
            return Collections.emptySet();
        }
        List<Long> workshopIds = sysUserWorkshopService.getWorkshopIds(userId);
        List<Long> stationIds = sysUserWorkshopService.getTestStationIds(userId);
        Set<Long> all = new HashSet<>();
        if (workshopIds != null) {
            all.addAll(workshopIds);
        }
        if (stationIds != null) {
            all.addAll(stationIds);
        }
        return all;
    }

    /**
     * 校验用户是否有权访问指定车间，无权访问时抛出 {@link BusinessStateException}。
     *
     * @param userId     用户ID
     * @param workshopId 待校验的车间ID
     * @throws BusinessStateException 当用户绑定车间列表中不含该 workshopId 时
     */
    public void checkWorkshopAccess(Long userId, Long workshopId) {
        if (workshopId == null) {
            return;
        }
        Set<Long> accessible = getAccessibleWorkshopIds(userId);
        if (!accessible.contains(workshopId)) {
            throw new BusinessStateException("无权访问该车间数据");
        }
    }
}
