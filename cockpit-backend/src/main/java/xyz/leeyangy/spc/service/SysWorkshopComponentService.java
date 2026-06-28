package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import xyz.leeyangy.spc.entity.SysWorkshopComponent;
import xyz.leeyangy.spc.mapper.SysWorkshopComponentMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 车间-数据组件 关联 Service。
 *
 * 用于数据中心动态渲染: 每个标记 data_center_visible=1 的车间
 * 可关联多个前端组件 (component_key), 用户进入数据中心选车间后
 * 按此表返回的组件列表动态渲染。
 *
 * SPC tab 保持独立, 不使用此表。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SysWorkshopComponentService extends ServiceImpl<SysWorkshopComponentMapper, SysWorkshopComponent> {

    /**
     * 查询车间已启用的组件列表 (按 sort_order 升序)。
     * 供数据中心主页面渲染使用。
     */
    public List<SysWorkshopComponent> listEnabledByWorkshop(Long workshopId) {
        if (workshopId == null) return Collections.emptyList();
        return list(new LambdaQueryWrapper<SysWorkshopComponent>()
                .eq(SysWorkshopComponent::getWorkshopId, workshopId)
                .eq(SysWorkshopComponent::getEnabled, 1)
                .eq(SysWorkshopComponent::getDeleted, 0)
                .orderByAsc(SysWorkshopComponent::getSortOrder));
    }

    /**
     * 查询车间所有关联组件 (含禁用, 供后台管理使用)。
     */
    public List<SysWorkshopComponent> listAllByWorkshop(Long workshopId) {
        if (workshopId == null) return Collections.emptyList();
        return list(new LambdaQueryWrapper<SysWorkshopComponent>()
                .eq(SysWorkshopComponent::getWorkshopId, workshopId)
                .eq(SysWorkshopComponent::getDeleted, 0)
                .orderByAsc(SysWorkshopComponent::getSortOrder));
    }

    /**
     * 重新设置车间的组件关联 (全量替换)。
     *
     * @param workshopId    车间ID
     * @param components    新的组件关联列表 (component_key + sort_order + enabled)
     */
    @Transactional(rollbackFor = Exception.class)
    public void rebindComponents(Long workshopId, List<SysWorkshopComponent> components) {
        // 物理删除旧关联 (关联表无审计需求, 物理删除避免唯一索引冲突)
        baseMapper.delete(new LambdaQueryWrapper<SysWorkshopComponent>()
                .eq(SysWorkshopComponent::getWorkshopId, workshopId));
        // 写入新关联
        if (components != null && !components.isEmpty()) {
            List<SysWorkshopComponent> toInsert = new ArrayList<>();
            for (SysWorkshopComponent c : components) {
                if (c.getComponentKey() == null || c.getComponentKey().trim().isEmpty()) continue;
                SysWorkshopComponent rel = new SysWorkshopComponent();
                rel.setWorkshopId(workshopId);
                rel.setComponentKey(c.getComponentKey().trim());
                rel.setSortOrder(c.getSortOrder() != null ? c.getSortOrder() : 0);
                rel.setEnabled(c.getEnabled() != null ? c.getEnabled() : 1);
                toInsert.add(rel);
            }
            if (!toInsert.isEmpty()) saveBatch(toInsert);
        }
        log.info("[WorkshopComponent] 车间 {} 重新关联组件: {} 个", workshopId,
                components != null ? components.size() : 0);
    }

    /**
     * 查询多个车间的组件关联 (一次批量查, 用于列表展示)。
     */
    public List<SysWorkshopComponent> listByWorkshopIds(List<Long> workshopIds) {
        if (workshopIds == null || workshopIds.isEmpty()) return Collections.emptyList();
        return list(new LambdaQueryWrapper<SysWorkshopComponent>()
                .in(SysWorkshopComponent::getWorkshopId, workshopIds)
                .eq(SysWorkshopComponent::getDeleted, 0)
                .orderByAsc(SysWorkshopComponent::getWorkshopId)
                .orderByAsc(SysWorkshopComponent::getSortOrder));
    }

    /**
     * 提取车间已启用的 component_key 列表 (按 sort_order 升序)。
     */
    public List<String> getEnabledComponentKeys(Long workshopId) {
        return listEnabledByWorkshop(workshopId).stream()
                .map(SysWorkshopComponent::getComponentKey)
                .collect(Collectors.toList());
    }
}
