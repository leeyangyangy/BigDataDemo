package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import xyz.leeyangy.spc.entity.SysUserWorkshop;
import xyz.leeyangy.spc.mapper.SysUserWorkshopMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 用户-车间/测试站 关联 Service。
 *
 * 提供多车间绑定与测试站绑定的统一管理。
 * 测试站 (workshop_type='测试车间') 通过 bind_type='TEST_STATION' 区分。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SysUserWorkshopService extends ServiceImpl<SysUserWorkshopMapper, SysUserWorkshop> {

    /** 绑定类型常量 */
    public static final String BIND_WORKSHOP = "WORKSHOP";
    public static final String BIND_TEST_STATION = "TEST_STATION";

    /**
     * 查询用户绑定的车间列表 (WORKSHOP)。
     */
    public List<SysUserWorkshop> listWorkshops(Long userId) {
        return list(new LambdaQueryWrapper<SysUserWorkshop>()
                .eq(SysUserWorkshop::getUserId, userId)
                .eq(SysUserWorkshop::getBindType, BIND_WORKSHOP)
                .eq(SysUserWorkshop::getDeleted, 0));
    }

    /**
     * 查询用户绑定的测试站列表 (TEST_STATION)。
     */
    public List<SysUserWorkshop> listTestStations(Long userId) {
        return list(new LambdaQueryWrapper<SysUserWorkshop>()
                .eq(SysUserWorkshop::getUserId, userId)
                .eq(SysUserWorkshop::getBindType, BIND_TEST_STATION)
                .eq(SysUserWorkshop::getDeleted, 0));
    }

    /**
     * 重新绑定用户的车间 (全量替换)。
     * @param userId        用户ID
     * @param workshopIds   车间ID列表
     * @param primaryId     主车间ID (必须在 workshopIds 中, 可为 null)
     */
    @Transactional(rollbackFor = Exception.class)
    public void rebindWorkshops(Long userId, List<Long> workshopIds, Long primaryId) {
        // 物理删除旧绑定 (关联表无审计需求, 物理删除避免唯一索引冲突)
        baseMapper.physicalDeleteByUserAndType(userId, BIND_WORKSHOP);
        // 写入新绑定
        if (workshopIds != null) {
            List<SysUserWorkshop> toInsert = new ArrayList<>();
            for (Long wid : workshopIds) {
                SysUserWorkshop rel = new SysUserWorkshop();
                rel.setUserId(userId);
                rel.setWorkshopId(wid);
                rel.setBindType(BIND_WORKSHOP);
                rel.setIsPrimary(wid.equals(primaryId) ? 1 : 0);
                toInsert.add(rel);
            }
            if (!toInsert.isEmpty()) saveBatch(toInsert);
        }
        log.info("[UserWorkshop] 用户 {} 重新绑定车间: {} 个, 主车间={}", userId,
                workshopIds != null ? workshopIds.size() : 0, primaryId);
    }

    /**
     * 重新绑定用户的测试站 (全量替换)。
     */
    @Transactional(rollbackFor = Exception.class)
    public void rebindTestStations(Long userId, List<Long> stationIds) {
        baseMapper.physicalDeleteByUserAndType(userId, BIND_TEST_STATION);
        if (stationIds != null) {
            List<SysUserWorkshop> toInsert = new ArrayList<>();
            for (Long sid : stationIds) {
                SysUserWorkshop rel = new SysUserWorkshop();
                rel.setUserId(userId);
                rel.setWorkshopId(sid);
                rel.setBindType(BIND_TEST_STATION);
                rel.setIsPrimary(0);
                toInsert.add(rel);
            }
            if (!toInsert.isEmpty()) saveBatch(toInsert);
        }
        log.info("[UserWorkshop] 用户 {} 重新绑定测试站: {} 个", userId,
                stationIds != null ? stationIds.size() : 0);
    }

    /**
     * 查询用户主车间 (兼容旧 sys_user.workshop_id)。
     * 优先从关联表取 is_primary=1 的, 没有则取第一条。
     */
    public Long getPrimaryWorkshopId(Long userId) {
        List<SysUserWorkshop> list = listWorkshops(userId);
        if (list == null || list.isEmpty()) return null;
        return list.stream()
                .filter(r -> r.getIsPrimary() != null && r.getIsPrimary() == 1)
                .map(SysUserWorkshop::getWorkshopId)
                .findFirst()
                .orElse(list.get(0).getWorkshopId());
    }

    public List<Long> getWorkshopIds(Long userId) {
        return listWorkshops(userId).stream()
                .map(SysUserWorkshop::getWorkshopId)
                .collect(Collectors.toList());
    }

    public List<Long> getTestStationIds(Long userId) {
        return listTestStations(userId).stream()
                .map(SysUserWorkshop::getWorkshopId)
                .collect(Collectors.toList());
    }
}
