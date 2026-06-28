package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.entity.SysWorkshopComponent;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.service.SysWorkshopComponentService;
import xyz.leeyangy.spc.service.WorkshopService;
import xyz.leeyangy.spc.vo.WorkshopComponentVO;
import xyz.leeyangy.spc.vo.WorkshopVO;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 数据中心后台管理接口
 *
 * 职责: 管理"车间-数据组件"关联关系
 *   - 列出所有标记为 data_center_visible=1 的车间
 *   - 查询/更新车间关联的组件
 *   - 返回前端注册表中的可用组件清单
 *
 * 注意:
 *   - 车间标记 (data_center_visible) 的开关在 AdminWorkshopController 完成
 *   - 用户-车间绑定管理在独立接口 (SysUserWorkshopController) 完成
 *   - SPC tab 保持独立, 不受此接口影响
 */
@Slf4j
@RestController
@RequestMapping("/api/admin/data-center")
@RequiredArgsConstructor
public class AdminDataCenterController {

    private final WorkshopService workshopService;
    private final SysWorkshopComponentService sysWorkshopComponentService;

    /**
     * 列出所有标记为数据中心可见的车间
     */
    @GetMapping("/workshops")
    public R<List<WorkshopVO>> listDataCenterWorkshops() {
        List<Workshop> list = workshopService.list(new LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getDataCenterVisible, 1)
                .eq(Workshop::getDeleted, 0)
                .orderByAsc(Workshop::getSortOrder));
        return R.ok(list.stream().map(WorkshopVO::from).collect(Collectors.toList()));
    }

    /**
     * 查询车间已关联的组件列表 (含禁用, 供后台编辑使用)
     */
    @GetMapping("/workshop/{workshopId}/components")
    public R<List<WorkshopComponentVO>> listComponents(@PathVariable Long workshopId) {
        List<SysWorkshopComponent> list = sysWorkshopComponentService.listAllByWorkshop(workshopId);
        return R.ok(list.stream().map(WorkshopComponentVO::from).collect(Collectors.toList()));
    }

    /**
     * 批量更新车间关联的组件 (全量替换)
     * 请求体: [{ componentKey, sortOrder, enabled }, ...]
     */
    @OperationLog(module = "DATA_CENTER", action = "UPDATE_COMPONENTS",
            targetType = "Workshop", content = "'更新车间组件关联 workshopId=' + #workshopId",
            targetId = "#workshopId")
    @PutMapping("/workshop/{workshopId}/components")
    public R<List<WorkshopComponentVO>> updateComponents(
            @PathVariable Long workshopId,
            @RequestBody List<SysWorkshopComponent> components) {
        Workshop w = workshopService.getById(workshopId);
        if (w == null || (w.getDataCenterVisible() == null || w.getDataCenterVisible() != 1)) {
            return R.fail("车间不存在或未标记为数据中心可见");
        }
        sysWorkshopComponentService.rebindComponents(workshopId, components);
        log.info("[Admin] 更新车间 {} 组件关联: {} 个", workshopId, components != null ? components.size() : 0);
        List<SysWorkshopComponent> list = sysWorkshopComponentService.listAllByWorkshop(workshopId);
        return R.ok(list.stream().map(WorkshopComponentVO::from).collect(Collectors.toList()));
    }

    /**
     * 返回前端注册表中的可用组件清单
     * 前端维护实际组件实现, 此处仅返回清单用于后台勾选展示
     *
     * 当前可用组件 (与前端 registry.js 对齐):
     *   - yield_dashboard: 良率监控 (综合良率趋势、产品码分布、对比、明细表、趋势图)
     */
    @GetMapping("/available-components")
    public R<List<Map<String, String>>> availableComponents() {
        List<Map<String, String>> list = new ArrayList<>();
        list.add(buildComponent("yield_dashboard", "良率监控",
                "综合良率趋势、产品码分布、对比、明细表、趋势图"));
        // 后续新增数据中心专用组件在此追加
        return R.ok(list);
    }

    private Map<String, String> buildComponent(String key, String name, String desc) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("key", key);
        m.put("name", name);
        m.put("description", desc);
        return m;
    }
}
