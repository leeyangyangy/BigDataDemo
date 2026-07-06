package xyz.leeyangy.spc.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.common.StatusMsg;
import xyz.leeyangy.spc.common.constants.RoleConstants;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.service.SysUserWorkshopService;
import xyz.leeyangy.spc.service.SysWorkshopComponentService;
import xyz.leeyangy.spc.service.WorkshopService;
import xyz.leeyangy.spc.service.YieldService;
import xyz.leeyangy.spc.vo.YieldDataVO;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 数据中心接口 (原良率监控, 改名仅 UI 文案, 路径保留 /api/yield 不变)
 *
 * 权限模型 (双层过滤):
 *   1. ADMIN: 可见所有 data_center_visible=1 的车间
 *   2. 其他用户: 可见 (绑定车间 ∩ data_center_visible=1) 的车间
 *   3. 可见车间为空时, 前端隐藏数据中心入口, 后端返回 403
 *
 * 数据中心动态渲染:
 *   用户选车间后调用 /api/yield/components 查询该车间的组件列表
 *   前端按 component_key 动态渲染对应组件
 *
 * SPC tab 保持独立, 不受数据中心改造影响。
 */
@Slf4j
@RestController
@RequestMapping("/api/yield")
@RequiredArgsConstructor
public class YieldController {

    private final YieldService yieldService;
    private final SysUserWorkshopService sysUserWorkshopService;
    private final SysWorkshopComponentService sysWorkshopComponentService;
    private final WorkshopService workshopService;

    /**
     * 获取良率数据（可按车间、日期范围筛选）
     * 非管理员只能查询自己绑定车间的数据
     */
    @GetMapping("/data")
    public R<YieldDataVO> getData(
            HttpServletRequest request,
            @RequestParam(required = false) String workshop,
            @RequestParam(required = false, name = "start_date") String startDate,
            @RequestParam(required = false, name = "end_date") String endDate) {
        Long userId = (Long) request.getAttribute("userId");
        String role = (String) request.getAttribute("role");
        // 非管理员必须绑定车间
        String scopedWorkshop = scopeWorkshop(userId, role, workshop);
        if (scopedWorkshop == null && !RoleConstants.ADMIN.equals(role)) {
            return R.fail(StatusCode.FORBIDDEN, StatusMsg.NO_WORKSHOP_ACCESS);
        }
        return R.ok(yieldService.getYieldData(scopedWorkshop, startDate, endDate));
    }

    /**
     * 搜索良率数据
     * 非管理员只能搜索自己绑定车间的数据
     */
    @GetMapping("/search")
    public R<YieldDataVO> search(
            HttpServletRequest request,
            @RequestParam(required = false) String workshop,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false, name = "start_date") String startDate,
            @RequestParam(required = false, name = "end_date") String endDate) {
        Long userId = (Long) request.getAttribute("userId");
        String role = (String) request.getAttribute("role");
        String scopedWorkshop = scopeWorkshop(userId, role, workshop);
        if (scopedWorkshop == null && !RoleConstants.ADMIN.equals(role)) {
            return R.fail(StatusCode.FORBIDDEN, StatusMsg.NO_WORKSHOP_ACCESS);
        }
        return R.ok(yieldService.searchYieldData(scopedWorkshop, keyword, startDate, endDate));
    }

    /**
     * 获取可用车间列表 (仅返回用户有权限的车间)
     * 非管理员只返回 (绑定车间 ∩ data_center_visible=1) 的车间
     */
    @GetMapping("/workshops")
    public R<List<String>> workshops(HttpServletRequest request) {
        Long userId = (Long) request.getAttribute("userId");
        String role = (String) request.getAttribute("role");
        if (RoleConstants.ADMIN.equals(role)) {
            return R.ok(getAllDataCenterWorkshopNames());
        }
        List<String> workshopNames = getBoundWorkshopNames(userId);
        if (workshopNames.isEmpty()) {
            return R.fail(StatusCode.FORBIDDEN, StatusMsg.NO_WORKSHOP_ACCESS);
        }
        return R.ok(workshopNames);
    }

    /**
     * 检查当前用户数据中心访问权限 (前端据此决定是否显示数据中心 tab)
     * ADMIN 始终 accessible=true; 其他用户需绑定至少一个 data_center_visible=1 的车间
     */
    @GetMapping("/access")
    public R<Map<String, Object>> access(HttpServletRequest request) {
        Long userId = (Long) request.getAttribute("userId");
        String role = (String) request.getAttribute("role");
        Map<String, Object> data = new HashMap<>();
        if (RoleConstants.ADMIN.equals(role)) {
            data.put("accessible", true);
            data.put("workshops", getAllDataCenterWorkshopNames());
            return R.ok(data);
        }
        // 非管理员: 必须有可见车间 (绑定车间 ∩ data_center_visible=1)
        List<String> workshops = getBoundWorkshopNames(userId);
        data.put("accessible", !workshops.isEmpty());
        data.put("workshops", workshops);
        return R.ok(data);
    }

    /**
     * 查询指定车间在数据中心需渲染的组件列表 (按 sort_order 升序)
     * 前端数据中心主页面选车间后调用此接口动态渲染
     *
     * @param workshop 车间名称 (用户可见范围内)
     * @return 组件 component_key 列表
     */
    @GetMapping("/components")
    public R<List<String>> components(
            HttpServletRequest request,
            @RequestParam String workshop) {
        Long userId = (Long) request.getAttribute("userId");
        String role = (String) request.getAttribute("role");
        // 权限校验: workshop 必须在用户可见范围内
        List<String> visible = RoleConstants.ADMIN.equals(role)
                ? getAllDataCenterWorkshopNames()
                : getBoundWorkshopNames(userId);
        if (workshop == null || workshop.isEmpty() || !visible.contains(workshop)) {
            return R.fail(StatusCode.FORBIDDEN, StatusMsg.WORKSHOP_ACCESS_DENIED);
        }
        // 查车间ID
        Workshop w = workshopService.getOne(new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getWorkshopName, workshop)
                .eq(Workshop::getDeleted, 0)
                .last("LIMIT 1"));
        if (w == null) return R.ok(new ArrayList<>());
        List<String> keys = sysWorkshopComponentService.getEnabledComponentKeys(w.getId());
        // 兜底: 车间未配置组件时返回默认 yield_dashboard
        if (keys.isEmpty()) keys.add("yield_dashboard");
        return R.ok(keys);
    }

    /**
     * 范围控制: 非管理员只能查询自己绑定车间的数据
     * @return 用户请求的 workshop 如果在可见范围内则原样返回, 否则返回第一个可见车间 (强制限定)
     */
    private String scopeWorkshop(Long userId, String role, String requestedWorkshop) {
        if (RoleConstants.ADMIN.equals(role)) return requestedWorkshop;
        List<String> bound = getBoundWorkshopNames(userId);
        if (bound.isEmpty()) return null;
        if (requestedWorkshop == null || requestedWorkshop.isEmpty()) return bound.get(0);
        if (bound.contains(requestedWorkshop)) return requestedWorkshop;
        // 请求了未绑定的车间, 强制降级到第一个绑定车间
        return bound.get(0);
    }

    /**
     * 获取用户绑定车间名称列表 (生产车间 + 测试站), 仅保留 data_center_visible=1 的车间
     * 双层过滤: (绑定车间) ∩ (data_center_visible=1)
     */
    private List<String> getBoundWorkshopNames(Long userId) {
        List<Long> workshopIds = sysUserWorkshopService.getWorkshopIds(userId);
        List<Long> stationIds = sysUserWorkshopService.getTestStationIds(userId);
        List<Long> allIds = new ArrayList<>();
        allIds.addAll(workshopIds);
        allIds.addAll(stationIds);
        if (allIds.isEmpty()) return new ArrayList<>();
        return workshopService.listByIds(allIds).stream()
                .filter(w -> w.getWorkshopName() != null)
                .filter(w -> w.getDataCenterVisible() != null && w.getDataCenterVisible() == 1)
                .map(Workshop::getWorkshopName)
                .collect(Collectors.toList());
    }

    /**
     * 获取所有标记为数据中心可见的车间名称 (ADMIN 用)
     */
    private List<String> getAllDataCenterWorkshopNames() {
        return workshopService.list(new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getDataCenterVisible, 1)
                .eq(Workshop::getDeleted, 0)
                .orderByAsc(Workshop::getSortOrder))
                .stream()
                .map(Workshop::getWorkshopName)
                .collect(Collectors.toList());
    }
}
