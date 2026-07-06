package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.UserWorkshopRebindDTO;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.entity.SysUserWorkshop;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.service.SysUserService;
import xyz.leeyangy.spc.service.SysUserWorkshopService;
import xyz.leeyangy.spc.service.WorkshopService;
import xyz.leeyangy.spc.vo.WorkshopVO;

import javax.validation.Valid;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 用户-车间绑定管理接口 (数据中心权限页用)
 *
 * 职责:
 *   - 列出用户及其车间绑定状态 (用于数据中心权限管理)
 *   - 给用户绑定/解绑车间 (生产车间 + 测试站)
 *   - 查询单个用户的绑定详情
 *
 * 注意:
 *   - 绑定车间是否进入数据中心由 spc_workshop.data_center_visible 控制
 *   - 用户可见车间 = (绑定车间) ∩ (data_center_visible=1)
 *   - 此接口只管理绑定关系, 不控制 data_center_visible
 */
@Slf4j
@RestController
@RequestMapping("/api/admin/user-workshop")
@RequiredArgsConstructor
public class SysUserWorkshopController {

    private final SysUserService sysUserService;
    private final SysUserWorkshopService sysUserWorkshopService;
    private final WorkshopService workshopService;

    /**
     * 列出所有用户及其车间绑定状态 (按创建时间降序)
     * 仅返回基础信息 + 绑定的车间列表, 不含密码等敏感字段
     */
    @GetMapping("/users")
    public R<List<Map<String, Object>>> listUsersWithBindings() {
        List<SysUser> users = sysUserService.list(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getDeleted, 0)
                .orderByDesc(SysUser::getCreatedAt));
        if (users.isEmpty()) return R.ok(new ArrayList<>());

        List<Map<String, Object>> result = new ArrayList<>();
        for (SysUser u : users) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", u.getId());
            m.put("empNo", u.getEmpNo());
            m.put("username", u.getUsername());
            m.put("role", u.getRole());
            m.put("status", u.getStatus());
            m.put("workshopIds", sysUserWorkshopService.getWorkshopIds(u.getId()));
            m.put("primaryWorkshopId", sysUserWorkshopService.getPrimaryWorkshopId(u.getId()));
            m.put("testStationIds", sysUserWorkshopService.getTestStationIds(u.getId()));
            result.add(m);
        }
        return R.ok(result);
    }

    /**
     * 查询单个用户的绑定详情 (含车间和测试站)
     */
    @GetMapping("/{userId}")
    public R<Map<String, Object>> getBindings(@PathVariable Long userId) {
        SysUser user = sysUserService.getById(userId);
        if (user == null) return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("userId", userId);
        m.put("empNo", user.getEmpNo());
        m.put("username", user.getUsername());
        m.put("role", user.getRole());
        m.put("workshopIds", sysUserWorkshopService.getWorkshopIds(userId));
        m.put("primaryWorkshopId", sysUserWorkshopService.getPrimaryWorkshopId(userId));
        m.put("testStationIds", sysUserWorkshopService.getTestStationIds(userId));
        return R.ok(m);
    }

    /**
     * 重新绑定用户的车间 (全量替换)
     * 请求体: { workshopIds: [...], primaryWorkshopId: null, testStationIds: [...] }
     */
    @OperationLog(module = "USER_WORKSHOP", action = "REBIND",
            targetType = "SysUser", content = "'重新绑定车间 userId=' + #userId",
            targetId = "#userId")
    @PutMapping("/{userId}")
    public R<Map<String, Object>> rebind(
            @PathVariable Long userId,
            @Valid @RequestBody UserWorkshopRebindDTO body) {
        SysUser user = sysUserService.getById(userId);
        if (user == null) return R.fail(StatusCode.DATA_NOT_FOUND, "用户不存在");

        List<Long> workshopIds = body.getWorkshopIds() != null ? body.getWorkshopIds() : new ArrayList<>();
        List<Long> testStationIds = body.getTestStationIds() != null ? body.getTestStationIds() : new ArrayList<>();
        Long primaryId = body.getPrimaryWorkshopId();

        // 校验主车间必须在 workshopIds 中
        if (primaryId != null && !workshopIds.contains(primaryId)) {
            return R.fail("主车间必须在所选车间列表中，请重新指定");
        }

        sysUserWorkshopService.rebindWorkshops(userId, workshopIds, primaryId);
        sysUserWorkshopService.rebindTestStations(userId, testStationIds);

        // 同步主车间到 sys_user.workshop_id (兼容旧代码)
        sysUserService.update(new com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper<SysUser>()
                .eq(SysUser::getId, userId)
                .set(SysUser::getWorkshopId, primaryId));

        log.info("[Admin] 用户 {} 重新绑定车间: {} 个, 测试站: {} 个, 主车间={}",
                userId, workshopIds.size(), testStationIds.size(), primaryId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("workshopIds", workshopIds);
        result.put("primaryWorkshopId", primaryId);
        result.put("testStationIds", testStationIds);
        return R.ok("绑定成功", result);
    }

    /**
     * 列出所有可被绑定的车间 (供下拉选择)
     * 返回启用状态的车间, 含生产车间和测试车间
     */
    @GetMapping("/workshops")
    public R<List<WorkshopVO>> listAllWorkshops() {
        List<Workshop> list = workshopService.list(new LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getStatus, 1)
                .eq(Workshop::getDeleted, 0)
                .orderByAsc(Workshop::getSortOrder));
        return R.ok(list.stream().map(WorkshopVO::from).collect(Collectors.toList()));
    }
}
