package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Equipment;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.entity.ProcessParam;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.EquipmentService;
import xyz.leeyangy.spc.service.ParamService;
import xyz.leeyangy.spc.service.ProcessParamService;
import xyz.leeyangy.spc.service.ProcessService;
import xyz.leeyangy.spc.service.SysUserService;

import java.util.List;

@RestController
@RequestMapping("/api/spc/process")
@RequiredArgsConstructor
public class ProcessController {

    private final ProcessService processService;
    private final EquipmentService equipmentService;
    private final ParamService paramService;
    private final ProcessParamService processParamService;
    private final SysUserService sysUserService;

    @GetMapping("/page")
    public R<Page<Process>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestAttribute(required = false) Long userId) {
        LambdaQueryWrapper<Process> wrapper = new LambdaQueryWrapper<Process>()
                .orderByAsc(Process::getSortOrder)
                .orderByDesc(Process::getCreatedAt);
        if (userId != null) {
            SysUser user = sysUserService.getById(userId);
            if (user != null && user.getWorkshopId() != null) {
                wrapper.eq(Process::getWorkshopId, user.getWorkshopId());
            }
        }
        return R.ok(processService.page(new Page<>(current, size), wrapper));
    }

    @GetMapping("/{id}/equipment")
    public R<List<Equipment>> listEquipment(@PathVariable Long id) {
        return R.ok(equipmentService.listByProcessId(id));
    }

    @GetMapping("/{id}/params")
    public R<List<ProcessParam>> getParams(@PathVariable Long id) {
        return R.ok(processParamService.getBindingsByProcessId(id));
    }

    @GetMapping("/{id}/param-ids")
    public R<List<Long>> getParamIds(@PathVariable Long id) {
        return R.ok(processParamService.getParamIdsByProcessId(id));
    }

    @PostMapping("/{id}/params/bind")
    public R<Void> bindParams(
            @PathVariable Long id,
            @RequestBody List<ProcessParamService.BindItem> items) {
        if (processService.getById(id) == null) {
            return R.fail("工序不存在");
        }
        processParamService.bindParams(id, items);
        return R.ok();
    }

    @PostMapping("/{id}/params/{paramId}/bind")
    public R<Void> bindParam(
            @PathVariable Long id,
            @PathVariable Long paramId) {
        if (processService.getById(id) == null) return R.fail("工序不存在");
        if (paramService.getById(paramId) == null) return R.fail("参数不存在");
        processParamService.bindParam(id, paramId);
        return R.ok();
    }

    @DeleteMapping("/{id}/params/{paramId}")
    public R<Void> unbindParam(
            @PathVariable Long id,
            @PathVariable Long paramId) {
        boolean ok = processParamService.unbindParam(id, paramId);
        return ok ? R.ok() : R.fail("解绑失败");
    }

    @PostMapping
    public R<Process> create(@RequestBody Process process) {
        processService.save(process);
        return R.ok(process);
    }

    @GetMapping("/{id}")
    public R<Process> getById(@PathVariable Long id) {
        return R.ok(processService.getById(id));
    }
}