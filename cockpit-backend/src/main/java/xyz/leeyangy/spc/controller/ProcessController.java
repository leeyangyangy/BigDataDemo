package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Equipment;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.entity.SysUser;
import xyz.leeyangy.spc.service.EquipmentService;
import xyz.leeyangy.spc.service.ProcessService;
import xyz.leeyangy.spc.service.SysUserService;

import java.util.List;

@RestController
@RequestMapping("/api/spc/process")
@RequiredArgsConstructor
public class ProcessController {

    private final ProcessService processService;
    private final EquipmentService equipmentService;
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