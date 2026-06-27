package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.EquipmentCreateDTO;
import xyz.leeyangy.spc.dto.EquipmentUpdateDTO;
import xyz.leeyangy.spc.entity.Equipment;
import xyz.leeyangy.spc.service.EquipmentService;
import xyz.leeyangy.spc.vo.EquipmentVO;

import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/api/admin/equipment")
@RequiredArgsConstructor
public class AdminEquipmentController {

    private final EquipmentService equipmentService;

    @GetMapping("/page")
    public R<Page<EquipmentVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) Long processId) {
        Page<Equipment> page = new Page<>(current, size);
        LambdaQueryWrapper<Equipment> wrapper = new LambdaQueryWrapper<Equipment>()
                .and(keyword != null && !keyword.isEmpty(), w -> w
                        .like(Equipment::getEquipCode, keyword)
                        .or().like(Equipment::getEquipName, keyword)
                        .or().like(Equipment::getEquipModel, keyword))
                .eq(processId != null, Equipment::getProcessId, processId)
                .orderByDesc(Equipment::getCreatedAt);
        return R.ok(PageConvert.convert(equipmentService.page(page, wrapper), EquipmentVO::from));
    }

    @GetMapping("/{id}")
    public R<EquipmentVO> getById(@PathVariable Long id) {
        Equipment equipment = equipmentService.getById(id);
        if (equipment == null) return R.fail("设备不存在");
        return R.ok(EquipmentVO.from(equipment));
    }

    @OperationLog(module = "EQUIPMENT", action = "CREATE", targetType = "Equipment",
            content = "'创建设备: ' + #result.data.equipCode + ' - ' + #result.data.equipName",
            targetId = "#result.data.id")
    @PostMapping
    public R<EquipmentVO> create(@Valid @RequestBody EquipmentCreateDTO req) {
        if (req.getEquipCode() == null || req.getEquipCode().trim().isEmpty()) {
            return R.fail("设备编码不能为空");
        }
        if (req.getEquipName() == null || req.getEquipName().trim().isEmpty()) {
            return R.fail("设备名称不能为空");
        }

        long count = equipmentService.count(new LambdaQueryWrapper<Equipment>()
                .eq(Equipment::getEquipCode, req.getEquipCode().trim())
                .eq(Equipment::getDeleted, 0));
        if (count > 0) return R.fail("设备编码已存在");

        Equipment equipment = new Equipment();
        equipment.setEquipCode(req.getEquipCode().trim());
        equipment.setEquipName(req.getEquipName().trim());
        equipment.setEquipType(req.getEquipType());
        equipment.setEquipModel(req.getEquipModel());
        equipment.setLineId(req.getLineId());
        equipment.setProcessId(req.getProcessId());
        equipment.setLocation(req.getLocation());
        equipment.setStatus(req.getStatus() != null ? req.getStatus() : "正常");
        equipment.setRemark(req.getRemark());

        equipmentService.save(equipment);
        log.info("[Admin] 创建设备: code={} name={}", equipment.getEquipCode(), equipment.getEquipName());
        return R.ok("创建成功", EquipmentVO.from(equipment));
    }

    @OperationLog(module = "EQUIPMENT", action = "UPDATE", targetType = "Equipment",
            content = "'更新设备: ' + #result.data.equipCode + ' - ' + #result.data.equipName",
            targetId = "#id")
    @PutMapping("/{id}")
    public R<EquipmentVO> update(@PathVariable Long id, @Valid @RequestBody EquipmentUpdateDTO req) {
        Equipment exist = equipmentService.getById(id);
        if (exist == null) return R.fail("设备不存在");

        if (req.getEquipName() != null) exist.setEquipName(req.getEquipName().trim());
        if (req.getEquipType() != null) exist.setEquipType(req.getEquipType());
        if (req.getEquipModel() != null) exist.setEquipModel(req.getEquipModel());
        if (req.getLineId() != null) exist.setLineId(req.getLineId());
        if (req.getLocation() != null) exist.setLocation(req.getLocation());
        if (req.getStatus() != null) exist.setStatus(req.getStatus());
        if (req.getRemark() != null) exist.setRemark(req.getRemark());

        if (Boolean.TRUE.equals(req.getClearProcessId())) {
            equipmentService.update(new LambdaUpdateWrapper<Equipment>()
                    .eq(Equipment::getId, id)
                    .set(Equipment::getProcessId, null));
            log.info("[Admin] 解绑设备: id={} code={} processId=null", id, exist.getEquipCode());
        } else {
            if (req.getProcessId() != null) exist.setProcessId(req.getProcessId());
            equipmentService.updateById(exist);
            log.info("[Admin] 更新设备: id={} code={} processId={}", id, exist.getEquipCode(), exist.getProcessId());
        }

        return R.ok("更新成功", EquipmentVO.from(equipmentService.getById(id)));
    }

    @OperationLog(module = "EQUIPMENT", action = "DELETE", targetType = "Equipment",
            content = "'删除设备 id=' + #id",
            targetId = "#id")
    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        Equipment equipment = equipmentService.getById(id);
        if (equipment == null) return R.fail("设备不存在");
        equipmentService.update(new LambdaUpdateWrapper<Equipment>()
                .eq(Equipment::getId, id)
                .set(Equipment::getDeleted, 1));
        log.info("[Admin] 删除设备: id={} code={}", id, equipment.getEquipCode());
        return R.ok(null);
    }
}
