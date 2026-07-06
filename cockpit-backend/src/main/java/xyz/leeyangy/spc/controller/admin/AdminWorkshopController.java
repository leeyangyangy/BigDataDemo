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
import xyz.leeyangy.spc.dto.WorkshopCreateDTO;
import xyz.leeyangy.spc.dto.WorkshopUpdateDTO;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.service.WorkshopService;
import xyz.leeyangy.spc.vo.WorkshopVO;

import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/api/admin/workshop")
@RequiredArgsConstructor
public class AdminWorkshopController {

    private final WorkshopService workshopService;

    @GetMapping("/page")
    public R<Page<WorkshopVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword) {
        Page<Workshop> page = new Page<>(current, size);
        LambdaQueryWrapper<Workshop> wrapper = new LambdaQueryWrapper<Workshop>()
                .and(keyword != null && !keyword.isEmpty(), w -> w
                        .like(Workshop::getWorkshopCode, keyword)
                        .or().like(Workshop::getWorkshopName, keyword))
                .orderByDesc(Workshop::getCreatedAt);
        return R.ok(PageConvert.convert(workshopService.page(page, wrapper), WorkshopVO::from));
    }

    @GetMapping("/list")
    public R<java.util.List<WorkshopVO>> listAll(
            @RequestParam(required = false) String workshopType) {
        return R.ok(workshopService.list(new LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getStatus, 1)
                .eq(workshopType != null && !workshopType.isEmpty(), Workshop::getWorkshopType, workshopType)
                .orderByAsc(Workshop::getSortOrder))
                .stream()
                .map(WorkshopVO::from)
                .collect(java.util.stream.Collectors.toList()));
    }

    @GetMapping("/{id}")
    public R<WorkshopVO> getById(@PathVariable Long id) {
        return R.ok(WorkshopVO.from(workshopService.getById(id)));
    }

    @OperationLog(module = "WORKSHOP", action = "CREATE", targetType = "Workshop",
            content = "'创建车间: ' + #workshop.workshopCode + ' - ' + #workshop.workshopName",
            targetId = "#workshop.id")
    @PostMapping
    public R<WorkshopVO> create(@Valid @RequestBody WorkshopCreateDTO workshop) {
        long count = workshopService.count(new LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getWorkshopCode, workshop.getWorkshopCode().trim())
                .eq(Workshop::getDeleted, 0));
        if (count > 0) return R.fail("车间编码已存在");
        Workshop entity = new Workshop();
        entity.setWorkshopCode(workshop.getWorkshopCode());
        entity.setWorkshopName(workshop.getWorkshopName());
        entity.setWorkshopType(workshop.getWorkshopType());
        entity.setDataCenterVisible(workshop.getDataCenterVisible());
        entity.setDescription(workshop.getDescription());
        entity.setStatus(workshop.getStatus() != null ? workshop.getStatus() : 1);
        entity.setSortOrder(workshop.getSortOrder());
        workshopService.save(entity);
        log.info("[Admin] 创建车间: code={} name={}", entity.getWorkshopCode(), entity.getWorkshopName());
        return R.ok("创建成功", WorkshopVO.from(entity));
    }

    @OperationLog(module = "WORKSHOP", action = "UPDATE", targetType = "Workshop",
            content = "'更新车间: ' + #result.data.workshopCode + ' - ' + #result.data.workshopName",
            targetId = "#id")
    @PutMapping("/{id}")
    public R<WorkshopVO> update(@PathVariable Long id, @Valid @RequestBody WorkshopUpdateDTO dto) {
        Workshop exist = workshopService.getById(id);
        if (exist == null) return R.fail("车间不存在");
        if (dto.getWorkshopName() != null) exist.setWorkshopName(dto.getWorkshopName().trim());
        if (dto.getWorkshopType() != null) exist.setWorkshopType(dto.getWorkshopType());
        if (dto.getDataCenterVisible() != null) exist.setDataCenterVisible(dto.getDataCenterVisible());
        if (dto.getDescription() != null) exist.setDescription(dto.getDescription());
        if (dto.getStatus() != null) exist.setStatus(dto.getStatus());
        if (dto.getSortOrder() != null) exist.setSortOrder(dto.getSortOrder());
        workshopService.updateById(exist);
        log.info("[Admin] 更新车间: id={} code={}", id, exist.getWorkshopCode());
        return R.ok("更新成功", WorkshopVO.from(exist));
    }

    @OperationLog(module = "WORKSHOP", action = "DELETE", targetType = "Workshop",
            content = "'删除车间 id=' + #id",
            targetId = "#id")
    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        Workshop workshop = workshopService.getById(id);
        if (workshop == null) return R.fail("车间不存在");
        workshopService.update(new LambdaUpdateWrapper<Workshop>()
                .eq(Workshop::getId, id)
                .set(Workshop::getDeleted, 1));
        log.info("[Admin] 删除车间: id={} code={}", id, workshop.getWorkshopCode());
        return R.ok(null);
    }
}
