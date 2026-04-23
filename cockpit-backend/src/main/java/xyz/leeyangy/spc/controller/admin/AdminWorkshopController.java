package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Workshop;
import xyz.leeyangy.spc.service.WorkshopService;

@RestController
@RequestMapping("/api/admin/workshop")
@RequiredArgsConstructor
public class AdminWorkshopController {

    private final WorkshopService workshopService;

    @GetMapping("/page")
    public R<Page<Workshop>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword) {
        Page<Workshop> page = new Page<>(current, size);
        LambdaQueryWrapper<Workshop> wrapper = new LambdaQueryWrapper<Workshop>()
                .and(keyword != null && !keyword.isEmpty(), w -> w
                        .like(Workshop::getWorkshopCode, keyword)
                        .or().like(Workshop::getWorkshopName, keyword))
                .orderByDesc(Workshop::getCreatedAt);
        return R.ok(workshopService.page(page, wrapper));
    }

    @GetMapping("/list")
    public R<java.util.List<Workshop>> listAll() {
        return R.ok(workshopService.list(new LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getStatus, 1)
                .orderByAsc(Workshop::getSortOrder)));
    }

    @GetMapping("/{id}")
    public R<Workshop> getById(@PathVariable Long id) {
        return R.ok(workshopService.getById(id));
    }

    @PostMapping
    public R<Workshop> create(@RequestBody Workshop workshop) {
        if (workshop.getWorkshopCode() == null || workshop.getWorkshopCode().trim().isEmpty()) {
            return R.fail("车间编码不能为空");
        }
        if (workshop.getWorkshopName() == null || workshop.getWorkshopName().trim().isEmpty()) {
            return R.fail("车间名称不能为空");
        }
        long count = workshopService.count(new LambdaQueryWrapper<Workshop>()
                .eq(Workshop::getWorkshopCode, workshop.getWorkshopCode().trim())
                .eq(Workshop::getDeleted, 0));
        if (count > 0) return R.fail("车间编码已存在");
        workshop.setStatus(workshop.getStatus() != null ? workshop.getStatus() : 1);
        workshopService.save(workshop);
        return R.ok("创建成功", workshop);
    }

    @PutMapping("/{id}")
    public R<Workshop> update(@PathVariable Long id, @RequestBody Workshop workshop) {
        Workshop exist = workshopService.getById(id);
        if (exist == null) return R.fail("车间不存在");
        if (workshop.getWorkshopName() != null) exist.setWorkshopName(workshop.getWorkshopName().trim());
        if (workshop.getWorkshopType() != null) exist.setWorkshopType(workshop.getWorkshopType());
        if (workshop.getDescription() != null) exist.setDescription(workshop.getDescription());
        if (workshop.getStatus() != null) exist.setStatus(workshop.getStatus());
        if (workshop.getSortOrder() != null) exist.setSortOrder(workshop.getSortOrder());
        workshopService.updateById(exist);
        return R.ok("更新成功", exist);
    }

    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        Workshop workshop = workshopService.getById(id);
        if (workshop == null) return R.fail("车间不存在");
        workshopService.update(new LambdaUpdateWrapper<Workshop>()
                .eq(Workshop::getId, id)
                .set(Workshop::getDeleted, 1));
        return R.ok(null);
    }
}