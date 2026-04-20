package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.service.ProcessService;

@Slf4j
@RestController
@RequestMapping("/api/admin/process")
@RequiredArgsConstructor
public class AdminProcessController {

    private final ProcessService processService;

    @GetMapping("/page")
    public R<Page<Process>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) Long workshopId,
            @RequestParam(required = false) Integer status) {
        Page<Process> page = new Page<>(current, size);
        LambdaQueryWrapper<Process> wrapper = new LambdaQueryWrapper<Process>()
                .and(keyword != null && !keyword.isEmpty(), w -> w
                        .like(Process::getProcessCode, keyword)
                        .or().like(Process::getProcessName, keyword))
                .eq(workshopId != null, Process::getWorkshopId, workshopId)
                .eq(status != null, Process::getStatus, status)
                .orderByAsc(Process::getSortOrder)
                .orderByDesc(Process::getCreatedAt);
        return R.ok(processService.page(page, wrapper));
    }

    @GetMapping("/{id}")
    public R<Process> getById(@PathVariable Long id) {
        Process process = processService.getById(id);
        if (process == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工序不存在");
        }
        return R.ok(process);
    }

    @PostMapping
    public R<Process> create(@RequestBody ProcessCreateRequest req) {
        if (req.getProcessCode() == null || req.getProcessCode().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "工序编码不能为空");
        }
        if (req.getProcessName() == null || req.getProcessName().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "工序名称不能为空");
        }

        long count = processService.count(new LambdaQueryWrapper<Process>()
                .eq(Process::getProcessCode, req.getProcessCode())
                .eq(Process::getDeleted, 0));
        if (count > 0) {
            return R.fail(StatusCode.CONFLICT, "工序编码已存在");
        }

        Process process = new Process();
        process.setProcessCode(req.getProcessCode().trim());
        process.setProcessName(req.getProcessName().trim());
        process.setProcessType(req.getProcessType());
        process.setWorkshopId(req.getWorkshopId());
        process.setDescription(req.getDescription());
        process.setStatus(req.getStatus() != null ? req.getStatus() : 1);
        process.setSortOrder(req.getSortOrder() != null ? req.getSortOrder() : 0);

        processService.save(process);
        log.info("[Admin] 创建工序: code={} name={}", process.getProcessCode(), process.getProcessName());
        return R.ok("创建成功", process);
    }

    @PutMapping("/{id}")
    public R<Process> update(@PathVariable Long id, @RequestBody ProcessUpdateRequest req) {
        Process existProcess = processService.getById(id);
        if (existProcess == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工序不存在");
        }

        if (req.getProcessName() != null) {
            existProcess.setProcessName(req.getProcessName().trim());
        }
        if (req.getProcessType() != null) {
            existProcess.setProcessType(req.getProcessType());
        }
        if (req.getWorkshopId() != null) {
            existProcess.setWorkshopId(req.getWorkshopId());
        }
        if (req.getDescription() != null) {
            existProcess.setDescription(req.getDescription());
        }
        if (req.getStatus() != null) {
            existProcess.setStatus(req.getStatus());
        }
        if (req.getSortOrder() != null) {
            existProcess.setSortOrder(req.getSortOrder());
        }

        processService.updateById(existProcess);
        log.info("[Admin] 更新工序: id={} code={}", id, existProcess.getProcessCode());
        return R.ok("更新成功", existProcess);
    }

    @PostMapping("/{id}/duplicate")
    public R<Process> duplicate(@PathVariable Long id) {
        Process source = processService.getById(id);
        if (source == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工序不存在");
        }

        String baseCode = source.getProcessCode();
        String baseName = source.getProcessName();
        String newCode = generateUniqueCode(baseCode, "process");
        String newName = generateUniqueName(baseName, "process");

        Process copy = new Process();
        copy.setProcessCode(newCode);
        copy.setProcessName(newName);
        copy.setProcessType(source.getProcessType());
        copy.setWorkshopId(source.getWorkshopId());
        copy.setDescription(source.getDescription() != null ? source.getDescription() + " (副本)" : "(副本)");
        copy.setStatus(source.getStatus());
        copy.setSortOrder(source.getSortOrder());

        processService.save(copy);
        log.info("[Admin] 复制工序: {} -> {}", source.getProcessCode(), newCode);
        return R.ok("复制成功", copy);
    }

    private String generateUniqueCode(String baseCode, String type) {
        String candidate = baseCode + "_副本";
        for (int i = 2; i <= 100; i++) {
            long count = processService.count(new LambdaQueryWrapper<Process>()
                    .eq(Process::getProcessCode, candidate)
                    .eq(Process::getDeleted, 0));
            if (count == 0) return candidate;
            candidate = baseCode + "_副本" + i;
        }
        return baseCode + "_copy_" + System.currentTimeMillis();
    }

    private String generateUniqueName(String baseName, String type) {
        String candidate = baseName + " (副本)";
        for (int i = 2; i <= 100; i++) {
            long count = processService.count(new LambdaQueryWrapper<Process>()
                    .eq(Process::getProcessName, candidate)
                    .eq(Process::getDeleted, 0));
            if (count == 0) return candidate;
            candidate = baseName + " (副本" + i + ")";
        }
        return baseName + " (copy_" + System.currentTimeMillis() + ")";
    }

    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        Process process = processService.getById(id);
        if (process == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工序不存在");
        }
        processService.update(new LambdaUpdateWrapper<Process>()
                .eq(Process::getId, id)
                .set(Process::getDeleted, 1));
        log.info("[Admin] 删除工序: id={} code={}", id, process.getProcessCode());
        return R.ok(null);
    }

    @Data
    public static class ProcessCreateRequest {
        private String processCode;
        private String processName;
        private String processType;
        private Long workshopId;
        private String description;
        private Integer status;
        private Integer sortOrder;
    }

    @Data
    public static class ProcessUpdateRequest {
        private String processName;
        private String processType;
        private Long workshopId;
        private String description;
        private Integer status;
        private Integer sortOrder;
    }
}
