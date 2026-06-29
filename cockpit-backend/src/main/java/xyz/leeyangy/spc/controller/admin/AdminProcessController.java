package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.ProcessCreateDTO;
import xyz.leeyangy.spc.dto.ProcessUpdateDTO;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.mapper.ProcessMapper;
import xyz.leeyangy.spc.service.ProcessService;
import xyz.leeyangy.spc.vo.ProcessVO;

import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/api/admin/process")
@RequiredArgsConstructor
public class AdminProcessController {

    private final ProcessService processService;
    private final ProcessMapper processMapper;

    @GetMapping("/page")
    public R<Page<ProcessVO>> page(
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
        return R.ok(PageConvert.convert(processService.page(page, wrapper), ProcessVO::from));
    }

    @GetMapping("/{id}")
    public R<ProcessVO> getById(@PathVariable Long id) {
        Process process = processService.getById(id);
        if (process == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工序不存在");
        }
        return R.ok(ProcessVO.from(process));
    }

    @OperationLog(module = "PROCESS", action = "CREATE", targetType = "Process",
            content = "'创建工序: ' + #result.data.processCode + ' - ' + #result.data.processName",
            targetId = "#result.data.id")
    @PostMapping
    public R<ProcessVO> create(@Valid @RequestBody ProcessCreateDTO req) {
        if (req.getProcessCode() == null || req.getProcessCode().trim().isEmpty()) {
            return R.fail("工序编码不能为空");
        }
        if (req.getProcessName() == null || req.getProcessName().trim().isEmpty()) {
            return R.fail("工序名称不能为空");
        }

        long count = processService.count(new LambdaQueryWrapper<Process>()
                .eq(Process::getProcessCode, req.getProcessCode())
                .eq(Process::getDeleted, 0));
        if (count > 0) {
            return R.fail("工序编码已存在");
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
        return R.ok("创建成功", ProcessVO.from(process));
    }

    @OperationLog(module = "PROCESS", action = "UPDATE", targetType = "Process",
            content = "'更新工序: ' + #result.data.processCode + ' - ' + #result.data.processName",
            targetId = "#id")
    @PutMapping("/{id}")
    public R<ProcessVO> update(@PathVariable Long id, @Valid @RequestBody ProcessUpdateDTO req) {
        Process existProcess = processService.getById(id);
        if (existProcess == null) {
            return R.fail("工序不存在");
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
        return R.ok("更新成功", ProcessVO.from(existProcess));
    }

    @OperationLog(module = "PROCESS", action = "DUPLICATE", targetType = "Process",
            content = "'复制工序: ' + #result.data.processCode",
            targetId = "#result.data.id")
    @PostMapping("/{id}/duplicate")
    public R<ProcessVO> duplicate(@PathVariable Long id) {
        Process source = processService.getById(id);
        if (source == null) {
            return R.fail("工序不存在");
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
        return R.ok("复制成功", ProcessVO.from(copy));
    }

    /**
     * 生成唯一的 process_code：查询包含软删除记录的所有记录，避免命名冲突。
     * 修复：原实现使用 LambdaQueryWrapper（受 @TableLogic 影响只查未删除），
     * 导致软删除副本后再次复制会生成同名记录，触发唯一索引冲突或数据重复。
     */
    private String generateUniqueCode(String baseCode, String type) {
        String candidate = baseCode + "_副本";
        if (processMapper.countByCodeAll(candidate) == 0) return candidate;
        for (int i = 2; i <= 100; i++) {
            candidate = baseCode + "_副本" + i;
            if (processMapper.countByCodeAll(candidate) == 0) return candidate;
        }
        return baseCode + "_copy_" + System.currentTimeMillis();
    }

    /**
     * 生成唯一的 process_name：同上，查询包含软删除记录。
     */
    private String generateUniqueName(String baseName, String type) {
        String candidate = baseName + " (副本)";
        if (processMapper.countByNameAll(candidate) == 0) return candidate;
        for (int i = 2; i <= 100; i++) {
            candidate = baseName + " (副本" + i + ")";
            if (processMapper.countByNameAll(candidate) == 0) return candidate;
        }
        return baseName + " (copy_" + System.currentTimeMillis() + ")";
    }

    @OperationLog(module = "PROCESS", action = "DELETE", targetType = "Process",
            content = "'删除工序 id=' + #id",
            targetId = "#id")
    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        Process process = processService.getById(id);
        if (process == null) {
            return R.fail("工序不存在");
        }
        processService.update(new LambdaUpdateWrapper<Process>()
                .eq(Process::getId, id)
                .set(Process::getDeleted, 1));
        log.info("[Admin] 删除工序: id={} code={}", id, process.getProcessCode());
        return R.ok(null);
    }
}
