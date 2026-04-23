package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.OperationLog;
import xyz.leeyangy.spc.service.OperationLogService;

@Slf4j
@RestController
@RequestMapping("/api/admin/operation-log")
@RequiredArgsConstructor
public class AdminOperationLogController {

    private final OperationLogService operationLogService;

    @GetMapping("/page")
    public R<Page<OperationLog>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String module,
            @RequestParam(required = false) String action,
            @RequestParam(required = false) String result,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {
        return R.ok(operationLogService.queryPage(
                new Page<>(current, size), module, action, result, startDate, endDate));
    }

    @GetMapping("/{id}")
    public R<OperationLog> detail(@PathVariable Long id) {
        return R.ok(operationLogService.getById(id));
    }

    @DeleteMapping("/{id}")
    public R<Boolean> delete(@PathVariable Long id) {
        log.info("[Admin] 删除操作日志: id={}", id);
        return R.ok(operationLogService.removeById(id));
    }

    @DeleteMapping("/clean")
    public R<Boolean> cleanBefore(@RequestParam String beforeDate) {
        long count = operationLogService.count();
        boolean ok = operationLogService.remove(
                new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<OperationLog>()
                        .le(OperationLog::getCreatedAt, beforeDate + " 23:59:59"));
        log.info("[Admin] 清理操作日志: before={}, 删除{}条", beforeDate, count);
        return R.ok(ok);
    }
}
