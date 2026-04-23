package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.StandardChangeLog;
import xyz.leeyangy.spc.service.ParamService;
import xyz.leeyangy.spc.service.StandardChangeLogService;

@Slf4j
@RestController
@RequestMapping("/api/admin/change-log")
@RequiredArgsConstructor
public class AdminLogController {

    private final StandardChangeLogService changeLogService;
    private final ParamService paramService;

    @GetMapping("/page")
    public R<Page<StandardChangeLog>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long paramId,
            @RequestParam(required = false) String changeType,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {
        Page<StandardChangeLog> page = new Page<>(current, size);
        LambdaQueryWrapper<StandardChangeLog> wrapper = new LambdaQueryWrapper<StandardChangeLog>()
                .eq(paramId != null, StandardChangeLog::getParamId, paramId)
                .eq(changeType != null && !changeType.isEmpty(), StandardChangeLog::getChangeType, changeType)
                .ge(startDate != null && !startDate.isEmpty(), StandardChangeLog::getCreatedAt, startDate)
                .le(endDate != null && !endDate.isEmpty(), StandardChangeLog::getCreatedAt, endDate + " 23:59:59")
                .eq(StandardChangeLog::getDeleted, 0)
                .orderByDesc(StandardChangeLog::getCreatedAt);
        return R.ok(changeLogService.page(page, wrapper));
    }

    @GetMapping("/{id}")
    public R<StandardChangeLog> detail(@PathVariable Long id) {
        return R.ok(changeLogService.getById(id));
    }

    @DeleteMapping("/{id}")
    public R<Boolean> delete(@PathVariable Long id) {
        log.info("[Admin] 删除变更日志: id={}", id);
        return R.ok(changeLogService.removeById(id));
    }
}
