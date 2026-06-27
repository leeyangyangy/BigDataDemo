package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.StandardChangeLog;
import xyz.leeyangy.spc.service.StandardChangeLogService;
import xyz.leeyangy.spc.vo.StandardChangeLogVO;

@Slf4j
@RestController
@RequestMapping("/api/admin/change-log")
@RequiredArgsConstructor
public class AdminLogController {

    private final StandardChangeLogService changeLogService;

    @GetMapping("/page")
    public R<Page<StandardChangeLogVO>> page(
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
        return R.ok(PageConvert.convert(changeLogService.page(page, wrapper), StandardChangeLogVO::from));
    }

    @GetMapping("/{id}")
    public R<StandardChangeLogVO> detail(@PathVariable Long id) {
        return R.ok(StandardChangeLogVO.from(changeLogService.getById(id)));
    }

    @DeleteMapping("/{id}")
    public R<Boolean> delete(@PathVariable Long id) {
        log.info("[Admin] 删除变更日志: id={}", id);
        return R.ok(changeLogService.removeById(id));
    }
}
