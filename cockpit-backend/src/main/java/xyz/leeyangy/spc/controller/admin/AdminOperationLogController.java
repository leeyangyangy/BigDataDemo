package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.service.OperationLogService;
import xyz.leeyangy.spc.vo.OperationLogVO;

@Slf4j
@RestController
@RequestMapping("/api/admin/operation-log")
@RequiredArgsConstructor
public class AdminOperationLogController {

    private final OperationLogService operationLogService;

    @GetMapping("/page")
    public R<Page<OperationLogVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String module,
            @RequestParam(required = false) String action,
            @RequestParam(required = false) String result,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {
        return R.ok(PageConvert.convert(operationLogService.queryPage(
                new Page<>(current, size), module, action, result, startDate, endDate), OperationLogVO::from));
    }

    @GetMapping("/{id}")
    public R<OperationLogVO> detail(@PathVariable Long id) {
        return R.ok(OperationLogVO.from(operationLogService.getById(id)));
    }

    // 等保三级要求: 审计日志不可删除, 已移除原 @DeleteMapping("/{id}") 和 @DeleteMapping("/clean") 端点
    // 如需日志归档, 请通过数据库运维流程执行, 并记录归档操作
}
