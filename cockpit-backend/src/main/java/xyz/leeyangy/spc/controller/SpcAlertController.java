package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.AlertHandleDTO;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.service.SpcAlertService;
import xyz.leeyangy.spc.vo.AlertHandleResultVO;
import xyz.leeyangy.spc.vo.SpcAlertVO;

import javax.validation.Valid;

import java.time.LocalDateTime;

@Slf4j
@RestController
@RequestMapping("/api/spc/alert")
@RequiredArgsConstructor
public class SpcAlertController {

    private final SpcAlertService spcAlertService;

    @GetMapping("/page")
    public R<Page<SpcAlertVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long paramVersionId,
            @RequestParam(required = false) Long paramId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {
        return R.ok(PageConvert.convert(spcAlertService.pageByCondition(
                new Page<>(current, size), paramVersionId, paramId, status, startTime, endTime), SpcAlertVO::from));
    }

    @OperationLog(module = "SPC_ALERT", action = "CONFIRM", targetType = "SpcAlert",
            content = "'确认报警 id=' + #id",
            targetId = "#id")
    @PutMapping("/{id}/confirm")
    public R<Void> confirm(@PathVariable Long id, @RequestAttribute Long userId) {
        try {
            SpcAlert alert = spcAlertService.getById(id);
            spcAlertService.confirmAlert(id, userId);
            return R.ok(null);
        } catch (RuntimeException e) {
            log.error("[Alert] 确认报警失败: {}", e.getMessage());
            return R.fail(e.getMessage());
        }
    }

    @OperationLog(module = "SPC_ALERT", action = "HANDLE", targetType = "SpcAlert",
            content = "'处理报警 id=' + #id + ' result=' + #dto.handleResult + (#dto.handleRemark != null ? ' remark=' + #dto.handleRemark : '')",
            targetId = "#id")
    @PutMapping("/{id}/handle")
    public R<AlertHandleResultVO> handle(@PathVariable Long id,
                                          @Valid @RequestBody AlertHandleDTO dto,
                                          @RequestAttribute Long userId) {
        try {
            SpcAlert alert = spcAlertService.getById(id);
            AlertHandleResultVO result = spcAlertService.handleAlertWithResult(
                    id, dto.getHandleResult(), dto.getHandleRemark(), userId);
            return R.ok(result);
        } catch (RuntimeException e) {
            log.error("[Alert] 处理报警失败: {}", e.getMessage());
            return R.fail(e.getMessage());
        }
    }
}
