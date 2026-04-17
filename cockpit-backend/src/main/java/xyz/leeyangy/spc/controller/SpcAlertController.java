package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.service.SpcAlertService;

@RestController
@RequestMapping("/api/spc/alert")
@RequiredArgsConstructor
public class SpcAlertController {

    private final SpcAlertService spcAlertService;

    @GetMapping("/page")
    public R<Page<SpcAlert>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long paramVersionId,
            @RequestParam(required = false) String batchId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String alertType,
            @RequestParam(required = false) Integer alertLevel) {
        return R.ok(spcAlertService.pageByCondition(
                new Page<>(current, size), paramVersionId, batchId, status, alertType, alertLevel));
    }

    @PutMapping("/{id}/ack")
    public R<Boolean> acknowledge(@PathVariable Long id) {
        SpcAlert alert = spcAlertService.getById(id);
        if (alert == null) return R.fail("报警不存在");
        alert.setStatus("ACK");
        alert.setAcknowledgedAt(java.time.LocalDateTime.now());
        return R.ok(spcAlertService.updateById(alert));
    }

    @PutMapping("/{id}/resolve")
    public R<Boolean> resolve(@PathVariable Long id, @RequestParam(required = false) String remark) {
        SpcAlert alert = spcAlertService.getById(id);
        if (alert == null) return R.fail("报警不存在");
        alert.setStatus("RESOLVED");
        alert.setResolveRemark(remark);
        return R.ok(spcAlertService.updateById(alert));
    }
}
