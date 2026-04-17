package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.service.SpcStatService;

@RestController
@RequestMapping("/api/spc/stat")
@RequiredArgsConstructor
public class SpcStatController {

    private final SpcStatService spcStatService;

    @GetMapping("/latest")
    public R<SpcStatResult> getLatestStat(
            @RequestParam Long paramVersionId,
            @RequestParam(required = false) String batchId) {
        return R.ok(spcStatService.getLatestStat(paramVersionId, batchId));
    }

    @PostMapping("/calculate")
    public R<SpcStatResult> calculate(
            @RequestParam Long paramVersionId,
            @RequestParam(required = false) String batchId) {
        return R.ok(spcStatService.calculateAndSave(paramVersionId, batchId));
    }
}
