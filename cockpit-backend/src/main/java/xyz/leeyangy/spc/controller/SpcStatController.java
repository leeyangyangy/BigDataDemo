package xyz.leeyangy.spc.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.service.SpcStatService;
import xyz.leeyangy.spc.vo.SpcStatResultVO;

@RestController
@RequestMapping("/api/spc/stat")
@RequiredArgsConstructor
public class SpcStatController {

    private final SpcStatService spcStatService;

    @GetMapping("/latest")
    public R<SpcStatResultVO> getLatestStat(
            @RequestParam Long paramVersionId,
            @RequestParam(required = false) String batchId) {
        return R.ok(SpcStatResultVO.from(spcStatService.getLatestStat(paramVersionId, batchId)));
    }

    @PostMapping("/calculate")
    public R<SpcStatResultVO> calculate(
            @RequestParam Long paramVersionId,
            @RequestParam(required = false) String batchId) {
        return R.ok(SpcStatResultVO.from(spcStatService.calculateAndSave(paramVersionId, batchId)));
    }
}
