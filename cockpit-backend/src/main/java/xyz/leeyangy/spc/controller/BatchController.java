package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Batch;
import xyz.leeyangy.spc.service.BatchService;

@RestController
@RequestMapping("/api/spc/batch")
@RequiredArgsConstructor
public class BatchController {

    private final BatchService batchService;

    @GetMapping("/page")
    public R<Page<Batch>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) Long processId,
            @RequestParam(required = false) String batchStatus) {
        return R.ok(batchService.pageByCondition(new Page<>(current, size), productId, processId, batchStatus));
    }

    @PostMapping
    public R<Batch> create(@RequestBody Batch batch) {
        batchService.save(batch);
        return R.ok(batch);
    }

    @GetMapping("/{id}")
    public R<Batch> getById(@PathVariable Long id) {
        return R.ok(batchService.getById(id));
    }
}
