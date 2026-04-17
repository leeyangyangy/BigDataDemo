package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.service.SpcDataService;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/spc/data")
@RequiredArgsConstructor
public class SpcDataController {

    private final SpcDataService spcDataService;

    @PostMapping("/upload")
    public R<SpcData> upload(@RequestBody SpcData data) {
        return R.ok(spcDataService.uploadData(data));
    }

    @PostMapping("/batch-upload")
    public R<List<SpcData>> batchUpload(@RequestBody List<SpcData> dataList) {
        return R.ok(spcDataService.batchUpload(dataList));
    }

    @GetMapping("/page")
    public R<Page<SpcData>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long paramVersionId,
            @RequestParam(required = false) String batchId,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) Long paramId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {
        return R.ok(spcDataService.pageByCondition(
                new Page<>(current, size), paramVersionId, batchId, productId, paramId, startTime, endTime));
    }

    @GetMapping("/recent")
    public R<List<SpcData>> recentData(
            @RequestParam Long paramVersionId,
            @RequestParam(defaultValue = "30") Integer limit,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {
        return R.ok(spcDataService.listRecentData(paramVersionId, limit, startTime, endTime));
    }
}
