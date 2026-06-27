package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.vo.SpcDataVO;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/spc/data")
@RequiredArgsConstructor
public class SpcDataController {

    private final SpcDataService spcDataService;

    @OperationLog(module = "SPC_DATA", action = "UPLOAD", targetType = "SpcData",
            content = "'录入数据: paramVersionId=' + #data.paramVersionId + ' value=' + #data.measuredValue",
            targetId = "#result.data.id")
    @PostMapping("/upload")
    public R<SpcDataVO> upload(@RequestBody SpcData data, @RequestAttribute Long userId, @RequestAttribute String role) {
        data.setCreatedBy(userId);
        SpcData saved = spcDataService.uploadData(data, role);
        return R.ok(SpcDataVO.from(saved));
    }

    @OperationLog(module = "SPC_DATA", action = "BATCH_UPLOAD", targetType = "SpcData",
            content = "'批量录入数据: 共' + #dataList.size() + '条, 成功' + #result.data.size() + '条'",
            resultExpression = "#result.data.size() == #dataList.size() ? 'SUCCESS' : 'PARTIAL'")
    @PostMapping("/batch-upload")
    public R<List<SpcDataVO>> batchUpload(@RequestBody List<SpcData> dataList, @RequestAttribute Long userId,
                                          @RequestAttribute String role) {
        dataList.forEach(d -> d.setCreatedBy(userId));
        List<SpcData> result = spcDataService.batchUpload(dataList, role);
        return R.ok(result.stream().map(SpcDataVO::from).collect(Collectors.toList()));
    }

    @GetMapping("/page")
    public R<Page<SpcDataVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long paramVersionId,
            @RequestParam(required = false) String batchId,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) Long paramId,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {
        return R.ok(PageConvert.convert(spcDataService.pageByCondition(
                new Page<>(current, size), paramVersionId, batchId, productId, paramId, startTime, endTime), SpcDataVO::from));
    }

    @GetMapping("/recent")
    public R<List<SpcDataVO>> recentData(
            @RequestParam Long paramVersionId,
            @RequestParam(defaultValue = "30") Integer limit,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {
        return R.ok(spcDataService.listRecentData(paramVersionId, limit, startTime, endTime).stream()
                .map(SpcDataVO::from)
                .collect(Collectors.toList()));
    }
}
