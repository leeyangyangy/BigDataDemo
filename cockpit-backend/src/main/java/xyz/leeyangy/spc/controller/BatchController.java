package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.BatchCreateDTO;
import xyz.leeyangy.spc.dto.BatchUpdateDTO;
import xyz.leeyangy.spc.entity.Batch;
import xyz.leeyangy.spc.service.BatchService;
import xyz.leeyangy.spc.vo.BatchVO;

import javax.validation.Valid;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class BatchController {

    private final BatchService batchService;

    @OperationLog(module = "BATCH", action = "QUERY", targetType = "Batch",
            content = "'查询批次列表: productId=' + #productId + ' equipmentId=' + #equipmentId + ' count=' + #result.data.size()")
    @GetMapping("/list")
    public R<List<BatchVO>> list(@RequestParam(required = false) Long productId,
                                 @RequestParam(required = false) Long equipmentId) {
        return R.ok(batchService.listByProductAndEquipment(productId, equipmentId).stream()
                .map(BatchVO::from)
                .collect(Collectors.toList()));
    }

    @GetMapping("/page")
    public R<Page<BatchVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) String keyword) {
        return R.ok(PageConvert.convert(batchService.page(new Page<>(current, size), productId, keyword), BatchVO::from));
    }

    @OperationLog(module = "BATCH", action = "CREATE", targetType = "Batch",
            content = "'创建批次: ' + #result.data.batchCode",
            targetId = "#result.data.id")
    @PostMapping
    public R<BatchVO> create(@Valid @RequestBody BatchCreateDTO dto) {
        Batch batch = new Batch();
        batch.setBatchCode(dto.getBatchCode());
        batch.setProductId(dto.getProductId());
        batch.setProcessId(dto.getProcessId());
        batch.setWorkshopId(dto.getWorkshopId());
        batch.setLineId(dto.getLineId());
        batch.setLotSize(dto.getLotSize());
        batch.setBatchStatus(dto.getBatchStatus());
        batch.setStartTime(dto.getStartTime());
        batch.setEndTime(dto.getEndTime());
        batch.setStatus(dto.getStatus());
        Batch created = batchService.createBatch(batch);
        log.info("[Batch] 创建批次: id={} code={}", created.getId(), created.getBatchCode());
        return R.ok(BatchVO.from(created));
    }

    @OperationLog(module = "BATCH", action = "UPDATE", targetType = "Batch",
            content = "'更新批次: ' + #result.data.batchCode",
            targetId = "#id")
    @PutMapping("/{id}")
    public R<BatchVO> update(@PathVariable Long id, @Valid @RequestBody BatchUpdateDTO dto) {
        Batch batch = new Batch();
        batch.setBatchCode(dto.getBatchCode());
        batch.setProductId(dto.getProductId());
        batch.setProcessId(dto.getProcessId());
        batch.setWorkshopId(dto.getWorkshopId());
        batch.setLineId(dto.getLineId());
        batch.setLotSize(dto.getLotSize());
        batch.setBatchStatus(dto.getBatchStatus());
        batch.setStartTime(dto.getStartTime());
        batch.setEndTime(dto.getEndTime());
        batch.setStatus(dto.getStatus());
        Batch updated = batchService.updateBatch(id, batch);
        log.info("[Batch] 更新批次: id={} code={}", id, updated.getBatchCode());
        return R.ok(BatchVO.from(updated));
    }

    @OperationLog(module = "BATCH", action = "DELETE", targetType = "Batch",
            content = "'删除批次 id=' + #id",
            targetId = "#id")
    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        try {
            batchService.deleteBatch(id);
            log.info("[Batch] 删除批次: id={}", id);
            return R.ok(null);
        } catch (RuntimeException e) {
            return R.fail(e.getMessage());
        }
    }
}
