package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.common.constants.RoleConstants;
import xyz.leeyangy.spc.common.util.WorkshopAccessHelper;
import xyz.leeyangy.spc.dto.SpcDataUploadDTO;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.vo.SpcDataVO;

import javax.validation.Valid;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/spc/data")
@RequiredArgsConstructor
public class SpcDataController {

    private final SpcDataService spcDataService;
    private final WorkshopAccessHelper workshopAccessHelper;

    @OperationLog(module = "SPC_DATA", action = "UPLOAD", targetType = "SpcData",
            content = "'录入数据: paramVersionId=' + #data.paramVersionId + ' value=' + #data.measuredValue",
            targetId = "#result.data.id")
    @PostMapping("/upload")
    public R<SpcDataVO> upload(@Valid @RequestBody SpcDataUploadDTO data, @RequestAttribute Long userId, @RequestAttribute String role) {
        SpcData entity = toEntity(data);
        entity.setCreatedBy(userId);
        SpcData saved = spcDataService.uploadData(entity, role);
        return R.ok(SpcDataVO.from(saved));
    }

    @OperationLog(module = "SPC_DATA", action = "BATCH_UPLOAD", targetType = "SpcData",
            content = "'批量录入数据: 共' + #dataList.size() + '条, 成功' + #result.data.size() + '条'",
            resultExpression = "#result.data.size() == #dataList.size() ? 'SUCCESS' : 'PARTIAL'")
    @PostMapping("/batch-upload")
    public R<List<SpcDataVO>> batchUpload(@Valid @RequestBody List<@Valid SpcDataUploadDTO> dataList,
                                          @RequestAttribute Long userId, @RequestAttribute String role) {
        List<SpcData> entities = dataList.stream().map(this::toEntity).collect(Collectors.toList());
        entities.forEach(e -> e.setCreatedBy(userId));
        List<SpcData> result = spcDataService.batchUpload(entities, role);
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
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestAttribute Long userId,
            @RequestAttribute String role) {
        // IDOR 修复: 非管理员仅能查询本人绑定车间下的数据; ADMIN 不限制
        Set<Long> workshopIds = RoleConstants.ADMIN.equals(role)
                ? null : workshopAccessHelper.getAccessibleWorkshopIds(userId);
        return R.ok(PageConvert.convert(spcDataService.pageByCondition(
                new Page<>(current, size), paramVersionId, batchId, productId, paramId, startTime, endTime, workshopIds), SpcDataVO::from));
    }

    @GetMapping("/recent")
    public R<List<SpcDataVO>> recentData(
            @RequestParam Long paramVersionId,
            @RequestParam(defaultValue = "30") Integer limit,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestAttribute Long userId,
            @RequestAttribute String role) {
        // IDOR 修复: 非管理员仅能查询本人绑定车间下的数据; ADMIN 不限制
        Set<Long> workshopIds = RoleConstants.ADMIN.equals(role)
                ? null : workshopAccessHelper.getAccessibleWorkshopIds(userId);
        return R.ok(spcDataService.listRecentData(paramVersionId, limit, startTime, endTime, workshopIds).stream()
                .map(SpcDataVO::from)
                .collect(Collectors.toList()));
    }

    /** 将录入 DTO 转换为 SpcData 实体 (仅拷贝业务字段，审计字段由服务端控制) */
    private SpcData toEntity(SpcDataUploadDTO dto) {
        SpcData entity = new SpcData();
        entity.setParamVersionId(dto.getParamVersionId());
        entity.setBatchId(dto.getBatchId());
        entity.setProductId(dto.getProductId());
        entity.setProcessId(dto.getProcessId());
        entity.setParamId(dto.getParamId());
        entity.setEquipmentId(dto.getEquipmentId());
        entity.setWorkstationNo(dto.getWorkstationNo());
        entity.setMeasuredValue(dto.getMeasuredValue());
        entity.setSampleSize(dto.getSampleSize());
        entity.setSubgroupIdx(dto.getSubgroupIdx());
        entity.setSubgroupSize(dto.getSubgroupSize());
        entity.setCollectTime(dto.getCollectTime());
        entity.setFillTime(dto.getFillTime());
        entity.setDataSource(dto.getDataSource());
        entity.setMsgId(dto.getMsgId());
        return entity;
    }
}
