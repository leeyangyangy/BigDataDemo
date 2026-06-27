package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.ProcessParamBindDTO;
import xyz.leeyangy.spc.entity.Equipment;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.service.EquipmentService;
import xyz.leeyangy.spc.service.ProcessParamService;
import xyz.leeyangy.spc.service.ProcessService;
import xyz.leeyangy.spc.vo.EquipmentVO;
import xyz.leeyangy.spc.vo.ParamSimpleVO;
import xyz.leeyangy.spc.vo.ProcessVO;

import javax.validation.Valid;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/process")
@RequiredArgsConstructor
public class ProcessController {

    private final ProcessService processService;
    private final EquipmentService equipmentService;
    private final ProcessParamService processParamService;

    @OperationLog(module = "PROCESS", action = "QUERY", targetType = "Process",
            content = "'查询工序列表: productId=' + #productId + ' workshopId=' + #workshopId + ' count=' + #result.data.size()")
    @GetMapping("/list")
    public R<List<ProcessVO>> list(@RequestParam(required = false) Long productId,
                                   @RequestParam(required = false) Long workshopId) {
        return R.ok(processService.listByProduct(productId, workshopId).stream()
                .map(ProcessVO::from)
                .collect(Collectors.toList()));
    }

    @GetMapping("/page")
    public R<Page<ProcessVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) String keyword) {
        return R.ok(PageConvert.convert(processService.page(new Page<>(current, size), productId, keyword), ProcessVO::from));
    }

    @GetMapping("/{processId}/equipment")
    public R<List<EquipmentVO>> getProcessEquipment(@PathVariable Long processId) {
        List<Equipment> equipment = equipmentService.listByProcessId(processId);
        return R.ok(equipment.stream().map(EquipmentVO::from).collect(Collectors.toList()));
    }

    @GetMapping("/{processId}/params")
    public R<List<ParamSimpleVO>> getProcessParams(@PathVariable Long processId) {
        List<ParamSimpleVO> params = processParamService.getProcessParams(processId);
        return R.ok(params);
    }

    @GetMapping("/{processId}/param-ids")
    public R<List<Long>> getProcessParamIds(@PathVariable Long processId) {
        List<Long> paramIds = processParamService.getParamIdsByProcessId(processId);
        return R.ok(paramIds);
    }

    @PostMapping("/{processId}/params/bind")
    public R<Void> bindProcessParams(@PathVariable Long processId,
                                     @Valid @RequestBody ProcessParamBindDTO dto) {
        processParamService.bindParams(processId,
                dto.getItems().stream()
                        .map(item -> new ProcessParamService.BindItem() {{
                            setParamId(item.getParamId());
                        }}).collect(Collectors.toList()));
        return R.ok(null);
    }

    @DeleteMapping("/{processId}/params/{paramId}")
    public R<Void> unbindProcessParam(@PathVariable Long processId,
                                      @PathVariable Long paramId) {
        processParamService.unbindParam(processId, paramId);
        return R.ok(null);
    }
}
