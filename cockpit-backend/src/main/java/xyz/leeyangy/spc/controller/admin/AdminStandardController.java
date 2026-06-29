package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.ParamCreateDTO;
import xyz.leeyangy.spc.dto.ParamUpdateDTO;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.mapper.ParamMapper;
import xyz.leeyangy.spc.service.ParamService;
import xyz.leeyangy.spc.vo.ParamVO;

import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/api/admin/standard")
@RequiredArgsConstructor
public class AdminStandardController {

    private final ParamService paramService;
    private final ParamMapper paramMapper;

    @GetMapping("/page")
    public R<Page<ParamVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) Long processId) {
        Page<Param> page = new Page<>(current, size);
        LambdaQueryWrapper<Param> wrapper = new LambdaQueryWrapper<Param>()
                .and(keyword != null && !keyword.isEmpty(), w -> w
                        .like(Param::getParamCode, keyword)
                        .or().like(Param::getParamName, keyword))
                .eq(processId != null, Param::getProcessId, processId)
                .orderByDesc(Param::getCreatedAt);
        return R.ok(PageConvert.convert(paramService.page(page, wrapper), ParamVO::from));
    }

    @GetMapping("/{id}")
    public R<ParamVO> getById(@PathVariable Long id) {
        Param param = paramService.getById(id);
        if (param == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工艺参数不存在");
        }
        return R.ok(ParamVO.from(param));
    }

    @OperationLog(module = "PARAM", action = "CREATE", targetType = "Param",
            content = "'创建工艺参数: ' + #result.data.paramCode + ' - ' + #result.data.paramName",
            targetId = "#result.data.id")
    @PostMapping
    public R<ParamVO> create(@Valid @RequestBody ParamCreateDTO req) {
        long count = paramService.count(new LambdaQueryWrapper<Param>()
                .eq(Param::getParamCode, req.getParamCode())
                .eq(Param::getDeleted, 0));
        if (count > 0) {
            return R.fail(StatusCode.CONFLICT, "工艺参数编码已存在");
        }
        Param param = new Param();
        param.setParamCode(req.getParamCode());
        param.setParamName(req.getParamName());
        param.setProcessId(req.getProcessId());
        param.setUnit(req.getUnit());
        param.setStatus(req.getStatus() != null ? req.getStatus() : 1);
        paramService.save(param);
        log.info("[Admin] 创建工艺参数: id={} code={}", param.getId(), param.getParamCode());
        return R.ok(ParamVO.from(param));
    }

    @OperationLog(module = "PARAM", action = "UPDATE", targetType = "Param",
            content = "'更新工艺参数: ' + #result.data.paramCode + ' - ' + #result.data.paramName",
            targetId = "#id")
    @PutMapping("/{id}")
    public R<ParamVO> update(@PathVariable Long id, @Valid @RequestBody ParamUpdateDTO req) {
        Param exist = paramService.getById(id);
        if (exist == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工艺参数不存在");
        }

        if (req.getParamCode() != null && !req.getParamCode().equals(exist.getParamCode())) {
            long count = paramService.count(new LambdaQueryWrapper<Param>()
                    .eq(Param::getParamCode, req.getParamCode())
                    .ne(Param::getId, id)
                    .eq(Param::getDeleted, 0));
            if (count > 0) {
                return R.fail(StatusCode.CONFLICT, "工艺参数编码已存在");
            }
        }

        LambdaUpdateWrapper<Param> wrapper = new LambdaUpdateWrapper<Param>().eq(Param::getId, id);
        if (req.getParamCode() != null) wrapper.set(Param::getParamCode, req.getParamCode());
        if (req.getParamName() != null) wrapper.set(Param::getParamName, req.getParamName());
        if (req.getProcessId() != null) wrapper.set(Param::getProcessId, req.getProcessId());
        if (req.getUnit() != null) wrapper.set(Param::getUnit, req.getUnit());
        if (req.getStatus() != null) wrapper.set(Param::getStatus, req.getStatus());
        if (req.getDataType() != null) wrapper.set(Param::getDataType, req.getDataType());

        paramService.update(wrapper);
        log.info("[Admin] 更新工艺参数: id={} code={}", id, exist.getParamCode());
        return R.ok(ParamVO.from(paramService.getById(id)));
    }

    @OperationLog(module = "PARAM", action = "DUPLICATE", targetType = "Param",
            content = "'复制工艺参数: ' + #result.data.paramCode",
            targetId = "#result.data.id")
    @PostMapping("/{id}/duplicate")
    public R<ParamVO> duplicate(@PathVariable Long id) {
        Param source = paramService.getById(id);
        if (source == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工艺参数不存在");
        }

        String baseCode = source.getParamCode();
        String baseName = source.getParamName();
        String newCode = generateUniqueParamCode(baseCode);
        String newName = generateUniqueParamName(baseName);

        Param copy = new Param();
        copy.setParamCode(newCode);
        copy.setParamName(newName);
        copy.setParamType(source.getParamType());
        copy.setUnit(source.getUnit());
        copy.setDataType(source.getDataType());
        copy.setDecimalPlaces(source.getDecimalPlaces());
        copy.setStatus(source.getStatus());

        paramService.save(copy);
        log.info("[Admin] 复制工艺参数: {} -> {}", source.getParamCode(), newCode);
        return R.ok("复制成功", ParamVO.from(copy));
    }

    /**
     * 生成唯一的 param_code：查询包含软删除记录的所有记录，避免命名冲突。
     * 修复：原实现使用 LambdaQueryWrapper（受 @TableLogic 影响只查未删除），
     * 导致软删除副本后再次复制会生成同名记录，触发唯一索引冲突或数据重复。
     */
    private String generateUniqueParamCode(String baseCode) {
        String candidate = baseCode + "_副本";
        if (paramMapper.countByCodeAll(candidate) == 0) return candidate;
        for (int i = 2; i <= 100; i++) {
            candidate = baseCode + "_副本" + i;
            if (paramMapper.countByCodeAll(candidate) == 0) return candidate;
        }
        return baseCode + "_copy_" + System.currentTimeMillis();
    }

    /**
     * 生成唯一的 param_name：同上，查询包含软删除记录。
     */
    private String generateUniqueParamName(String baseName) {
        String candidate = baseName + " (副本)";
        if (paramMapper.countByNameAll(candidate) == 0) return candidate;
        for (int i = 2; i <= 100; i++) {
            candidate = baseName + " (副本" + i + ")";
            if (paramMapper.countByNameAll(candidate) == 0) return candidate;
        }
        return baseName + " (copy_" + System.currentTimeMillis() + ")";
    }

    @OperationLog(module = "PARAM", action = "DELETE", targetType = "Param",
            content = "'删除工艺参数 id=' + #id",
            targetId = "#id")
    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        Param exist = paramService.getById(id);
        if (exist == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工艺参数不存在");
        }
        paramService.update(new LambdaUpdateWrapper<Param>()
                .eq(Param::getId, id)
                .set(Param::getDeleted, 1));
        log.info("[Admin] 删除工艺参数: id={} code={}", id, exist.getParamCode());
        return R.ok(null);
    }
}
