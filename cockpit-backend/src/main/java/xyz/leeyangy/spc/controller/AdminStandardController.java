package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.service.ParamService;

@Slf4j
@RestController
@RequestMapping("/api/admin/standard")
@RequiredArgsConstructor
public class AdminStandardController {

    private final ParamService paramService;

    @GetMapping("/page")
    public R<Page<Param>> page(
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
        return R.ok(paramService.page(page, wrapper));
    }

    @GetMapping("/{id}")
    public R<Param> getById(@PathVariable Long id) {
        Param param = paramService.getById(id);
        if (param == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "工艺参数不存在");
        }
        return R.ok(param);
    }

    @PostMapping
    public R<Param> create(@RequestBody CreateRequest req) {
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
        return R.ok(param);
    }

    @PutMapping("/{id}")
    public R<Param> update(@PathVariable Long id, @RequestBody UpdateRequest req) {
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

        paramService.update(wrapper);
        log.info("[Admin] 更新工艺参数: id={} code={}", id, exist.getParamCode());
        return R.ok(paramService.getById(id));
    }

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

    @Data
    public static class CreateRequest {
        private String paramCode;
        private String paramName;
        private Long processId;
        private String unit;
        private Integer status;
    }

    @Data
    public static class UpdateRequest {
        private String paramCode;
        private String paramName;
        private Long processId;
        private String unit;
        private Integer status;
    }
}
