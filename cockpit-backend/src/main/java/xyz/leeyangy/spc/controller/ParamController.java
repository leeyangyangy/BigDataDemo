package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.service.ParamService;

@RestController
@RequestMapping("/api/spc/param")
@RequiredArgsConstructor
public class ParamController {

    private final ParamService paramService;

    @GetMapping("/page")
    public R<Page<Param>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) Long processId) {
        LambdaQueryWrapper<Param> wrapper = new LambdaQueryWrapper<Param>()
                .eq(Param::getStatus, 1)
                .and(keyword != null && !keyword.isBlank(), w -> w
                        .like(Param::getParamCode, keyword)
                        .or().like(Param::getParamName, keyword))
                .eq(processId != null, Param::getProcessId, processId)
                .orderByDesc(Param::getCreatedAt);
        return R.ok(paramService.page(new Page<>(current, size), wrapper));
    }

    @GetMapping("/{id}")
    public R<Param> getById(@PathVariable Long id) {
        return R.ok(paramService.getById(id));
    }
}
