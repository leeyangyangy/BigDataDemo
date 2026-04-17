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
            @RequestParam(required = false) String keyword) {
        LambdaQueryWrapper<Param> wrapper = new LambdaQueryWrapper<Param>()
                .like(keyword != null && !keyword.isBlank(), Param::getParamCode, keyword)
                .or()
                .like(keyword != null && !keyword.isBlank(), Param::getParamName, keyword)
                .orderByDesc(Param::getCreatedAt);
        return R.ok(paramService.page(new Page<>(current, size), wrapper));
    }

    @PostMapping
    public R<Param> create(@RequestBody Param param) {
        paramService.save(param);
        return R.ok(param);
    }

    @GetMapping("/{id}")
    public R<Param> getById(@PathVariable Long id) {
        return R.ok(paramService.getById(id));
    }

    @PutMapping("/{id}")
    public R<Param> update(@PathVariable Long id, @RequestBody Param param) {
        param.setId(id);
        paramService.updateById(param);
        return R.ok(paramService.getById(id));
    }

    @DeleteMapping("/{id}")
    public R<Boolean> delete(@PathVariable Long id) {
        return R.ok(paramService.removeById(id));
    }
}
