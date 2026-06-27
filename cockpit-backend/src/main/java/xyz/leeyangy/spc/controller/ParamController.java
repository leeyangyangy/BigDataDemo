package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Param;
import xyz.leeyangy.spc.service.ParamService;
import xyz.leeyangy.spc.vo.ParamVO;

@RestController
@RequestMapping("/api/spc/param")
@RequiredArgsConstructor
public class ParamController {

    private final ParamService paramService;

    @GetMapping("/page")
    public R<Page<ParamVO>> page(
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
        return R.ok(PageConvert.convert(paramService.page(new Page<>(current, size), wrapper), ParamVO::from));
    }

    @GetMapping("/{id}")
    public R<ParamVO> getById(@PathVariable Long id) {
        return R.ok(ParamVO.from(paramService.getById(id)));
    }
}
