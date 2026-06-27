package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.vo.ParamVersionVO;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/spc/param-version")
@RequiredArgsConstructor
public class ParamVersionController {

    private final ParamVersionService paramVersionService;

    @GetMapping("/current")
    public R<ParamVersionVO> getCurrentVersion(@RequestParam Long paramId, @RequestParam(required = false) Long productId) {
        if (productId != null) {
            return R.ok(ParamVersionVO.from(paramVersionService.getCurrentVersion(paramId, productId)));
        }
        return R.ok(ParamVersionVO.from(paramVersionService.getAnyCurrentVersion(paramId)));
    }

    @GetMapping("/history")
    public R<List<ParamVersionVO>> getVersionHistory(@RequestParam Long paramId, @RequestParam(required = false) Long productId) {
        return R.ok(paramVersionService.getVersionHistory(paramId, productId).stream()
                .map(ParamVersionVO::from)
                .collect(Collectors.toList()));
    }

    @GetMapping("/page")
    public R<Page<ParamVersionVO>> pageVersions(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "10") Integer size,
            @RequestParam(required = false) Long paramId,
            @RequestParam(required = false) Long productId) {
        return R.ok(PageConvert.convert(paramVersionService.pageVersions(new Page<>(current, size), paramId, productId), ParamVersionVO::from));
    }

    @GetMapping("/{id}")
    public R<ParamVersionVO> getById(@PathVariable Long id) {
        return R.ok(ParamVersionVO.from(paramVersionService.getById(id)));
    }
}
