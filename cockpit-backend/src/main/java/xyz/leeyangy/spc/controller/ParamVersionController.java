package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.service.ParamVersionService;

import java.util.List;

@RestController
@RequestMapping("/api/spc/param-version")
@RequiredArgsConstructor
public class ParamVersionController {

    private final ParamVersionService paramVersionService;

    @GetMapping("/current")
    public R<ParamVersion> getCurrentVersion(@RequestParam Long paramId, @RequestParam(required = false) Long productId) {
        if (productId != null) {
            return R.ok(paramVersionService.getCurrentVersion(paramId, productId));
        }
        return R.ok(paramVersionService.getAnyCurrentVersion(paramId));
    }

    @GetMapping("/history")
    public R<List<ParamVersion>> getVersionHistory(@RequestParam Long paramId, @RequestParam(required = false) Long productId) {
        return R.ok(paramVersionService.getVersionHistory(paramId, productId));
    }

    @GetMapping("/page")
    public R<Page<ParamVersion>> pageVersions(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "10") Integer size,
            @RequestParam(required = false) Long paramId,
            @RequestParam(required = false) Long productId) {
        return R.ok(paramVersionService.pageVersions(new Page<>(current, size), paramId, productId));
    }

    @GetMapping("/{id}")
    public R<ParamVersion> getById(@PathVariable Long id) {
        return R.ok(paramVersionService.getById(id));
    }
}
