package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.StandardChangeLog;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.StandardChangeLogService;

import java.util.List;

@RestController
@RequestMapping("/api/spc/param-version")
@RequiredArgsConstructor
public class ParamVersionController {

    private final ParamVersionService paramVersionService;
    private final StandardChangeLogService changeLogService;

    @GetMapping("/current")
    public R<ParamVersion> getCurrentVersion(@RequestParam Long paramId, @RequestParam Long productId) {
        return R.ok(paramVersionService.getCurrentVersion(paramId, productId));
    }

    @GetMapping("/history")
    public R<List<ParamVersion>> getVersionHistory(@RequestParam Long paramId, @RequestParam Long productId) {
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

    @PostMapping("/create")
    public R<ParamVersion> createNewVersion(@RequestBody ParamVersion newVersion) {
        ParamVersion oldVersion = paramVersionService.getCurrentVersion(
                newVersion.getParamId(), newVersion.getProductId());

        ParamVersion created = paramVersionService.createNewVersion(newVersion);

        changeLogService.recordChange(oldVersion, created,
                newVersion.getChangeType() != null ? newVersion.getChangeType() : "LIMIT_ADJUST",
                newVersion.getChangeReason(),
                true);

        return R.ok(created);
    }
}
