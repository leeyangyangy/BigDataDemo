package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.StandardChangeLog;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.StandardChangeLogService;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/admin/param-version")
@RequiredArgsConstructor
public class AdminParamVersionController {

    private final ParamVersionService paramVersionService;
    private final StandardChangeLogService changeLogService;

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

    @PostMapping("/create")
    public R<ParamVersion> createNewVersion(@RequestBody CreateRequest req) {
        ParamVersion newVersion = new ParamVersion();
        newVersion.setParamId(req.getParamId());
        newVersion.setProductId(req.getProductId());
        newVersion.setUsl(req.getUsl());
        newVersion.setLsl(req.getLsl());
        newVersion.setTarget(req.getTarget());
        newVersion.setUcl(req.getUcl());
        newVersion.setLcl(req.getLcl());
        newVersion.setCl(req.getCl());
        newVersion.setChartType(req.getChartType() != null ? req.getChartType() : "I_MR");
        newVersion.setChangeReason(req.getChangeReason());
        newVersion.setChangeType(req.getChangeType());

        ParamVersion oldVersion = (newVersion.getProductId() != null)
                ? paramVersionService.getCurrentVersion(newVersion.getParamId(), newVersion.getProductId())
                : paramVersionService.getAnyCurrentVersion(newVersion.getParamId());

        ParamVersion created = paramVersionService.createNewVersion(newVersion);

        changeLogService.recordChange(oldVersion, created,
                newVersion.getChangeType() != null ? newVersion.getChangeType() : "LIMIT_ADJUST",
                newVersion.getChangeReason(),
                true);

        log.info("[Admin] 创建标准版本: id={} paramId={}", created.getId(), created.getParamId());
        return R.ok(created);
    }

    @PutMapping("/{id}/enable")
    public R<Boolean> enableVersion(@PathVariable Long id) {
        log.info("[Admin] 启用标准版本: id={}", id);
        return R.ok(paramVersionService.enableVersion(id));
    }

    @PutMapping("/{id}/disable")
    public R<Boolean> disableVersion(@PathVariable Long id) {
        log.info("[Admin] 停用标准版本: id={}", id);
        return R.ok(paramVersionService.disableVersion(id));
    }

    @PutMapping("/{id}")
    public R<Boolean> updateVersion(@PathVariable Long id, @RequestBody UpdateRequest req) {
        log.info("[Admin] 更新标准版本: id={}", id);

        ParamVersion updated = new ParamVersion();
        updated.setId(id);
        if (req.getUsl() != null) updated.setUsl(req.getUsl());
        if (req.getLsl() != null) updated.setLsl(req.getLsl());
        if (req.getTarget() != null) updated.setTarget(req.getTarget());
        if (req.getUcl() != null) updated.setUcl(req.getUcl());
        if (req.getLcl() != null) updated.setLcl(req.getLcl());
        if (req.getCl() != null) updated.setCl(req.getCl());
        if (req.getChartType() != null) updated.setChartType(req.getChartType());
        if (req.getStatus() != null) updated.setStatus(req.getStatus());

        return R.ok(paramVersionService.updateVersion(id, updated));
    }

    @DeleteMapping("/{id}")
    public R<Boolean> deleteVersion(@PathVariable Long id) {
        log.info("[Admin] 删除标准版本: id={}", id);
        return R.ok(paramVersionService.deleteVersion(id));
    }

    @Data
    public static class CreateRequest {
        private Long paramId;
        private Long productId;
        private java.math.BigDecimal usl;
        private java.math.BigDecimal lsl;
        private java.math.BigDecimal target;
        private java.math.BigDecimal ucl;
        private java.math.BigDecimal lcl;
        private java.math.BigDecimal cl;
        private String chartType;
        private String changeReason;
        private String changeType;
    }

    @Data
    public static class UpdateRequest {
        private java.math.BigDecimal usl;
        private java.math.BigDecimal lsl;
        private java.math.BigDecimal target;
        private java.math.BigDecimal ucl;
        private java.math.BigDecimal lcl;
        private java.math.BigDecimal cl;
        private String chartType;
        private Integer status;
    }
}
