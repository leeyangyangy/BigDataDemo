package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.dto.ParamVersionCreateDTO;
import xyz.leeyangy.spc.dto.ParamVersionUpdateDTO;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.StandardChangeLogService;
import xyz.leeyangy.spc.vo.ParamVersionVO;

import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/api/admin/param-version")
@RequiredArgsConstructor
public class AdminParamVersionController {

    private final ParamVersionService paramVersionService;
    private final StandardChangeLogService changeLogService;

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

    @PostMapping("/create")
    public R<ParamVersionVO> createNewVersion(@Valid @RequestBody ParamVersionCreateDTO req) {
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
        newVersion.setSubgroupSize(req.getSubgroupSize() != null ? req.getSubgroupSize(): 5);

        ParamVersion oldVersion = (newVersion.getProductId() != null)
                ? paramVersionService.getCurrentVersion(newVersion.getParamId(), newVersion.getProductId())
                : paramVersionService.getAnyCurrentVersion(newVersion.getParamId());

        ParamVersion created = paramVersionService.createNewVersion(newVersion);

        changeLogService.recordChange(oldVersion, created,
                newVersion.getChangeType() != null ? newVersion.getChangeType() : "LIMIT_ADJUST",
                newVersion.getChangeReason(),
                true);

        log.info("[Admin] 创建标准版本: id={} paramId={}", created.getId(), created.getParamId());
        return R.ok(ParamVersionVO.from(created));
    }

    @PutMapping("/{id}/enable")
    public R<Boolean> enableVersion(@PathVariable Long id) {
        log.info("[Admin] 启用标准版本: id={}", id);
        boolean ok = paramVersionService.enableVersion(id);
        if (ok) {
            ParamVersion v = paramVersionService.getById(id);
            try {
                changeLogService.recordVersionSwitch(v, "VERSION_SWITCH", "管理员切换生效版本");
            } catch (Exception e) {
                log.warn("[Admin] 记录版本切换日志失败(不影响操作): {}", e.getMessage());
            }
        }
        return R.ok(ok);
    }

    @PutMapping("/{id}/disable")
    public R<Boolean> disableVersion(@PathVariable Long id) {
        log.info("[Admin] 停用标准版本: id={}", id);
        boolean ok = paramVersionService.disableVersion(id);
        if (ok) {
            ParamVersion v = paramVersionService.getById(id);
            try {
                changeLogService.recordVersionSwitch(v, "VERSION_DISABLE", "管理员停用版本");
            } catch (Exception e) {
                log.warn("[Admin] 记录版本停用日志失败(不影响操作): {}", e.getMessage());
            }
        }
        return R.ok(ok);
    }

    @PutMapping("/{id}")
    public R<Boolean> updateVersion(@PathVariable Long id, @Valid @RequestBody ParamVersionUpdateDTO req) {
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
        if (req.getSubgroupSize() != null) updated.setSubgroupSize(req.getSubgroupSize());

        boolean ok = paramVersionService.updateVersion(id, updated);

        if (ok) {
            ParamVersion v = paramVersionService.getById(id);
            try {
                changeLogService.recordVersionSwitch(v, "VERSION_UPDATE", req.getChangeReason() != null ? req.getChangeReason() : "管理员更新规格限");
            } catch (Exception e) {
                log.warn("[Admin] 记录版本更新日志失败(不影响操作): {}", e.getMessage());
            }
        }

        return R.ok(ok);
    }

    @DeleteMapping("/{id}")
    public R<Boolean> deleteVersion(@PathVariable Long id) {
        log.info("[Admin] 删除标准版本: id={}", id);
        return R.ok(paramVersionService.deleteVersion(id));
    }
}
