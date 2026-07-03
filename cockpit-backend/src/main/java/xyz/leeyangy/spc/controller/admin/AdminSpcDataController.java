package xyz.leeyangy.spc.controller.admin;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.vo.SpcDataDetailVO;
import xyz.leeyangy.spc.vo.SpcDataVO;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 后台 SPC 数据管理 Controller
 *
 * <p>提供采集数据的分页查询（多表联查携带名称）、详情、删除、批量删除功能。
 * 管理员可按产品/参数/工序/批次/时间范围/OOC/OOS状态筛选数据。
 */
@Slf4j
@RestController
@RequestMapping("/api/admin/spc-data")
@RequiredArgsConstructor
public class AdminSpcDataController {

    private final SpcDataService spcDataService;

    /**
     * 分页查询 SPC 采集数据（多表联查，携带产品/工序/参数/设备/版本名称）
     */
    @GetMapping("/page")
    public R<Page<SpcDataDetailVO>> pageData(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) Long paramVersionId,
            @RequestParam(required = false) String batchId,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) Long paramId,
            @RequestParam(required = false) Long processId,
            @RequestParam(required = false) Long equipmentId,
            @RequestParam(required = false) Integer isOoc,
            @RequestParam(required = false) Integer isOos,
            @RequestParam(required = false) String dataSource,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {

        Page<SpcDataDetailVO> page = new Page<>(current, size);
        // 使用返回值：缓存命中时返回的是反序列化对象，未命中时为原地填充的 page
        Page<SpcDataDetailVO> result = spcDataService.pageDetail(page,
                paramVersionId, batchId, productId, paramId,
                processId, equipmentId, isOoc, isOos,
                dataSource, startTime, endTime);
        return R.ok(result);
    }

    /**
     * 查询数据详情
     */
    @GetMapping("/{id}")
    public R<SpcDataVO> getById(@PathVariable Long id) {
        SpcData data = spcDataService.getById(id);
        return R.ok(SpcDataVO.from(data));
    }

    /**
     * 删除单条数据（软删除）：先清列表缓存再删除
     */
    @DeleteMapping("/{id}")
    public R<Boolean> deleteData(@PathVariable Long id) {
        log.info("[Admin] 删除SPC数据: id={}", id);
        return R.ok(spcDataService.deleteData(id));
    }

    /**
     * 批量删除数据（软删除）：先清列表缓存再删除
     */
    @PostMapping("/batch-delete")
    public R<Boolean> batchDelete(@RequestBody List<Long> ids) {
        log.info("[Admin] 批量删除SPC数据: count={}", ids != null ? ids.size() : 0);
        if (ids == null || ids.isEmpty()) {
            return R.ok(false);
        }
        return R.ok(spcDataService.batchDeleteData(ids));
    }
}
