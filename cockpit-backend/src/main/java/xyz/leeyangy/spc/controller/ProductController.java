package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.entity.Product;
import xyz.leeyangy.spc.entity.ProductProcess;
import xyz.leeyangy.spc.service.ProcessService;
import xyz.leeyangy.spc.service.ProductProcessService;
import xyz.leeyangy.spc.service.ProductService;

import java.util.List;

@RestController
@RequestMapping("/api/spc/product")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;
    private final ProductProcessService productProcessService;
    private final ProcessService processService;

    @GetMapping("/page")
    public R<Page<Product>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size) {
        return R.ok(productService.page(new Page<>(current, size)));
    }

    @PostMapping
    public R<Product> create(@RequestBody Product product) {
        productService.save(product);
        return R.ok(product);
    }

    @GetMapping("/{id}")
    public R<Product> getById(@PathVariable Long id) {
        return R.ok(productService.getById(id));
    }

    @GetMapping("/{id}/processes")
    public R<List<ProductProcess>> getProcesses(@PathVariable Long id) {
        return R.ok(productProcessService.getBindingsByProductId(id));
    }

    @GetMapping("/{id}/process-ids")
    public R<List<Long>> getProcessIds(@PathVariable Long id) {
        return R.ok(productProcessService.getProcessIdsByProductId(id));
    }

    @PostMapping("/{id}/processes/bind")
    public R<Void> bindProcesses(
            @PathVariable Long id,
            @RequestBody List<ProductProcessService.BindItem> items) {
        if (productService.getById(id) == null) {
            return R.fail("产品不存在");
        }
        productProcessService.bindProcesses(id, items);
        return R.ok();
    }

    @PostMapping("/{id}/processes/{processId}/bind")
    public R<Void> bindProcess(
            @PathVariable Long id,
            @PathVariable Long processId) {
        if (productService.getById(id) == null) return R.fail("产品不存在");
        if (processService.getById(processId) == null) return R.fail("工序不存在");
        productProcessService.bindProcess(id, processId);
        return R.ok();
    }

    @DeleteMapping("/{id}/processes/{processId}")
    public R<Void> unbindProcess(
            @PathVariable Long id,
            @PathVariable Long processId) {
        boolean ok = productProcessService.unbindProcess(id, processId);
        return ok ? R.ok() : R.fail("解绑失败");
    }
}
