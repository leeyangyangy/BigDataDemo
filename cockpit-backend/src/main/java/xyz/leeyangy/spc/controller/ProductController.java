package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.PageConvert;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.annotation.OperationLog;
import xyz.leeyangy.spc.dto.ProductCreateDTO;
import xyz.leeyangy.spc.dto.ProductProcessBindDTO;
import xyz.leeyangy.spc.entity.Product;
import xyz.leeyangy.spc.service.ProductProcessService;
import xyz.leeyangy.spc.service.ProductService;
import xyz.leeyangy.spc.vo.ProcessBindingVO;
import xyz.leeyangy.spc.vo.ProductVO;

import javax.validation.Valid;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/product")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;
    private final ProductProcessService productProcessService;

    @OperationLog(module = "PRODUCT", action = "QUERY", targetType = "Product",
            content = "'查询产品列表 count=' + #result.data.size()")
    @GetMapping("/list")
    public R<List<ProductVO>> list() {
        return R.ok(productService.listActive().stream()
                .map(ProductVO::from)
                .collect(Collectors.toList()));
    }

    @GetMapping("/page")
    public R<Page<ProductVO>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword) {
        return R.ok(PageConvert.convert(productService.page(new Page<>(current, size), keyword), ProductVO::from));
    }

    @PostMapping
    public R<ProductVO> create(@Valid @RequestBody ProductCreateDTO dto) {
        Product product = new Product();
        product.setProductCode(dto.getProductCode());
        product.setProductName(dto.getProductName());
        product.setProductType(dto.getProductType());
        product.setSpecification(dto.getSpecification());
        product.setStatus(dto.getStatus());
        productService.save(product);
        return R.ok(ProductVO.from(product));
    }

    @GetMapping("/{productId}/processes")
    public R<List<ProcessBindingVO>> getProductProcesses(@PathVariable Long productId) {
        List<ProcessBindingVO> bindings = productProcessService.getProductProcessBindings(productId);
        return R.ok(bindings);
    }

    @PostMapping("/{productId}/processes/bind")
    public R<Void> bindProductProcesses(@PathVariable Long productId,
                                        @Valid @RequestBody ProductProcessBindDTO dto) {
        productProcessService.bindProcesses(productId,
                dto.getItems().stream()
                        .map(item -> new ProductProcessService.BindItem() {{
                            setProcessId(item.getProcessId());
                        }}).collect(Collectors.toList()));
        return R.ok(null);
    }

    @DeleteMapping("/{productId}/processes/{processId}")
    public R<Void> unbindProductProcess(@PathVariable Long productId,
                                        @PathVariable Long processId) {
        productProcessService.unbindProcess(productId, processId);
        return R.ok(null);
    }
}
