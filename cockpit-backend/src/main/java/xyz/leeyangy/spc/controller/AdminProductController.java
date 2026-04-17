package xyz.leeyangy.spc.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import xyz.leeyangy.spc.common.R;
import xyz.leeyangy.spc.common.StatusCode;
import xyz.leeyangy.spc.entity.Product;
import xyz.leeyangy.spc.service.ProductService;

@Slf4j
@RestController
@RequestMapping("/api/admin/product")
@RequiredArgsConstructor
public class AdminProductController {

    private final ProductService productService;

    @GetMapping("/page")
    public R<Page<Product>> page(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) Integer status) {
        Page<Product> page = new Page<>(current, size);
        LambdaQueryWrapper<Product> wrapper = new LambdaQueryWrapper<Product>()
                .and(keyword != null && !keyword.isEmpty(), w -> w
                        .like(Product::getProductCode, keyword)
                        .or().like(Product::getProductName, keyword)
                        .or().like(Product::getProductType, keyword))
                .eq(status != null, Product::getStatus, status)
                .orderByDesc(Product::getCreatedAt);
        return R.ok(productService.page(page, wrapper));
    }

    @GetMapping("/{id}")
    public R<Product> getById(@PathVariable Long id) {
        Product product = productService.getById(id);
        if (product == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "产品不存在");
        }
        return R.ok(product);
    }

    @PostMapping
    public R<Product> create(@RequestBody ProductCreateRequest req) {
        if (req.getProductCode() == null || req.getProductCode().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "产品编码不能为空");
        }
        if (req.getProductName() == null || req.getProductName().trim().isEmpty()) {
            return R.fail(StatusCode.PARAM_REQUIRED, "产品名称不能为空");
        }

        long count = productService.count(new LambdaQueryWrapper<Product>()
                .eq(Product::getProductCode, req.getProductCode())
                .eq(Product::getDeleted, 0));
        if (count > 0) {
            return R.fail(StatusCode.PRODUCT_EXISTS, "产品编码已存在");
        }

        Product product = new Product();
        product.setProductCode(req.getProductCode().trim());
        product.setProductName(req.getProductName().trim());
        product.setProductType(req.getProductType());
        product.setSpecification(req.getSpecification());
        product.setStatus(req.getStatus() != null ? req.getStatus() : 1);

        productService.save(product);
        log.info("[Admin] 创建产品: code={} name={}", product.getProductCode(), product.getProductName());
        return R.ok("创建成功", product);
    }

    @PutMapping("/{id}")
    public R<Product> update(@PathVariable Long id, @RequestBody ProductUpdateRequest req) {
        Product existProduct = productService.getById(id);
        if (existProduct == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "产品不存在");
        }

        if (req.getProductName() != null) {
            existProduct.setProductName(req.getProductName().trim());
        }
        if (req.getProductType() != null) {
            existProduct.setProductType(req.getProductType());
        }
        if (req.getSpecification() != null) {
            existProduct.setSpecification(req.getSpecification());
        }
        if (req.getStatus() != null) {
            existProduct.setStatus(req.getStatus());
        }

        productService.updateById(existProduct);
        log.info("[Admin] 更新产品: id={} code={}", id, existProduct.getProductCode());
        return R.ok("更新成功", existProduct);
    }

    @DeleteMapping("/{id}")
    public R<Void> delete(@PathVariable Long id) {
        Product product = productService.getById(id);
        if (product == null) {
            return R.fail(StatusCode.DATA_NOT_FOUND, "产品不存在");
        }
        product.setDeleted(1);
        productService.updateById(product);
        log.info("[Admin] 删除产品: id={} code={}", id, product.getProductCode());
        return R.ok(null);
    }

    @Data
    public static class ProductCreateRequest {
        private String productCode;
        private String productName;
        private String productType;
        private String specification;
        private Integer status;
    }

    @Data
    public static class ProductUpdateRequest {
        private String productName;
        private String productType;
        private String specification;
        private Integer status;
    }
}
