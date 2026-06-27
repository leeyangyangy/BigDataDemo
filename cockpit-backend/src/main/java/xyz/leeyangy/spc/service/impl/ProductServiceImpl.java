package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import xyz.leeyangy.spc.entity.Product;
import xyz.leeyangy.spc.mapper.ProductMapper;
import xyz.leeyangy.spc.service.ProductService;

import java.util.List;

@Service
public class ProductServiceImpl extends ServiceImpl<ProductMapper, Product> implements ProductService {

    @Override
    public List<Product> listActive() {
        return list(new LambdaQueryWrapper<Product>()
                .eq(Product::getStatus, 1)
                .eq(Product::getDeleted, 0)
                .orderByAsc(Product::getProductName));
    }

    @Override
    public Page<Product> page(Page<Product> page, String keyword) {
        LambdaQueryWrapper<Product> wrapper = new LambdaQueryWrapper<Product>()
                .eq(Product::getDeleted, 0)
                .and(StringUtils.hasText(keyword), w -> w
                        .like(Product::getProductCode, keyword)
                        .or()
                        .like(Product::getProductName, keyword))
                .orderByDesc(Product::getCreatedAt);
        return page(page, wrapper);
    }
}
