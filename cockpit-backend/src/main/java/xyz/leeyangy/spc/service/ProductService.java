package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.Product;

import java.util.List;

/**
 * 产品 Service 接口
 */
public interface ProductService extends IService<Product> {

    List<Product> listActive();

    Page<Product> page(Page<Product> page, String keyword);
}
