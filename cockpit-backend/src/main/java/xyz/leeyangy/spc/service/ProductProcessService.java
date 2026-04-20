package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.ProductProcess;
import xyz.leeyangy.spc.mapper.ProductProcessMapper;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductProcessService extends ServiceImpl<ProductProcessMapper, ProductProcess> {

    public List<Long> getProcessIdsByProductId(Long productId) {
        return list(new LambdaQueryWrapper<ProductProcess>()
                .eq(ProductProcess::getProductId, productId)
                .orderByAsc(ProductProcess::getSortOrder))
                .stream()
                .map(ProductProcess::getProcessId)
                .collect(Collectors.toList());
    }

    public List<ProductProcess> getBindingsByProductId(Long productId) {
        return list(new LambdaQueryWrapper<ProductProcess>()
                .eq(ProductProcess::getProductId, productId)
                .orderByAsc(ProductProcess::getSortOrder));
    }

    public boolean bindProcesses(Long productId, List<BindItem> items) {
        baseMapper.physicalDeleteByProductId(productId);
        if (items == null || items.isEmpty()) return true;
        int sort = 0;
        for (BindItem item : items) {
            ProductProcess pp = new ProductProcess();
            pp.setProductId(productId);
            pp.setProcessId(item.getProcessId());
            pp.setSortOrder(sort++);
            save(pp);
        }
        return true;
    }

    public boolean bindProcess(Long productId, Long processId) {
        long count = count(new LambdaQueryWrapper<ProductProcess>()
                .eq(ProductProcess::getProductId, productId)
                .eq(ProductProcess::getProcessId, processId));
        if (count > 0) return true;
        ProductProcess pp = new ProductProcess();
        pp.setProductId(productId);
        pp.setProcessId(processId);
        pp.setSortOrder((int) count(new LambdaQueryWrapper<ProductProcess>().eq(ProductProcess::getProductId, productId)));
        return save(pp);
    }

    public boolean unbindProcess(Long productId, Long processId) {
        return baseMapper.physicalDelete(productId, processId) > 0;
    }

    @lombok.Data
    public static class BindItem {
        private Long processId;
    }
}
