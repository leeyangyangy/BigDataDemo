package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.ProductProcess;
import xyz.leeyangy.spc.mapper.ProductProcessMapper;
import xyz.leeyangy.spc.service.ProductProcessService;
import xyz.leeyangy.spc.vo.ProcessBindingVO;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductProcessServiceImpl extends ServiceImpl<ProductProcessMapper, ProductProcess> implements ProductProcessService {

    @Override
    public List<Long> getProcessIdsByProductId(Long productId) {
        return list(new LambdaQueryWrapper<ProductProcess>()
                .eq(ProductProcess::getProductId, productId)
                .orderByAsc(ProductProcess::getSortOrder))
                .stream()
                .map(ProductProcess::getProcessId)
                .collect(Collectors.toList());
    }

    @Override
    public List<ProductProcess> getBindingsByProductId(Long productId) {
        return list(new LambdaQueryWrapper<ProductProcess>()
                .eq(ProductProcess::getProductId, productId)
                .orderByAsc(ProductProcess::getSortOrder));
    }

    @Override
    public List<ProcessBindingVO> getProductProcessBindings(Long productId) {
        List<ProductProcess> entities = getBindingsByProductId(productId);
        return entities.stream().map(entity -> {
            ProcessBindingVO vo = new ProcessBindingVO();
            vo.setProcessId(entity.getProcessId());
            vo.setSortOrder(entity.getSortOrder());
            return vo;
        }).collect(Collectors.toList());
    }

    @Override
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

    @Override
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

    @Override
    public boolean unbindProcess(Long productId, Long processId) {
        return baseMapper.physicalDelete(productId, processId) > 0;
    }
}
