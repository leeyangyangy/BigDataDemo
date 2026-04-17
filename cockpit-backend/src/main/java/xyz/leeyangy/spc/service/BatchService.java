package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.Batch;
import xyz.leeyangy.spc.mapper.BatchMapper;

@Service
@RequiredArgsConstructor
public class BatchService extends ServiceImpl<BatchMapper, Batch> {

    public Batch getByCode(String batchCode) {
        return getOne(new LambdaQueryWrapper<Batch>()
                .eq(Batch::getBatchCode, batchCode)
                .eq(Batch::getDeleted, 0));
    }

    public Page<Batch> pageByCondition(Page<Batch> page, Long productId, Long processId, String batchStatus) {
        return page(page, new LambdaQueryWrapper<Batch>()
                .eq(productId != null, Batch::getProductId, productId)
                .eq(processId != null, Batch::getProcessId, processId)
                .eq(batchStatus != null, Batch::getBatchStatus, batchStatus)
                .eq(Batch::getDeleted, 0)
                .orderByDesc(Batch::getCreatedAt));
    }
}
