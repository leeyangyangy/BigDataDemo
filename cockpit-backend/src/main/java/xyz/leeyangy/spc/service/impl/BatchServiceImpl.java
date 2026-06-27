package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import xyz.leeyangy.spc.common.exception.ResourceNotFoundException;
import xyz.leeyangy.spc.entity.Batch;
import xyz.leeyangy.spc.mapper.BatchMapper;
import xyz.leeyangy.spc.service.BatchService;

import java.util.List;

@Service
public class BatchServiceImpl extends ServiceImpl<BatchMapper, Batch> implements BatchService {

    @Override
    public List<Batch> listByProductAndEquipment(Long productId, Long processId) {
        LambdaQueryWrapper<Batch> wrapper = new LambdaQueryWrapper<Batch>()
                .eq(Batch::getDeleted, 0)
                .eq(Batch::getStatus, 1)
                .orderByDesc(Batch::getCreatedAt);
        if (productId != null) {
            wrapper.eq(Batch::getProductId, productId);
        }
        if (processId != null) {
            wrapper.eq(Batch::getProcessId, processId);
        }
        return list(wrapper);
    }

    @Override
    public Page<Batch> page(Page<Batch> page, Long productId, String keyword) {
        LambdaQueryWrapper<Batch> wrapper = new LambdaQueryWrapper<Batch>()
                .eq(Batch::getDeleted, 0)
                .eq(productId != null, Batch::getProductId, productId)
                .and(StringUtils.hasText(keyword), w -> w
                        .like(Batch::getBatchCode, keyword))
                .orderByDesc(Batch::getCreatedAt);
        return page(page, wrapper);
    }

    @Override
    public Batch getByCode(String batchCode) {
        return getOne(new LambdaQueryWrapper<Batch>()
                .eq(Batch::getBatchCode, batchCode)
                .eq(Batch::getDeleted, 0));
    }

    @Override
    public Page<Batch> pageByCondition(Page<Batch> page, Long productId, Long processId, String batchStatus) {
        return page(page, new LambdaQueryWrapper<Batch>()
                .eq(productId != null, Batch::getProductId, productId)
                .eq(processId != null, Batch::getProcessId, processId)
                .eq(batchStatus != null, Batch::getBatchStatus, batchStatus)
                .eq(Batch::getDeleted, 0)
                .orderByDesc(Batch::getCreatedAt));
    }

    @Override
    public Batch createBatch(Batch batch) {
        batch.setStatus(1);
        save(batch);
        return batch;
    }

    @Override
    public Batch updateBatch(Long id, Batch batch) {
        batch.setId(id);
        updateById(batch);
        return batch;
    }

    @Override
    public boolean deleteBatch(Long id) {
        Batch exist = getById(id);
        if (exist == null) {
            throw new ResourceNotFoundException("批次", id);
        }
        return removeById(id);
    }
}
