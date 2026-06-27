package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.Batch;

import java.util.List;

/**
 * 批次 Service 接口
 */
public interface BatchService extends IService<Batch> {

    List<Batch> listByProductAndEquipment(Long productId, Long processId);

    Page<Batch> page(Page<Batch> page, Long productId, String keyword);

    Batch getByCode(String batchCode);

    Page<Batch> pageByCondition(Page<Batch> page, Long productId, Long processId, String batchStatus);

    Batch createBatch(Batch batch);

    Batch updateBatch(Long id, Batch batch);

    boolean deleteBatch(Long id);
}
