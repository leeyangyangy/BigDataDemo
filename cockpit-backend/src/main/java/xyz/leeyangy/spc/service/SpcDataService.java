package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.SpcData;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SPC 数据 Service 接口
 */
public interface SpcDataService extends IService<SpcData> {

    SpcData uploadData(SpcData data, String role);

    List<SpcData> batchUpload(List<SpcData> dataList, String role);

    Page<SpcData> pageByCondition(Page<SpcData> page,
                                   Long paramVersionId, String batchId,
                                   Long productId, Long paramId,
                                   LocalDateTime startTime, LocalDateTime endTime);

    List<SpcData> listRecentData(Long paramVersionId, int limit, LocalDateTime startTime, LocalDateTime endTime);
}
