package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.vo.SpcDataDetailVO;

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

    /**
     * 后台分页联查：携带产品/工序/参数/设备/参数版本的名称等展示字段。
     *
     * <p>采用 cache-aside：先查 Redis 缓存，命中直接返回；未命中回源 DB 后回写缓存。
     * 缓存 key 由全部查询条件拼接，TTL 由实现决定。
     */
    Page<SpcDataDetailVO> pageDetail(Page<SpcDataDetailVO> page,
                                      Long paramVersionId, String batchId,
                                      Long productId, Long paramId,
                                      Long processId, Long equipmentId,
                                      Integer isOoc, Integer isOos,
                                      String dataSource,
                                      LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 删除单条采集数据：先清除列表缓存，再执行删除。
     */
    boolean deleteData(Long id);

    /**
     * 批量删除采集数据：先清除列表缓存，再执行删除。
     */
    boolean batchDeleteData(List<Long> ids);
}
