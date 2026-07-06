package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.vo.SpcDataDetailVO;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

/**
 * SPC 数据 Service 接口
 */
public interface SpcDataService extends IService<SpcData> {

    SpcData uploadData(SpcData data, String role);

    List<SpcData> batchUpload(List<SpcData> dataList, String role);

    /**
     * 分页查询 SPC 数据（不带车间过滤，兼容后台/内部调用）。
     */
    default Page<SpcData> pageByCondition(Page<SpcData> page,
                                          Long paramVersionId, String batchId,
                                          Long productId, Long paramId,
                                          LocalDateTime startTime, LocalDateTime endTime) {
        return pageByCondition(page, paramVersionId, batchId, productId, paramId, startTime, endTime, null);
    }

    /**
     * 分页查询 SPC 数据（带车间可见性过滤）。
     *
     * <p>车间过滤语义：
     * <ul>
     *   <li>{@code workshopIds == null}：不限制（ADMIN 可见全部）</li>
     *   <li>{@code workshopIds 为空集合}：返回空页（用户无任何车间绑定）</li>
     *   <li>{@code workshopIds 非空}：仅返回这些车间下工序产生的数据</li>
     * </ul>
     */
    Page<SpcData> pageByCondition(Page<SpcData> page,
                                   Long paramVersionId, String batchId,
                                   Long productId, Long paramId,
                                   LocalDateTime startTime, LocalDateTime endTime,
                                   Collection<Long> workshopIds);

    /**
     * 查询最近采集数据（不带车间过滤，兼容内部调用）。
     */
    default List<SpcData> listRecentData(Long paramVersionId, int limit, LocalDateTime startTime, LocalDateTime endTime) {
        return listRecentData(paramVersionId, limit, startTime, endTime, null);
    }

    /**
     * 查询最近采集数据（带车间可见性过滤）。
     *
     * <p>车间过滤语义同 {@link #pageByCondition}。
     */
    List<SpcData> listRecentData(Long paramVersionId, int limit, LocalDateTime startTime, LocalDateTime endTime,
                                  Collection<Long> workshopIds);

    /**
     * 后台分页联查：携带产品/工序/参数/设备/参数版本的名称等展示字段（不带车间过滤，兼容后台调用）。
     *
     * <p>采用 cache-aside：先查 Redis 缓存，命中直接返回；未命中回源 DB 后回写缓存。
     * 缓存 key 由全部查询条件拼接，TTL 由实现决定。
     */
    default Page<SpcDataDetailVO> pageDetail(Page<SpcDataDetailVO> page,
                                              Long paramVersionId, String batchId,
                                              Long productId, Long paramId,
                                              Long processId, Long equipmentId,
                                              Integer isOoc, Integer isOos,
                                              String dataSource,
                                              LocalDateTime startTime, LocalDateTime endTime) {
        return pageDetail(page, paramVersionId, batchId, productId, paramId,
                processId, equipmentId, isOoc, isOos, dataSource, startTime, endTime, null);
    }

    /**
     * 后台分页联查（带车间可见性过滤）。
     *
     * <p>车间过滤语义同 {@link #pageByCondition}。
     */
    Page<SpcDataDetailVO> pageDetail(Page<SpcDataDetailVO> page,
                                      Long paramVersionId, String batchId,
                                      Long productId, Long paramId,
                                      Long processId, Long equipmentId,
                                      Integer isOoc, Integer isOos,
                                      String dataSource,
                                      LocalDateTime startTime, LocalDateTime endTime,
                                      Collection<Long> workshopIds);

    /**
     * 删除单条采集数据：先清除列表缓存，再执行删除。
     */
    boolean deleteData(Long id);

    /**
     * 批量删除采集数据：先清除列表缓存，再执行删除。
     */
    boolean batchDeleteData(List<Long> ids);
}
