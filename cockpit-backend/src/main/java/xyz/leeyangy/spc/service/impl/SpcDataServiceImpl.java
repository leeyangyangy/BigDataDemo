package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import xyz.leeyangy.spc.common.StatusMsg;
import xyz.leeyangy.spc.common.constants.RoleConstants;
import xyz.leeyangy.spc.common.exception.BusinessStateException;
import xyz.leeyangy.spc.common.exception.ParamValidationException;
import xyz.leeyangy.spc.common.exception.ResourceNotFoundException;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.Process;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.mapper.ProcessMapper;
import xyz.leeyangy.spc.mapper.SpcDataMapper;
import xyz.leeyangy.spc.mapper.SpcStatResultMapper;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.service.SpcRuleEngine;
import xyz.leeyangy.spc.vo.SpcDataDetailVO;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpcDataServiceImpl extends ServiceImpl<SpcDataMapper, SpcData> implements SpcDataService {

    private final ParamVersionService paramVersionService;
    private final StringRedisTemplate redisTemplate;
    private final SpcRuleEngine spcRuleEngine;
    private final ObjectMapper objectMapper;
    private final SpcStatResultMapper spcStatResultMapper;
    private final ProcessMapper processMapper;

    private static final String IDEMPOTENT_PREFIX = "cockpit:idempotent:";
    private static final String LATEST_DATA_PREFIX = "cockpit:latest:";

    /** 后台 cockpit 数据列表缓存 key 前缀 */
    private static final String LIST_CACHE_PREFIX = "cockpit:admin:data:page:";
    /** 列表缓存 TTL */
    private static final Duration LIST_CACHE_TTL = Duration.ofMinutes(5);
    /** 图表查询缓存 key 前缀（与 cockpitChartController 中保持一致） */
    private static final String CHART_CACHE_PREFIX = "cockpit:chart:";

    /** 列表缓存值结构（仅缓存必要字段，避免序列化 MyBatis-Plus Page 内部字段） */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class PageCache {
        private List<SpcDataDetailVO> records;
        private long total;
        private long current;
        private long size;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public SpcData uploadData(SpcData data, String role) {
        if (data.getParamId() == null || data.getParamId() <= 0) {
            throw new ParamValidationException(StatusMsg.PARAM_REQUIRED_FOR_DATA);
        }
        if (data.getProductId() == null || data.getProductId() <= 0) {
            throw new ParamValidationException(StatusMsg.PRODUCT_REQUIRED_FOR_DATA);
        }
        if (data.getProcessId() == null || data.getProcessId() <= 0) {
            throw new ParamValidationException(StatusMsg.PROCESS_REQUIRED_FOR_DATA);
        }
        if (data.getEquipmentId() == null || data.getEquipmentId() <= 0) {
            throw new ParamValidationException(StatusMsg.EQUIPMENT_ID_REQUIRED);
        }
        if (data.getMeasuredValue() == null) {
            throw new ParamValidationException(StatusMsg.MEASURE_VALUE_REQUIRED);
        }

        if (data.getMsgId() != null) {
            Boolean isNew = redisTemplate.opsForValue().setIfAbsent(
                    IDEMPOTENT_PREFIX + data.getMsgId(), "1", Duration.ofHours(24));
            if (isNew != null && !isNew) {
                log.warn("[DataUpload] 重复消息: msgId={}", data.getMsgId());
                return null;
            }
        }

        ParamVersion version = null;

        if (data.getParamVersionId() != null) {
            version = paramVersionService.getById(data.getParamVersionId());
            if (version == null) {
                throw new ResourceNotFoundException("参数标准版本", data.getParamVersionId());
            }
            log.info("[DataUpload] 使用前端指定版本: paramVersionId={}", data.getParamVersionId());
        } else {
            version = paramVersionService.getCurrentVersion(
                    data.getParamId(), data.getProductId());
            if (version == null) {
                if (RoleConstants.ADMIN.equals(role) || RoleConstants.ENGINEER.equals(role)) {
                    log.info("[DataUpload] 自动创建默认参数版本: paramId={} productId={} role={}", data.getParamId(), data.getProductId(), role);
                    ParamVersion defaultVersion = new ParamVersion();
                    defaultVersion.setParamId(data.getParamId());
                    defaultVersion.setProductId(data.getProductId() != null ? data.getProductId() : 0L);
                    defaultVersion.setVersionNo(1);
                    defaultVersion.setIsCurrent(1);
                    defaultVersion.setDeleted(0);
                    defaultVersion.setStatus(1);
                    defaultVersion.setEffectiveFrom(LocalDateTime.now());
                    defaultVersion.setChartType("I_MR");
                    defaultVersion.setChangeReason("系统自动创建(首次数据提交)");
                    paramVersionService.save(defaultVersion);
                    version = defaultVersion;
                } else {
                    throw new BusinessStateException(StatusMsg.PARAM_VERSION_NOT_CONFIGURED);
                }
            }
        }

        data.setParamVersionId(version.getId());

        if (data.getCollectTime() == null) {
            data.setCollectTime(data.getFillTime() != null ? data.getFillTime() : LocalDateTime.now());
        }
        if (data.getFillTime() == null) {
            data.setFillTime(LocalDateTime.now());
        }

        // 计数型图(P/NP/U)数据校验: 样本量必填且 >0；C 图不依赖样本量无需校验
        String ct = version.getChartType() != null
                ? version.getChartType().replace("-", "_").replace(" ", "").toUpperCase() : "";
        if ("P".equals(ct) || "NP".equals(ct) || "U".equals(ct)) {
            if (data.getSampleSize() == null || data.getSampleSize() <= 0) {
                throw new ParamValidationException("计数型图(" + ct + ")要求样本量 sampleSize > 0");
            }
        }

        evaluateData(data, version);

        save(data);

        // 写入新数据后清除列表缓存，避免 5 分钟 TTL 内后台列表与实际写入不同步
        clearListCache();

        try {
            cacheLatestData(data);
        } catch (Throwable e) {
            log.warn("[Redis] 缓存最新数据失败(不影响已写入数据): {}", e.getMessage());
        }

        try {
            LambdaQueryWrapper<SpcData> recentWrapper = new LambdaQueryWrapper<SpcData>()
                    .eq(SpcData::getParamVersionId, version.getId())
                    .eq(SpcData::getDeleted, 0)
                    .orderByDesc(SpcData::getCollectTime)
                    .last("LIMIT 30");
            List<SpcData> recentData = list(recentWrapper);
            if (recentData.size() >= 2) {
                spcRuleEngine.saveAndLogAlerts(recentData, version);
            }
        } catch (Exception e) {
            log.warn("[RuleEngine] 判异检测异常(不影响数据写入): {}", e.getMessage());
        }

        log.info("[DataUpload] 数据写入成功: id={} paramVersionId={} value={}",
                data.getId(), version.getId(), data.getMeasuredValue());
        return data;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public List<SpcData> batchUpload(List<SpcData> dataList, String role) {
        if (dataList == null || dataList.isEmpty()) {
            throw new ParamValidationException(StatusMsg.SUBMIT_DATA_REQUIRED);
        }
        for (SpcData data : dataList) {
            uploadData(data, role);
        }
        return dataList;
    }

    private void evaluateData(SpcData data, ParamVersion version) {
        BigDecimal value = data.getMeasuredValue();
        if (value == null) return;

        BigDecimal target = version.getTarget();
        if (target != null) {
            data.setDeviation(value.subtract(target));
        }

        // 判断图表类型: 计数图(P/NP/C/U)需特化处理
        String ct = version.getChartType() != null
                ? version.getChartType().replace("-", "_").replace(" ", "").toUpperCase() : "";
        boolean isCountChart = "P".equals(ct) || "NP".equals(ct) || "C".equals(ct) || "U".equals(ct);
        boolean isRateChart = "P".equals(ct) || "U".equals(ct);

        BigDecimal ucl = version.getUcl();
        BigDecimal lcl = version.getLcl();
        BigDecimal usl = version.getUsl();
        BigDecimal lsl = version.getLsl();
        BigDecimal cl = version.getCl();

        // 计数图: version 手动限为空时, 从最新 SpcStatResult 取动态计算限
        // 计数图控制限由 p̄/c̄/ū 等算法得出, 通常不存于 ParamVersion
        if (isCountChart && ucl == null && lcl == null && cl == null) {
            try {
                SpcStatResult stat = spcStatResultMapper.selectOne(
                        new LambdaQueryWrapper<SpcStatResult>()
                                .eq(SpcStatResult::getParamVersionId, version.getId())
                                .eq(SpcStatResult::getDeleted, 0)
                                .orderByDesc(SpcStatResult::getStatTime)
                                .last("LIMIT 1"));
                if (stat != null) {
                    ucl = stat.getCalcUcl();
                    lcl = stat.getCalcLcl();
                    cl = stat.getCalcCl();
                }
            } catch (Exception e) {
                log.warn("[Evaluate] 计数图查询历史统计结果失败, 跳过动态限: {}", e.getMessage());
            }
        }

        // 计数图 P/U 图描点值为比率(d/n), 与比率控制限比较
        BigDecimal plotValue = value;
        if (isRateChart && data.getSampleSize() != null && data.getSampleSize() > 0) {
            plotValue = value.divide(BigDecimal.valueOf(data.getSampleSize()), 10, RoundingMode.HALF_UP);
        }

        data.setIsOoc(0);
        data.setIsOos(0);

        if (ucl != null && plotValue.compareTo(ucl) > 0) {
            data.setIsOoc(1);
        }
        if (lcl != null && plotValue.compareTo(lcl) < 0) {
            data.setIsOoc(1);
        }
        if (usl != null && plotValue.compareTo(usl) > 0) {
            data.setIsOos(1);
        }
        if (lsl != null && plotValue.compareTo(lsl) < 0) {
            data.setIsOos(1);
        }

        // zone 计算: 中心线优先用 cl(计数图 cl 即均值), 不强制 target 存在
        BigDecimal centerLine = cl != null ? cl : target;
        if (ucl != null && lcl != null && centerLine != null) {
            BigDecimal range = ucl.subtract(lcl);
            if (range.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal sigma = range.divide(BigDecimal.valueOf(6), 6, RoundingMode.HALF_UP);
                BigDecimal deviation = plotValue.subtract(centerLine).abs();
                data.setSigmaLevel(deviation.divide(sigma, 4, RoundingMode.HALF_UP));

                BigDecimal zoneWidth = range.divide(BigDecimal.valueOf(6), 6, RoundingMode.HALF_UP);
                BigDecimal distFromCl = plotValue.subtract(centerLine).abs();
                if (distFromCl.compareTo(zoneWidth) <= 0) {
                    data.setZone(1);
                } else if (distFromCl.compareTo(zoneWidth.multiply(BigDecimal.valueOf(2))) <= 0) {
                    data.setZone(2);
                } else {
                    data.setZone(3);
                }
            }
        }
    }

    private void cacheLatestData(SpcData data) {
        String key = LATEST_DATA_PREFIX + data.getBatchId() + ":" + data.getParamId();
        try {
            String val = data.getMeasuredValue() + "|" + data.getCollectTime()
                    .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            redisTemplate.opsForValue().set(key, val, Duration.ofHours(2));
        } catch (Exception e) {
            log.warn("[Redis] 缓存最新数据失败", e);
        }
    }

    @Override
    public Page<SpcData> pageByCondition(Page<SpcData> page,
                                          Long paramVersionId, String batchId,
                                          Long productId, Long paramId,
                                          LocalDateTime startTime, LocalDateTime endTime,
                                          Collection<Long> workshopIds) {
        // workshopIds == null → 不限制 (ADMIN); 空集合 → 无绑定, 返回空页
        Collection<Long> processIds = resolveAccessibleProcessIds(workshopIds);
        if (workshopIds != null && processIds.isEmpty()) {
            return page;
        }
        return page(page, new LambdaQueryWrapper<SpcData>()
                .eq(paramVersionId != null, SpcData::getParamVersionId, paramVersionId)
                .eq(batchId != null && !batchId.isEmpty(), SpcData::getBatchId, batchId)
                .eq(productId != null, SpcData::getProductId, productId)
                .eq(paramId != null, SpcData::getParamId, paramId)
                .ge(startTime != null, SpcData::getCollectTime, startTime)
                .le(endTime != null, SpcData::getCollectTime, endTime)
                .in(processIds != null && !processIds.isEmpty(), SpcData::getProcessId, processIds)
                .eq(SpcData::getDeleted, 0)
                .orderByDesc(SpcData::getCollectTime));
    }

    @Override
    public List<SpcData> listRecentData(Long paramVersionId, int limit, LocalDateTime startTime, LocalDateTime endTime,
                                         Collection<Long> workshopIds) {
        // workshopIds == null → 不限制 (ADMIN); 空集合 → 无绑定, 返回空列表
        Collection<Long> processIds = resolveAccessibleProcessIds(workshopIds);
        if (workshopIds != null && processIds.isEmpty()) {
            return new ArrayList<>();
        }
        LambdaQueryWrapper<SpcData> wrapper = new LambdaQueryWrapper<SpcData>()
                .eq(SpcData::getParamVersionId, paramVersionId)
                .eq(SpcData::getDeleted, 0)
                .ge(startTime != null, SpcData::getCollectTime, startTime)
                .le(endTime != null, SpcData::getCollectTime, endTime)
                .in(processIds != null && !processIds.isEmpty(), SpcData::getProcessId, processIds)
                .orderByDesc(SpcData::getCollectTime);
        if (startTime == null && endTime == null) {
            wrapper.last("LIMIT " + limit);
        }
        return list(wrapper);
    }

    @Override
    public Page<SpcDataDetailVO> pageDetail(Page<SpcDataDetailVO> page,
                                             Long paramVersionId, String batchId,
                                             Long productId, Long paramId,
                                             Long processId, Long equipmentId,
                                             Integer isOoc, Integer isOos,
                                             String dataSource,
                                             LocalDateTime startTime, LocalDateTime endTime,
                                             Collection<Long> workshopIds) {
        // workshopIds == null → 不限制 (ADMIN); 空集合 → 无绑定, 返回空页
        Collection<Long> accessibleProcessIds = resolveAccessibleProcessIds(workshopIds);
        if (workshopIds != null && accessibleProcessIds.isEmpty()) {
            return page;
        }

        // 1. 构建缓存 key（全部查询条件拼接 + 车间范围，可读且无碰撞）
        String cacheKey = buildListCacheKey(page.getCurrent(), page.getSize(),
                paramVersionId, batchId, productId, paramId,
                processId, equipmentId, isOoc, isOos, dataSource, startTime, endTime, workshopIds);

        // 2. 先查缓存：命中直接返回
        try {
            String cached = redisTemplate.opsForValue().get(cacheKey);
            if (cached != null) {
                PageCache cache = objectMapper.readValue(cached, PageCache.class);
                Page<SpcDataDetailVO> cachedPage = new Page<>(cache.getCurrent(), cache.getSize());
                cachedPage.setRecords(cache.getRecords());
                cachedPage.setTotal(cache.getTotal());
                log.debug("[Cache] SPC数据列表缓存命中: {}", cacheKey);
                return cachedPage;
            }
        } catch (Exception e) {
            log.warn("[Cache] SPC数据列表缓存读取失败，回源DB: {}", e.getMessage());
        }

        // 3. 未命中：回源 DB（MyBatis-Plus 拦截器原地填充 page）
        baseMapper.selectDetailPage(page,
                paramVersionId, batchId, productId, paramId,
                processId, equipmentId, isOoc, isOos,
                dataSource, startTime, endTime, accessibleProcessIds);

        // 4. 回写缓存
        try {
            PageCache cache = new PageCache();
            cache.setRecords(page.getRecords());
            cache.setTotal(page.getTotal());
            cache.setCurrent(page.getCurrent());
            cache.setSize(page.getSize());
            redisTemplate.opsForValue().set(cacheKey, objectMapper.writeValueAsString(cache), LIST_CACHE_TTL);
        } catch (Exception e) {
            log.warn("[Cache] SPC数据列表缓存写入失败: {}", e.getMessage());
        }
        return page;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean deleteData(Long id) {
        // 先清缓存，再删除（缓存非事务，删除失败回滚后下次查询会重建）
        clearListCache();
        return removeById(id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean batchDeleteData(List<Long> ids) {
        clearListCache();
        return removeByIds(ids);
    }

    /** 拼接缓存 key：可读 + 无碰撞（含车间范围，避免不同权限用户缓存串读） */
    private String buildListCacheKey(long current, long size,
                                     Long paramVersionId, String batchId,
                                     Long productId, Long paramId,
                                     Long processId, Long equipmentId,
                                     Integer isOoc, Integer isOos,
                                     String dataSource,
                                     LocalDateTime startTime, LocalDateTime endTime,
                                     Collection<Long> workshopIds) {
        return LIST_CACHE_PREFIX + current + ":" + size
                + ":" + paramVersionId
                + ":" + batchId
                + ":" + productId
                + ":" + paramId
                + ":" + processId
                + ":" + equipmentId
                + ":" + isOoc
                + ":" + isOos
                + ":" + dataSource
                + ":" + startTime
                + ":" + endTime
                + ":w=" + (workshopIds == null ? "all" : new HashSet<>(workshopIds));
    }

    /**
     * 将用户可访问的车间 ID 集合解析为对应的工序 ID 集合。
     *
     * <p>关联链：spc_data.process_id → spc_process.id → spc_process.workshop_id。
     * SpcData 无直接 workshopId 字段，需通过 Process 间接关联车间。
     *
     * @param workshopIds 车间ID集合；{@code null} 表示不限制（ADMIN），返回 {@code null}
     * @return 工序ID集合；{@code null} 表示不过滤，空集合表示车间下无工序
     */
    private Collection<Long> resolveAccessibleProcessIds(Collection<Long> workshopIds) {
        if (workshopIds == null) {
            return null;
        }
        if (workshopIds.isEmpty()) {
            return Collections.emptySet();
        }
        List<Process> processes = processMapper.selectList(new LambdaQueryWrapper<Process>()
                .select(Process::getId)
                .in(Process::getWorkshopId, workshopIds)
                .eq(Process::getDeleted, 0));
        return processes.stream()
                .map(Process::getId)
                .collect(Collectors.toSet());
    }

    /**
     * 清除所有数据相关缓存（SCAN + 批量删除，生产安全）
     * 1. 后台 SPC 数据列表缓存 cockpit:admin:data:page:*
     * 2. 图表查询缓存 cockpit:chart:* （/control 与 /data 端点）
     * 数据写入/删除后调用, 确保下次查询走 DB 重新计算并刷新缓存
     */
    private void clearListCache() {
        List<String> keys = new ArrayList<>();
        try {
            for (String prefix : new String[]{LIST_CACHE_PREFIX, CHART_CACHE_PREFIX}) {
                ScanOptions options = ScanOptions.scanOptions()
                        .match(prefix + "*").count(200).build();
                try (Cursor<String> cursor = redisTemplate.scan(options)) {
                    while (cursor.hasNext()) {
                        keys.add(cursor.next());
                    }
                }
            }
            if (!keys.isEmpty()) {
                redisTemplate.delete(keys);
                log.info("[Cache] 清除SPC数据相关缓存: {} 个key", keys.size());
            }
        } catch (Exception e) {
            log.warn("[Cache] 清除SPC数据相关缓存失败: {}", e.getMessage());
        }
    }
}
