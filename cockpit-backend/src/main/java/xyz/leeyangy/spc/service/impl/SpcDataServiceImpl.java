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
import xyz.leeyangy.spc.common.constants.RoleConstants;
import xyz.leeyangy.spc.common.exception.BusinessStateException;
import xyz.leeyangy.spc.common.exception.ParamValidationException;
import xyz.leeyangy.spc.common.exception.ResourceNotFoundException;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.mapper.SpcDataMapper;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.service.SpcRuleEngine;
import xyz.leeyangy.spc.vo.SpcDataDetailVO;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpcDataServiceImpl extends ServiceImpl<SpcDataMapper, SpcData> implements SpcDataService {

    private final ParamVersionService paramVersionService;
    private final StringRedisTemplate redisTemplate;
    private final SpcRuleEngine spcRuleEngine;
    private final ObjectMapper objectMapper;

    private static final String IDEMPOTENT_PREFIX = "spc:idempotent:";
    private static final String LATEST_DATA_PREFIX = "spc:latest:";

    /** 后台 SPC 数据列表缓存 key 前缀 */
    private static final String LIST_CACHE_PREFIX = "spc:admin:data:page:";
    /** 列表缓存 TTL */
    private static final Duration LIST_CACHE_TTL = Duration.ofMinutes(5);

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
        if (data.getEquipmentId() == null) {
            throw new ParamValidationException("设备ID不能为空，请选择设备");
        }
        if (data.getMeasuredValue() == null) {
            throw new ParamValidationException("测量值不能为空");
        }
        if (data.getParamId() == null) {
            throw new ParamValidationException("工艺参数ID不能为空");
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
                    throw new BusinessStateException("该工艺参数尚未配置标准版本，请联系工程师在【后台管理-工艺参数管理-版本】中创建版本后再提交数据");
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

        evaluateData(data, version);

        save(data);

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

        BigDecimal ucl = version.getUcl();
        BigDecimal lcl = version.getLcl();
        BigDecimal usl = version.getUsl();
        BigDecimal lsl = version.getLsl();

        data.setIsOoc(0);
        data.setIsOos(0);

        if (ucl != null && value.compareTo(ucl) > 0) {
            data.setIsOoc(1);
        }
        if (lcl != null && value.compareTo(lcl) < 0) {
            data.setIsOoc(1);
        }
        if (usl != null && value.compareTo(usl) > 0) {
            data.setIsOos(1);
        }
        if (lsl != null && value.compareTo(lsl) < 0) {
            data.setIsOos(1);
        }

        if (ucl != null && lcl != null && target != null) {
            BigDecimal range = ucl.subtract(lcl);
            BigDecimal sigma = range.divide(BigDecimal.valueOf(6), 6, BigDecimal.ROUND_HALF_UP);
            if (sigma.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal deviation = value.subtract(target).abs();
                data.setSigmaLevel(deviation.divide(sigma, 4, BigDecimal.ROUND_HALF_UP));

                BigDecimal zoneWidth = range.divide(BigDecimal.valueOf(6), 6, BigDecimal.ROUND_HALF_UP);
                BigDecimal distFromCl = value.subtract(target).abs();
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
                                          LocalDateTime startTime, LocalDateTime endTime) {
        return page(page, new LambdaQueryWrapper<SpcData>()
                .eq(paramVersionId != null, SpcData::getParamVersionId, paramVersionId)
                .eq(batchId != null && !batchId.isEmpty(), SpcData::getBatchId, batchId)
                .eq(productId != null, SpcData::getProductId, productId)
                .eq(paramId != null, SpcData::getParamId, paramId)
                .ge(startTime != null, SpcData::getCollectTime, startTime)
                .le(endTime != null, SpcData::getCollectTime, endTime)
                .eq(SpcData::getDeleted, 0)
                .orderByDesc(SpcData::getCollectTime));
    }

    @Override
    public List<SpcData> listRecentData(Long paramVersionId, int limit, LocalDateTime startTime, LocalDateTime endTime) {
        LambdaQueryWrapper<SpcData> wrapper = new LambdaQueryWrapper<SpcData>()
                .eq(SpcData::getParamVersionId, paramVersionId)
                .eq(SpcData::getDeleted, 0)
                .ge(startTime != null, SpcData::getCollectTime, startTime)
                .le(endTime != null, SpcData::getCollectTime, endTime)
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
                                             LocalDateTime startTime, LocalDateTime endTime) {
        // 1. 构建缓存 key（全部查询条件拼接，可读且无碰撞）
        String cacheKey = buildListCacheKey(page.getCurrent(), page.getSize(),
                paramVersionId, batchId, productId, paramId,
                processId, equipmentId, isOoc, isOos, dataSource, startTime, endTime);

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
                dataSource, startTime, endTime);

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

    /** 拼接缓存 key：可读 + 无碰撞 */
    private String buildListCacheKey(long current, long size,
                                     Long paramVersionId, String batchId,
                                     Long productId, Long paramId,
                                     Long processId, Long equipmentId,
                                     Integer isOoc, Integer isOos,
                                     String dataSource,
                                     LocalDateTime startTime, LocalDateTime endTime) {
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
                + ":" + endTime;
    }

    /** 清除所有后台 SPC 数据列表缓存（SCAN + 批量删除，生产安全） */
    private void clearListCache() {
        try {
            ScanOptions options = ScanOptions.scanOptions()
                    .match(LIST_CACHE_PREFIX + "*").count(200).build();
            List<String> keys = new ArrayList<>();
            try (Cursor<String> cursor = redisTemplate.scan(options)) {
                while (cursor.hasNext()) {
                    keys.add(cursor.next());
                }
            }
            if (!keys.isEmpty()) {
                redisTemplate.delete(keys);
                log.info("[Cache] 清除SPC数据列表缓存: {} 个key", keys.size());
            }
        } catch (Exception e) {
            log.warn("[Cache] 清除SPC数据列表缓存失败: {}", e.getMessage());
        }
    }
}
