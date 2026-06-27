package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpcDataServiceImpl extends ServiceImpl<SpcDataMapper, SpcData> implements SpcDataService {

    private final ParamVersionService paramVersionService;
    private final StringRedisTemplate redisTemplate;
    private final SpcRuleEngine spcRuleEngine;

    private static final String IDEMPOTENT_PREFIX = "spc:idempotent:";
    private static final String LATEST_DATA_PREFIX = "spc:latest:";

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
}
