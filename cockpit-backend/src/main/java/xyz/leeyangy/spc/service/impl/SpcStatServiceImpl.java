package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.common.constants.TriggerSourceConstants;
import xyz.leeyangy.spc.common.exception.ResourceNotFoundException;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.mapper.SpcStatResultMapper;
import xyz.leeyangy.spc.service.ParamVersionService;
import xyz.leeyangy.spc.service.SpcDataService;
import xyz.leeyangy.spc.service.SpcStatService;
import xyz.leeyangy.spc.service.calculator.SpcCalculator;

import java.util.List;

/**
 * SPC 统计 Service 实现
 *
 * <p>负责业务编排（数据查询、结果保存、旧记录清理），统计算法委托给
 * {@link SpcCalculator}，遵循单一职责原则。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SpcStatServiceImpl extends ServiceImpl<SpcStatResultMapper, SpcStatResult> implements SpcStatService {

    private final SpcDataService spcDataService;
    private final ParamVersionService paramVersionService;
    private final SpcCalculator spcCalculator;

    @Override
    public SpcStatResult calculateAndSave(Long paramVersionId, String batchId) {
        return calculateAndSave(paramVersionId, batchId, TriggerSourceConstants.MANUAL);
    }

    @Override
    public SpcStatResult calculateAndSave(Long paramVersionId, String batchId, String triggerSource) {
        ParamVersion version = paramVersionService.getById(paramVersionId);
        if (version == null) {
            throw new ResourceNotFoundException("参数标准版本", paramVersionId);
        }

        LambdaQueryWrapper<SpcData> wrapper = new LambdaQueryWrapper<SpcData>()
                .eq(SpcData::getParamVersionId, paramVersionId)
                .eq(batchId != null, SpcData::getBatchId, batchId)
                .eq(SpcData::getDeleted, 0)
                .orderByAsc(SpcData::getCollectTime);

        List<SpcData> dataList = spcDataService.list(wrapper);
        if (dataList.isEmpty()) {
            return null;
        }

        SpcStatResult result = spcCalculator.computeStatistics(dataList, version, paramVersionId, batchId, triggerSource);
        save(result);
        return result;
    }

    @Override
    public void regenerateForNewVersion(Long newVersionId) {
        regenerateForNewVersion(newVersionId, TriggerSourceConstants.VERSION_CREATE);
    }

    @Override
    public void regenerateForNewVersion(Long newVersionId, String triggerSource) {
        ParamVersion newVersion = paramVersionService.getById(newVersionId);
        if (newVersion == null) return;

        if (!TriggerSourceConstants.AUTO.equals(triggerSource)) {
            LambdaQueryWrapper<SpcStatResult> cleanWrapper = new LambdaQueryWrapper<SpcStatResult>()
                    .eq(SpcStatResult::getParamVersionId, newVersionId)
                    .eq(SpcStatResult::getDeleted, 0);
            remove(cleanWrapper);
            log.info("[SPC-Regenerate] 已清除旧统计记录: versionId={} source={}", newVersionId, triggerSource);
        }

        calculateAndSave(newVersionId, null, triggerSource);
        log.info("[SPC-Regenerate] 已为新版本生成统计: versionId={} source={}", newVersionId, triggerSource);
    }

    @Override
    public SpcStatResult getLatestStat(Long paramVersionId, String batchId) {
        return getOne(new LambdaQueryWrapper<SpcStatResult>()
                .eq(SpcStatResult::getParamVersionId, paramVersionId)
                .eq(batchId != null, SpcStatResult::getBatchId, batchId)
                .eq(SpcStatResult::getDeleted, 0)
                .orderByDesc(SpcStatResult::getStatTime)
                .last("LIMIT 1"));
    }
}
