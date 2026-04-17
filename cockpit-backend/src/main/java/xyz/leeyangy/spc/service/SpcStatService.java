package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.entity.SpcStatResult;
import xyz.leeyangy.spc.mapper.SpcStatResultMapper;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpcStatService extends ServiceImpl<SpcStatResultMapper, SpcStatResult> {

    private final SpcDataService spcDataService;
    private final ParamVersionService paramVersionService;

    public SpcStatResult calculateAndSave(Long paramVersionId, String batchId) {
        ParamVersion version = paramVersionService.getById(paramVersionId);
        if (version == null) {
            throw new RuntimeException("参数标准版本不存在: " + paramVersionId);
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

        SpcStatResult result = computeStatistics(dataList, version, paramVersionId, batchId);
        save(result);
        return result;
    }

    public void regenerateForNewVersion(Long newVersionId) {
        ParamVersion newVersion = paramVersionService.getById(newVersionId);
        if (newVersion == null) return;

        calculateAndSave(newVersionId, null);
        log.info("[SPC-Regenerate] 已为新版本生成统计: versionId={}", newVersionId);
    }

    private SpcStatResult computeStatistics(List<SpcData> dataList, ParamVersion version,
                                             Long paramVersionId, String batchId) {
        SpcStatResult result = new SpcStatResult();
        result.setParamVersionId(paramVersionId);
        result.setBatchId(batchId);
        result.setProductId(version.getProductId());
        result.setProcessId(dataList.get(0).getProcessId());
        result.setParamId(version.getParamId());
        result.setStatType(version.getChartType() != null ? version.getChartType() : "XBAR_R");
        result.setSampleCount(dataList.size());
        result.setStatTime(LocalDateTime.now());

        BigDecimal sum = BigDecimal.ZERO;
        BigDecimal sumSq = BigDecimal.ZERO;
        BigDecimal minVal = null;
        BigDecimal maxVal = null;

        for (SpcData d : dataList) {
            BigDecimal v = d.getMeasuredValue();
            sum = sum.add(v);
            sumSq = sumSq.add(v.multiply(v));
            if (minVal == null || v.compareTo(minVal) < 0) minVal = v;
            if (maxVal == null || v.compareTo(maxVal) > 0) maxVal = v;
        }

        int n = dataList.size();
        BigDecimal mean = sum.divide(BigDecimal.valueOf(n), 6, RoundingMode.HALF_UP);
        BigDecimal variance = sumSq.divide(BigDecimal.valueOf(n), 6, RoundingMode.HALF_UP)
                .subtract(mean.multiply(mean));
        BigDecimal stdDev = variance.compareTo(BigDecimal.ZERO) > 0
                ? sqrt(variance, 6) : BigDecimal.ZERO;
        BigDecimal range = maxVal != null ? maxVal.subtract(minVal) : BigDecimal.ZERO;

        result.setMeanValue(mean);
        result.setStdDev(stdDev);
        result.setRangeValue(range);

        BigDecimal usl = version.getUsl();
        BigDecimal lsl = version.getLsl();

        if (usl != null && lsl != null && stdDev.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal specRange = usl.subtract(lsl);
            BigDecimal sixSigma = stdDev.multiply(BigDecimal.valueOf(6));

            result.setCp(specRange.divide(sixSigma, 4, RoundingMode.HALF_UP));

            BigDecimal cpu = usl.subtract(mean).divide(stdDev.multiply(BigDecimal.valueOf(3)), 4, RoundingMode.HALF_UP);
            BigDecimal cpl = mean.subtract(lsl).divide(stdDev.multiply(BigDecimal.valueOf(3)), 4, RoundingMode.HALF_UP);
            result.setCpk(cpu.min(cpl));
        }

        BigDecimal sigmaWidth = version.getSigmaWidth() != null ? version.getSigmaWidth() : BigDecimal.valueOf(3);
        result.setCalcCl(mean);
        result.setCalcUcl(mean.add(sigmaWidth.multiply(stdDev)));
        result.setCalcLcl(mean.subtract(sigmaWidth.multiply(stdDev)));

        return result;
    }

    private BigDecimal sqrt(BigDecimal value, int scale) {
        BigDecimal x0 = BigDecimal.ZERO;
        BigDecimal x1 = BigDecimal.valueOf(Math.sqrt(value.doubleValue()));
        while (!x0.equals(x1)) {
            x0 = x1;
            x1 = value.divide(x0, scale, RoundingMode.HALF_UP)
                    .add(x0).divide(BigDecimal.valueOf(2), scale, RoundingMode.HALF_UP);
        }
        return x1;
    }

    public SpcStatResult getLatestStat(Long paramVersionId, String batchId) {
        return getOne(new LambdaQueryWrapper<SpcStatResult>()
                .eq(SpcStatResult::getParamVersionId, paramVersionId)
                .eq(batchId != null, SpcStatResult::getBatchId, batchId)
                .eq(SpcStatResult::getDeleted, 0)
                .orderByDesc(SpcStatResult::getStatTime)
                .last("LIMIT 1"));
    }
}
