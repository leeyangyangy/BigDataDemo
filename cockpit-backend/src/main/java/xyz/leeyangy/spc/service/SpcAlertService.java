package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.mapper.SpcAlertMapper;

@Service
@RequiredArgsConstructor
public class SpcAlertService extends ServiceImpl<SpcAlertMapper, SpcAlert> {

    public Page<SpcAlert> pageByCondition(Page<SpcAlert> page,
                                           Long paramVersionId, String batchId,
                                           String status, String alertType,
                                           Integer alertLevel) {
        return page(page, new LambdaQueryWrapper<SpcAlert>()
                .eq(paramVersionId != null, SpcAlert::getParamVersionId, paramVersionId)
                .eq(batchId != null, SpcAlert::getBatchId, batchId)
                .eq(status != null, SpcAlert::getStatus, status)
                .eq(alertType != null, SpcAlert::getAlertType, alertType)
                .eq(alertLevel != null, SpcAlert::getAlertLevel, alertLevel)
                .eq(SpcAlert::getDeleted, 0)
                .orderByDesc(SpcAlert::getAlertTime));
    }
}
