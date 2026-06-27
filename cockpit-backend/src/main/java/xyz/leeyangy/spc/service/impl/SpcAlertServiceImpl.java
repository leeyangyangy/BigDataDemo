package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.common.constants.AlertConstants;
import xyz.leeyangy.spc.common.exception.ResourceNotFoundException;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.mapper.SpcAlertMapper;
import xyz.leeyangy.spc.service.SpcAlertService;
import xyz.leeyangy.spc.vo.AlertHandleResultVO;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpcAlertServiceImpl extends ServiceImpl<SpcAlertMapper, SpcAlert> implements SpcAlertService {

    @Override
    public Page<SpcAlert> pageByCondition(Page<SpcAlert> page,
                                           Long paramVersionId, Long paramId,
                                           String status,
                                           LocalDateTime startTime, LocalDateTime endTime) {
        return page(page, new LambdaQueryWrapper<SpcAlert>()
                .eq(paramVersionId != null, SpcAlert::getParamVersionId, paramVersionId)
                .eq(paramId != null, SpcAlert::getParamId, paramId)
                .eq(status != null && !status.isEmpty(), SpcAlert::getStatus, status)
                .ge(startTime != null, SpcAlert::getAlertTime, startTime)
                .le(endTime != null, SpcAlert::getAlertTime, endTime)
                .eq(SpcAlert::getDeleted, 0)
                .orderByDesc(SpcAlert::getAlertTime));
    }

    @Override
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

    @Override
    public boolean confirmAlert(Long id, Long userId) {
        SpcAlert alert = getById(id);
        if (alert == null) {
            throw new ResourceNotFoundException("报警记录", id);
        }
        if (AlertConstants.STATUS_CONFIRMED.equals(alert.getStatus()) || AlertConstants.STATUS_HANDLED.equals(alert.getStatus())) {
            log.warn("[Alert] 报警已确认/处理: id={}", id);
            return true;
        }
        return update(new LambdaUpdateWrapper<SpcAlert>()
                .eq(SpcAlert::getId, id)
                .set(SpcAlert::getStatus, AlertConstants.STATUS_CONFIRMED)
                .set(SpcAlert::getConfirmedBy, userId)
                .set(SpcAlert::getConfirmedAt, LocalDateTime.now()));
    }

    @Override
    public AlertHandleResultVO handleAlertWithResult(Long id, String handleResult, String handleRemark, Long userId) {
        handleAlert(id, handleResult, handleRemark, userId);

        AlertHandleResultVO result = new AlertHandleResultVO();
        result.setAlertId(id);
        result.setHandled(true);
        result.setHandleTime(LocalDateTime.now());
        return result;
    }

    @Override
    public boolean handleAlert(Long id, String handleResult, String handleRemark, Long userId) {
        SpcAlert alert = getById(id);
        if (alert == null) {
            throw new ResourceNotFoundException("报警记录", id);
        }
        if (AlertConstants.STATUS_HANDLED.equals(alert.getStatus())) {
            log.warn("[Alert] 报警已处理: id={}", id);
            return true;
        }
        LambdaUpdateWrapper<SpcAlert> wrapper = new LambdaUpdateWrapper<SpcAlert>()
                .eq(SpcAlert::getId, id)
                .set(SpcAlert::getStatus, AlertConstants.STATUS_HANDLED)
                .set(SpcAlert::getHandleResult, handleResult)
                .set(SpcAlert::getHandledBy, userId)
                .set(SpcAlert::getHandledAt, LocalDateTime.now());
        if (handleRemark != null && !handleRemark.isEmpty()) {
            wrapper.set(SpcAlert::getHandleRemark, handleRemark);
        }
        return update(wrapper);
    }
}
