package xyz.leeyangy.spc.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import xyz.leeyangy.spc.common.constants.TriggerSourceConstants;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.StandardChangeLog;
import xyz.leeyangy.spc.mapper.StandardChangeLogMapper;
import xyz.leeyangy.spc.service.SpcStatService;
import xyz.leeyangy.spc.service.StandardChangeLogService;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class StandardChangeLogServiceImpl extends ServiceImpl<StandardChangeLogMapper, StandardChangeLog> implements StandardChangeLogService {

    private final SpcStatService spcStatService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public StandardChangeLog recordChange(ParamVersion oldVersion, ParamVersion newVersion,
                                           String changeType, String changeReason,
                                           boolean regenerateSpc) {
        StandardChangeLog changeLog = new StandardChangeLog();
        changeLog.setParamId(newVersion.getParamId());
        changeLog.setProductId(newVersion.getProductId());
        changeLog.setOldVersionId(oldVersion != null ? oldVersion.getId() : null);
        changeLog.setNewVersionId(newVersion.getId());
        changeLog.setOldVersionNo(oldVersion != null ? oldVersion.getVersionNo() : null);
        changeLog.setNewVersionNo(newVersion.getVersionNo());
        changeLog.setChangeType(changeType);
        changeLog.setChangeReason(changeReason);

        if (oldVersion != null) {
            changeLog.setOldUsl(oldVersion.getUsl());
            changeLog.setNewUsl(newVersion.getUsl());
            changeLog.setOldLsl(oldVersion.getLsl());
            changeLog.setNewLsl(newVersion.getLsl());
            changeLog.setOldTarget(oldVersion.getTarget());
            changeLog.setNewTarget(newVersion.getTarget());
            changeLog.setOldUcl(oldVersion.getUcl());
            changeLog.setNewUcl(newVersion.getUcl());
            changeLog.setOldLcl(oldVersion.getLcl());
            changeLog.setNewLcl(newVersion.getLcl());
        } else {
            changeLog.setNewUsl(newVersion.getUsl());
            changeLog.setNewLsl(newVersion.getLsl());
            changeLog.setNewTarget(newVersion.getTarget());
            changeLog.setNewUcl(newVersion.getUcl());
            changeLog.setNewLcl(newVersion.getLcl());
        }

        changeLog.setRegenerateSpc(regenerateSpc ? 1 : 0);
        changeLog.setRegenerateStatus(regenerateSpc ? "PENDING" : null);

        save(changeLog);

        if (regenerateSpc) {
            triggerRegenerate(changeLog);
        }

        return changeLog;
    }

    private void triggerRegenerate(StandardChangeLog changeLog) {
        try {
            changeLog.setRegenerateStatus("RUNNING");
            changeLog.setRegenerateStartedAt(LocalDateTime.now());
            updateById(changeLog);

            spcStatService.regenerateForNewVersion(changeLog.getNewVersionId(), TriggerSourceConstants.VERSION_CREATE);

            changeLog.setRegenerateStatus("COMPLETED");
            changeLog.setRegenerateFinishedAt(LocalDateTime.now());
            updateById(changeLog);

            log.info("[StandardChange] SPC重生成完成: paramId={} productId={} newVersion={}",
                    changeLog.getParamId(), changeLog.getProductId(), changeLog.getNewVersionNo());
        } catch (Exception e) {
            changeLog.setRegenerateStatus("FAILED");
            changeLog.setRegenerateFinishedAt(LocalDateTime.now());
            updateById(changeLog);
            log.error("[StandardChange] SPC重生成失败", e);
        }
    }

    @Override
    public void recordVersionSwitch(ParamVersion version, String operation, String reason) {
        StandardChangeLog record = new StandardChangeLog();
        record.setParamId(version.getParamId());
        record.setProductId(version.getProductId());
        record.setNewVersionId(version.getId());
        record.setNewVersionNo(version.getVersionNo());
        record.setChangeType(operation);
        record.setChangeReason(reason);
        record.setNewUsl(version.getUsl());
        record.setNewLsl(version.getLsl());
        record.setNewTarget(version.getTarget());
        record.setNewUcl(version.getUcl());
        record.setNewLcl(version.getLcl());
        record.setRegenerateSpc(1);
        record.setRegenerateStatus("COMPLETED");

        save(record);

        log.info("[StandardChange] 版本操作已记录: operation={} versionId={} versionNo={}",
                operation, version.getId(), version.getVersionNo());
    }
}
