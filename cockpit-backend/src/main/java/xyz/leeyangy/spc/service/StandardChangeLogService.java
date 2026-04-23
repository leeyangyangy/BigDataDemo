package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.StandardChangeLog;
import xyz.leeyangy.spc.mapper.StandardChangeLogMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class StandardChangeLogService extends ServiceImpl<StandardChangeLogMapper, StandardChangeLog> {

    private final ParamVersionService paramVersionService;
    private final SpcStatService spcStatService;

    @Transactional(rollbackFor = Exception.class)
    public StandardChangeLog recordChange(ParamVersion oldVersion, ParamVersion newVersion,
                                           String changeType, String changeReason,
                                           boolean regenerateSpc) {
        StandardChangeLog log = new StandardChangeLog();
        log.setParamId(newVersion.getParamId());
        log.setProductId(newVersion.getProductId());
        log.setOldVersionId(oldVersion != null ? oldVersion.getId() : null);
        log.setNewVersionId(newVersion.getId());
        log.setOldVersionNo(oldVersion != null ? oldVersion.getVersionNo() : null);
        log.setNewVersionNo(newVersion.getVersionNo());
        log.setChangeType(changeType);
        log.setChangeReason(changeReason);

        if (oldVersion != null) {
            log.setOldUsl(oldVersion.getUsl());
            log.setNewUsl(newVersion.getUsl());
            log.setOldLsl(oldVersion.getLsl());
            log.setNewLsl(newVersion.getLsl());
            log.setOldTarget(oldVersion.getTarget());
            log.setNewTarget(newVersion.getTarget());
            log.setOldUcl(oldVersion.getUcl());
            log.setNewUcl(newVersion.getUcl());
            log.setOldLcl(oldVersion.getLcl());
            log.setNewLcl(newVersion.getLcl());
        } else {
            log.setNewUsl(newVersion.getUsl());
            log.setNewLsl(newVersion.getLsl());
            log.setNewTarget(newVersion.getTarget());
            log.setNewUcl(newVersion.getUcl());
            log.setNewLcl(newVersion.getLcl());
        }

        log.setRegenerateSpc(regenerateSpc ? 1 : 0);
        log.setRegenerateStatus(regenerateSpc ? "PENDING" : null);

        save(log);

        if (regenerateSpc) {
            triggerRegenerate(log);
        }

        return log;
    }

    private void triggerRegenerate(StandardChangeLog changeLog) {
        try {
            changeLog.setRegenerateStatus("RUNNING");
            changeLog.setRegenerateStartedAt(LocalDateTime.now());
            updateById(changeLog);

            spcStatService.regenerateForNewVersion(changeLog.getNewVersionId(), "VERSION_CREATE");

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
