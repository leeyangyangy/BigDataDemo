package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.SpcStatResult;

/**
 * SPC 统计 Service 接口
 */
public interface SpcStatService extends IService<SpcStatResult> {

    SpcStatResult calculateAndSave(Long paramVersionId, String batchId);

    SpcStatResult calculateAndSave(Long paramVersionId, String batchId, String triggerSource);

    void regenerateForNewVersion(Long newVersionId);

    void regenerateForNewVersion(Long newVersionId, String triggerSource);

    SpcStatResult getLatestStat(Long paramVersionId, String batchId);
}
