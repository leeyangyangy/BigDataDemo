package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.StandardChangeLog;

/**
 * 标准变更日志 Service 接口
 */
public interface StandardChangeLogService extends IService<StandardChangeLog> {

    StandardChangeLog recordChange(ParamVersion oldVersion, ParamVersion newVersion,
                                    String changeType, String changeReason,
                                    boolean regenerateSpc);

    void recordVersionSwitch(ParamVersion version, String operation, String reason);
}
