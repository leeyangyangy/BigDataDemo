package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import xyz.leeyangy.spc.entity.OperationLog;

import javax.servlet.http.HttpServletRequest;

/**
 * 操作日志 Service 接口
 */
public interface OperationLogService extends IService<OperationLog> {

    void record(String module, String action, String result,
                HttpServletRequest request, Long operatorId, String operatorName);

    void record(String module, String action, Long targetId, String targetType,
                String content, String result, String errorMsg,
                int durationMs,
                HttpServletRequest request, Long operatorId, String operatorName);

    Page<OperationLog> queryPage(Page<OperationLog> page,
                                  String module, String action, String result,
                                  String startDate, String endDate);
}
