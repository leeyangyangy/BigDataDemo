package xyz.leeyangy.spc.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import xyz.leeyangy.spc.entity.OperationLog;
import xyz.leeyangy.spc.mapper.OperationLogMapper;
import xyz.leeyangy.spc.common.IpUtil;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class OperationLogService extends ServiceImpl<OperationLogMapper, OperationLog> {

    public void record(String module, String action, String result,
                       HttpServletRequest request, Long operatorId, String operatorName) {
        record(module, action, null, null, null, result, null, 0, request, operatorId, operatorName);
    }

    public void record(String module, String action, Long targetId, String targetType,
                       String content, String result, String errorMsg,
                       int durationMs,
                       HttpServletRequest request, Long operatorId, String operatorName) {
        try {
            OperationLog logRecord = new OperationLog();
            logRecord.setModule(module);
            logRecord.setAction(action);
            logRecord.setTargetId(targetId);
            logRecord.setTargetType(targetType);
            logRecord.setContent(truncateContent(content));
            logRecord.setResult(result != null ? result : "SUCCESS");
            logRecord.setErrorMsg(truncateErrorMsg(errorMsg));
            logRecord.setDurationMs(durationMs > 0 ? durationMs : null);
            logRecord.setOperatorId(operatorId);
            logRecord.setOperatorName(operatorName);
            logRecord.setCreatedAt(LocalDateTime.now());

            if (request != null) {
                logRecord.setIpAddress(IpUtil.getClientIp(request));
                logRecord.setUserAgent(truncateUserAgent(request.getHeader("User-Agent")));
            }

            save(logRecord);
        } catch (Exception e) {
            log.error("[OperationLog] 写入操作日志失败(不影响业务): module={} action={}", module, action, e);
        }
    }

    public Page<OperationLog> queryPage(Page<OperationLog> page,
                                        String module, String action, String result,
                                        String startDate, String endDate) {
        LambdaQueryWrapper<OperationLog> wrapper = new LambdaQueryWrapper<OperationLog>()
                .eq(module != null && !module.isEmpty(), OperationLog::getModule, module)
                .eq(action != null && !action.isEmpty(), OperationLog::getAction, action)
                .eq(result != null && !result.isEmpty(), OperationLog::getResult, result)
                .ge(startDate != null && !startDate.isEmpty(), OperationLog::getCreatedAt, startDate)
                .le(endDate != null && !endDate.isEmpty(), OperationLog::getCreatedAt, endDate + " 23:59:59")
                .orderByDesc(OperationLog::getCreatedAt);
        return page(page, wrapper);
    }

    private String truncateUserAgent(String ua) {
        if (ua == null) return null;
        return ua.length() > 500 ? ua.substring(0, 500) : ua;
    }

    private String truncateErrorMsg(String msg) {
        if (msg == null) return null;
        if (msg.length() <= 512) return msg;
        return msg.substring(0, 509) + "...";
    }

    private String truncateContent(String content) {
        if (content == null) return null;
        if (content.length() <= 2000) return content;
        return content.substring(0, 1997) + "...";
    }
}
