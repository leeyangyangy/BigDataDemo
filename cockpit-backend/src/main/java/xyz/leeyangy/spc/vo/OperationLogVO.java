package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.OperationLog;

import java.time.LocalDateTime;

@Data
public class OperationLogVO {

    private Long id;
    private String module;
    private String action;
    private Long targetId;
    private String targetType;
    private String content;
    private String ipAddress;
    private String userAgent;
    private Long operatorId;
    private String operatorName;
    private String result;
    private String errorMsg;
    private Integer durationMs;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    public static OperationLogVO from(OperationLog entity) {
        if (entity == null) {
            return null;
        }
        OperationLogVO vo = new OperationLogVO();
        vo.setId(entity.getId());
        vo.setModule(entity.getModule());
        vo.setAction(entity.getAction());
        vo.setTargetId(entity.getTargetId());
        vo.setTargetType(entity.getTargetType());
        vo.setContent(entity.getContent());
        vo.setIpAddress(entity.getIpAddress());
        vo.setUserAgent(entity.getUserAgent());
        vo.setOperatorId(entity.getOperatorId());
        vo.setOperatorName(entity.getOperatorName());
        vo.setResult(entity.getResult());
        vo.setErrorMsg(entity.getErrorMsg());
        vo.setDurationMs(entity.getDurationMs());
        vo.setCreatedAt(entity.getCreatedAt());
        return vo;
    }
}
