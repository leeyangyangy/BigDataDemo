package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("spc_operation_log")
public class OperationLog {

    @TableId(type = IdType.AUTO)
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
}
