package xyz.leeyangy.spc.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import java.time.LocalDateTime;

/**
 * 创建批次请求 DTO
 *
 * <p>仅包含业务字段，排除 id / deleted / createdBy / createdAt / updatedBy / updatedAt 等审计字段
 * (审计字段由 BaseEntity 自动填充或服务端控制，禁止客户端传入)。
 */
@Data
public class BatchCreateDTO {

    /** 批次编码 */
    @NotBlank(message = "批次编码不能为空")
    private String batchCode;

    /** 产品ID */
    private Long productId;

    /** 工序ID */
    private Long processId;

    /** 车间ID */
    private Long workshopId;

    /** 产线ID */
    private Long lineId;

    /** 批次大小 */
    private Integer lotSize;

    /** 批次状态 */
    private String batchStatus;

    /** 开始时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime startTime;

    /** 结束时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime endTime;

    /** 状态 */
    private Integer status;
}
