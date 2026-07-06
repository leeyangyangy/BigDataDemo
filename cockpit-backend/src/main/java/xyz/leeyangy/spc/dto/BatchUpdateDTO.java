package xyz.leeyangy.spc.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 更新批次请求 DTO（字段全部可选，仅更新传入的字段）
 *
 * <p>排除 id / deleted / createdBy / createdAt / updatedBy / updatedAt 等审计字段。
 */
@Data
public class BatchUpdateDTO {

    /** 批次编码 */
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
