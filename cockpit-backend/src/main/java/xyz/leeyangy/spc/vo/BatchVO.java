package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.Batch;

import java.time.LocalDateTime;

@Data
public class BatchVO {

    private Long id;
    private String batchCode;
    private Long productId;
    private Long processId;
    private Long workshopId;
    private Long lineId;
    private Integer lotSize;
    private String batchStatus;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime startTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime endTime;

    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static BatchVO from(Batch entity) {
        if (entity == null) {
            return null;
        }
        BatchVO vo = new BatchVO();
        vo.setId(entity.getId());
        vo.setBatchCode(entity.getBatchCode());
        vo.setProductId(entity.getProductId());
        vo.setProcessId(entity.getProcessId());
        vo.setWorkshopId(entity.getWorkshopId());
        vo.setLineId(entity.getLineId());
        vo.setLotSize(entity.getLotSize());
        vo.setBatchStatus(entity.getBatchStatus());
        vo.setStartTime(entity.getStartTime());
        vo.setEndTime(entity.getEndTime());
        vo.setStatus(entity.getStatus());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
