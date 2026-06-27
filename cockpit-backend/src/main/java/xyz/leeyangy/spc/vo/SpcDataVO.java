package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.SpcData;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class SpcDataVO {

    private Long id;
    private Long paramVersionId;
    private String batchId;
    private Long productId;
    private Long processId;
    private Long paramId;
    private Long equipmentId;
    private String workstationNo;
    private BigDecimal measuredValue;
    private Integer subgroupIdx;
    private Integer subgroupSize;
    private Long subgroupSeq;
    private BigDecimal deviation;
    private Integer isOoc;
    private Integer isOos;
    private BigDecimal sigmaLevel;
    private Integer zone;
    private String spcFlags;
    private String dataSource;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime collectTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime fillTime;

    private Integer dataQuality;
    private String traceId;
    private String msgId;
    private String sourceSystem;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static SpcDataVO from(SpcData entity) {
        if (entity == null) {
            return null;
        }
        SpcDataVO vo = new SpcDataVO();
        vo.setId(entity.getId());
        vo.setParamVersionId(entity.getParamVersionId());
        vo.setBatchId(entity.getBatchId());
        vo.setProductId(entity.getProductId());
        vo.setProcessId(entity.getProcessId());
        vo.setParamId(entity.getParamId());
        vo.setEquipmentId(entity.getEquipmentId());
        vo.setWorkstationNo(entity.getWorkstationNo());
        vo.setMeasuredValue(entity.getMeasuredValue());
        vo.setSubgroupIdx(entity.getSubgroupIdx());
        vo.setSubgroupSize(entity.getSubgroupSize());
        vo.setSubgroupSeq(entity.getSubgroupSeq());
        vo.setDeviation(entity.getDeviation());
        vo.setIsOoc(entity.getIsOoc());
        vo.setIsOos(entity.getIsOos());
        vo.setSigmaLevel(entity.getSigmaLevel());
        vo.setZone(entity.getZone());
        vo.setSpcFlags(entity.getSpcFlags());
        vo.setDataSource(entity.getDataSource());
        vo.setCollectTime(entity.getCollectTime());
        vo.setFillTime(entity.getFillTime());
        vo.setDataQuality(entity.getDataQuality());
        vo.setTraceId(entity.getTraceId());
        vo.setMsgId(entity.getMsgId());
        vo.setSourceSystem(entity.getSourceSystem());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
