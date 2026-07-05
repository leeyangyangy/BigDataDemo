package xyz.leeyangy.spc.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import xyz.leeyangy.spc.common.BaseEntity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = true)
@TableName("spc_data")
public class SpcData extends BaseEntity {

    private Long paramVersionId;
    private String batchId;
    private Long productId;
    private Long processId;
    private Long paramId;
    private Long equipmentId;
    private String workstationNo;
    private BigDecimal measuredValue;
    private Integer sampleSize;
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
}
