package xyz.leeyangy.spc.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * SPC 数据录入请求 DTO
 *
 * <p>仅包含客户端可上传的业务字段，排除 id / deleted / createdBy / createdAt / updatedBy / updatedAt
 * 等审计字段，以及由服务端判异引擎计算的衍生字段 (deviation / isOoc / isOos / sigmaLevel / zone / spcFlags 等)，
 * 防止客户端伪造判异结果或审计信息。
 */
@Data
public class SpcDataUploadDTO {

    /** 参数版本ID (可选，为空时由服务端按 paramId+productId 取当前版本) */
    private Long paramVersionId;

    /** 批次ID */
    private String batchId;

    /** 产品ID */
    @NotNull(message = "产品不能为空")
    private Long productId;

    /** 工序ID */
    @NotNull(message = "工序不能为空")
    private Long processId;

    /** 工艺参数ID */
    @NotNull(message = "工艺参数不能为空")
    private Long paramId;

    /** 设备ID */
    @NotNull(message = "设备ID不能为空")
    private Long equipmentId;

    /** 工位号 */
    private String workstationNo;

    /** 测量值 */
    @NotNull(message = "测量值不能为空")
    private BigDecimal measuredValue;

    /** 样本量 (计数型图 P/NP/U 必填且 >0，由服务端按图型校验) */
    private Integer sampleSize;

    /** 子组序号 */
    private Integer subgroupIdx;

    /** 子组大小 */
    private Integer subgroupSize;

    /** 采集时间 (为空时服务端默认取 fillTime 或当前时间) */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime collectTime;

    /** 录入时间 (为空时服务端默认当前时间) */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime fillTime;

    /** 数据来源 */
    private String dataSource;

    /** 消息ID (用于幂等去重，可选) */
    private String msgId;
}
