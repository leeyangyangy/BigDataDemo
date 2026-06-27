package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.StandardChangeLog;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class StandardChangeLogVO {

    private Long id;
    private Long paramId;
    private Long productId;
    private Long oldVersionId;
    private Long newVersionId;
    private Integer oldVersionNo;
    private Integer newVersionNo;
    private String changeType;
    private String changeReason;
    private BigDecimal oldUsl;
    private BigDecimal newUsl;
    private BigDecimal oldLsl;
    private BigDecimal newLsl;
    private BigDecimal oldTarget;
    private BigDecimal newTarget;
    private BigDecimal oldUcl;
    private BigDecimal newUcl;
    private BigDecimal oldLcl;
    private BigDecimal newLcl;
    private Integer regenerateSpc;
    private String regenerateStatus;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime regenerateStartedAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime regenerateFinishedAt;

    private Long affectedDataCount;
    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static StandardChangeLogVO from(StandardChangeLog entity) {
        if (entity == null) {
            return null;
        }
        StandardChangeLogVO vo = new StandardChangeLogVO();
        vo.setId(entity.getId());
        vo.setParamId(entity.getParamId());
        vo.setProductId(entity.getProductId());
        vo.setOldVersionId(entity.getOldVersionId());
        vo.setNewVersionId(entity.getNewVersionId());
        vo.setOldVersionNo(entity.getOldVersionNo());
        vo.setNewVersionNo(entity.getNewVersionNo());
        vo.setChangeType(entity.getChangeType());
        vo.setChangeReason(entity.getChangeReason());
        vo.setOldUsl(entity.getOldUsl());
        vo.setNewUsl(entity.getNewUsl());
        vo.setOldLsl(entity.getOldLsl());
        vo.setNewLsl(entity.getNewLsl());
        vo.setOldTarget(entity.getOldTarget());
        vo.setNewTarget(entity.getNewTarget());
        vo.setOldUcl(entity.getOldUcl());
        vo.setNewUcl(entity.getNewUcl());
        vo.setOldLcl(entity.getOldLcl());
        vo.setNewLcl(entity.getNewLcl());
        vo.setRegenerateSpc(entity.getRegenerateSpc());
        vo.setRegenerateStatus(entity.getRegenerateStatus());
        vo.setRegenerateStartedAt(entity.getRegenerateStartedAt());
        vo.setRegenerateFinishedAt(entity.getRegenerateFinishedAt());
        vo.setAffectedDataCount(entity.getAffectedDataCount());
        vo.setStatus(entity.getStatus());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
