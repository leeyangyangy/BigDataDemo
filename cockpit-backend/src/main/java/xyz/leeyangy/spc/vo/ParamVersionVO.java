package xyz.leeyangy.spc.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import xyz.leeyangy.spc.entity.ParamVersion;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class ParamVersionVO {

    private Long id;
    private Long paramId;
    private Long productId;
    private Integer versionNo;
    private BigDecimal usl;
    private BigDecimal lsl;
    private BigDecimal target;
    private BigDecimal ucl;
    private BigDecimal lcl;
    private BigDecimal cl;
    private BigDecimal sigmaWidth;
    private Integer subgroupSize;
    private String chartType;
    private String calcMethod;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime effectiveFrom;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime effectiveTo;

    private Integer isCurrent;
    private String changeReason;
    private String changeType;
    private Long prevVersionId;
    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    private Long createdBy;
    private Long updatedBy;

    public static ParamVersionVO from(ParamVersion entity) {
        if (entity == null) {
            return null;
        }
        ParamVersionVO vo = new ParamVersionVO();
        vo.setId(entity.getId());
        vo.setParamId(entity.getParamId());
        vo.setProductId(entity.getProductId());
        vo.setVersionNo(entity.getVersionNo());
        vo.setUsl(entity.getUsl());
        vo.setLsl(entity.getLsl());
        vo.setTarget(entity.getTarget());
        vo.setUcl(entity.getUcl());
        vo.setLcl(entity.getLcl());
        vo.setCl(entity.getCl());
        vo.setSigmaWidth(entity.getSigmaWidth());
        vo.setSubgroupSize(entity.getSubgroupSize());
        vo.setChartType(entity.getChartType());
        vo.setCalcMethod(entity.getCalcMethod());
        vo.setEffectiveFrom(entity.getEffectiveFrom());
        vo.setEffectiveTo(entity.getEffectiveTo());
        vo.setIsCurrent(entity.getIsCurrent());
        vo.setChangeReason(entity.getChangeReason());
        vo.setChangeType(entity.getChangeType());
        vo.setPrevVersionId(entity.getPrevVersionId());
        vo.setStatus(entity.getStatus());
        vo.setCreatedAt(entity.getCreatedAt());
        vo.setUpdatedAt(entity.getUpdatedAt());
        vo.setCreatedBy(entity.getCreatedBy());
        vo.setUpdatedBy(entity.getUpdatedBy());
        return vo;
    }
}
