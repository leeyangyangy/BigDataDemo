package xyz.leeyangy.spc.service.rule;

import lombok.Builder;
import lombok.Data;
import xyz.leeyangy.spc.common.constants.AlertConstants;
import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.entity.SpcData;
import xyz.leeyangy.spc.service.SpcRuleEngine;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * SPC 判异规则检测上下文
 *
 * <p>封装一次检测所需的全部数据与控制限参数，由规则策略类读取。
 * 同时提供 {@link #createAlert} 工厂方法，统一构造报警对象。</p>
 */
@Data
@Builder
public class SpcRuleContext {

    /** 原始数据列表 */
    private List<SpcData> dataList;

    /** 测量值数组（与 dataList 索引一一对应，便于下标访问） */
    private double[] values;

    /** 参数标准版本 */
    private ParamVersion version;

    /** 中心线 */
    private BigDecimal cl;

    /** 控制上限 */
    private BigDecimal ucl;

    /** 控制下限 */
    private BigDecimal lcl;

    /** 1 倍 sigma */
    private BigDecimal oneSigma;

    /** 2 倍 sigma */
    private BigDecimal twoSigma;

    /** 3 倍 sigma */
    private BigDecimal threeSigma;

    /**
     * 创建报警对象（由规则命中后调用）。
     *
     * @param data     命中数据点
     * @param ruleCode 规则代码
     * @param message  报警描述
     */
    public SpcAlert createAlert(SpcData data, String ruleCode, String message) {
        ParamVersion version = this.version;
        SpcAlert alert = new SpcAlert();
        alert.setAlertCode("SPC-" + ruleCode + "-" + System.currentTimeMillis());
        alert.setParamVersionId(version.getId());
        alert.setBatchId(data.getBatchId());
        alert.setProcessId(data.getProcessId());
        alert.setParamId(data.getParamId());
        alert.setProductId(data.getProductId());
        alert.setDataId(data.getId());
        alert.setRuleNumber(ruleCode);
        alert.setRuleName(SpcRuleEngine.RULE_NAMES.getOrDefault(ruleCode, "未知规则"));
        alert.setMeasuredValue(data.getMeasuredValue());
        alert.setUcl(version.getUcl());
        alert.setLcl(version.getLcl());
        alert.setUsl(version.getUsl());
        alert.setLsl(version.getLsl());
        alert.setCl(version.getCl());
        alert.setDeviation(data.getDeviation());
        alert.setSigmaLevel(data.getSigmaLevel());
        alert.setMessage(message);
        alert.setDetailJson("{\"value\":" + data.getMeasuredValue()
                + ",\"collectTime\":\"" + data.getCollectTime() + "\"}");
        alert.setStatus(AlertConstants.STATUS_ACTIVE);
        alert.setAlertLevel(ruleCode.equals(SpcRuleEngine.RULE_1_BEYOND_3SIGMA) ? AlertConstants.LEVEL_CRITICAL :
                ruleCode.equals(SpcRuleEngine.RULE_2_NINE_ONE_SIDE) || ruleCode.equals(SpcRuleEngine.RULE_3_SIX_TREND) ? AlertConstants.LEVEL_WARNING : AlertConstants.LEVEL_INFO);
        alert.setAlertType(AlertConstants.TYPE_RULE_VIOLATION);
        alert.setSeverity(alert.getAlertLevel() == AlertConstants.LEVEL_CRITICAL ? AlertConstants.SEVERITY_CRITICAL :
                alert.getAlertLevel() == AlertConstants.LEVEL_WARNING ? AlertConstants.SEVERITY_WARNING : AlertConstants.SEVERITY_INFO);
        alert.setAlertTime(LocalDateTime.now());
        return alert;
    }
}
