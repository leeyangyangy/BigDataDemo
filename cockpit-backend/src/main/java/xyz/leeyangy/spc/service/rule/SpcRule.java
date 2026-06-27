package xyz.leeyangy.spc.service.rule;

import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * SPC 判异规则策略接口
 *
 * <p>每条判异规则实现该接口，由 {@link xyz.leeyangy.spc.service.SpcRuleEngine}
 * 统一调度。新增规则只需新增实现类，符合开闭原则。</p>
 */
public interface SpcRule {

    /** 规则序号 (1-8) */
    int ruleId();

    /** 规则代码，如 "N1" */
    String ruleCode();

    /** 规则中文名 */
    String ruleName();

    /**
     * 执行规则检测，将命中的报警追加到 alerts 列表。
     *
     * @param ctx         检测上下文（含数据、控制限、sigma 等）
     * @param flagged     已标记的样本索引集合（避免同一数据点被多规则重复报警）
     * @param alerts      报警结果收集列表
     */
    void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts);
}
