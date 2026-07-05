package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则1：数据点超出3σ控制限 (N1)
 */
@Component
public class Rule1Beyond3Sigma implements SpcRule {

    @Override
    public int ruleId() { return 1; }

    @Override
    public String ruleCode() { return "N1"; }

    @Override
    public String ruleName() { return "超出3σ控制限"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        double[] values = ctx.getValues();
        Double ucl = ctx.getUcl() != null ? ctx.getUcl().doubleValue() : null;
        Double lcl = ctx.getLcl() != null ? ctx.getLcl().doubleValue() : null;
        double tolerance = ctx.getThreeSigma().doubleValue() * 0.001;

        for (int i = 0; i < values.length; i++) {
            if (flagged.contains(i)) continue;
            double v = values[i];
            if (ucl != null && v > ucl + tolerance) {
                alerts.add(ctx.createAlert(ctx.getDataList().get(i), ruleCode(),
                        "数据点超出3σ控制限: 值=" + v + " > UCL"));
                flagged.add(i);
            } else if (lcl != null && v < lcl - tolerance) {
                alerts.add(ctx.createAlert(ctx.getDataList().get(i), ruleCode(),
                        "数据点超出3σ控制限: 值=" + v + " < LCL"));
                flagged.add(i);
            }
        }
    }
}
