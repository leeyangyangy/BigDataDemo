package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则2：连续9点在中心线同侧 (N2)
 */
@Component
public class Rule2NineOneSide implements SpcRule {

    @Override
    public int ruleId() { return 2; }

    @Override
    public String ruleCode() { return "N2"; }

    @Override
    public String ruleName() { return "连续9点在中心线同侧"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        double[] values = ctx.getValues();
        int n = values.length;
        if (n < 9) return;
        double centerLine = ctx.getCl().doubleValue();

        for (int end = 9; end <= n; end++) {
            boolean allAbove = true;
            boolean allBelow = true;
            for (int i = end - 9; i < end && (allAbove || allBelow); i++) {
                if (values[i] >= centerLine) allBelow = false;
                if (values[i] <= centerLine) allAbove = false;
            }
            if (allAbove || allBelow) {
                int triggerIdx = end - 1;
                if (!flagged.contains(triggerIdx)) {
                    alerts.add(ctx.createAlert(ctx.getDataList().get(triggerIdx), ruleCode(),
                            "连续9点在中心线" + (allAbove ? "上方" : "下方")));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }
}
