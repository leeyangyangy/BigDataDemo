package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则4：连续14点上下交替 (N4)
 */
@Component
public class Rule4FourteenAlternate implements SpcRule {

    @Override
    public int ruleId() { return 4; }

    @Override
    public String ruleCode() { return "N4"; }

    @Override
    public String ruleName() { return "连续14点上下交替"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        double[] values = ctx.getValues();
        int n = values.length;
        if (n < 14) return;

        for (int start = 0; start <= n - 14; start++) {
            boolean alternating = true;
            for (int i = start; i < start + 13; i++) {
                double currentDiff = values[i + 1] - values[i];
                if (i > start) {
                    double prevDiff = values[i] - values[i - 1];
                    if (currentDiff * prevDiff >= 0) {
                        alternating = false;
                        break;
                    }
                }
            }
            if (alternating) {
                int triggerIdx = start + 13;
                if (!flagged.contains(triggerIdx)) {
                    alerts.add(ctx.createAlert(ctx.getDataList().get(triggerIdx), ruleCode(),
                            "连续14点上下交替震荡"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }
}
