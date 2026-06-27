package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则3：连续6点单调递增/递减 (N3)
 */
@Component
public class Rule3SixTrend implements SpcRule {

    @Override
    public int ruleId() { return 3; }

    @Override
    public String ruleCode() { return "N3"; }

    @Override
    public String ruleName() { return "连续6点单调递增/递减"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        double[] values = ctx.getValues();
        int n = values.length;
        if (n < 6) return;

        for (int start = 0; start <= n - 6; start++) {
            boolean increasing = true;
            boolean decreasing = true;
            for (int i = start; i < start + 5; i++) {
                if (values[i + 1] <= values[i]) increasing = false;
                if (values[i + 1] >= values[i]) decreasing = false;
            }
            if (increasing || decreasing) {
                int triggerIdx = start + 5;
                if (!flagged.contains(triggerIdx)) {
                    alerts.add(ctx.createAlert(ctx.getDataList().get(triggerIdx), ruleCode(),
                            "连续6点" + (increasing ? "递增" : "递减") + "趋势"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }
}
