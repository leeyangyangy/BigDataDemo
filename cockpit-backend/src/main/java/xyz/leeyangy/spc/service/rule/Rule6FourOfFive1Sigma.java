package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则6：连续5点中有4点超出1σ (N6)
 */
@Component
public class Rule6FourOfFive1Sigma implements SpcRule {

    @Override
    public int ruleId() { return 6; }

    @Override
    public String ruleCode() { return "N6"; }

    @Override
    public String ruleName() { return "连续5点中有4点超出1σ"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        double[] values = ctx.getValues();
        int n = values.length;
        if (n < 5) return;
        double centerLine = ctx.getCl().doubleValue();
        double upper1sigma = centerLine + ctx.getOneSigma().doubleValue();
        double lower1sigma = centerLine - ctx.getOneSigma().doubleValue();

        for (int end = 5; end <= n; end++) {
            int countAbove1S = 0;
            int countBelow1S = 0;
            int lastAboveIdx = -1;
            int lastBelowIdx = -1;
            for (int i = end - 5; i < end; i++) {
                if (values[i] > upper1sigma) { countAbove1S++; lastAboveIdx = i; }
                if (values[i] < lower1sigma) { countBelow1S++; lastBelowIdx = i; }
            }
            if (countAbove1S >= 4 || countBelow1S >= 4) {
                int triggerIdx = (countAbove1S >= 4) ? lastAboveIdx : lastBelowIdx;
                if (triggerIdx >= 0 && !flagged.contains(triggerIdx)) {
                    alerts.add(ctx.createAlert(ctx.getDataList().get(triggerIdx), ruleCode(),
                            "连续5点中有4点超出1σ范围"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }
}
