package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则5：连续3点中有2点超出2σ (N5)
 */
@Component
public class Rule5TwoOfThree2Sigma implements SpcRule {

    @Override
    public int ruleId() { return 5; }

    @Override
    public String ruleCode() { return "N5"; }

    @Override
    public String ruleName() { return "连续3点中有2点超出2σ"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        double[] values = ctx.getValues();
        int n = values.length;
        if (n < 3) return;
        double centerLine = ctx.getCl().doubleValue();
        double upper2sigma = centerLine + ctx.getTwoSigma().doubleValue();
        double lower2sigma = centerLine - ctx.getTwoSigma().doubleValue();
        // 单边场景只检查存在的侧
        boolean checkUpper = !"LOWER".equals(ctx.getSided());
        boolean checkBelow = !"UPPER".equals(ctx.getSided());

        for (int end = 3; end <= n; end++) {
            int countAbove2S = 0;
            int countBelow2S = 0;
            int lastAboveIdx = -1;
            int lastBelowIdx = -1;
            for (int i = end - 3; i < end; i++) {
                if (checkUpper && values[i] > upper2sigma) { countAbove2S++; lastAboveIdx = i; }
                if (checkBelow && values[i] < lower2sigma) { countBelow2S++; lastBelowIdx = i; }
            }
            if (countAbove2S >= 2 || countBelow2S >= 2) {
                int triggerIdx = (countAbove2S >= 2) ? lastAboveIdx : lastBelowIdx;
                if (triggerIdx >= 0 && !flagged.contains(triggerIdx)) {
                    alerts.add(ctx.createAlert(ctx.getDataList().get(triggerIdx), ruleCode(),
                            "连续3点中有2点超出2σ范围"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }
}
