package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则7：连续15点落在1σ内（分层）(N7)
 */
@Component
public class Rule7FifteenIn1Sigma implements SpcRule {

    @Override
    public int ruleId() { return 7; }

    @Override
    public String ruleCode() { return "N7"; }

    @Override
    public String ruleName() { return "连续15点在1σ内(分层)"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        // Rule7 检测连续15点落在1σ内(分层)，依赖双侧1σ区间，单边控制限场景跳过
        if (!"BOTH".equals(ctx.getSided())) return;
        double[] values = ctx.getValues();
        int n = values.length;
        if (n < 15) return;
        double centerLine = ctx.getCl().doubleValue();
        double upper1sigma = centerLine + ctx.getOneSigma().doubleValue();
        double lower1sigma = centerLine - ctx.getOneSigma().doubleValue();

        for (int end = 15; end <= n; end++) {
            boolean allWithin = true;
            for (int i = end - 15; i < end; i++) {
                if (values[i] > upper1sigma || values[i] < lower1sigma) {
                    allWithin = false;
                    break;
                }
            }
            if (allWithin) {
                int triggerIdx = end - 1;
                if (!flagged.contains(triggerIdx)) {
                    alerts.add(ctx.createAlert(ctx.getDataList().get(triggerIdx), ruleCode(),
                            "连续15点落在1σ范围内(可能存在分层)"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }
}
