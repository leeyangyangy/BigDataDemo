package xyz.leeyangy.spc.service.rule;

import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.entity.SpcAlert;

import java.util.List;
import java.util.Set;

/**
 * 规则8：连续8点都在1σ之外（混合来源）(N8)
 */
@Component
public class Rule8EightOutside1Sigma implements SpcRule {

    @Override
    public int ruleId() { return 8; }

    @Override
    public String ruleCode() { return "N8"; }

    @Override
    public String ruleName() { return "连续8点在1σ外(分层)"; }

    @Override
    public void check(SpcRuleContext ctx, Set<Integer> flagged, List<SpcAlert> alerts) {
        double[] values = ctx.getValues();
        int n = values.length;
        if (n < 8) return;
        double centerLine = ctx.getCl().doubleValue();
        double upper1sigma = centerLine + ctx.getOneSigma().doubleValue();
        double lower1sigma = centerLine - ctx.getOneSigma().doubleValue();

        for (int end = 8; end <= n; end++) {
            boolean allOutside = true;
            boolean hasAbove = false;
            boolean hasBelow = false;
            for (int i = end - 8; i < end; i++) {
                if (values[i] >= lower1sigma && values[i] <= upper1sigma) {
                    allOutside = false;
                    break;
                }
                if (values[i] > upper1sigma) hasAbove = true;
                if (values[i] < lower1sigma) hasBelow = true;
            }
            // Nelson Rule 8 标准要求：8点都在1σ外，且两侧都有点
            if (allOutside && hasAbove && hasBelow) {
                int triggerIdx = end - 1;
                if (!flagged.contains(triggerIdx)) {
                    alerts.add(ctx.createAlert(ctx.getDataList().get(triggerIdx), ruleCode(),
                            "连续8点都在1σ范围之外且两侧分布(可能存在混合来源)"));
                    flagged.add(triggerIdx);
                }
                break;
            }
        }
    }
}
