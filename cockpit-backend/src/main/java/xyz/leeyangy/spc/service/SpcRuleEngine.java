package xyz.leeyangy.spc.service;

import xyz.leeyangy.spc.entity.ParamVersion;
import xyz.leeyangy.spc.entity.SpcAlert;
import xyz.leeyangy.spc.entity.SpcData;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SPC 判异规则引擎接口
 *
 * <p>定义 8 条判异规则常量及规则名映射，提供判异检测入口。</p>
 */
public interface SpcRuleEngine {

    String RULE_1_BEYOND_3SIGMA = "N1";
    String RULE_2_NINE_ONE_SIDE = "N2";
    String RULE_3_SIX_TREND = "N3";
    String RULE_4_FOURTEEN_ALTERNATE = "N4";
    String RULE_5_TWO_OF_THREE_2SIGMA = "N5";
    String RULE_6_FOUR_OF_FIVE_1SIGMA = "N6";
    String RULE_7_FIFTEEN_IN_1SIGMA = "N7";
    String RULE_8_EIGHT_OUTSIDE_1SIGMA = "N8";

    /**
     * 规则代码 → 规则中文名映射
     */
    Map<String, String> RULE_NAMES = Map.of(
            RULE_1_BEYOND_3SIGMA, "超出3σ控制限",
            RULE_2_NINE_ONE_SIDE, "连续9点在中心线同侧",
            RULE_3_SIX_TREND, "连续6点单调递增/递减",
            RULE_4_FOURTEEN_ALTERNATE, "连续14点上下交替",
            RULE_5_TWO_OF_THREE_2SIGMA, "连续3点中有2点超出2σ",
            RULE_6_FOUR_OF_FIVE_1SIGMA, "连续5点中有4点超出1σ",
            RULE_7_FIFTEEN_IN_1SIGMA, "连续15点在1σ内(分层)",
            RULE_8_EIGHT_OUTSIDE_1SIGMA, "连续8点在1σ外(分层)"
    );

    List<SpcAlert> detectRules(List<SpcData> dataList, ParamVersion version);

    List<SpcAlert> detectRules(List<SpcData> dataList, ParamVersion version, Set<Integer> enabledRuleIds);

    void saveAndLogAlerts(List<SpcData> dataList, ParamVersion version);
}
