package xyz.leeyangy.spc.common;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class SpcRuleConstants {
    private SpcRuleConstants() {}

    public static final int MIN_RULE_ID = 1;
    public static final int MAX_RULE_ID = 8;
    public static final int TOTAL_RULES = MAX_RULE_ID - MIN_RULE_ID + 1;
    public static final int SELECT_ALL_FLAG = 0;

    public static final Set<Integer> VALID_RULE_IDS = Collections.unmodifiableSet(
            IntStream.rangeClosed(MIN_RULE_ID, MAX_RULE_ID).boxed().collect(Collectors.toSet())
    );

    public static boolean isValidRuleId(Integer id) {
        return id != null && id >= MIN_RULE_ID && id <= MAX_RULE_ID;
    }

    public static String formatValidRange() {
        return "[" + MIN_RULE_ID + "-" + MAX_RULE_ID + "]";
    }
}