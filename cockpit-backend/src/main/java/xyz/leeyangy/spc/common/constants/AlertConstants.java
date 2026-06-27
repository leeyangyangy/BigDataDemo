package xyz.leeyangy.spc.common.constants;

/**
 * 报警相关常量：状态、严重级别、类型。
 */
public final class AlertConstants {

    private AlertConstants() {}

    /** 报警状态 */
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_CONFIRMED = "CONFIRMED";
    public static final String STATUS_HANDLED = "HANDLED";

    /** 报警严重级别 */
    public static final String SEVERITY_CRITICAL = "CRITICAL";
    public static final String SEVERITY_WARNING = "WARNING";
    public static final String SEVERITY_INFO = "INFO";

    /** 报警类型 */
    public static final String TYPE_RULE_VIOLATION = "RULE_VIOLATION";

    /** 报警级别（数字） */
    public static final int LEVEL_CRITICAL = 3;
    public static final int LEVEL_WARNING = 2;
    public static final int LEVEL_INFO = 1;
}
