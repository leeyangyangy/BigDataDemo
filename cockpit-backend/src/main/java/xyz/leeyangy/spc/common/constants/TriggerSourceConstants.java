package xyz.leeyangy.spc.common.constants;

/**
 * 统计计算触发来源常量。
 * 用于 SpcStatResult.trigger_source 字段，标识统计结果由何种动作触发。
 */
public final class TriggerSourceConstants {

    private TriggerSourceConstants() {}

    /** 手动触发计算 */
    public static final String MANUAL = "MANUAL";

    /** 版本创建触发（新建参数版本后自动重算） */
    public static final String VERSION_CREATE = "VERSION_CREATE";

    /** 版本切换触发（启用版本后重算） */
    public static final String VERSION_SWITCH = "VERSION_SWITCH";

    /** 版本规格更新触发 */
    public static final String VERSION_UPDATE = "VERSION_UPDATE";

    /** 系统自动触发（非用户显式操作） */
    public static final String AUTO = "AUTO";
}
