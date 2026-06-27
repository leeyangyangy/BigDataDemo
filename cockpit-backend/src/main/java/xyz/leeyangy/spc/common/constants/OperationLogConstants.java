package xyz.leeyangy.spc.common.constants;

/**
 * 操作日志相关常量：操作结果、业务模块。
 */
public final class OperationLogConstants {

    private OperationLogConstants() {}

    /** 操作结果 */
    public static final String RESULT_SUCCESS = "SUCCESS";
    public static final String RESULT_FAIL = "FAIL";
    public static final String RESULT_PARTIAL = "PARTIAL";

    /** 业务模块 */
    public static final String MODULE_SPC_DATA = "SPC_DATA";
    public static final String MODULE_PRODUCT = "PRODUCT";
    public static final String MODULE_PROCESS = "PROCESS";
    public static final String MODULE_PARAM = "PARAM";
    public static final String MODULE_BATCH = "BATCH";
    public static final String MODULE_EQUIPMENT = "EQUIPMENT";
    public static final String MODULE_WORKSHOP = "WORKSHOP";
    public static final String MODULE_ALERT = "ALERT";
    public static final String MODULE_USER = "USER";
    public static final String MODULE_STANDARD = "STANDARD";
    public static final String MODULE_SYSTEM = "SYSTEM";

    /** 操作动作 */
    public static final String ACTION_UPLOAD = "UPLOAD";
    public static final String ACTION_BATCH_UPLOAD = "BATCH_UPLOAD";
    public static final String ACTION_IMPORT_EXCEL = "IMPORT_EXCEL";
    public static final String ACTION_EXPORT_EXCEL = "EXPORT_EXCEL";
    public static final String ACTION_CREATE = "CREATE";
    public static final String ACTION_UPDATE = "UPDATE";
    public static final String ACTION_DELETE = "DELETE";
}
