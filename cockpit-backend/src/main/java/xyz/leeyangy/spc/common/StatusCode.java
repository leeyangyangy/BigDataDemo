package xyz.leeyangy.spc.common;

public final class StatusCode {

    private StatusCode() {}

    public static final int SUCCESS = 200;
    public static final int FAIL = 500;

    public static final int UNAUTHORIZED = 401;
    public static final int FORBIDDEN = 403;
    public static final int NOT_FOUND = 404;
    public static final int BAD_REQUEST = 400;
    public static final int CONFLICT = 409;
    public static final int PAYLOAD_TOO_LARGE = 413;

    public static final int AUTH_LOGIN_FAILED = 1001;
    public static final int AUTH_ACCOUNT_DISABLED = 1002;
    public static final int AUTH_TOKEN_EXPIRED = 1003;
    public static final int AUTH_CAPTCHA_ERROR = 1004;

    public static final int PARAM_ERROR = 2001;
    public static final int PARAM_REQUIRED = 2002;
    public static final int PARAM_FORMAT_ERROR = 2003;

    public static final int DATA_NOT_FOUND = 3001;
    public static final int DATA_SAVE_FAILED = 3002;
    public static final int DATA_DELETE_FAILED = 3003;
    public static final int DATA_OUT_OF_RANGE = 3004;

    public static final int PRODUCT_NOT_FOUND = 4001;
    public static final int PRODUCT_EXISTS = 4002;
    public static final int PRODUCT_DISABLED = 4003;

    public static final int PROCESS_NOT_FOUND = 5001;
    public static final int PARAM_NOT_FOUND = 5002;
    public static final int BATCH_NOT_FOUND = 5003;
    public static final int VERSION_NOT_FOUND = 5004;
    public static final int VERSION_CONFLICT = 5005;

    public static final int SPC_CALC_FAILED = 6001;
    public static final int SPC_NO_DATA = 6002;
    public static final int SPC_OUT_OF_CONTROL = 6003;

    public static final int ALERT_NOT_FOUND = 7001;
    public static final int ALERT_ACK_FAILED = 7002;
    public static final int ALERT_RESOLVE_FAILED = 7003;

    public static final int SYSTEM_BUSY = 9001;
    public static final int SYSTEM_ERROR = 9999;
}
