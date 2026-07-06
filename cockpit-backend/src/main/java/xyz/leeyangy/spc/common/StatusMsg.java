package xyz.leeyangy.spc.common;

/**
 * 常用响应消息常量。
 * <p>
 * 与 {@link StatusCode} 配合使用: 后端所有 R.ok(msg) / R.fail(msg) 中出现的固定文案
 * 一律引用本类常量, 避免散落在各 Controller/Service 中的硬编码字符串。
 * 含动态内容(如 "导出失败: " + e.getMessage())的消息保持拼接, 仅固定部分常量化。
 */
public final class StatusMsg {

    private StatusMsg() {}

    // ============ 通用成功 ============
    public static final String SUCCESS = "操作成功";
    public static final String CREATE_SUCCESS = "创建成功";
    public static final String UPDATE_SUCCESS = "更新成功";
    public static final String DELETE_SUCCESS = "删除成功";
    public static final String COPY_SUCCESS = "复制成功";
    public static final String BIND_SUCCESS = "绑定成功";
    public static final String ADD_SUCCESS = "添加成功";

    // ============ 通用错误 ============
    public static final String SYSTEM_ERROR = "系统内部错误，请稍后重试";
    public static final String FORBIDDEN = "权限不足";
    public static final String UNAUTHORIZED = "登录已过期，请重新登录";
    public static final String TOKEN_INVALID = "Token已失效，请重新登录";
    public static final String NOT_LOGGED_IN = "未登录或Token已过期，请先登录";
    public static final String UPLOAD_TOO_LARGE = "文件大小超出限制";
    public static final String PARAM_TYPE_ERROR_PREFIX = "参数 '";
    public static final String PARAM_TYPE_ERROR_SUFFIX = "' 类型错误";

    // ============ 资源不存在 ============
    public static final String USER_NOT_FOUND = "用户不存在";
    public static final String PRODUCT_NOT_FOUND = "产品不存在";
    public static final String PROCESS_NOT_FOUND = "工序不存在";
    public static final String EQUIPMENT_NOT_FOUND = "设备不存在";
    public static final String WORKSHOP_NOT_FOUND = "车间不存在";
    public static final String STANDARD_NOT_FOUND = "工艺参数不存在";
    public static final String PARAM_VERSION_NOT_FOUND = "参数版本不存在";
    public static final String BATCH_NOT_FOUND = "批次不存在";

    // ============ 参数校验 (必填) ============
    public static final String EMP_NO_REQUIRED = "工号不能为空";
    public static final String NAME_REQUIRED = "姓名不能为空";
    public static final String PASSWORD_REQUIRED = "密码不能为空";
    public static final String PRODUCT_CODE_REQUIRED = "产品编码不能为空";
    public static final String PRODUCT_NAME_REQUIRED = "产品名称不能为空";
    public static final String PROCESS_CODE_REQUIRED = "工序编码不能为空";
    public static final String PROCESS_NAME_REQUIRED = "工序名称不能为空";
    public static final String EQUIPMENT_CODE_REQUIRED = "设备编码不能为空";
    public static final String EQUIPMENT_NAME_REQUIRED = "设备名称不能为空";

    // ============ 冲突 ============
    public static final String EMP_NO_EXISTS = "工号已存在";
    public static final String PRODUCT_CODE_EXISTS = "产品编码已存在";
    public static final String PROCESS_CODE_EXISTS = "工序编码已存在";
    public static final String EQUIPMENT_CODE_EXISTS = "设备编码已存在";
    public static final String WORKSHOP_CODE_EXISTS = "车间编码已存在";
    public static final String STANDARD_CODE_EXISTS = "工艺参数编码已存在";

    // ============ 业务状态 ============
    public static final String NO_WORKSHOP_ACCESS = "无绑定车间, 无法查看数据中心数据";
    public static final String WORKSHOP_ACCESS_DENIED = "无权访问该车间数据";
    public static final String VERSION_DELETE_CONFLICT = "当前生效的版本不能删除，请先切换到其他版本";
    public static final String MAIN_WORKSHOP_REQUIRED = "主车间必须在所选车间列表中，请重新指定";
    public static final String WORKSHOP_NOT_DATA_CENTER = "车间不存在或未标记为数据中心可见";

    // ============ 鉴权 ============
    public static final String ORIGINAL_PASSWORD_ERROR = "原密码错误";
    public static final String PASSWORD_REUSED = "新密码不能与最近使用过的密码相同, 请更换";
    public static final String PASSWORD_SAME_AS_CURRENT = "新密码不能与当前密码相同";
    public static final String KEY_EXCHANGE_PARAMS_REQUIRED = "encKey/encIv 不能为空";
    public static final String KEY_EXCHANGE_FAILED = "key-exchange 失败";
    public static final String QRCODE_EXPIRED = "扫码已过期，请重新扫码";
    public static final String LOGIN_FAILED = "用户不存在或密码错误";
    public static final String ACCOUNT_DISABLED = "账号已停用，请联系管理员";
    public static final String WECOM_CODE_INVALID = "企业微信授权码无效";
    public static final String WECOM_NOT_BOUND = "未找到关联的账号，请先联系管理员绑定企业微信";

    // ============ SPC / 数据 ============
    public static final String IMPORT_FILE_REQUIRED = "导入文件不能为空";
    public static final String IMPORT_PARAM_REQUIRED = "请先选择工艺参数后再导入数据";
    public static final String IMPORT_PRODUCT_REQUIRED = "请先选择产品后再导入数据";
    public static final String IMPORT_USER_NOT_LOGIN = "用户未登录，无法导入数据";
    public static final String IMPORT_ROLE_MISSING = "用户角色缺失，无法导入数据";
    public static final String NO_SPC_DATA_FOR_REPORT = "所选范围内无 SPC 数据,无法生成报告";
    public static final String PARAM_REQUIRED_FOR_DATA = "工艺参数不能为空，请先选择工艺参数";
    public static final String PRODUCT_REQUIRED_FOR_DATA = "产品不能为空，请先选择产品";
    public static final String PROCESS_REQUIRED_FOR_DATA = "工序不能为空，请先选择工序";
    public static final String EQUIPMENT_ID_REQUIRED = "设备ID不能为空，请选择设备";
    public static final String MEASURE_VALUE_REQUIRED = "测量值不能为空";
    public static final String SUBMIT_DATA_REQUIRED = "提交数据不能为空";
    public static final String PARAM_VERSION_NOT_CONFIGURED = "该工艺参数尚未配置标准版本，请联系工程师在【后台管理-工艺参数管理-版本】中创建版本后再提交数据";
}
