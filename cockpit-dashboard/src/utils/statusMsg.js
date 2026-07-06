/**
 * 常用响应消息常量 (与后端 xyz.leeyangy.spc.common.StatusMsg 保持一致)。
 *
 * 统一约定: 所有 API 响应的提示文案 (msg) 一律取自后端响应体 result.msg;
 * 此处常量仅用于前端兜底提示 (当后端未返回 msg 时) 及少量本地校验提示,
 * 不再维护按状态码反查消息的映射表。
 */
export const StatusMsg = Object.freeze({
  // 通用成功
  SUCCESS: '操作成功',
  CREATE_SUCCESS: '创建成功',
  UPDATE_SUCCESS: '更新成功',
  DELETE_SUCCESS: '删除成功',
  COPY_SUCCESS: '复制成功',
  BIND_SUCCESS: '绑定成功',
  ADD_SUCCESS: '添加成功',

  // 通用错误
  SYSTEM_ERROR: '系统内部错误，请稍后重试',
  FORBIDDEN: '权限不足',
  UNAUTHORIZED: '登录已过期，请重新登录',
  TOKEN_INVALID: 'Token已失效，请重新登录',
  NOT_LOGGED_IN: '未登录或Token已过期，请先登录',
  UPLOAD_TOO_LARGE: '文件大小超出限制',
  NETWORK_ERROR: '网络连接失败，请检查网络或服务是否启动',
  REQUEST_FAILED: '请求失败',
  OPERATION_FAILED: '操作失败',
  SAVE_FAILED: '保存失败',
  BIND_FAILED: '绑定失败',
  UPDATE_FAILED: '更新失败',
  CREATE_FAILED: '创建失败',
  CREATE_VERSION_FAILED: '创建版本失败',
  DISABLE_FAILED: '停用失败',
  DELETE_FAILED: '删除失败',
  SUBMIT_EXCEPTION: '提交异常',
  LOGIN_FAILED: '登录失败',

  // 资源不存在
  USER_NOT_FOUND: '用户不存在',
  PRODUCT_NOT_FOUND: '产品不存在',
  PROCESS_NOT_FOUND: '工序不存在',
  EQUIPMENT_NOT_FOUND: '设备不存在',
  WORKSHOP_NOT_FOUND: '车间不存在',
  STANDARD_NOT_FOUND: '工艺参数不存在',

  // 鉴权
  LOGIN_CREDENTIAL_FAILED: '用户不存在或密码错误',
  ACCOUNT_DISABLED: '账号已停用，请联系管理员',
  QRCODE_EXPIRED: '扫码已过期，请重新扫码',
  WECOM_NOT_CONFIGURED: '企业微信登录未配置，请联系管理员',
  SCAN_VERIFY_FAILED: '扫码验证失败',
  WECOM_BIND_FAILED: '绑定失败',

  // 业务
  NO_WORKSHOP_ACCESS: '无绑定车间, 无法查看数据中心数据'
})

export default StatusMsg
