const BASE_URL = '/api'

const TOKEN_KEY = 'spc_token'
const USER_KEY = 'spc_user'

import {
  isEncryptionEnabled,
  getEncryptedBody,
  decryptResponse,
  ensurePublicKey,
  generateAndExchangeKey,
  clearKey
} from './crypto.js'
import {
  secureSetToken,
  secureRemoveToken,
  isTokenExpired,
  updateLastActivity
} from './tokenSecurity.js'

export function getToken() {
  const token = localStorage.getItem(TOKEN_KEY)
  // Token 过期前端预检 (等保三级: 会话超时)
  if (token && isTokenExpired(token)) {
    console.warn('[Auth] Token 已过期, 自动清除')
    secureRemoveToken()
    window.dispatchEvent(new CustomEvent('auth:expired'))
    return null
  }
  return token
}

export function setToken(token) {
  secureSetToken(token)
}

export function removeToken() {
  secureRemoveToken()
}

export function getUser() {
  const raw = localStorage.getItem(USER_KEY)
  return raw ? JSON.parse(raw) : null
}

export function setUser(user) {
  localStorage.setItem(USER_KEY, JSON.stringify(user))
}

export function isLoggedIn() {
  const token = localStorage.getItem(TOKEN_KEY)
  return !!token && !isTokenExpired(token)
}

const StatusCodeMsg = {
  200: '操作成功',

  401: '登录已过期，请重新登录',
  403: '权限不足，无法执行此操作',
  404: '请求的资源不存在',
  400: '请求参数有误',
  409: '数据冲突，请刷新后重试',

  1001: '工号或密码错误',
  1002: '账号已停用，请联系管理员',
  1003: 'Token已过期，请重新登录',
  1004: '验证码错误',

  2001: '参数校验失败',
  2002: '缺少必填参数',
  2003: '参数格式不正确',

  3001: '数据未找到',
  3002: '数据保存失败',
  3003: '数据删除失败',
  3004: '数据超出范围',

  4001: '产品不存在',
  4002: '产品编码已存在',
  4003: '产品已停用',

  5001: '工序未找到',
  5002: '参数未找到',
  5003: '批次未找到',
  5004: '版本配置未找到',
  5005: '版本存在冲突',

  6001: 'SPC计算失败',
  6002: '没有足够的数据进行计算',
  6003: '数据超出控制限',

  7001: '报警记录未找到',
  7002: '报警确认失败',
  7003: '报警处理失败',

  9001: '系统繁忙，请稍后重试',
  9999: '系统内部错误'
}

function getStatusMsg(code, serverMsg) {
  if (StatusCodeMsg[code]) {
    return serverMsg || StatusCodeMsg[code]
  }
  if (code >= 400 && code < 500) {
    return serverMsg || '请求失败'
  }
  if (code >= 500) {
    return serverMsg || '服务器异常'
  }
  return serverMsg || '未知错误'
}

class ApiClient {
  constructor(baseURL) {
    this.baseURL = baseURL
  }

  async request(url, options = {}) {
    const token = getToken()
    const headers = {
      'Content-Type': 'application/json',
      ...options.headers
    }
    if (token) {
      headers['Authorization'] = `Bearer ${token}`
    }

    if (isEncryptionEnabled()) {
      headers['X-Encrypted'] = 'true'
    }

    let body = options.body
    // /auth/login 和 /auth/key-exchange 跳过加密: 登录时 KV 未建立, key-exchange 是 KV 建立本身
    const skipEncrypt = url.includes('/auth/login') || url.includes('/auth/key-exchange')
    if (body && isEncryptionEnabled() && !skipEncrypt && headers['Content-Type']?.includes('json')) {
      try {
        body = JSON.stringify(getEncryptedBody(JSON.parse(body)))
        console.log(`[Crypto] 请求已加密: ${url}`)
      } catch (e) {
        console.warn('[Crypto] 加密跳过（非JSON或登录请求）:', url)
      }
    }

    const config = { headers, ...options, body }

    try {
      const response = await fetch(`${this.baseURL}${url}`, config)

      // 请求成功发出即更新活动时间 (用于会话超时检测)
      if (token) {
        updateLastActivity()
      }

      if (response.status === 401) {
        removeToken()
        window.dispatchEvent(new CustomEvent('auth:expired'))
        throw new Error(StatusCodeMsg[401])
      }

      if (response.status === 403) {
        throw new Error(StatusCodeMsg[403])
      }

      if (!response.ok && response.status !== 200) {
        const body = await response.json().catch(() => ({}))
        const msg = getStatusMsg(response.code || response.status, body.msg)
        throw new Error(msg)
      }

      if (response.status === 204) {
        return null
      }

      let result = await response.json()

      if (isEncryptionEnabled() && result.encrypted) {
        console.log(`[Crypto] 收到加密响应: ${url}, data长度=${result.data?.length || 0}`)
        const before = result
        result = decryptResponse(result)
        const ok = result && typeof result === 'object' && result.code !== undefined
        console.log(`[Crypto] 解密${ok ? '成功' : '失败/异常'}: ${url}`, ok ? '' : '返回类型=' + typeof result, '原文data前40字符=' + (before.data || '').substring(0, 40))
      }

      if (result.code !== undefined && result.code !== 200) {
        const msg = getStatusMsg(result.code, result.msg)
        console.warn(`[API] ${url} 返回业务错误: code=${result.code}, msg=${msg}`)
        return result
      }

      return result
    } catch (error) {
      if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
        error.message = '网络连接失败，请检查网络或服务是否启动'
      }
      console.error(`[API] ${url} 请求失败:`, error.message)
      throw error
    }
  }

  get(url, params = {}) {
    const filtered = Object.fromEntries(Object.entries(params).filter(([_, v]) => v != null && v !== ''))
    const queryString = new URLSearchParams(filtered).toString()
    const fullUrl = queryString ? `${url}?${queryString}` : url
    return this.request(fullUrl, { method: 'GET' })
  }

  post(url, data = {}) {
    return this.request(url, {
      method: 'POST',
      body: JSON.stringify(data)
    })
  }

  put(url, data = {}) {
    return this.request(url, {
      method: 'PUT',
      body: JSON.stringify(data)
    })
  }

  delete(url, params = {}) {
    const queryString = new URLSearchParams(params).toString()
    const fullUrl = queryString ? `${url}?${queryString}` : url
    return this.request(fullUrl, { method: 'DELETE' })
  }
}

const api = new ApiClient(BASE_URL)

export default api

export const authApi = {
  login: (empNo, password) => api.post('/auth/login', { empNo, password }),
  getUserInfo: () => api.get('/auth/info'),
  logout: () => api.post('/auth/logout'),
  getWeComConfig: () => api.get('/auth/wecom/config'),
  weComCallback: (code) => api.post('/auth/wecom/callback', { code }),
  weComBind: (wecomUserId, empNo) => api.post('/auth/wecom/bind', { wecomUserId, empNo }),
  getPublicKey: () => api.get('/auth/public-key'),
  keyExchange: (encKey, encIv) => api.post('/auth/key-exchange', { encKey, encIv })
}

/**
 * 登录成功后调用: 拉取后端 RSA 公钥, 生成随机 AES KV 上送。
 * 上送成功后, 后续请求/响应自动走加密通道。
 *
 * 注意: 调用此函数前必须先 setToken(), 因为 /auth/key-exchange 需要鉴权。
 * 在 dev 环境下 (MODE === 'development') 此函数为 no-op。
 */
export async function ensureCryptoReady() {
  if (!isEncryptionEnabled()) return
  await ensurePublicKey(() => authApi.getPublicKey())
  await generateAndExchangeKey((encKey, encIv) => authApi.keyExchange(encKey, encIv))
}

export const yieldApi = {
  getData: (params) => api.get('/yield/data', params),
  search: (params) => api.get('/yield/search', params),
  getWorkshops: () => api.get('/yield/workshops'),
  checkAccess: () => api.get('/yield/access'),
  /** 数据中心: 查询指定车间需渲染的组件 key 列表 */
  getComponents: (workshop) => api.get('/yield/components', { workshop })
}

export const spcApi = {
  getControlChart: (params) => api.get('/spc/chart/control', params),
  getDataByEquipment: (params) => api.get('/spc/chart/data', params),
  getLatestStat: (params) => api.get('/spc/stat/latest', params),
  calculateStat: (params) => api.post(`/spc/stat/calculate?${new URLSearchParams(params)}`),
  getDataPage: (params) => api.get('/spc/data/page', params),
  uploadData: (data) => api.post('/spc/data/upload', data),
  batchUploadData: (dataList) => api.post('/spc/data/batch-upload', dataList),
  getAlertPage: (params) => api.get('/spc/alert/page', params),
  ackAlert: (id) => api.put(`/spc/alert/${id}/ack`),
  resolveAlert: (id, remark) => api.put(`/spc/alert/${id}/resolve?remark=${remark || ''}`),
  getParamVersionCurrent: (params) => api.get('/spc/param-version/current', params),
  getParamVersionHistory: (params) => api.get('/spc/param-version/history', params),
  getBatchPage: (params) => api.get('/batch/page', params),
  createBatch: (data) => api.post('/batch', data),
  getProductPage: (params) => api.get('/product/page', params),
  createProduct: (data) => api.post('/product', data),
  getProductProcesses: (productId) => api.get(`/product/${productId}/processes`),
  getProductProcessIds: (productId) => api.get(`/product/${productId}/process-ids`),
  bindProductProcesses: (productId, items) => api.post(`/product/${productId}/processes/bind`, items),
  bindProductProcess: (productId, processId) => api.post(`/product/${productId}/processes/${processId}/bind`),
  unbindProductProcess: (productId, processId) => api.delete(`/product/${productId}/processes/${processId}`),
  getProcessPage: (params) => api.get('/process/page', params),
  getProcessEquipment: (processId) => api.get(`/process/${processId}/equipment`),
  getProcessParams: (processId) => api.get(`/process/${processId}/params`),
  getProcessParamIds: (processId) => api.get(`/process/${processId}/param-ids`),
  bindProcessParams: (processId, items) => api.post(`/process/${processId}/params/bind`, items),
  bindProcessParam: (processId, paramId) => api.post(`/process/${processId}/params/${paramId}/bind`),
  unbindProcessParam: (processId, paramId) => api.delete(`/process/${processId}/params/${paramId}`),
  getParamPage: (params) => api.get('/spc/param/page', params),
  getAlerts: (params) => api.get('/spc/chart/alerts', params),
  importData: (formData) => api.post('/spc/data/import', formData, { headers: { 'Content-Type': 'multipart/form-data' } }),
  downloadTemplate: () => '/api/spc/data/template',
  exportReport: (params) => {
    const queryString = new URLSearchParams(params).toString()
    return `/api/spc/data/export/report?${queryString}`
  }
}

export const adminApi = {
  user: {
    getPage: (params) => api.get('/admin/user/page', params),
    getById: (id) => api.get(`/admin/user/${id}`),
    create: (data) => api.post('/admin/user', data),
    update: (id, data) => api.put(`/admin/user/${id}`, data),
    toggleStatus: (id, status) => api.put(`/admin/user/${id}/status`, { status }),
    delete: (id) => api.delete(`/admin/user/${id}`)
  },
  product: {
    getPage: (params) => api.get('/admin/product/page', params),
    getById: (id) => api.get(`/admin/product/${id}`),
    create: (data) => api.post('/admin/product', data),
    update: (id, data) => api.put(`/admin/product/${id}`, data),
    delete: (id) => api.delete(`/admin/product/${id}`),
    getProcesses: (productId) => api.get(`/spc/product/${productId}/processes`),
    bindProcesses: (productId, items) => api.post(`/spc/product/${productId}/processes/bind`, items)
  },
  process: {
    getPage: (params) => api.get('/admin/process/page', params),
    getById: (id) => api.get(`/admin/process/${id}`),
    create: (data) => api.post('/admin/process', data),
    update: (id, data) => api.put(`/admin/process/${id}`, data),
    delete: (id) => api.delete(`/admin/process/${id}`),
    duplicate: (id) => api.post(`/admin/process/${id}/duplicate`),
    getParams: (processId) => api.get(`/spc/process/${processId}/params`),
    bindParams: (processId, items) => api.post(`/spc/process/${processId}/params/bind`, items)
  },
  standard: {
    getPage: (params) => api.get('/admin/standard/page', params),
    getById: (id) => api.get(`/admin/standard/${id}`),
    create: (data) => api.post('/admin/standard', data),
    update: (id, data) => api.put(`/admin/standard/${id}`, data),
    delete: (id) => api.delete(`/admin/standard/${id}`),
    duplicate: (id) => api.post(`/admin/standard/${id}/duplicate`)
  },
  equipment: {
    getPage: (params) => api.get('/admin/equipment/page', params),
    getById: (id) => api.get(`/admin/equipment/${id}`),
    create: (data) => api.post('/admin/equipment', data),
    update: (id, data) => api.put(`/admin/equipment/${id}`, data),
    delete: (id) => api.delete(`/admin/equipment/${id}`)
  },
  workshop: {
    getPage: (params) => api.get('/admin/workshop/page', params),
    listAll: (workshopType) => api.get('/admin/workshop/list', workshopType ? { workshopType } : {}),
    getById: (id) => api.get(`/admin/workshop/${id}`),
    create: (data) => api.post('/admin/workshop', data),
    update: (id, data) => api.put(`/admin/workshop/${id}`, data),
    delete: (id) => api.delete(`/admin/workshop/${id}`)
  },
  paramVersion: {
    getPage: (params) => api.get('/admin/param-version/page', params),
    getById: (id) => api.get(`/admin/param-version/${id}`),
    create: (data) => api.post('/admin/param-version/create', data),
    enable: (id) => api.put(`/admin/param-version/${id}/enable`),
    disable: (id) => api.put(`/admin/param-version/${id}/disable`),
    update: (id, data) => api.put(`/admin/param-version/${id}`, data),
    delete: (id) => api.delete(`/admin/param-version/${id}`)
  },
  spcData: {
    getPage: (params) => api.get('/admin/spc-data/page', params),
    getById: (id) => api.get(`/admin/spc-data/${id}`),
    delete: (id) => api.delete(`/admin/spc-data/${id}`),
    batchDelete: (ids) => api.post('/admin/spc-data/batch-delete', ids)
  },
  changeLog: {
    getPage: (params) => api.get('/admin/change-log/page', params),
    getDetail: (id) => api.get(`/admin/change-log/${id}`),
    delete: (id) => api.delete(`/admin/change-log/${id}`)
  },
  operationLog: {
    getPage: (params) => api.get('/admin/operation-log/page', params),
    getDetail: (id) => api.get(`/admin/operation-log/${id}`),
    delete: (id) => api.delete(`/admin/operation-log/${id}`),
    cleanBefore: (date) => api.delete(`/admin/operation-log/clean?beforeDate=${date}`)
  },
  // 数据中心: 车间-组件关联管理 (后台管理用)
  dataCenter: {
    // 列出所有标记为 data_center_visible=1 的车间
    listWorkshops: () => api.get('/admin/data-center/workshops'),
    // 查询车间已关联的组件 (含禁用)
    listComponents: (workshopId) => api.get(`/admin/data-center/workshop/${workshopId}/components`),
    // 批量更新车间关联的组件 (全量替换)
    updateComponents: (workshopId, components) => api.put(`/admin/data-center/workshop/${workshopId}/components`, components),
    // 可用组件清单 (与前端 registry.js 对齐)
    listAvailableComponents: () => api.get('/admin/data-center/available-components')
  },
  // 数据中心: 用户-车间绑定管理 (后台管理用)
  userWorkshop: {
    listUsers: () => api.get('/admin/user-workshop/users'),
    getBindings: (userId) => api.get(`/admin/user-workshop/${userId}`),
    rebind: (userId, data) => api.put(`/admin/user-workshop/${userId}`, data),
    listWorkshops: () => api.get('/admin/user-workshop/workshops')
  }
}
