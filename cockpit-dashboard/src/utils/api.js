const BASE_URL = '/api'

const TOKEN_KEY = 'spc_token'
const USER_KEY = 'spc_user'

import { isEncryptionEnabled, getEncryptedBody, decryptResponse } from './crypto.js'

export function getToken() {
  return localStorage.getItem(TOKEN_KEY)
}

export function setToken(token) {
  localStorage.setItem(TOKEN_KEY, token)
}

export function removeToken() {
  localStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(USER_KEY)
}

export function getUser() {
  const raw = localStorage.getItem(USER_KEY)
  return raw ? JSON.parse(raw) : null
}

export function setUser(user) {
  localStorage.setItem(USER_KEY, JSON.stringify(user))
}

export function isLoggedIn() {
  return !!getToken()
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
    if (body && isEncryptionEnabled() && !url.includes('/auth/login') && headers['Content-Type']?.includes('json')) {
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
        result = decryptResponse(result)
        console.log(`[Crypto] 响应已解密: ${url}`)
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
  weComBind: (wecomUserId, empNo) => api.post('/auth/wecom/bind', { wecomUserId, empNo })
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
  getBatchPage: (params) => api.get('/spc/batch/page', params),
  createBatch: (data) => api.post('/spc/batch', data),
  getProductPage: (params) => api.get('/spc/product/page', params),
  createProduct: (data) => api.post('/spc/product', data),
  getProcessPage: (params) => api.get('/spc/process/page', params),
  getProcessEquipment: (processId) => api.get(`/spc/process/${processId}/equipment`),
  getParamPage: (params) => api.get('/spc/param/page', params),
  getAlerts: (params) => api.get('/spc/chart/alerts', params),
  importData: (formData) => api.post('/spc/data/import', formData, { headers: { 'Content-Type': 'multipart/form-data' } }),
  downloadTemplate: () => '/api/spc/data/template/download',
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
    delete: (id) => api.delete(`/admin/product/${id}`)
  },
  process: {
    getPage: (params) => api.get('/admin/process/page', params),
    getById: (id) => api.get(`/admin/process/${id}`),
    create: (data) => api.post('/admin/process', data),
    update: (id, data) => api.put(`/admin/process/${id}`, data),
    delete: (id) => api.delete(`/admin/process/${id}`)
  },
  standard: {
    getPage: (params) => api.get('/admin/standard/page', params),
    getById: (id) => api.get(`/admin/standard/${id}`),
    create: (data) => api.post('/admin/standard', data),
    update: (id, data) => api.put(`/admin/standard/${id}`, data),
    delete: (id) => api.delete(`/admin/standard/${id}`)
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
    listAll: () => api.get('/admin/workshop/list'),
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
  }
}
