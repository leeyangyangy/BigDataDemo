/**
 * Token 安全增强工具
 *
 * <p>符合等保三级 "身份鉴别" 与 "会话管理" 要求:</p>
 * <ul>
 *   <li>JWT 过期前端预检 (避免发送过期 Token)</li>
 *   <li>会话超时检测 (长时间无操作自动登出, 与后端 8h 过期对齐)</li>
 *   <li>多标签页同步登出 (一个标签页登出, 其他标签页自动登出)</li>
 *   <li>Token 写入时附带过期时间, 便于前端预判</li>
 * </ul>
 */

const TOKEN_KEY = 'cockpit_token'
const USER_KEY = 'cockpit_user'
const TOKEN_EXPIRY_KEY = 'cockpit_token_exp'
const LAST_ACTIVITY_KEY = 'cockpit_last_activity'

/** 会话超时阈值 (毫秒), 等保三级要求空闲超时 15-30 分钟 */
export const SESSION_TIMEOUT_MS = 30 * 60 * 1000

/** 无操作超时检查间隔 (毫秒) */
const IDLE_CHECK_INTERVAL_MS = 60 * 1000

let idleCheckTimer = null
let onSessionExpiredCallback = null

/**
 * 从 JWT 中解析过期时间 (exp 字段, 秒级 Unix 时间戳)。
 *
 * @param {string} token JWT
 * @returns {number|null} 过期时间戳 (毫秒), 解析失败返回 null
 */
export function parseTokenExpiry(token) {
  if (!token || typeof token !== 'string') return null
  const parts = token.split('.')
  if (parts.length !== 3) return null
  try {
    // Base64Url -> Base64
    let payload = parts[1]
    payload = payload.replace(/-/g, '+').replace(/_/g, '/')
    // 补齐 padding
    const pad = payload.length % 4
    if (pad) payload += '='.repeat(4 - pad)
    const decoded = JSON.parse(atob(payload))
    if (decoded && typeof decoded.exp === 'number') {
      return decoded.exp * 1000
    }
    return null
  } catch (e) {
    console.warn('[TokenSecurity] 解析 JWT 过期时间失败:', e.message)
    return null
  }
}

/**
 * 检查 Token 是否已过期。
 */
export function isTokenExpired(token) {
  if (!token) return true
  const expiry = parseTokenExpiry(token)
  if (!expiry) return false // 无法解析时不阻断, 交给后端判定
  return Date.now() >= expiry
}

/**
 * 安全地保存 Token, 同时记录过期时间。
 */
export function secureSetToken(token) {
  if (!token) return
  localStorage.setItem(TOKEN_KEY, token)
  const expiry = parseTokenExpiry(token)
  if (expiry) {
    localStorage.setItem(TOKEN_EXPIRY_KEY, String(expiry))
  }
  updateLastActivity()
}

/**
 * 安全地移除 Token 及相关元数据。
 *
 * <p>removeItem 本身会触发 storage 事件(newValue=null), 其他标签页的监听器
 * 可据此同步登出, 无需额外 setItem 空字符串(那样反而会留下空值 key)。
 */
export function secureRemoveToken() {
  localStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(USER_KEY)
  localStorage.removeItem(TOKEN_EXPIRY_KEY)
  localStorage.removeItem(LAST_ACTIVITY_KEY)
}

/**
 * 更新最后活动时间。
 */
export function updateLastActivity() {
  localStorage.setItem(LAST_ACTIVITY_KEY, String(Date.now()))
}

/**
 * 检查会话是否因无操作超时。
 */
export function isSessionIdle() {
  const last = localStorage.getItem(LAST_ACTIVITY_KEY)
  if (!last) return false
  const lastMs = parseInt(last, 10)
  if (isNaN(lastMs)) return false
  return Date.now() - lastMs > SESSION_TIMEOUT_MS
}

/**
 * 启动会话超时检测。
 *
 * @param {Function} onExpired 会话过期时的回调 (通常跳转登录页)
 */
export function startSessionWatcher(onExpired) {
  onSessionExpiredCallback = onExpired

  // 监听用户活动 (鼠标移动、键盘、点击、滚动)
  const events = ['mousedown', 'keydown', 'touchstart', 'scroll']
  let throttleTimer = null
  const activityHandler = () => {
    if (throttleTimer) return
    throttleTimer = setTimeout(() => {
      updateLastActivity()
      throttleTimer = null
    }, 5000) // 5 秒内只更新一次, 避免频繁写入
  }
  events.forEach(evt => window.addEventListener(evt, activityHandler, { passive: true }))

  // 定期检查会话超时
  if (idleCheckTimer) clearInterval(idleCheckTimer)
  idleCheckTimer = setInterval(() => {
    const token = localStorage.getItem(TOKEN_KEY)
    if (!token) return
    if (isTokenExpired(token) || isSessionIdle()) {
      console.warn('[TokenSecurity] 会话已过期, 自动登出')
      secureRemoveToken()
      if (onSessionExpiredCallback) onSessionExpiredCallback()
    }
  }, IDLE_CHECK_INTERVAL_MS)

  // 多标签页同步: 监听 storage 事件
  window.addEventListener('storage', (e) => {
    if (e.key === TOKEN_KEY) {
      const newValue = e.newValue
      if (!newValue) {
        // 其他标签页已登出
        console.log('[TokenSecurity] 检测到其他标签页登出, 同步登出')
        localStorage.removeItem(USER_KEY)
        localStorage.removeItem(TOKEN_EXPIRY_KEY)
        if (onSessionExpiredCallback) onSessionExpiredCallback()
      } else if (isTokenExpired(newValue)) {
        // 其他标签页写入了过期 Token
        localStorage.removeItem(TOKEN_KEY)
        if (onSessionExpiredCallback) onSessionExpiredCallback()
      }
    }
  })
}

/**
 * 停止会话超时检测 (登出时调用)。
 */
export function stopSessionWatcher() {
  if (idleCheckTimer) {
    clearInterval(idleCheckTimer)
    idleCheckTimer = null
  }
  onSessionExpiredCallback = null
}
