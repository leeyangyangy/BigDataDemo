import { defineStore } from 'pinia'
import Cookies from 'js-cookie'

/**
 * 基于 js-cookie 的 Storage 适配器，供 pinia-plugin-persistedstate 持久化到 Cookie。
 * 序列化/反序列化由插件用 JSON 处理，这里只负责字符串存取。
 */
const cookieStorage = {
  getItem: (key) => Cookies.get(key) ?? null,
  setItem: (key, value) => Cookies.set(key, value, { expires: 365, path: '/', sameSite: 'Lax' }),
  removeItem: (key) => Cookies.remove(key, { path: '/' })
}

/**
 * UI 偏好状态：当前仅管理 SPC 数据管理页的默认分页大小。
 * 通过 cookie 持久化，刷新页面后保持用户选择，且可跨标签页/会话保留。
 */
export const useUiStore = defineStore('ui', {
  state: () => ({
    spcAdminPageSize: 20
  }),
  persist: {
    key: 'spc_admin_page_size',
    storage: cookieStorage
  }
})
