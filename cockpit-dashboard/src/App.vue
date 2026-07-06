<template>
  <div class="app-container">
    <div class="header-bar">
      <h1 class="header">飓芯科技工艺保障驾驶舱</h1>
      <div class="header-right">
        <ThemeSwitcher />
        <div class="user-area" v-if="loggedIn">
          <span class="user-badge" @click.stop="showUserMenu = !showUserMenu">
            <span class="user-avatar">{{ userInfo?.username?.[0] || 'U' }}</span>
            <span class="user-name">{{ userInfo?.username }}</span>
          </span>
          <div class="user-dropdown" v-if="showUserMenu" @click.stop>
            <div class="dropdown-header">
              <span class="dropdown-avatar">{{ userInfo?.username?.[0] || 'U' }}</span>
              <div class="dropdown-user-info">
                <strong>{{ userInfo?.username }}</strong>
                <small>{{ roleLabel }} · {{ userInfo?.empNo || '-' }}</small>
              </div>
            </div>
            <div class="dropdown-divider"></div>
            <div class="dropdown-item logout-item" @click="handleLogout">
              <span class="logout-icon">🚪</span> 退出登录
            </div>
          </div>
        </div>
        <button class="login-trigger" v-else @click="showLogin = true">
          <span class="login-icon">👤</span>
          <span>登录</span>
        </button>
      </div>
    </div>

    <div class="page-content">
      <DataCenterDashboard
        v-if="activeNav === 'data'"
        :key="'data'"
      />
      <SpcDashboard
        v-else-if="!activeNav.startsWith('admin')"
        :key="'dashboard'"
        :isLoggedIn="loggedIn"
        :userInfo="userInfo"
        @require-login="showLogin = true"
      />
      <template v-else-if="isAdmin">
        <UserManagement v-if="activeNav === 'admin-user'" :key="'admin-user'" />
        <ProductManagement v-else-if="activeNav === 'admin-product'" :key="'admin-product'" />
        <ProcessManagement v-else-if="activeNav === 'admin-process'" :key="'admin-process'" />
        <WorkshopManagement v-else-if="activeNav === 'admin-workshop'" :key="'admin-workshop'" />
        <StandardManagement v-else-if="activeNav === 'admin-standard'" :key="'admin-standard'" />
        <EquipmentManagement v-else-if="activeNav === 'admin-equipment'" :key="'admin-equipment'" />
        <DataCenterManagement v-else-if="activeNav === 'admin-datacenter-mgmt'" :key="'admin-datacenter-mgmt'" />
        <ChangeLogManagement v-else-if="activeNav === 'admin-changelog'" :key="'admin-changelog'" />
        <OperationLogManagement v-else-if="activeNav === 'admin-operationlog'" :key="'admin-operationlog'" />
        <SpcDataManagement v-else-if="activeNav === 'admin-spc-data'" :key="'admin-spc-data'" />

<!--        <AdminPanel v-else-if="activeNav === 'admi' +-->
<!--         'n-standard' || activeNav === 'admin-equipment' || activeNav === 'admin-changelog' || activeNav === 'admin-operationlog'" :key="activeNav" :defaultTab="activeNav === 'admin-changelog' ? 'changelog' : (activeNav === 'admin-operationlog' ? 'operationlog' : (activeNav === 'admin-standard' ? 'standard' : 'equipment'))" />-->
      </template>
      <div v-else-if="activeNav.startsWith('admin') && loggedIn" class="admin-gate">
        <div class="gate-card">
          <span class="gate-icon">🚫</span>
          <h3>权限不足</h3>
          <p>后台管理功能仅限管理员访问</p>
          <button class="gate-btn" @click="handleNavigate('home')">返回首页</button>
        </div>
      </div>
      <div v-else-if="activeNav.startsWith('admin')" class="admin-gate">
        <div class="gate-card">
          <span class="gate-icon">🔐</span>
          <h3>需要登录</h3>
          <p>请先登录后访问后台管理功能</p>
          <button class="gate-btn" @click="showLogin = true">立即登录</button>
        </div>
      </div>
    </div>

    <BottomNav
      :activeIndex="navKeyToIndex(activeNav)"
      :userRole="userInfo?.role || ''"
      :activeKey="activeNav"
      :showYield="yieldAccessible"
      @navigate="handleNavigate"
    />

    <LoginForm
      v-if="showLogin"
      @close="showLogin = false"
      @success="onLoginSuccess"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import SpcDashboard from './components/SpcDashboard.vue'
import DataCenterDashboard from './components/datacenter/DataCenterDashboard.vue'
import UserManagement from './components/admin/UserManagement.vue'
import ProductManagement from './components/admin/ProductManagement.vue'
import ProcessManagement from './components/admin/ProcessManagement.vue'
import WorkshopManagement from './components/admin/WorkshopManagement.vue'
import DataCenterManagement from './components/admin/DataCenterManagement.vue'
import BottomNav from './components/BottomNav.vue'
import ThemeSwitcher from './components/ThemeSwitcher.vue'
import LoginForm from './components/LoginForm.vue'
import { getToken, getUser, removeToken, isLoggedIn, yieldApi } from './utils/api.js'
import { StatusCode } from './utils/statusCode.js'
import { startSessionWatcher, stopSessionWatcher } from './utils/tokenSecurity.js'
import './styles/theme.css'
import OperationLogManagement from "@/components/admin/OperationLogManagement.vue";
import ChangeLogManagement from "@/components/admin/ChangeLogManagement.vue";
import EquipmentManagement from "@/components/admin/EquipmentManagement.vue";
import StandardManagement from "@/components/admin/StandardManagement.vue";
import SpcDataManagement from "@/components/admin/SpcDataManagement.vue";

const NAV_KEY = 'spc_active_nav'
const ADMIN_NAV_KEY = 'spc_admin_last_nav'

const showLogin = ref(false)
const showUserMenu = ref(false)
const activeNav = ref(localStorage.getItem(NAV_KEY) || 'home')

const loggedIn = ref(false)
const userInfo = ref(null)
const yieldAccessible = ref(false)

const YIELD_ACCESS_KEY = 'spc_yield_accessible'

const roleLabel = computed(() => {
  const map = { ADMIN: '管理员', ENGINEER: '工程师', OPERATOR: '操作员', VIEWER: '观察者' }
  return map[userInfo.value?.role] || userInfo.value?.role || ''
})

const isAdmin = computed(() => (userInfo.value?.role || '').toUpperCase() === 'ADMIN')

function checkAuth() {
  loggedIn.value = isLoggedIn()
  if (loggedIn.value) {
    userInfo.value = getUser()
    // 查询数据中心访问权限 (管理员自动放行, 其他用户需绑定车间 + data_center_visible=1)
    if (isAdmin.value) {
      yieldAccessible.value = true
    } else {
      const cached = localStorage.getItem(YIELD_ACCESS_KEY)
      yieldAccessible.value = cached === '1'
      // 异步刷新缓存
      refreshYieldAccess()
    }
  } else {
    userInfo.value = null
    yieldAccessible.value = false
    localStorage.removeItem(YIELD_ACCESS_KEY)
  }
  if (activeNav.value.startsWith('admin') && !isAdmin.value) {
    activeNav.value = 'home'
    localStorage.setItem(NAV_KEY, 'home')
  }
  // 若数据中心 tab 不可见但当前在数据中心页, 跳回首页
  if (!yieldAccessible.value && activeNav.value === 'data') {
    activeNav.value = 'home'
    localStorage.setItem(NAV_KEY, 'home')
  }
  // 兼容旧值: yield -> data
  if (activeNav.value === 'yield') {
    activeNav.value = 'data'
    localStorage.setItem(NAV_KEY, 'data')
  }
}

async function refreshYieldAccess() {
  try {
    const res = await yieldApi.checkAccess()
    if (res && res.code === StatusCode.SUCCESS && res.data) {
      const ok = res.data.accessible === true
      yieldAccessible.value = ok
      localStorage.setItem(YIELD_ACCESS_KEY, ok ? '1' : '0')
      // 若权限被收回且当前在数据中心页, 跳回首页
      if (!ok && activeNav.value === 'data') {
        activeNav.value = 'home'
        localStorage.setItem(NAV_KEY, 'home')
      }
    } else {
      yieldAccessible.value = false
      localStorage.setItem(YIELD_ACCESS_KEY, '0')
    }
  } catch (e) {
    console.warn('[DataCenter] 查询数据中心权限失败:', e.message)
  }
}

function onLoginSuccess(data) {
  showLogin.value = false
  checkAuth()
}

function handleLogout() {
  removeToken()
  stopSessionWatcher()
  showUserMenu.value = false
  yieldAccessible.value = false
  localStorage.removeItem(YIELD_ACCESS_KEY)
  // 刷新页面以重置所有状态 (reload 确保 SPA 完全重新初始化)
  window.location.reload()
}

function handleNavigate(key) {
  if (key.startsWith('admin') && !isAdmin.value) {
    return
  }
  if (key === 'admin') {
    const lastAdmin = localStorage.getItem(ADMIN_NAV_KEY)
    key = lastAdmin || 'admin-product'
  }
  if (key.startsWith('admin')) {
    localStorage.setItem(ADMIN_NAV_KEY, key)
  }
  activeNav.value = key
  localStorage.setItem(NAV_KEY, key)
}

function navKeyToIndex(key) {
  const isAdmin = key.startsWith('admin')
  if (isAdmin) {
    const map = { 'home': 0, 'admin-product': 1, 'admin-process': 2, 'admin-standard': 3, 'admin-equipment': 4, 'admin-workshop': 5, 'admin-user': 6, 'admin-spc-data': 7, 'admin-datacenter-mgmt': 8, 'admin-changelog': 9, 'admin-operationlog': 10 }
    return map[key] ?? 1
  }
  const map = { home: 0, data: 1, admin: 2 }
  return map[key] ?? 0
}

function onAuthExpired() {
  checkAuth()
  showLogin.value = true
}

function closeUserMenu(e) {
  const target = e.target
  const userArea = target.closest('.user-area')
  if (showUserMenu.value && !userArea) {
    showUserMenu.value = false
  }
}

onMounted(() => {
  checkAuth()
  // 启动会话超时检测 (等保三级: 会话超时自动退出)
  startSessionWatcher(() => {
    console.warn('[Session] 会话已超时, 自动登出')
    loggedIn.value = false
    userInfo.value = null
    showLogin.value = true
  })
  window.addEventListener('auth:expired', onAuthExpired)
  document.addEventListener('click', closeUserMenu)
})

onUnmounted(() => {
  stopSessionWatcher()
  window.removeEventListener('auth:expired', onAuthExpired)
  document.removeEventListener('click', closeUserMenu)
})
</script>

<style>
.app-container {
  min-height: 100vh;
  padding-bottom: 80px;
}

.header-bar {
  max-width: 1400px;
  margin: 0 auto;
  padding: 16px 20px;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header {
  color: var(--text-primary);
  font-size: 22px;
  font-weight: 600;
}

.header-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.user-area {
  position: relative;
}

.user-badge {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 20px;
  cursor: pointer;
  transition: all 0.3s;
}

.user-badge:hover {
  border-color: var(--accent-primary);
  box-shadow: 0 2px 8px rgba(var(--accent-rgb), 0.15);
}

.user-avatar {
  width: 28px;
  height: 28px;
  border-radius: 50%;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 70%, white));
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 13px;
  font-weight: 600;
}

.user-name {
  font-size: 13px;
  color: var(--text-primary);
  font-weight: 500;
}

.user-role {
  font-size: 11px;
  color: var(--accent-primary);
  background: rgba(var(--accent-rgb), 0.1);
  padding: 2px 8px;
  border-radius: 10px;
}

.user-dropdown {
  position: absolute;
  top: calc(100% + 8px);
  right: 0;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  box-shadow: var(--shadow-lg);
  overflow: hidden;
  min-width: 200px;
  z-index: 1100;
}

.dropdown-item {
  padding: 10px 16px;
  font-size: 13px;
  color: var(--text-primary);
  cursor: pointer;
  transition: background 0.2s;
}

.dropdown-item:hover {
  background: rgba(var(--accent-rgb), 0.08);
  color: var(--accent-primary);
}

.dropdown-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 14px 16px;
}

.dropdown-avatar {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 70%, white));
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 15px;
  font-weight: 700;
  flex-shrink: 0;
}

.dropdown-user-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.dropdown-user-info strong {
  font-size: 13px;
  color: var(--text-primary);
}

.dropdown-user-info small {
  font-size: 11px;
  color: var(--text-tertiary);
}

.dropdown-divider {
  height: 1px;
  background: var(--border-color);
  margin: 0 12px;
}

.logout-item {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #cf1322 !important;
  font-weight: 500;
}
.logout-item:hover {
  background: #fff1f0 !important;
}

.login-trigger {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 80%, black));
  color: white;
  border: none;
  border-radius: 20px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.3s;
}

.login-trigger:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(var(--accent-rgb), 0.4);
}

.login-icon {
  font-size: 16px;
}

.page-content {
  max-width: 1400px;
  margin: 0 auto;
  padding: 0 20px;
}

.admin-gate {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 50vh;
}

.gate-card {
  text-align: center;
  padding: 48px 40px;
  background: var(--bg-secondary);
  border-radius: 20px;
  border: 1px solid var(--border-color);
  box-shadow: var(--shadow-lg);
  max-width: 380px;
}

.gate-icon {
  font-size: 48px;
  display: block;
  margin-bottom: 16px;
}

.gate-card h3 {
  font-size: 20px;
  font-weight: 700;
  color: var(--text-primary);
  margin-bottom: 8px;
}

.gate-card p {
  font-size: 14px;
  color: var(--text-tertiary);
  margin-bottom: 24px;
}

.gate-btn {
  padding: 10px 32px;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 85%, white));
  color: white;
  border: none;
  border-radius: 12px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.3s;
}

.gate-btn:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 20px rgba(var(--accent-rgb), 0.4);
}

@media (max-width: 640px) {
  .user-name { display: none; }
  .user-avatar { width: 32px; height: 32px; font-size: 14px; }
}
</style>
