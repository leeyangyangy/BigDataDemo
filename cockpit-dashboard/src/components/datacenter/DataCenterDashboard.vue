<template>
  <!--
    数据中心主页面
    - 动态渲染: 不显示车间选择器 (由各组件内部处理)
    - 当前用户可见车间 = (绑定车间) ∩ (data_center_visible=1)
    - 数据中心内多个车间组件关联相同 (用户有权限的车间集合), 故直接渲染所有可用组件
    - 兜底: 无可用组件时渲染 yield_dashboard
  -->
  <div class="data-center-page">
    <div class="page-header">
      <h2>📊 数据中心</h2>
    </div>

    <div v-if="error" class="error-message">
      ⚠ {{ error }}
    </div>

    <div v-else-if="loading" class="loading">加载中...</div>

    <div v-else-if="components.length === 0" class="empty-state">
      <p>未配置数据中心组件</p>
      <p class="hint">请联系管理员在后台配置车间-组件关联</p>
    </div>

    <div v-else class="components-list">
      <component
        v-for="key in components"
        :key="key"
        :is="resolveComponent(key)"
      />
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { yieldApi } from '@/utils/api.js'
import { StatusCode } from '@/utils/statusCode'
import { getComponent } from './registry.js'

const components = ref([])
const loading = ref(false)
const error = ref(null)

/** 解析 component_key 到实际组件, 未注册的 key 跳过 */
function resolveComponent(key) {
  const def = getComponent(key)
  return def ? def.component : null
}

/**
 * 加载用户在数据中心可见的组件列表
 *
 * 策略:
 *   - 调用 /api/yield/access 获取用户可见车间
 *   - 取第一个可见车间, 查询其关联的组件 (兜底 yield_dashboard)
 *   - 多车间场景下组件关联通常一致, 故用首个车间查询即可
 *     如未来需要按车间切换组件集合, 可在此扩展车间选择器
 */
async function loadComponents() {
  loading.value = true
  error.value = null
  try {
    const accessRes = await yieldApi.checkAccess()
    if (!accessRes || accessRes.code !== StatusCode.SUCCESS) {
      error.value = '无数据中心访问权限'
      return
    }
    const workshops = (accessRes.data && accessRes.data.workshops) || []
    if (workshops.length === 0) {
      error.value = '无可访问的车间, 请联系管理员绑定车间'
      return
    }
    // 取首个可见车间查询其关联组件 (兜底会返回 yield_dashboard)
    const res = await yieldApi.getComponents(workshops[0])
    if (res && res.code === StatusCode.SUCCESS && Array.isArray(res.data)) {
      components.value = res.data
    } else {
      // 兜底: 默认 yield_dashboard
      components.value = ['yield_dashboard']
    }
  } catch (e) {
    if (e.message && e.message.includes('403')) {
      error.value = '无数据中心访问权限, 请联系管理员'
    } else {
      error.value = e.message || '加载失败'
    }
  } finally {
    loading.value = false
  }
}

onMounted(loadComponents)
</script>

<style scoped>
.data-center-page {
  padding: 16px;
  max-width: 1400px;
  margin: 0 auto;
}

.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
  padding: 0 4px;
}

.page-header h2 {
  margin: 0;
  font-size: 20px;
  font-weight: 600;
}

.workshop-select {
  padding: 8px 12px;
  border-radius: 8px;
  border: 1px solid var(--border-color, #ddd);
  background: var(--bg-input, #fff);
  color: var(--text-primary, #333);
  font-size: 14px;
  min-width: 180px;
}

.error-message,
.loading,
.empty-state {
  text-align: center;
  padding: 40px;
  color: var(--text-muted, #888);
}

.error-message {
  color: #ef4444;
}

.empty-state .hint {
  font-size: 13px;
  margin-top: 8px;
  color: var(--text-muted, #aaa);
}

.components-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}
</style>
