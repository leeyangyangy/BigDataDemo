<template>
  <div class="admin-panel">
    <div class="admin-header">
<!--      <h2 class="admin-title">后台管理</h2>-->
      <p class="admin-subtitle">系统配置与数据管理</p>
    </div>

<!--    <div class="tab-bar">-->
<!--      <button-->
<!--        v-for="tab in tabs"-->
<!--        :key="tab.key"-->
<!--        class="tab-btn"-->
<!--        :class="{ active: activeTab === tab.key }"-->
<!--        @click="activeTab = tab.key"-->
<!--      >-->
<!--        <span class="tab-icon">{{ tab.icon }}</span>-->
<!--        {{ tab.label }}-->
<!--      </button>-->
<!--    </div>-->

    <!-- 用户管理 -->
    <div v-if="activeTab === 'user'" class="tab-content">
      <UserManagement />
    </div>

    <!-- 产品管理 -->
    <div v-if="activeTab === 'product'" class="tab-content">
      <ProductManagement />
    </div>

    <!-- 工序管理 -->
    <div v-if="activeTab === 'process'" class="tab-content">
      <ProcessManagement />
    </div>

    <!-- 标准管理 -->
    <div v-if="activeTab === 'standard'" class="tab-content">
      <StandardManagement />
    </div>

    <!-- 设备管理 -->
    <div v-if="activeTab === 'equipment'" class="tab-content">
      <EquipmentManagement />
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import UserManagement from './admin/UserManagement.vue'
import ProductManagement from './admin/ProductManagement.vue'
import ProcessManagement from './admin/ProcessManagement.vue'
import StandardManagement from './admin/StandardManagement.vue'
import EquipmentManagement from './admin/EquipmentManagement.vue'

const props = defineProps({
  defaultTab: { type: String, default: 'user' }
})

const activeTab = ref(props.defaultTab || 'user')

const tabs = [
  { key: 'user', icon: '👥', label: '用户管理' },
  { key: 'product', icon: '📦', label: '产品管理' },
  { key: 'process', icon: '⚙️', label: '工序管理' },
  { key: 'standard', icon: '📏', label: '标准管理' },
  { key: 'equipment', icon: '🔧', label: '设备管理' }
]
</script>

<style scoped>
.admin-panel {
  max-width: 1200px;
  margin: 0 auto;
}

.admin-header {
  margin-bottom: 20px;
}

.admin-title {
  font-size: 22px;
  font-weight: 700;
  color: var(--text-primary);
  margin-bottom: 4px;
}

.admin-subtitle {
  font-size: 13px;
  color: var(--text-tertiary);
}

.tab-bar {
  display: flex;
  gap: 4px;
  background: var(--bg-secondary);
  padding: 4px;
  border-radius: 12px;
  margin-bottom: 20px;
  border: 1px solid var(--border-color);
}

.tab-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 10px 20px;
  background: transparent;
  border: none;
  border-radius: 10px;
  font-size: 14px;
  font-weight: 500;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.25s ease;
}

.tab-btn:hover {
  color: var(--text-primary);
  background: rgba(var(--accent-rgb), 0.06);
}

.tab-btn.active {
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 85%, white));
  color: white;
  box-shadow: 0 2px 8px rgba(var(--accent-rgb), 0.3);
}

.tab-icon {
  font-size: 16px;
}

.tab-content {
  animation: fadeIn 0.3s ease;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(8px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>
