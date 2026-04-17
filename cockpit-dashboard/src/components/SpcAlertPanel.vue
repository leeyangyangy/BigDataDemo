<template>
  <div class="spc-alert-panel">
    <div class="alert-header">
      <span class="alert-title">⚠️ 异常检测</span>
      <div class="alert-actions">
        <button class="btn-toggle-rules" @click="showRules = !showRules">
          {{ showRules ? '隐藏规则' : '规则设置' }}
        </button>
        <span class="alert-count" v-if="alerts.length > 0" :class="{ 'has-alert': alerts.length > 0 }">
          {{ alerts.length }} 条报警
        </span>
      </div>
    </div>

    <div class="rules-config" v-show="showRules">
      <div class="rules-grid">
        <label v-for="rule in nelsonRules" :key="rule.id" class="rule-item" :class="{ active: enabledRules.includes(rule.id) }">
          <input type="checkbox" :value="rule.id" v-model="enabledRules" />
          <div class="rule-info">
            <strong>规则{{ rule.id }}</strong>
            <small>{{ rule.name }}</small>
          </div>
          <span class="rule-desc">{{ rule.description }}</span>
        </label>
      </div>
      <div class="rules-actions">
        <button class="btn-select-all" @click="selectAllRules">全选</button>
        <button class="btn-select-none" @click="enabledRules = []">全不选</button>
        <button class="btn-apply-rules" @click="applyRules">应用规则</button>
      </div>
    </div>

    <div class="alerts-list" v-if="alerts.length > 0">
      <div class="alert-item" v-for="(alert, index) in sortedAlerts" :key="index"
           :class="[`severity-${alert.severity || 'warning'}`]">
        <div class="alert-icon">{{ getSeverityIcon(alert.severity) }}</div>
        <div class="alert-content">
          <div class="alert-main">
            <span class="alert-rule">规则{{ alert.ruleId }}: {{ alert.ruleName }}</span>
            <span class="alert-time">{{ alert.time }}</span>
          </div>
          <div class="alert-detail">
            <span class="alert-value">值: {{ alert.value }}</span>
            <span class="alert-index">第{{ alert.index + 1 }}个数据点</span>
            <span class="alert-message" v-if="alert.message">{{ alert.message }}</span>
          </div>
        </div>
      </div>
    </div>

    <div class="no-alerts" v-else-if="!loading">
      <span class="no-alert-icon">✓</span>
      <span>未检测到异常</span>
    </div>

    <div class="loading-alerts" v-if="loading">
      <span class="spin-icon">↻</span>
      <span>正在分析数据...</span>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'

const props = defineProps({
  alerts: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false }
})

const emit = defineEmits(['rules-change'])

const showRules = ref(false)
const enabledRules = ref([1, 2, 3, 4, 5, 6, 7, 8])

const nelsonRules = [
  { id: 1, name: '超出3σ', description: '任一点超出控制限' },
  { id: 2, name: '连续9点同侧', description: '连续9点在中心线同一侧' },
  { id: 3, name: '连续6点递增/减', description: '连续6点单调递增或递减' },
  { id: 4, name: '连续14点交替', description: '连续14点上下交替' },
  { id: 5, name: '2/3点在2σ外', description: '连续3点中有2点在2σ外（同侧）' },
  { id: 6, name: '4/5点在1σ外', description: '连续5点中有4点在1σ外（同侧）' },
  { id: 7, name: '连续15点在1σ内', description: '连续15点在中心线±1σ内' },
  { id: 8, name: '连续8点在1σ外', description: '连续8点在中心线±1σ外（两侧）' }
]

const sortedAlerts = computed(() => {
  return [...props.alerts].sort((a, b) => {
    const severityOrder = { critical: 0, major: 1, warning: 2 }
    return (severityOrder[a.severity] || 99) - (severityOrder[b.severity] || 99)
  })
})

function selectAllRules() {
  enabledRules.value = nelsonRules.map(r => r.id)
}

function applyRules() {
  emit('rules-change', [...enabledRules.value])
}

function getSeverityIcon(severity) {
  switch (severity) {
    case 'critical': return '🔴'
    case 'major': return '🟠'
    case 'warning': return '🟡'
    default: return '⚪'
  }
}

watch(() => props.alerts, () => {
}, { deep: true })

defineExpose({ getEnabledRules: () => [...enabledRules.value] })
</script>

<style scoped>
.spc-alert-panel {
  margin-top: 16px;
  background: var(--bg-secondary);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.alert-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  background: linear-gradient(135deg, #fff7e6 0%, #fffbe6 100%);
  border-bottom: 1px solid #ffe58f;
}

.alert-title {
  font-size: 14px;
  font-weight: 600;
  color: #d48806;
}

.alert-actions {
  display: flex;
  align-items: center;
  gap: 12px;
}

.btn-toggle-rules {
  padding: 4px 12px;
  border: 1px solid #d48806;
  border-radius: 4px;
  background: transparent;
  color: #d48806;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-toggle-rules:hover {
  background: #d48806;
  color: white;
}

.alert-count {
  font-size: 12px;
  color: #999;
  padding: 4px 10px;
  border-radius: 10px;
  background: #f5f5f5;
}

.alert-count.has-alert {
  background: #fff2f0;
  color: #cf1322;
  border: 1px solid #ffa39e;
  font-weight: 600;
}

.rules-config {
  padding: 16px;
  background: #fafafa;
  border-bottom: 1px solid var(--border-color);
}

.rules-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 10px;
  margin-bottom: 14px;
}

.rule-item {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 10px 12px;
  border: 1px solid #e8e8e8;
  border-radius: 8px;
  background: white;
  cursor: pointer;
  transition: all 0.2s;
}

.rule-item:hover {
  border-color: #1890ff;
  box-shadow: 0 2px 8px rgba(24, 144, 255, 0.15);
}

.rule-item.active {
  border-color: #1890ff;
  background: #e6f7ff;
}

.rule-item input[type="checkbox"] {
  margin-top: 2px;
  cursor: pointer;
}

.rule-info {
  display: flex;
  flex-direction: column;
  min-width: 60px;
}

.rule-info strong {
  font-size: 13px;
  color: #333;
}

.rule-info small {
  font-size: 11px;
  color: #888;
}

.rule-desc {
  flex: 1;
  font-size: 11px;
  color: #666;
  line-height: 1.4;
}

.rules-actions {
  display: flex;
  gap: 10px;
  justify-content: flex-end;
}

.btn-select-all,
.btn-select-none,
.btn-apply-rules {
  padding: 6px 16px;
  border: 1px solid #d9d9d9;
  border-radius: 4px;
  background: white;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-select-all:hover,
.btn-apply-rules:hover {
  border-color: #1890ff;
  color: #1890ff;
}

.btn-apply-rules {
  background: #1890ff;
  color: white;
  border-color: #1890ff;
}

.btn-apply-rules:hover {
  background: #40a9ff;
}

.alerts-list {
  max-height: 400px;
  overflow-y: auto;
}

.alert-item {
  display: flex;
  gap: 12px;
  padding: 12px 16px;
  border-bottom: 1px solid #f0f0f0;
  transition: background 0.2s;
}

.alert-item:hover {
  background: #fafafa;
}

.alert-item:last-child {
  border-bottom: none;
}

.alert-icon {
  font-size: 18px;
  line-height: 1;
  margin-top: 2px;
}

.alert-content {
  flex: 1;
}

.alert-main {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
}

.alert-rule {
  font-size: 13px;
  font-weight: 600;
  color: #333;
}

.alert-time {
  font-size: 11px;
  color: #999;
}

.alert-detail {
  display: flex;
  gap: 16px;
  font-size: 12px;
  color: #666;
  flex-wrap: wrap;
}

.alert-value {
  font-weight: 500;
}

.alert-message {
  color: #999;
  font-style: italic;
}

.severity-critical {
  background: linear-gradient(90deg, #fff2f0 0%, white 30%);
  border-left: 3px solid #f5222d;
}

.severity-major {
  background: linear-gradient(90deg, #fff7e6 0%, white 30%);
  border-left: 3px solid #fa8c16;
}

.severity-warning {
  background: linear-gradient(90deg, #fffbe6 0%, white 30%);
  border-left: 3px solid #faad14;
}

.no-alerts {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 24px;
  color: #52c41a;
  font-size: 13px;
}

.no-alert-icon {
  font-size: 18px;
  font-weight: bold;
}

.loading-alerts {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 24px;
  color: #999;
  font-size: 13px;
}

.spin-icon {
  animation: spin 1s linear infinite;
  display: inline-block;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
</style>