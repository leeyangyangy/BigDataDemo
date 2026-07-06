<template>
  <div class="yield-dashboard" :class="{ collapsed: !isExpanded }">
    <div class="dashboard-header" @click="toggleExpand">
      <div class="header-left">
        <span class="toggle-icon" :class="{ expanded: isExpanded }">▶</span>
        <h2 class="dashboard-title">📊 良率实时监控</h2>
        <span class="workshop-badge" v-if="selectedWorkshop">{{ selectedWorkshop }}</span>
      </div>
      <div class="header-right">
        <span class="update-time" v-if="isExpanded && updateTime">最后更新: {{ updateTime }}</span>
        <span class="record-count" v-if="isExpanded && currentYieldRates.length">共 {{ currentYieldRates.length }} 条</span>
      </div>
    </div>

    <transition name="slide-fade">
      <div class="dashboard-body" v-if="isExpanded">
        <div class="control-bar">
          <div class="control-group workshop-selector">
            <label>车间</label>
            <select v-model="selectedWorkshop" @change="onWorkshopChange" class="control-select">
              <option v-for="ws in workshops" :key="ws" :value="ws">{{ ws }}</option>
            </select>
          </div>

          <div class="control-group search-box">
            <label>搜索</label>
            <div class="search-input-wrapper">
              <input
                type="text"
                v-model="searchInput"
                @input="handleSearch"
                placeholder="产品名称 / 片号 / 分等..."
                class="control-input search-input"
              />
              <button v-if="searchInput" @click="handleClearSearch" class="clear-btn" title="清除">✕</button>
            </div>
          </div>

          <div class="control-group date-group">
            <label>开始</label>
            <input type="date" v-model="startDateInput" @change="handleDateChange" class="control-input date-input" />
          </div>

          <div class="control-group date-group">
            <label>结束</label>
            <input type="date" v-model="endDateInput" @change="handleDateChange" class="control-input date-input" />
          </div>

          <div class="quick-buttons">
            <button @click="setRecentDays(3)" class="quick-btn">近3天</button>
            <button @click="setRecentDays(7)" class="quick-btn">近7天</button>
          </div>

          <button
            v-if="startDateInput || endDateInput || searchInput"
            @click="handleClearFilters"
            class="clear-filter-btn"
          >清除筛选</button>

          <button @click="fetchData" class="refresh-btn" :disabled="loading" title="手动刷新">
            <span :class="{ spin: loading }">↻</span>
          </button>
        </div>

        <div class="filter-info" v-if="searchKeyword || startDateInput || endDateInput">
          筛选结果: <strong>{{ currentYieldRates.length }}</strong> 条
          <span v-if="searchKeyword"> | 关键词: "{{ searchKeyword }}"</span>
          <span v-if="startDateInput || endDateInput"> | 日期: {{ startDateInput || '不限' }} ~ {{ endDateInput || '不限' }}</span>
        </div>

        <div v-if="error" class="error-message">
          ⚠ {{ error }}
        </div>

        <div class="components-grid">
          <div class="grid-item full-width">
            <ProductComprehensiveChart
              :yieldRates="currentYieldRates"
              :historicalData="historicalData"
            />
          </div>

          <div class="grid-item full-width">
            <ProductCodeBarCharts :yieldRates="currentYieldRates" />
          </div>

          <div class="grid-item full-width">
            <ProductCodePositionCompare
              :yieldRates="currentYieldRates"
              :historicalData="historicalData"
            />
          </div>

          <div class="grid-item half">
            <YieldTable :yieldRates="currentYieldRates" />
          </div>

          <div class="grid-item half">
            <YieldTrendChart
              :yieldRates="currentYieldRates"
              :historicalData="historicalData"
            />
          </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import YieldTable from './YieldTable.vue'
import YieldTrendChart from './YieldTrendChart.vue'
import ProductComprehensiveChart from './ProductComprehensiveChart.vue'
import ProductCodeBarCharts from './ProductCodeBarCharts.vue'
import ProductCodePositionCompare from './ProductCodePositionCompare.vue'
import { yieldApi } from '@/utils/api.js'
import { StatusCode } from '@/utils/statusCode'

const REFRESH_INTERVAL = 5000
const STORAGE_KEY_WORKSHOP = 'yield-dashboard-workshop'
const STORAGE_KEY_START_DATE = 'yield-dashboard-start-date'
const STORAGE_KEY_END_DATE = 'yield-dashboard-end-date'
const STORAGE_KEY_EXPANDED = 'yield-dashboard-expanded'

const DEFAULT_WORKSHOP = '测试站'

const workshops = ref([DEFAULT_WORKSHOP])
const selectedWorkshop = ref(localStorage.getItem(STORAGE_KEY_WORKSHOP) || DEFAULT_WORKSHOP)

const currentYieldRates = ref([])
const historicalData = ref({})
const updateTime = ref('')
const loading = ref(false)
const error = ref(null)

const searchInput = ref('')
const searchKeyword = ref('')
const startDateInput = ref(localStorage.getItem(STORAGE_KEY_START_DATE) || '')
const endDateInput = ref(localStorage.getItem(STORAGE_KEY_END_DATE) || '')

const isExpanded = ref(localStorage.getItem(STORAGE_KEY_EXPANDED) !== 'false')

let intervalId = null
let searchTimeout = null

function toggleExpand() {
  isExpanded.value = !isExpanded.value
  localStorage.setItem(STORAGE_KEY_EXPANDED, String(isExpanded.value))
}

async function loadWorkshops() {
  try {
    const res = await yieldApi.getWorkshops()
    if (res && res.code === StatusCode.SUCCESS && Array.isArray(res.data) && res.data.length > 0) {
      workshops.value = res.data
      if (!workshops.value.includes(selectedWorkshop.value)) {
        selectedWorkshop.value = workshops.value[0]
      }
    } else if (res && res.code === StatusCode.FORBIDDEN) {
      error.value = '无良率数据查看权限, 请联系管理员'
    }
  } catch (e) {
    console.warn('[Yield] 加载车间列表失败，使用默认值:', e.message)
    if (e.message && e.message.includes('403')) {
      error.value = '无良率数据查看权限, 请联系管理员'
    }
  }
}

async function fetchData() {
  if (loading.value) return
  loading.value = true
  error.value = null
  try {
    const params = {
      workshop: selectedWorkshop.value,
      start_date: startDateInput.value || null,
      end_date: endDateInput.value || null
    }
    let res
    if (searchKeyword.value) {
      res = await yieldApi.search({ ...params, keyword: searchKeyword.value })
    } else {
      res = await yieldApi.getData(params)
    }
    if (res && res.code === StatusCode.SUCCESS && res.data) {
      currentYieldRates.value = res.data.currentYieldRates || []
      historicalData.value = res.data.historicalData || {}
      updateTime.value = formatNow()
    } else {
      currentYieldRates.value = []
      historicalData.value = {}
    }
  } catch (e) {
    if (e.message && e.message.includes('403')) {
      error.value = '无良率数据查看权限, 请联系管理员'
    } else {
      error.value = e.message || '获取良率数据失败'
    }
    console.error('[Yield] 获取数据失败:', e)
  } finally {
    loading.value = false
  }
}

function formatNow() {
  const d = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

function formatDateForInput(date) {
  const y = date.getFullYear()
  const m = String(date.getMonth() + 1).padStart(2, '0')
  const d = String(date.getDate()).padStart(2, '0')
  return `${y}-${m}-${d}`
}

function handleSearch() {
  if (searchTimeout) clearTimeout(searchTimeout)
  searchTimeout = setTimeout(() => {
    searchKeyword.value = searchInput.value.trim()
    fetchData()
  }, 300)
}

function handleClearSearch() {
  searchInput.value = ''
  if (searchKeyword.value) {
    searchKeyword.value = ''
    fetchData()
  }
}

function handleDateChange() {
  localStorage.setItem(STORAGE_KEY_START_DATE, startDateInput.value)
  localStorage.setItem(STORAGE_KEY_END_DATE, endDateInput.value)
  fetchData()
}

function setRecentDays(days) {
  const today = new Date()
  const start = new Date()
  start.setDate(today.getDate() - days + 1)
  startDateInput.value = formatDateForInput(start)
  endDateInput.value = formatDateForInput(today)
  handleDateChange()
}

function handleClearFilters() {
  searchInput.value = ''
  searchKeyword.value = ''
  startDateInput.value = ''
  endDateInput.value = ''
  localStorage.removeItem(STORAGE_KEY_START_DATE)
  localStorage.removeItem(STORAGE_KEY_END_DATE)
  fetchData()
}

function onWorkshopChange() {
  localStorage.setItem(STORAGE_KEY_WORKSHOP, selectedWorkshop.value)
  fetchData()
}

function startRefresh() {
  stopRefresh()
  intervalId = setInterval(fetchData, REFRESH_INTERVAL)
}

function stopRefresh() {
  if (intervalId) {
    clearInterval(intervalId)
    intervalId = null
  }
}

watch(isExpanded, (val) => {
  if (val) {
    fetchData()
    startRefresh()
  } else {
    stopRefresh()
  }
})

onMounted(async () => {
  await loadWorkshops()
  if (isExpanded.value) {
    await fetchData()
    startRefresh()
  }
})

onUnmounted(() => {
  stopRefresh()
  if (searchTimeout) clearTimeout(searchTimeout)
})
</script>

<style scoped>
.yield-dashboard {
  margin-bottom: 20px;
  background: var(--bg-secondary);
  border-radius: 16px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.dashboard-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 20px;
  cursor: pointer;
  user-select: none;
  background: linear-gradient(90deg, rgba(var(--accent-rgb), 0.08), rgba(0, 255, 136, 0.04));
  transition: background 0.2s;
}

.dashboard-header:hover {
  background: linear-gradient(90deg, rgba(var(--accent-rgb), 0.14), rgba(0, 255, 136, 0.08));
}

.header-left {
  display: flex;
  align-items: center;
  gap: 12px;
}

.toggle-icon {
  display: inline-block;
  font-size: 12px;
  color: var(--accent-primary);
  transition: transform 0.3s;
  width: 14px;
}

.toggle-icon.expanded {
  transform: rotate(90deg);
}

.dashboard-title {
  font-size: 18px;
  font-weight: 700;
  color: var(--text-primary);
  margin: 0;
}

.workshop-badge {
  background: var(--accent-primary);
  color: white;
  padding: 2px 10px;
  border-radius: 10px;
  font-size: 12px;
  font-weight: 600;
}

.header-right {
  display: flex;
  align-items: center;
  gap: 16px;
  font-size: 12px;
  color: var(--text-tertiary);
}

.update-time {
  font-family: 'SF Mono', Consolas, monospace;
}

.record-count {
  color: var(--accent-primary);
  font-weight: 600;
}

.dashboard-body {
  padding: 16px 20px 20px;
}

.control-bar {
  display: flex;
  align-items: flex-end;
  gap: 14px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.control-group {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.control-group label {
  font-size: 12px;
  color: var(--text-secondary);
  font-weight: 500;
}

.workshop-selector .control-select {
  min-width: 130px;
}

.search-box {
  flex: 1;
  min-width: 220px;
}

.search-input-wrapper {
  position: relative;
}

.search-input {
  width: 100%;
  padding-right: 36px;
}

.clear-btn {
  position: absolute;
  right: 8px;
  top: 50%;
  transform: translateY(-50%);
  background: var(--bg-input);
  border: 1px solid var(--border-input);
  border-radius: 50%;
  width: 24px;
  height: 24px;
  color: var(--text-secondary);
  cursor: pointer;
  font-size: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s;
}

.clear-btn:hover {
  background: var(--accent-primary);
  color: white;
  border-color: var(--accent-primary);
}

.date-group .date-input {
  min-width: 140px;
}

.control-select,
.control-input {
  padding: 8px 12px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  font-size: 13px;
  background: var(--bg-input);
  color: var(--text-primary);
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}

.control-select:focus,
.control-input:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.1);
}

.date-input::-webkit-calendar-picker-indicator {
  cursor: pointer;
  filter: invert(0.5);
}

.quick-buttons {
  display: flex;
  gap: 8px;
}

.quick-btn {
  padding: 8px 14px;
  border-radius: 8px;
  border: 1px solid rgba(var(--accent-rgb), 0.4);
  background: rgba(var(--accent-rgb), 0.08);
  color: var(--accent-primary);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
}

.quick-btn:hover {
  background: rgba(var(--accent-rgb), 0.18);
  border-color: var(--accent-primary);
}

.clear-filter-btn {
  padding: 8px 14px;
  border-radius: 8px;
  border: 1px solid rgba(245, 34, 45, 0.4);
  background: rgba(245, 34, 45, 0.08);
  color: #f5222d;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
}

.clear-filter-btn:hover {
  background: rgba(245, 34, 45, 0.16);
  border-color: #f5222d;
}

.refresh-btn {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  border: 1px solid var(--border-input);
  background: var(--bg-input);
  color: var(--accent-primary);
  font-size: 16px;
  cursor: pointer;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
}

.refresh-btn:hover:not(:disabled) {
  background: rgba(var(--accent-rgb), 0.12);
  border-color: var(--accent-primary);
}

.refresh-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.filter-info {
  font-size: 12px;
  color: var(--text-secondary);
  margin-bottom: 12px;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  border-radius: 6px;
  border-left: 3px solid var(--accent-primary);
}

.filter-info strong {
  color: var(--accent-primary);
}

.error-message {
  padding: 12px 16px;
  margin-bottom: 12px;
  background: rgba(245, 34, 45, 0.08);
  border: 1px solid rgba(245, 34, 45, 0.3);
  border-radius: 8px;
  color: #f5222d;
  font-size: 13px;
}

.components-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 16px;
}

.grid-item {
  min-width: 0;
}

.grid-item.full-width {
  grid-column: 1 / -1;
}

.grid-item.half {
  grid-column: span 1;
}

.spin {
  display: inline-block;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.slide-fade-enter-active {
  transition: all 0.3s ease-out;
}

.slide-fade-leave-active {
  transition: all 0.2s ease-in;
}

.slide-fade-enter-from,
.slide-fade-leave-to {
  opacity: 0;
  transform: translateY(-8px);
}

@media (max-width: 1024px) {
  .components-grid {
    grid-template-columns: 1fr;
  }

  .grid-item.half {
    grid-column: 1 / -1;
  }
}

@media (max-width: 768px) {
  .control-bar {
    gap: 10px;
  }

  .control-group {
    min-width: calc(50% - 5px);
    flex: 1;
  }

  .search-box {
    min-width: 100%;
    flex-basis: 100%;
  }

  .workshop-selector,
  .date-group {
    min-width: calc(50% - 5px);
  }

  .control-select,
  .date-input,
  .search-input {
    width: 100%;
  }

  .quick-buttons,
  .clear-filter-btn,
  .refresh-btn {
    flex: 1;
  }

  .dashboard-header {
    padding: 12px 14px;
  }

  .dashboard-title {
    font-size: 16px;
  }

  .header-right {
    display: none;
  }
}
</style>
