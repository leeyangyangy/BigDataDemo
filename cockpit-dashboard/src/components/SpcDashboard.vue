<template>
  <div class="spc-dashboard">
    <div class="auth-notice" v-if="!isLoggedIn">
      <span class="notice-icon">🔒</span>
      <span>当前为预览模式，<a class="notice-link" @click="$emit('require-login')">登录</a>后可填写和导出数据</span>
    </div>

    <div class="filter-bar">
      <div class="filter-group">
        <label>产品</label>
        <select v-model="selectedProduct" class="filter-select" @change="onProductChange">
          <option :value="null">请选择产品</option>
          <option v-for="p in products" :key="p.id" :value="p.id">{{ p.productName }} ({{ p.productCode }})</option>
        </select>
      </div>
      <div class="filter-group">
        <label>工序</label>
        <select v-model="selectedProcess" class="filter-select" @change="onProcessChange" :disabled="!selectedProduct">
          <option :value="null">请选择工序</option>
          <option v-for="p in processes" :key="p.id" :value="p.id">{{ p.processName }} ({{ p.processCode }})</option>
        </select>
      </div>
      <div class="filter-group">
        <label>工艺参数</label>
        <select v-model="selectedParam" class="filter-select" @change="onParamChange" :disabled="!selectedProcess">
          <option :value="null">请选择工艺参数</option>
          <option v-for="p in params" :key="p.id" :value="p.id">{{ p.paramName }} ({{ p.paramCode }}) <span v-if="p.unit">[{{ p.unit }}]</span></option>
        </select>
      </div>
      <div class="filter-group">
        <label>设备</label>
        <select v-model="selectedEquipment" class="filter-select" @change="onEquipmentChange" :disabled="!selectedProcess">
          <option :value="null">全部设备</option>
          <option v-for="eq in equipmentList" :key="eq.id" :value="eq.id">{{ eq.name }} ({{ eq.code }})</option>
        </select>
      </div>
      <div class="filter-group">
        <label>数据量</label>
        <select v-model="dataLimit" class="filter-select short">
          <option :value="50">最近50</option>
          <option :value="100">最近100</option>
          <option :value="200">最近200</option>
          <option :value="500">最近500</option>
        </select>
      </div>
      <div class="filter-group">
        <label>时间范围</label>
        <select v-model="timeRange" class="filter-select short" @change="onTimeRangeChange">
          <option value="">不限</option>
          <option value="7">近7天</option>
          <option value="14">近14天</option>
          <option value="30">近1月</option>
          <option value="90">近3月</option>
          <option value="180">近半年</option>
          <option value="365">近1年</option>
        </select>
      </div>
      <button class="btn-apply" @click="refreshAllCharts">查询控制图</button>
      <button class="btn-calc" @click="requireAuth(calculateStat)" v-if="isLoggedIn">计算统计</button>
    </div>

    <!-- 数据导入导出模块 -->
    <SpcDataImport
      :is-logged-in="isLoggedIn"
      :selected-product="selectedProduct"
      :selected-process="selectedProcess"
      :selected-param="selectedParam"
      :products="products"
      :processes="processes"
      :equipment-list="equipmentList"
      :params="params"
      :data-limit="dataLimit"
      :time-range="timeRange"
      @require-login="$emit('require-login')"
      @refresh="refreshAllCharts"
      @data-imported="handleDataImported"
    />

    <!-- 手动上下限输入 -->
    <div class="limit-input-bar" v-if="selectedParamObj">
      <div class="limit-header-row" @click="showLimitPanel = !showLimitPanel">
        <span class="limit-title">📐 手动设置上下限</span>
        <div class="limit-header-right">
          <span class="limit-hint" v-if="!showLimitPanel">留空则使用最新工艺参数版本值</span>
          <span class="limit-toggle" :class="{ expanded: showLimitPanel }">{{ showLimitPanel ? '▲' : '▼' }}</span>
        </div>
      </div>

      <transition name="slide-fade">
        <div class="limit-body" v-if="showLimitPanel">
          <span class="limit-hint-full">留空则使用最新工艺参数版本值</span>

      <div class="limit-groups">
        <!-- 规格限 -->
        <div class="limit-group spec-group">
          <div class="group-label">规格限 (Specification)</div>
          <div class="group-fields">
            <div class="limit-field">
              <label>USL</label>
              <span class="field-desc">规格上限</span>
              <input v-model.number="manualLimits.usl" type="number" step="0.000001" class="limit-input" :placeholder="currentVersion?.usl ?? '-'" />
            </div>
            <div class="limit-field">
              <label>LSL</label>
              <span class="field-desc">规格下限</span>
              <input v-model.number="manualLimits.lsl" type="number" step="0.000001" class="limit-input" :placeholder="currentVersion?.lsl ?? '-'" />
            </div>
            <div class="limit-field">
              <label>Target</label>
              <span class="field-desc">目标值</span>
              <input v-model.number="manualLimits.target" type="number" step="0.000001" class="limit-input" :placeholder="currentVersion?.target ?? '-'" />
            </div>
          </div>
        </div>

        <!-- 控制限 -->
        <div class="limit-group ctrl-group">
          <div class="group-label">控制限 (Control)</div>
          <div class="group-fields">
            <div class="limit-field">
              <label>UCL</label>
              <span class="field-desc">控制上限</span>
              <input v-model.number="manualLimits.ucl" type="number" step="0.000001" class="limit-input" :placeholder="currentVersion?.ucl ?? '-'" />
            </div>
            <div class="limit-field">
              <label>LCL</label>
              <span class="field-desc">控制下限</span>
              <input v-model.number="manualLimits.lcl" type="number" step="0.000001" class="limit-input" :placeholder="currentVersion?.lcl ?? '-'" />
            </div>
          </div>
        </div>
      </div>

      <div class="limit-actions">
        <button class="btn-apply-limit" @click="applyManualLimits" :disabled="!hasManualLimit">✓ 应用并重绘</button>
        <button class="btn-reset-limit" @click="resetManualLimits">↺ 恢复默认</button>
      </div>
        </div>
      </transition>
    </div>

    <div class="standard-info-bar" v-if="currentVersion || selectedParamObj">
      <div class="standard-info">
        <span class="info-label">当前标准:</span>
        <strong>{{ selectedParamObj?.paramName || '-' }}</strong>
        <span class="info-unit" v-if="selectedParamObj?.unit">单位: {{ selectedParamObj.unit }}</span>
        <span class="info-limits" v-if="currentVersion">
          USL={{ currentVersion.usl ?? '-' }}
          LSL={{ currentVersion.lsl ?? '-' }}
          Target={{ currentVersion.target ?? '-' }}
          | UCL={{ currentVersion.ucl ?? '-' }}
          LCL={{ currentVersion.lcl ?? '-' }}
        </span>
        <span class="version-tag-sm" v-if="currentVersion">V{{ currentVersion.versionNo }}</span>
        <button class="btn-version-sm" @click="showVersionHistory = true" v-if="currentVersion && isLoggedIn">版本历史</button>
      </div>
    </div>

    <div class="charts-section">
      <div v-if="processCharts.length > 0" class="chart-grid">
        <div v-for="pc in processCharts" :key="pc.equipmentId ? `${pc.paramId}-${pc.equipmentId}` : pc.paramId" class="chart-card-multi">
          <div class="chart-card-header">
            <span class="chart-title">{{ pc.paramName }}<span class="chart-unit" v-if="pc.unit">({{ pc.unit }})</span><span class="chart-equip-tag" v-if="pc.equipmentName">【{{ pc.equipmentName }}】</span></span>
            <span class="chart-version-tag" v-if="pc.version">V{{ pc.version.versionNo }}</span>
          </div>
          <SpcControlChart :ref="el => { if(el) chartRefs[pc.equipmentId ? `${pc.paramId}-${pc.equipmentId}` : pc.paramId] = el }" :chartData="pc.chartData" />
          <div class="chart-mini-stats" v-if="pc.chartData?.capability">
            <span>Cpk: <strong :class="getCpkClass(pc.chartData.capability.cpk)">{{ pc.chartData.capability.cpk ?? '-' }}</strong></span>
            <span>均值: {{ pc.chartData.capability.mean ?? '-' }}</span>
            <span>样本: {{ pc.chartData.capability.sampleCount ?? 0 }}</span>
          </div>
        </div>
      </div>
      <div v-else-if="selectedParam && chartData" class="chart-card">
        <SpcControlChart ref="chartRef" :chartData="chartData" />
      </div>
      <div v-else class="chart-empty-hint">
        <span v-if="selectedProcess && !selectedParam && selectedEquipment">该设备下暂无工艺参数数据</span>
        <span v-else-if="selectedProcess && !selectedParam">该工序下暂无工艺参数数据</span>
        <span v-else>请选择产品和工序查看SPC控制图</span>
      </div>
    </div>

    <div class="capability-section" v-if="selectedProcess && processCharts.length > 1 && capability">
      <div class="stat-grid">
        <div class="stat-card" :class="capabilityClass">
          <div class="stat-value">{{ capability?.cpk ?? '-' }}</div>
          <div class="stat-label">Cpk</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">{{ capability?.cp ?? '-' }}</div>
          <div class="stat-label">Cp</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">{{ capability?.mean ?? '-' }}</div>
          <div class="stat-label">均值</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">{{ capability?.stdDev ?? '-' }}</div>
          <div class="stat-label">标准差</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">{{ capability?.sampleCount ?? 0 }}</div>
          <div class="stat-label">样本数</div>
        </div>
      </div>
    </div>

    <SpcAlertPanel
      v-if="selectedParam && selectedProduct"
      :alerts="alertList"
      :loading="loadingAlerts"
      @rules-change="onRulesChange"
    />

    <div class="version-history-modal" v-if="showVersionHistory" @click.self="showVersionHistory = false">
      <div class="modal-content">
        <h3>工艺参数版本历史</h3>
        <table class="version-table">
          <thead>
            <tr>
              <th>版本</th>
              <th>USL</th>
              <th>LSL</th>
              <th>Target</th>
              <th>UCL</th>
              <th>LCL</th>
              <th>生效时间</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="v in versionHistory" :key="v.id" :class="{ current: v.isCurrent === 1 }">
              <td>V{{ v.versionNo }}</td>
              <td>{{ v.usl ?? '-' }}</td>
              <td>{{ v.lsl ?? '-' }}</td>
              <td>{{ v.target ?? '-' }}</td>
              <td>{{ v.ucl ?? '-' }}</td>
              <td>{{ v.lcl ?? '-' }}</td>
              <td>{{ v.effectiveFrom }}</td>
              <td>{{ v.isCurrent === 1 ? '当前' : '历史' }}</td>
            </tr>
          </tbody>
        </table>
        <button class="btn-close" @click="showVersionHistory = false">关闭</button>
      </div>
    </div>

    <div class="floating-actions">
      <button class="fab-btn fab-refresh" @click="handleRefresh" :title="'刷新数据'">
        <span v-if="!isRefreshing">↻</span>
        <span v-else class="spin">↻</span>
      </button>
      <transition name="fade-up">
        <button class="fab-btn fab-top" v-show="showBackTop" @click="scrollToTop" :title="'回到顶部'">↑</button>
      </transition>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch, reactive} from 'vue'
import SpcControlChart from './SpcControlChart.vue'
import SpcAlertPanel from './SpcAlertPanel.vue'
import SpcDataImport from './SpcDataImport.vue'
import { spcApi, adminApi } from '@/utils/api.js'

const props = defineProps({
  isLoggedIn: { type: Boolean, default: false }
})

const emit = defineEmits(['require-login'])

const selectedProduct = ref(null)
const selectedProcess = ref(null)
const selectedParam = ref(null)
const selectedEquipment = ref(null)
const dataLimit = ref(100)
const timeRange = ref('')

const showBackTop = ref(false)
const isRefreshing = ref(false)

const FILTER_KEY = 'spc_filter_state'

function saveFilterState() {
  const state = {
    productId: selectedProduct.value,
    processId: selectedProcess.value,
    paramId: selectedParam.value,
    equipmentId: selectedEquipment.value,
    dataLimit: dataLimit.value,
    timeRange: timeRange.value
  }
  try { localStorage.setItem(FILTER_KEY, JSON.stringify(state)) } catch (e) {}
}

function loadFilterState() {
  try {
    const raw = localStorage.getItem(FILTER_KEY)
    if (!raw) return null
    return JSON.parse(raw)
  } catch (e) { return null }
}

const CACHE_TTL = 5 * 60 * 1000
const dataCache = new Map()

function getCache(key) {
  const entry = dataCache.get(key)
  if (!entry) return null
  if (Date.now() - entry.ts > CACHE_TTL) { dataCache.delete(key); return null }
  return entry.data
}

function setCache(key, data) {
  dataCache.set(key, { data, ts: Date.now() })
}

function invalidateCache(prefix) {
  for (const k of dataCache.keys()) {
    if (k.startsWith(prefix)) dataCache.delete(k)
  }
}

const products = ref([])
const processes = ref([])
const params = ref([])
const equipmentList = ref([])
const chartData = ref(null)
const currentVersion = ref(null)
const versionHistory = ref([])
const showVersionHistory = ref(false)

const manualLimits = ref({ usl: null, lsl: null, target: null, ucl: null, lcl: null })
const useManualLimits = ref(false)
const showLimitPanel = ref(false)

const processCharts = ref([])
const chartRefs = ref({})
const alertList = ref([])
const loadingAlerts = ref(false)
const enabledAlertRules = ref([1, 2, 3, 4, 5, 6, 7, 8])

const capability = computed(() => chartData.value?.capability || null)
const capabilityClass = computed(() => {
  const cpk = capability.value?.cpk
  if (cpk == null) return ''
  if (cpk >= 1.33) return 'good'
  if (cpk >= 1.0) return 'warning'
  return 'danger'
})

const selectedParamObj = computed(() => {
  if (!selectedParam.value || !params.value.length) return null
  return params.value.find(p => p.id === selectedParam.value) || null
})

const hasManualLimit = computed(() => {
  const m = manualLimits.value
  return m.usl != null || m.lsl != null || m.target != null || m.ucl != null || m.lcl != null
})

function requireAuth(fn) {
  if (!props.isLoggedIn) {
    emit('require-login')
    return
  }
  fn()
}
// TODO
// ① 参数✓ 设备✓ loadSingleChart() 单条控制图 
// ② 参数✗ 设备✗ loadAllProcessCharts() 工序下全部参数图表 
// ③ 参数✗ 设备✓ loadEquipAllParams() ✨新增 该设备全部参数图表【带设备名】 
// ④ 参数✓ 设备✗ loadParamAcrossEquipments() 该参数全设备图表
//  考虑是否要绑定用户到车间，车间关联工序，用户被限制只能加载该车间有关的工序、参数、设备选择
//  数据预览不在向匿名用户开放，需要通过企业应用完成授权或者内网登录才能继续预览、填写、导出数据
onMounted(async () => {
  await loadProducts()
  const saved = loadFilterState()
  if (!saved) return

  if (saved.dataLimit != null) dataLimit.value = saved.dataLimit
  if (saved.timeRange != null) timeRange.value = saved.timeRange

  const pendingEquipId = saved.equipmentId || null
  const pendingParamId = saved.paramId || null
  let shouldRefresh = false

  if (saved.productId && products.value.some(p => p.id === saved.productId)) {
    selectedProduct.value = saved.productId
    await onProductChange(false)
    if (saved.processId && processes.value.some(p => p.id === saved.processId)) {
      selectedProcess.value = saved.processId
      await onProcessChange(false)

      if (pendingEquipId && equipmentList.value.length > 0 && equipmentList.value.some(e => e.id === pendingEquipId)) {
        selectedEquipment.value = pendingEquipId
      }

      if (pendingParamId && params.value.length > 0 && params.value.some(p => p.id === pendingParamId)) {
        selectedParam.value = pendingParamId
        await onParamChange(false)
      } else if (selectedProcess.value) {
        shouldRefresh = true
      }
    }
  }

  if (shouldRefresh) {
    await refreshAllCharts()
  }

  saveFilterState()
  window.addEventListener('scroll', onScroll, { passive: true })
})

onUnmounted(() => {
  window.removeEventListener('scroll', onScroll)
})

function onScroll() {
  showBackTop.value = window.scrollY > 200
}

async function handleRefresh() {
  if (isRefreshing.value) return
  isRefreshing.value = true
  dataCache.clear()
  try {
    await loadProducts()
    if (selectedProduct.value) {
      processes.value = []
      params.value = []
      equipmentList.value = []
      await onProductChange(false)
      if (selectedProcess.value) {
        await onProcessChange(false)
        if (selectedParam.value) {
          await onParamChange(false)
        } else if (params.value.length > 0) {
          await refreshAllCharts()
        }
      }
    } else {
      chartData.value = null
      processCharts.value = []
      currentVersion.value = null
    }
  } finally {
    setTimeout(() => { isRefreshing.value = false }, 600)
  }
}

function scrollToTop() {
  window.scrollTo({ top: 0, behavior: 'smooth' })
}

async function loadProducts() {
  const cached = getCache('products')
  if (cached) { products.value = cached; return }
  try {
    const res = await spcApi.getProductPage({ current: 1, size: 100 })
    if (res.code === 200) {
      products.value = res.data.records
      setCache('products', res.data.records)
    }
  } catch (e) { console.error('加载产品失败', e) }
}

async function onProductChange(save = true) {
  selectedProcess.value = null
  selectedParam.value = null
  selectedEquipment.value = null
  processes.value = []
  params.value = []
  equipmentList.value = []
  chartData.value = null
  currentVersion.value = null
  processCharts.value = []

  if (!selectedProduct.value) { if (save) saveFilterState(); return }

  try {
    const cached = getCache('processes')
    if (cached) { processes.value = cached; if (save) saveFilterState(); return }
    const processRes = await spcApi.getProcessPage({ current: 1, size: 100 })
    if (processRes.code === 200) {
      processes.value = processRes.data.records
      setCache('processes', processRes.data.records)
    }
  } catch (e) { console.error('加载工序失败', e) }
  if (save) saveFilterState()
}

async function onEquipmentChange() {
  saveFilterState()
  refreshAllCharts()
}

function onTimeRangeChange() {
  saveFilterState()
  refreshAllCharts()
}

function getTimeRangeParams() {
  if (!timeRange.value) return {}
  const days = Number(timeRange.value)
  const now = new Date()
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000)
  const pad = (n) => String(n).padStart(2, '0')
  const fmt = (d) => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  return { startTime: fmt(start), endTime: fmt(now) }
}

async function onProcessChange(save = true) {
  selectedParam.value = null
  selectedEquipment.value = null
  params.value = []
  equipmentList.value = []
  chartData.value = null
  currentVersion.value = null
  processCharts.value = []

  if (!selectedProcess.value || !selectedProduct.value) {
    if (save) saveFilterState();
    return
  }

  try {
    const cacheKey = `params:${selectedProcess.value}`
    const cached = getCache(cacheKey)
    if (cached) {
      params.value = cached
    } else {
      try {
        const res = await spcApi.getParamPage({current: 1, size: 100})
        if (res.code === 200) {
          const filtered = res.data.records.filter(p => p.processId === selectedProcess.value)
          params.value = filtered
          setCache(cacheKey, filtered)
        }
      } catch (e) {
        console.error('加载标准(参数)失败', e)
      }
    }

    await loadEquipmentByProcess()
    if (save) saveFilterState()
  } catch (e) {
    console.error('onProcessChange 异常', e)
  }
}

async function loadEquipmentByProcess() {
  equipmentList.value = []
  if (!selectedProcess.value) return

  const cacheKey = `equip:${selectedProcess.value}`
  const cached = getCache(cacheKey)
  if (cached) { equipmentList.value = cached; return }

  try {
    const res = await spcApi.getProcessEquipment(selectedProcess.value)
    if (res.code === 200 && res.data) {
      const mapped = res.data.map(eq => ({
        id: eq.id,
        code: eq.equipCode,
        name: eq.equipName || eq.equipCode
      }))
      equipmentList.value = mapped
      setCache(cacheKey, mapped)
    }
  } catch (e) { console.error('加载工序设备列表失败', e) }
}

async function loadAllProcessCharts() {
  processCharts.value = []
  chartData.value = null
  if (!selectedProduct.value || !selectedProcess.value || !params.value.length) return

  let targetEquipList = equipmentList.value
  if (!targetEquipList.length && selectedProcess.value) {
    try {
      const res = await spcApi.getProcessEquipment(selectedProcess.value)
      if (res.code === 200 && res.data) {
        targetEquipList = res.data.map(eq => ({
          id: eq.id,
          code: eq.equipCode,
          name: eq.equipName || eq.equipCode
        }))
      }
    } catch (e) { }
  }

  const equipList = selectedEquipment.value
    ? targetEquipList.filter(e => e.id === selectedEquipment.value)
    : targetEquipList

  const chartPromises = []
  params.value.forEach((param) => {
    equipList.forEach((eq) => {
      chartPromises.push((async () => {
        let version = null
        try {
          const verRes = await spcApi.getParamVersionCurrent({
            paramId: param.id,
            productId: selectedProduct.value
          })
          if (verRes.code === 200) version = verRes.data
        } catch (e) { }

        let chartD = null
        try {
          const res = await spcApi.getDataByEquipment({
            paramId: param.id,
            productId: selectedProduct.value,
            equipmentId: eq.id,
            limit: dataLimit.value,
            ...getTimeRangeParams()
          })
          if (res.code === 200) chartD = res.data
        } catch (e) { }

        if (chartD && useManualLimits.value && hasManualLimit.value && chartD.limits) {
          const m = manualLimits.value
          if (m.usl != null) chartD.limits.usl = m.usl
          if (m.lsl != null) chartD.limits.lsl = m.lsl
          if (m.target != null) chartD.limits.target = m.target
          if (m.ucl != null) chartD.limits.ucl = m.ucl
          if (m.lcl != null) chartD.limits.lcl = m.lcl
        }

        ensureChronologicalOrder(chartD)

        return {
          paramId: param.id,
          equipmentId: eq.id,
          equipmentName: eq.name || eq.code,
          paramName: param.paramName,
          unit: param.unit,
          version,
          chartData: chartD
        }
      })())
    })
  })

  try {
    processCharts.value = await Promise.all(chartPromises)
  } catch (e) { console.error('批量加载控制图失败', e) }
}

async function onParamChange(save = true) {
  chartData.value = null
  currentVersion.value = null
  processCharts.value = []

  if (!selectedParam.value) {
    if (selectedProcess.value && params.value.length > 0) {
      await loadAllProcessCharts()
    }
    if (save) saveFilterState()
    return
  }

  try {
    const res = await spcApi.getParamVersionCurrent({
      paramId: selectedParam.value,
      productId: selectedProduct.value
    })
    if (res.code === 200) currentVersion.value = res.data
  } catch (e) { console.error('加载版本失败', e) }

  await refreshAllCharts()
  if (save) saveFilterState()
}

async function loadSingleChart() {
  if (!selectedParam.value || !selectedProduct.value) return

  try {
    const baseParams = {
      paramId: selectedParam.value,
      productId: selectedProduct.value,
      limit: dataLimit.value,
      ...getTimeRangeParams()
    }

    let cd
    if (selectedEquipment.value) {
      const res = await spcApi.getDataByEquipment({
        ...baseParams,
        equipmentId: selectedEquipment.value
      })
      cd = res.code === 200 ? res.data : null
    } else {
      const res = await spcApi.getControlChart(baseParams)
      cd = res.code === 200 ? res.data : null
    }

    if (!cd) { chartData.value = null; return }

    if (useManualLimits.value && hasManualLimit.value && cd.limits) {
      const m = manualLimits.value
      if (m.usl != null) cd.limits.usl = m.usl
      if (m.lsl != null) cd.limits.lsl = m.lsl
      if (m.target != null) cd.limits.target = m.target
      if (m.ucl != null) cd.limits.ucl = m.ucl
      if (m.lcl != null) cd.limits.lcl = m.lcl
    }

    ensureChronologicalOrder(cd)
    chartData.value = cd
  } catch (e) { console.error('加载单参数控制图失败:', e) }
}

function applyManualLimits() {
  if (!hasManualLimit.value) return
  useManualLimits.value = true
  refreshAllCharts()
}

function resetManualLimits() {
  manualLimits.value = { usl: null, lsl: null, target: null, ucl: null, lcl: null }
  useManualLimits.value = false
  refreshAllCharts()
}

function getCpkClass(cpk) {
  if (cpk == null) return ''
  if (cpk >= 1.33) return 'good'
  if (cpk >= 1.0) return 'warning'
  return 'danger'
}

function ensureChronologicalOrder(chartD) {
  if (!chartD || !chartD.values || !chartD.timeSeries || chartD.values.length <= 1) return
  const paired = chartD.values.map((v, i) => ({ value: v, time: chartD.timeSeries[i] }))
    .sort((a, b) => {
      const ta = a.time ? new Date(a.time).getTime() : 0
      const tb = b.time ? new Date(b.time).getTime() : 0
      return ta - tb
    })
  chartD.values = paired.map(p => p.value)
  chartD.timeSeries = paired.map(p => p.time)
}

async function refreshAllCharts() {
  const hasParam = !!selectedParam.value
  const hasEquip = !!selectedEquipment.value

  if (hasParam && hasEquip) {
    await loadSingleChart()
  } else if (hasParam && !hasEquip) {
    await loadParamAcrossEquipments()
  } else if (!hasParam && hasEquip) {
    await loadEquipAllParams()
  } else {
    await loadAllProcessCharts()
  }
}

async function loadEquipAllParams() {
  processCharts.value = []
  chartData.value = null
  if (!selectedProduct.value || !selectedProcess.value || !selectedEquipment.value || !params.value.length) return

  const equipInfo = equipmentList.value.find(e => e.id === selectedEquipment.value)
  const equipName = equipInfo?.name || equipInfo?.code || ''

  const chartPromises = params.value.map(async (param) => {
    let version = null
    try {
      const verRes = await spcApi.getParamVersionCurrent({
        paramId: param.id,
        productId: selectedProduct.value
      })
      if (verRes.code === 200) version = verRes.data
    } catch (e) { }

    let chartD = null
    try {
      const res = await spcApi.getDataByEquipment({
        paramId: param.id,
        productId: selectedProduct.value,
        equipmentId: selectedEquipment.value,
        limit: dataLimit.value,
        ...getTimeRangeParams()
      })
      if (res.code === 200) chartD = res.data
    } catch (e) { }

    if (chartD && useManualLimits.value && hasManualLimit.value && chartD.limits) {
      const m = manualLimits.value
      if (m.usl != null) chartD.limits.usl = m.usl
      if (m.lsl != null) chartD.limits.lsl = m.lsl
      if (m.target != null) chartD.limits.target = m.target
      if (m.ucl != null) chartD.limits.ucl = m.ucl
      if (m.lcl != null) chartD.limits.lcl = m.lcl
    }

    ensureChronologicalOrder(chartD)

    return {
      paramId: param.id,
      equipmentId: selectedEquipment.value,
      equipmentName: equipName,
      paramName: param.paramName,
      unit: param.unit,
      version,
      chartData: chartD
    }
  })

  try {
    processCharts.value = await Promise.all(chartPromises)
  } catch (e) { console.error('加载设备全部参数控制图失败', e) }
}

async function loadParamAcrossEquipments() {
  processCharts.value = []
  chartData.value = null
  if (!selectedParam.value || !selectedProduct.value) return

  let version = null
  try {
    const verRes = await spcApi.getParamVersionCurrent({
      paramId: selectedParam.value,
      productId: selectedProduct.value
    })
    if (verRes.code === 200) version = verRes.data
  } catch (e) { }

  const paramInfo = params.value.find(p => p.id === selectedParam.value)
  const paramName = paramInfo?.paramName ?? ''
  const unit = paramInfo?.unit ?? ''

  let targetEquipList = equipmentList.value
  if (!targetEquipList.length && selectedProcess.value) {
    try {
      const res = await spcApi.getProcessEquipment(selectedProcess.value)
      if (res.code === 200 && res.data) {
        targetEquipList = res.data.map(eq => ({
          id: eq.id,
          code: eq.equipCode,
          name: eq.equipName || eq.equipCode
        }))
      }
    } catch (e) { }
  }

  if (!targetEquipList.length) {
    await loadSingleChart()
    return
  }

  const chartPromises = targetEquipList.map(async (eq) => {
    let chartD = null
    try {
      const res = await spcApi.getDataByEquipment({
        paramId: selectedParam.value,
        productId: selectedProduct.value,
        equipmentId: eq.id,
        limit: dataLimit.value,
        ...getTimeRangeParams()
      })
      if (res.code === 200) chartD = res.data
    } catch (e) { }

    if (chartD && useManualLimits.value && hasManualLimit.value && chartD.limits) {
      const m = manualLimits.value
      if (m.usl != null) chartD.limits.usl = m.usl
      if (m.lsl != null) chartD.limits.lsl = m.lsl
      if (m.target != null) chartD.limits.target = m.target
      if (m.ucl != null) chartD.limits.ucl = m.ucl
      if (m.lcl != null) chartD.limits.lcl = m.lcl
    }

    ensureChronologicalOrder(chartD)

    return {
      paramId: selectedParam.value,
      equipmentId: eq.id,
      equipmentName: eq.name,
      paramName,
      unit,
      version,
      chartData: chartD
    }
  })

  try {
    processCharts.value = await Promise.all(chartPromises)
  } catch (e) { console.error('加载参数跨设备控制图失败', e) }

  loadAlerts()
}

async function loadAlerts() {
  if (!selectedParam.value || !selectedProduct.value) {
    alertList.value = []
    return
  }

  loadingAlerts.value = true
  try {
    const res = await spcApi.getAlerts({
      paramId: selectedParam.value,
      productId: selectedProduct.value,
      equipmentId: selectedEquipment.value || null,
      limit: dataLimit.value,
      ...getTimeRangeParams()
    })
    if (res.code === 200 && res.data) {
      alertList.value = res.data.filter(a => enabledAlertRules.value.includes(a.ruleId))
    } else {
      alertList.value = []
    }
  } catch (e) {
    console.error('加载异常检测数据失败', e)
    alertList.value = []
  } finally {
    loadingAlerts.value = false
  }
}

function onRulesChange(rules) {
  enabledAlertRules.value = rules
  if (alertList.value.length > 0) {
    alertList.value = alertList.value.filter(a => enabledAlertRules.value.includes(a.ruleId))
  }
}

function handleDataImported() {
  dataCache.clear()
  refreshAllCharts()
}

async function calculateStat() {
  if (!currentVersion.value) return

  try {
    await spcApi.calculateStat({ paramVersionId: currentVersion.value.id })
    refreshAllCharts()
  } catch (e) { console.error('计算统计失败', e) }
}

async function loadVersionHistory() {
  if (!selectedParam.value || !selectedProduct.value) return

  try {
    const res = await spcApi.getParamVersionHistory({
      paramId: selectedParam.value,
      productId: selectedProduct.value
    })
    if (res.code === 200) versionHistory.value = res.data
  } catch (e) { console.error('加载版本历史失败', e) }
}

watch(dataLimit, () => { saveFilterState(); refreshAllCharts() })

watch(showVersionHistory, (val) => {
  if (val) loadVersionHistory()
})

watch(() => props.isLoggedIn, (val) => {
  if (val) refreshAllCharts()
})
</script>

<style scoped>
.spc-dashboard {
  max-width: 1400px;
  margin: 0 auto;
  padding: 0 0 20px;
}

.auth-notice {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  margin-bottom: 16px;
  background: rgba(250, 173, 20, 0.1);
  border: 1px solid rgba(250, 173, 20, 0.3);
  border-radius: 10px;
  font-size: 13px;
  color: var(--text-secondary);
}

.notice-icon {
  font-size: 16px;
}

.notice-link {
  color: var(--accent-primary);
  cursor: pointer;
  font-weight: 600;
  text-decoration: underline;
}

.filter-bar {
  display: flex;
  gap: 12px;
  align-items: flex-end;
  margin-bottom: 20px;
  flex-wrap: wrap;
}

.filter-group {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.filter-group label {
  font-size: 13px;
  color: var(--text-secondary);
  font-weight: 500;
}

.filter-select {
  padding: 8px 32px 8px 10px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  font-size: 13px;
  background-color: var(--bg-input);
  color: var(--text-primary);
  cursor: pointer;
  min-width: 160px;
  outline: none;
}

.filter-select.short {
  min-width: 100px;
}

.filter-select:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.1);
}

.btn-apply, .btn-calc, .btn-upload, .btn-version {
  padding: 8px 24px;
  border: none;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.3s;
}

.btn-apply {
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 80%, black));
  color: white;
}

.btn-calc {
  background: linear-gradient(135deg, #52c41a, #389e0d);
  color: white;
}

.btn-upload {
  background: linear-gradient(135deg, #722ed1, #531dab);
  color: white;
}

.btn-import,
.btn-export,
.btn-template {
  padding: 8px 16px;
  border: none;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.3s;
}

.btn-import {
  background: linear-gradient(135deg, #13c2c2, #08979c);
  color: white;
}

.btn-export {
  background: linear-gradient(135deg, #faad14, #d48806);
  color: white;
}

.btn-template {
  background: linear-gradient(135deg, #52c41a, #389e0d);
  color: white;
}

.btn-import:hover,
.btn-export:hover,
.btn-template:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
}

.btn-version {
  background: transparent;
  border: 1px solid var(--accent-primary);
  color: var(--accent-primary);
  padding: 4px 12px;
  font-size: 12px;
}

.btn-apply:hover, .btn-calc:hover, .btn-upload:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(var(--accent-rgb), 0.4);
}

.version-info {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
  padding: 10px 16px;
  background: var(--bg-secondary);
  border-radius: 8px;
  font-size: 13px;
  flex-wrap: wrap;
}

.version-tag {
  background: var(--accent-primary);
  color: white;
  padding: 2px 10px;
  border-radius: 4px;
  font-weight: 600;
}

.version-detail {
  color: var(--text-secondary);
}

.charts-section {
  margin-bottom: 20px;
}

.chart-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(580px, 1fr));
  gap: 20px;
}

.chart-card-multi {
  background: var(--bg-secondary);
  border-radius: 16px;
  padding: 16px;
  box-shadow: var(--shadow-sm);
  border-top: 3px solid var(--accent-primary);
}

.chart-card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.chart-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--text-primary);
}

.chart-unit {
  color: #52c41a;
  font-size: 12px;
  font-weight: 400;
}

.chart-version-tag {
  background: var(--accent-primary);
  color: white;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 600;
}

.chart-equip-tag {
  color: #1890ff;
  font-size: 12px;
  margin-left: 6px;
}

.chart-mini-stats {
  display: flex;
  gap: 16px;
  margin-top: 10px;
  padding-top: 10px;
  border-top: 1px solid var(--border-color);
  font-size: 12px;
  color: var(--text-secondary);
}

.chart-mini-stats strong.good { color: #52c41a; }
.chart-mini-stats strong.warning { color: #faad14; }
.chart-mini-stats strong.danger { color: #f5222d; }

.chart-empty-hint {
  text-align: center;
  padding: 60px 20px;
  color: var(--text-tertiary);
  font-size: 14px;
  background: var(--bg-secondary);
  border-radius: 16px;
}

.capability-section {
  margin-bottom: 20px;
}

.stat-grid {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 16px;
}

.stat-card {
  background: var(--bg-secondary);
  border-radius: 12px;
  padding: 16px;
  text-align: center;
  box-shadow: var(--shadow-sm);
  border-left: 4px solid var(--accent-primary);
}

.stat-card.good { border-left-color: #52c41a; }
.stat-card.warning { border-left-color: #faad14; }
.stat-card.danger { border-left-color: #f5222d; }

.stat-value {
  font-size: 28px;
  font-weight: 700;
  color: var(--text-primary);
}

.stat-label {
  font-size: 12px;
  color: var(--text-secondary);
  margin-top: 4px;
}

.version-history-modal, .upload-modal {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-content {
  background: var(--bg-secondary);
  border-radius: 16px;
  padding: 24px;
  max-width: 800px;
  width: 90%;
  max-height: 80vh;
  overflow-y: auto;
}

.modal-content h3 {
  margin-bottom: 16px;
  color: var(--text-primary);
}

.version-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.version-table th, .version-table td {
  padding: 8px 12px;
  border-bottom: 1px solid var(--border-color);
  text-align: center;
}

.version-table th {
  background: var(--bg-tertiary);
  font-weight: 600;
  color: var(--text-primary);
}

.version-table tr.current {
  background: rgba(var(--accent-rgb), 0.1);
}

.btn-close {
  margin-top: 16px;
  padding: 8px 24px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-secondary);
  color: var(--text-primary);
  cursor: pointer;
}

.upload-form {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.form-row {
  display: flex;
  gap: 14px;
}

.form-field {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.form-field.full {
  flex-basis: 100%;
}

.form-field label {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-secondary);
}

.form-input {
  padding: 8px 12px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  font-size: 13px;
  background: var(--bg-input);
  color: var(--text-primary);
  outline: none;
}

.form-input:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.1);
}

.form-input.input-error {
  border-color: #e74c3c;
}

.req {
  color: #e74c3c;
  font-weight: bold;
}

.field-error {
  font-size: 11px;
  color: #e74c3c;
}

.datetime-row {
  display: flex;
  gap: 8px;
  align-items: center;
}

.datetime-row .form-input {
  flex: 1;
}

.btn-now {
  padding: 8px 14px;
  border: 1px solid var(--accent-primary);
  border-radius: 8px;
  background: transparent;
  color: var(--accent-primary);
  font-size: 12px;
  cursor: pointer;
  white-space: nowrap;
}

.field-hint {
  font-size: 11px;
  color: var(--text-tertiary);
}

.form-actions {
  display: flex;
  gap: 12px;
  justify-content: flex-end;
  margin-top: 8px;
}

.btn-cancel {
  padding: 8px 20px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-secondary);
  color: var(--text-primary);
  cursor: pointer;
}

.btn-submit {
  padding: 8px 24px;
  border: none;
  border-radius: 8px;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 80%, black));
  color: white;
  font-weight: 500;
  cursor: pointer;
}

.btn-submit:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.upload-result {
  text-align: center;
  font-size: 13px;
  padding: 8px;
}

.upload-result .success { color: #52c41a; }
.upload-result .error { color: #f5222d; }

@media (max-width: 1024px) {
  .stat-grid { grid-template-columns: repeat(3, 1fr); }
  .chart-grid { grid-template-columns: 1fr; }
}

@media (max-width: 768px) {
  .filter-bar { gap: 8px; flex-wrap: wrap; justify-content: stretch; }
  .filter-group { min-width: calc(50% - 4px); flex: 1; }
  .filter-select { min-width: auto; width: 100%; font-size: 12px; padding: 8px 28px 8px 8px; }
  .btn-apply, .btn-calc { flex: 1; text-align: center; font-size: 12px; padding: 10px 16px; }
}

@media (max-width: 600px) {
  .filter-bar { flex-direction: column; align-items: stretch; }
  .filter-group { min-width: 100%; }
  .filter-select { min-width: 100%; }
  .btn-apply, .btn-calc { width: 100%; order: 99; margin-top: 4px; }

  .stat-grid { grid-template-columns: repeat(2, 1fr); }
  .form-row { flex-direction: column; }
  .chart-mini-stats { flex-wrap: wrap; gap: 8px; }

  .limit-input-bar { padding: 12px 14px; }
  .limit-groups { flex-direction: column; }
  .limit-group { min-width: 100%; }
  .group-fields { flex-direction: column; }
  .limit-field { width: 100%; }
  .limit-input { width: 100%; box-sizing: border-box; }
  .limit-actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .btn-apply-limit, .btn-reset-limit { flex: 1; min-width: 120px; text-align: center; }

  .standard-info-bar { flex-direction: column; align-items: flex-start; gap: 6px; }
  .standard-info { flex-direction: column; align-items: flex-start; gap: 4px; }
}

.standard-info-bar {
  margin-bottom: 16px;
  padding: 10px 16px;
  background: var(--bg-secondary);
  border-radius: 8px;
  border-left: 4px solid var(--accent-primary);
}

.standard-info {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 13px;
  flex-wrap: wrap;
}

.info-label { color: var(--text-secondary); }

.info-unit {
  background: rgba(82, 196, 26, 0.15);
  color: #52c41a;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 600;
}

.info-limits {
  color: var(--text-tertiary);
  font-size: 12px;
  font-family: monospace;
}

.version-tag-sm {
  background: var(--accent-primary);
  color: white;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 600;
}

.btn-version-sm {
  padding: 3px 10px;
  border: 1px solid var(--accent-primary);
  border-radius: 6px;
  background: transparent;
  color: var(--accent-primary);
  font-size: 11px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-version-sm:hover {
  background: var(--accent-primary);
  color: white;
}

.standard-limit-hint {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 14px;
  background: rgba(250, 173, 20, 0.08);
  border: 1px dashed rgba(250, 173, 20, 0.35);
  border-radius: 8px;
  font-size: 12px;
  font-family: monospace;
  color: var(--text-secondary);
  flex-wrap: wrap;
}

.hint-label {
  font-weight: 600;
  color: #faad14;
  white-space: nowrap;
}

.unit-hint {
  color: #52c41a;
  font-weight: 500;
}

.limit-input-bar {
  margin-bottom: 16px;
  padding: 16px 20px;
  background: linear-gradient(135deg, rgba(24,144,255,0.03), rgba(114,46,209,0.03));
  border: 1px solid rgba(24,144,255,0.12);
  border-radius: 14px;
}

.limit-header-row {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 14px;
  cursor: pointer;
  user-select: none;
  padding: 6px 0;
  transition: background 0.2s;
  border-radius: 8px;
}
.limit-header-row:hover {
  background: rgba(24,144,255,0.05);
}
.limit-title { font-size: 14px; font-weight: 700; color: var(--text-primary); }
.limit-hint { font-size: 11px; color: var(--text-tertiary); }

.limit-header-right {
  display: flex;
  align-items: center;
  gap: 10px;
}

.limit-toggle {
  font-size: 12px;
  color: #1890ff;
  transition: transform 0.3s ease;
  padding: 4px 8px;
  border-radius: 4px;
  background: rgba(24,144,255,0.08);
}
.limit-toggle.expanded {
  transform: rotate(180deg);
}

.limit-body {
  overflow: hidden;
}

.limit-hint-full {
  display: block;
  font-size: 11px;
  color: var(--text-tertiary);
  margin-bottom: 12px;
  padding: 8px 12px;
  background: rgba(250,173,20,0.06);
  border-radius: 6px;
  border-left: 3px solid #faad14;
}

.limit-groups {
  display: flex; gap: 16px; flex-wrap: wrap;
}

.limit-group {
  flex: 1; min-width: 260px;
  padding: 12px 16px; border-radius: 10px; border: 1px solid var(--border-light);
  background: var(--bg-card);
}
.spec-group { border-left: 3px solid #52c41a; }
.ctrl-group { border-left: 3px solid #1890ff; }

.group-label {
  font-size: 11px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .8px; margin-bottom: 10px;
  color: var(--text-tertiary);
}

.group-fields {
  display: flex; gap: 10px; flex-wrap: wrap;
}

.limit-field {
  display: flex; flex-direction: column; gap: 2px;
  min-width: 100px;
}

.limit-field label {
  font-size: 13px; font-weight: 700; color: var(--text-primary);
  font-family: 'SF Mono', Consolas, monospace;
}
.field-desc {
  font-size: 10px; color: var(--text-tertiary); font-weight: 400;
}

.limit-input {
  padding: 7px 10px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary);
  font-size: 13px; width: 100%; outline: none; font-family: monospace;
  transition: border-color .2s, box-shadow .2s;
}
.limit-input:focus { border-color: var(--accent-primary); box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.1); }
.limit-input::placeholder { color: var(--text-tertiary); opacity: .6; }

.limit-actions {
  display: flex; gap: 8px; justify-content: flex-end;
  margin-top: 12px; padding-top: 12px;
  border-top: 1px dashed var(--border-light);
}

.btn-apply-limit {
  padding: 7px 16px; border: none; border-radius: 8px;
  background: linear-gradient(135deg, #1890ff, #096dd9); color: white;
  font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap;
}
.btn-apply-limit:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 3px 10px rgba(24,144,255,0.35); }
.btn-apply-limit:disabled { opacity: 0.5; cursor: not-allowed; }

.btn-reset-limit {
  padding: 7px 14px; border: 1px solid var(--border-input); border-radius: 8px;
  background: transparent; color: var(--text-secondary); font-size: 12px;
  cursor: pointer; white-space: nowrap;
}
.btn-reset-limit:hover { border-color: var(--accent-primary); color: var(--accent-primary); }

.multi-param-section {
  margin: 16px 0;
}

.multi-param-label {
  display: block;
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 10px;
}

.param-checkbox-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 8px;
  max-height: 200px;
  overflow-y: auto;
  padding: 8px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-input);
}

.param-checkbox-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s;
  border: 1px solid transparent;
}

.param-checkbox-item:hover {
  background: rgba(var(--accent-rgb), 0.06);
}

.param-checkbox-item.checked {
  background: rgba(var(--accent-rgb), 0.1);
  border-color: var(--accent-primary);
}

.param-checkbox-item input[type="checkbox"] {
  width: 16px;
  height: 16px;
  accent-color: var(--accent-primary);
  cursor: pointer;
  flex-shrink: 0;
}

.param-checkbox-item .param-info {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.param-checkbox-item .param-info strong {
  font-size: 13px;
  color: var(--text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.param-checkbox-item .param-info small {
  font-size: 11px;
  color: var(--text-tertiary);
}

.multi-value-section {
  margin: 16px 0;
  padding: 12px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-secondary);
}

.multi-value-row {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  padding: 10px 0;
  border-bottom: 1px dashed var(--border-light);
}

.multi-value-row:last-child {
  border-bottom: none;
}

.param-col-left {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-shrink: 0;
  min-width: 120px;
}

.param-col-right {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.param-badge {
  padding: 4px 10px;
  background: linear-gradient(135deg, #667eea, #764ba2);
  color: white;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  white-space: nowrap;
  max-width: 110px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.value-input {
  width: 100%;
  min-width: 0;
}

.unit-tag {
  font-size: 11px;
  color: var(--text-tertiary);
  padding: 3px 6px;
  background: var(--bg-input);
  border-radius: 4px;
  white-space: nowrap;
}

.param-limit-info {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  font-family: monospace;
  color: var(--text-secondary);
  padding: 4px 8px;
  background: var(--bg-input);
  border-radius: 5px;
}

.limit-item {
  white-space: nowrap;
}

.limit-sep {
  color: var(--border-color);
}

.loading-hint {
  color: var(--accent-primary);
  font-family: inherit !important;
  font-style: italic;
}

.no-version-hint {
  color: #faad14;
  font-family: inherit !important;
}

.floating-actions {
  position: fixed;
  right: 28px;
  bottom: 28px;
  z-index: 999;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.fab-btn {
  width: 44px;
  height: 44px;
  border-radius: 50%;
  border: none;
  cursor: pointer;
  font-size: 18px;
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 3px 12px rgba(0,0,0,0.15);
  transition: transform 0.2s, box-shadow 0.2s, background-color 0.25s;
  -webkit-tap-highlight-color: transparent;
}

.fab-btn:hover {
  transform: scale(1.1);
  box-shadow: 0 5px 20px rgba(0,0,0,0.22);
}

.fab-btn:active {
  transform: scale(0.95);
}

.fab-refresh {
  background: linear-gradient(135deg, #1890ff, #096dd9);
  color: #fff;
}

.fab-refresh:hover {
  background: linear-gradient(135deg, #40a9ff, #1890ff);
}

.fab-top {
  background: linear-gradient(135deg, #52c41a, #389e0d);
  color: #fff;
}

.fab-top:hover {
  background: linear-gradient(135deg, #73d13d, #52c41a);
}

.spin {
  display: inline-block;
  animation: fab-spin 0.6s linear infinite;
}

@keyframes fab-spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.fade-up-enter-active,
.fade-up-leave-active {
  transition: opacity 0.3s, transform 0.3s;
}

.fade-up-enter-from,
.fade-up-leave-to {
  opacity: 0;
  transform: translateY(12px);
}

.batch-input-group {
  display: flex;
  gap: 8px;
  align-items: center;
}

.batch-input-group .form-input {
  flex: 1;
}

.btn-scan {
  padding: 8px 14px;
  border: 1px solid #d9d9d9;
  border-radius: 8px;
  background: linear-gradient(135deg, #722ed1, #531dab);
  color: white;
  cursor: pointer;
  font-size: 16px;
  transition: all 0.3s;
  white-space: nowrap;
}

.btn-scan:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(114, 46, 209, 0.4);
}

.scanner-container {
  margin-top: 12px;
  padding: 12px;
  background: #f6f8fa;
  border-radius: 8px;
  border: 1px solid #d9d9d9;
}

.scanner-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
  font-weight: 600;
  color: #333;
}

.btn-close-scanner {
  padding: 4px 12px;
  border: none;
  border-radius: 4px;
  background: #ff4d4f;
  color: white;
  cursor: pointer;
  font-size: 12px;
}

.btn-close-scanner:hover {
  background: #cf1322;
}

.qr-reader {
  width: 100%;
  max-width: 400px;
  margin: 0 auto;
  border-radius: 8px;
  overflow: hidden;
  background: #000;
}

.qr-reader video {
  border-radius: 8px;
}

.scanner-hint {
  text-align: center;
  margin-top: 8px;
  font-size: 12px;
  color: #888;
}

.import-modal {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.import-form .file-input {
  padding: 10px;
  border: 2px dashed #d9d9d9;
  border-radius: 8px;
  background: #fafafa;
  cursor: pointer;
  transition: all 0.3s;
}

.import-form .file-input:hover {
  border-color: #13c2c2;
  background: #e6fffb;
}

.import-preview {
  margin-top: 12px;
  padding: 12px;
  background: #f6f8fa;
  border-radius: 8px;
  max-height: 200px;
  overflow-y: auto;
}

.preview-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
  margin-top: 8px;
}

.preview-table th,
.preview-table td {
  padding: 4px 8px;
  border: 1px solid #e8e8e8;
  text-align: center;
}

.preview-table th {
  background: #f0f0f0;
  font-weight: 600;
}

.slide-fade-enter-active {
  transition: all 0.3s ease-out;
}
.slide-fade-leave-active {
  transition: all 0.2s ease-in;
}
.slide-fade-enter-from,
.slide-fade-leave-to {
  transform: translateY(-10px);
  opacity: 0;
}
</style>
