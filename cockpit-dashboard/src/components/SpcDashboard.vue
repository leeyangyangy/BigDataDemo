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
        <select v-model="selectedEquipment" class="filter-select" @change="onEquipmentChange" :disabled="!selectedParam">
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
      <button class="btn-apply" @click="refreshAllCharts">查询控制图</button>
      <button class="btn-calc" @click="requireAuth(calculateStat)" v-if="isLoggedIn">计算统计</button>
      <button class="btn-upload" @click="requireAuth(() => showUploadForm = true)" v-if="isLoggedIn">填写数据</button>
    </div>

    <!-- 手动上下限输入 -->
    <div class="limit-input-bar" v-if="selectedParamObj">
      <div class="limit-header-row">
        <span class="limit-title">📐 手动设置上下限</span>
        <span class="limit-hint">留空则使用最新工艺参数版本值</span>
      </div>

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
        <div v-for="pc in processCharts" :key="pc.paramId" class="chart-card-multi">
          <div class="chart-card-header">
            <span class="chart-title">{{ pc.paramName }}<span class="chart-unit" v-if="pc.unit">({{ pc.unit }})</span></span>
            <span class="chart-version-tag" v-if="pc.version">V{{ pc.version.versionNo }}</span>
          </div>
          <SpcControlChart :ref="el => { if(el) chartRefs[pc.paramId] = el }" :chartData="pc.chartData" />
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
        <span>请选择产品和工序查看该工序下所有标准的SPC控制图</span>
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

    <div class="upload-modal" v-if="showUploadForm" @click.self="showUploadForm = false">
      <div class="modal-content">
        <h3>填写SPC数据</h3>
        <div class="upload-form">
          <div class="form-row">
            <div class="form-field">
              <label>产品</label>
              <select v-model.number="uploadData.productId" class="form-input" @change="onUploadProductChange">
                <option :value="null">请选择产品</option>
                <option v-for="p in products" :key="p.id" :value="p.id">{{ p.productName }} ({{ p.productCode }})</option>
              </select>
            </div>
            <div class="form-field">
              <label>工序</label>
              <select v-model.number="uploadData.processId" class="form-input" @change="onUploadProcessChange" :disabled="!uploadData.productId">
                <option :value="null">请选择工序</option>
                <option v-for="p in uploadProcesses" :key="p.id" :value="p.id">{{ p.processName }}</option>
              </select>
            </div>
          </div>
          <div class="form-row">
            <div class="form-field">
              <label>标准(工艺参数)</label>
              <select v-model.number="uploadData.paramId" class="form-input" @change="onUploadParamChange" :disabled="!uploadData.processId">
                <option :value="null">请选择标准</option>
                <option v-for="p in uploadParams" :key="p.id" :value="p.id">{{ p.paramName }} ({{ p.paramCode }}) <span v-if="p.unit">[{{ p.unit }}]</span></option>
              </select>
            </div>
            <div class="form-field">
              <label>设备</label>
              <select v-model.number="uploadData.equipmentId" class="form-input">
                <option :value="null">请选择设备</option>
                <option v-for="eq in equipmentList" :key="eq.id" :value="eq.id">{{ eq.name }}</option>
              </select>
            </div>
          </div>
          <div class="form-row" v-if="uploadData.paramId && uploadData.productId">
            <div class="form-field full">
              <label>标准版本
                <span class="field-hint" v-if="uploadVersionList.length === 0">(暂无版本,将自动创建)</span>
              </label>
              <select v-model.number="uploadSelectedVersion" class="form-input" @change="onUploadVersionChange">
                <option :value="null">自动选择当前版本 / 自动创建新版本</option>
                <option v-for="v in uploadVersionList" :key="v.id" :value="v.id">
                  V{{ v.versionNo }}
                  {{ v.isCurrent === 1 ? '【当前】' : '【历史】' }}
                  (USL={{ v.usl ?? '-' }} LSL={{ v.lsl ?? '-' }})
                </option>
              </select>
            </div>
          </div>
          <div class="standard-limit-hint" v-if="uploadCurrentVersion">
            <span class="hint-label">当前工艺参数上下限:</span>
            USL={{ uploadCurrentVersion.usl ?? '-' }}
            LSL={{ uploadCurrentVersion.lsl ?? '-' }}
            Target={{ uploadCurrentVersion.target ?? '-' }}
            | UCL={{ uploadCurrentVersion.ucl ?? '-' }}
            LCL={{ uploadCurrentVersion.lcl ?? '-' }}
          </div>
          <div class="form-row">
            <div class="form-field">
              <label>测量值 <span class="unit-hint" v-if="uploadParamObj?.unit">({{ uploadParamObj.unit }})</span></label>
              <input v-model.number="uploadData.measuredValue" type="number" step="0.000001" class="form-input" placeholder="输入测量值" />
            </div>
            <div class="form-field">
              <label>批次号</label>
              <input v-model="uploadData.batchId" type="text" class="form-input" placeholder="批次号(可留空)" />
            </div>
          </div>
          <div class="form-row">
            <div class="form-field full">
              <label>填写时间</label>
              <div class="datetime-row">
                <input v-model="uploadData.fillTime" type="datetime-local" class="form-input" />
                <button class="btn-now" @click="uploadData.fillTime = ''">当前时间</button>
              </div>
              <span class="field-hint">不选择则默认为当前时间</span>
            </div>
          </div>
          <div class="form-actions">
            <button class="btn-cancel" @click="showUploadForm = false">取消</button>
            <button class="btn-submit" @click="submitData" :disabled="uploading || !uploadData.paramId || !uploadData.measuredValue && uploadData.measuredValue !== 0">
              {{ uploading ? '提交中...' : '提交' }}
            </button>
          </div>
          <div class="upload-result" v-if="uploadResult">
            <span :class="uploadResult.success ? 'success' : 'error'">{{ uploadResult.message }}</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import SpcControlChart from './SpcControlChart.vue'
import { spcApi, adminApi } from '../utils/api.js'

const props = defineProps({
  isLoggedIn: { type: Boolean, default: false }
})

const emit = defineEmits(['require-login'])

const selectedProduct = ref(null)
const selectedProcess = ref(null)
const selectedParam = ref(null)
const selectedEquipment = ref(null)
const dataLimit = ref(100)

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

const showUploadForm = ref(false)
const uploading = ref(false)
const uploadResult = ref(null)

const uploadProcesses = ref([])
const uploadParams = ref([])
const uploadCurrentVersion = ref(null)

const processCharts = ref([])
const chartRefs = ref({})

const now = new Date()
const pad = (n) => String(n).padStart(2, '0')
const defaultFillTime = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}T${pad(now.getHours())}:${pad(now.getMinutes())}`

const uploadData = ref({
  productId: null,
  processId: null,
  paramId: null,
  equipmentId: null,
  batchId: null,
  paramVersionId: null,
  measuredValue: null,
  fillTime: ''
})

const uploadVersionList = ref([])
const uploadSelectedVersion = ref(null)

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

const uploadParamObj = computed(() => {
  if (!uploadData.value.paramId || !uploadParams.value.length) return null
  return uploadParams.value.find(p => p.id === uploadData.value.paramId) || null
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

onMounted(async () => {
  await loadProducts()
  loadEquipmentList()
})

async function loadProducts() {
  try {
    const res = await spcApi.getProductPage({ current: 1, size: 100 })
    if (res.code === 200) products.value = res.data.records
  } catch (e) { console.error('加载产品失败', e) }
}

async function onProductChange() {
  selectedProcess.value = null
  selectedParam.value = null
  selectedEquipment.value = null
  processes.value = []
  params.value = []
  equipmentList.value = []
  chartData.value = null
  currentVersion.value = null
  processCharts.value = []

  if (!selectedProduct.value) return

  try {
    const processRes = await spcApi.getProcessPage({ current: 1, size: 100 })
    if (processRes.code === 200) processes.value = processRes.data.records
  } catch (e) { console.error('加载工序失败', e) }
}

async function onEquipmentChange() {
  if (selectedParam.value && selectedProduct.value) {
    refreshAllCharts()
  }
}

async function loadEquipmentList() {
  try {
    const res = await adminApi.equipment.getPage({ current: 1, size: 200 })
    if (res.code === 200 && res.data?.records) {
      equipmentList.value = res.data.records.map(eq => ({
        id: eq.id,
        code: eq.equipCode,
        name: eq.equipName || eq.equipCode
      }))
    }
  } catch (e) { console.error('加载设备列表失败', e) }
}

async function onProcessChange() {
  selectedParam.value = null
  params.value = []
  chartData.value = null
  currentVersion.value = null
  processCharts.value = []

  if (!selectedProcess.value || !selectedProduct.value) return

  try {
    const res = await spcApi.getParamPage({ current: 1, size: 100 })
    if (res.code === 200) {
      params.value = res.data.records.filter(p => p.processId === selectedProcess.value)
      if (params.value.length > 0) {
        loadAllProcessCharts()
      }
    }
  } catch (e) { console.error('加载标准(参数)失败', e) }
}

async function loadAllProcessCharts() {
  processCharts.value = []
  if (!selectedProduct.value || !selectedProcess.value || !params.value.length) return

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
      const chartRes = await spcApi.getControlChart({
        paramId: param.id,
        productId: selectedProduct.value,
        limit: dataLimit.value
      })
      if (chartRes.code === 200) chartD = chartRes.data
    } catch (e) { }

    if (chartD && useManualLimits.value && hasManualLimit.value && chartD.limits) {
      const m = manualLimits.value
      if (m.usl != null) chartD.limits.usl = m.usl
      if (m.lsl != null) chartD.limits.lsl = m.lsl
      if (m.target != null) chartD.limits.target = m.target
      if (m.ucl != null) chartD.limits.ucl = m.ucl
      if (m.lcl != null) chartD.limits.lcl = m.lcl
    }

    if (chartD && selectedEquipment.value && chartD.values) {
      const dataPage = await spcApi.getDataPage({
        current: 1, size: dataLimit.value,
        paramId: param.id, productId: selectedProduct.value
      }).catch(() => ({ code: 500 }))
      if (dataPage.code === 200 && dataPage.data?.records) {
        const filtered = dataPage.data.records.filter(d => d.equipmentId === selectedEquipment.value)
          .sort((a, b) => new Date(a.collectTime) - new Date(b.collectTime))
        chartD.values = filtered.map(d => d.measuredValue)
        chartD.timeSeries = filtered.map(d => d.collectTime?.toString() || d.fillTime?.toString() || '')
        chartD.totalPoints = filtered.length
      }
    }

    return { paramId: param.id, paramName: param.paramName, unit: param.unit, version, chartData: chartD }
  })

  try {
    processCharts.value = await Promise.all(chartPromises)
  } catch (e) { console.error('批量加载控制图失败', e) }
}

async function onParamChange() {
  selectedEquipment.value = null
  chartData.value = null
  currentVersion.value = null

  if (!selectedParam.value || !selectedProduct.value) return

  try {
    const res = await spcApi.getParamVersionCurrent({
      paramId: selectedParam.value,
      productId: selectedProduct.value
    })
    if (res.code === 200) currentVersion.value = res.data
  } catch (e) { console.error('加载版本失败', e) }

  loadEquipmentList()
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

async function loadChart() {
  if (!selectedParam.value) return

  try {
    const params = {
      paramId: selectedParam.value,
      limit: dataLimit.value
    }
    if (selectedProduct.value) params.productId = selectedProduct.value
    if (selectedEquipment.value) params.equipmentId = selectedEquipment.value

    const res = await spcApi.getDataByEquipment(params)
    if (res.code === 200) chartData.value = res.data
  } catch (e) { console.error('加载控制图失败', e) }
}

function refreshAllCharts() {
  if (selectedProcess.value) {
    loadAllProcessCharts()
  } else if (selectedParam.value) {
    loadChart()
  }
}

async function calculateStat() {
  if (!currentVersion.value) return

  try {
    await spcApi.calculateStat({ paramVersionId: currentVersion.value.id })
    await loadChart()
  } catch (e) { console.error('计算统计失败', e) }
}

async function onUploadProductChange() {
  uploadData.value.processId = null
  uploadData.value.paramId = null
  uploadProcesses.value = []
  uploadParams.value = []
  uploadCurrentVersion.value = null

  if (!uploadData.value.productId) return

  try {
    const res = await spcApi.getProcessPage({ current: 1, size: 100 })
    if (res.code === 200) uploadProcesses.value = res.data.records
  } catch (e) { console.error('加载工序失败', e) }
}

async function onUploadProcessChange() {
  uploadData.value.paramId = null
  uploadParams.value = []
  uploadCurrentVersion.value = null

  if (!uploadData.value.processId) return

  try {
    const res = await spcApi.getParamPage({ current: 1, size: 100 })
    if (res.code === 200) {
      uploadParams.value = res.data.records.filter(p => p.processId === uploadData.value.processId)
    }
  } catch (e) { console.error('加载标准(参数)失败', e) }
}

async function onUploadParamChange() {
  uploadCurrentVersion.value = null
  uploadSelectedVersion.value = null
  uploadVersionList.value = []

  if (!uploadData.value.paramId || !uploadData.value.productId) return

  try {
    const [verRes, histRes] = await Promise.all([
      spcApi.getParamVersionCurrent({
        paramId: uploadData.value.paramId,
        productId: uploadData.value.productId
      }),
      spcApi.getParamVersionHistory({
        paramId: uploadData.value.paramId,
        productId: uploadData.value.productId
      })
    ])
    if (verRes.code === 200) {
      uploadCurrentVersion.value = verRes.data
      if (verRes.data) uploadSelectedVersion.value = verRes.data.id
    }
    if (histRes.code === 200 && histRes.data) {
      uploadVersionList.value = histRes.data
    }
  } catch (e) { console.error('加载标准版本失败', e) }
}

function onUploadVersionChange() {
  const v = uploadVersionList.value.find(v => v.id === uploadSelectedVersion.value)
  uploadCurrentVersion.value = v || null
}

async function submitData() {
  uploading.value = true
  uploadResult.value = null

  try {
    const payload = { ...uploadData.value }
    if (uploadSelectedVersion.value) {
      payload.paramVersionId = uploadSelectedVersion.value
    }
    if (!payload.fillTime) {
      delete payload.fillTime
    } else {
      payload.fillTime = payload.fillTime.replace('T', ' ') + ':00'
    }

    const res = await spcApi.uploadData(payload)
    if (res.code === 200) {
      uploadResult.value = { success: true, message: '数据提交成功！' }
      uploadData.value = {
        productId: uploadData.value.productId,
        processId: uploadData.value.processId,
        paramId: uploadData.value.paramId,
        equipmentId: uploadData.value.equipmentId,
        batchId: null,
        paramVersionId: null,
        measuredValue: null,
        fillTime: ''
      }
      uploadSelectedVersion.value = null
      uploadCurrentVersion.value = null
      uploadVersionList.value = []
      setTimeout(() => {
        showUploadForm.value = false
        uploadResult.value = null
        loadChart()
      }, 1500)
    } else {
      uploadResult.value = { success: false, message: res.msg || '提交失败' }
    }
  } catch (e) {
    uploadResult.value = { success: false, message: '网络错误' }
  } finally {
    uploading.value = false
  }
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

watch(showVersionHistory, (val) => {
  if (val) loadVersionHistory()
})

watch(() => props.isLoggedIn, (val) => {
  if (val && selectedProduct.value) {
    uploadData.value.productId = selectedProduct.value
  }
  if (val && selectedProcess.value) {
    uploadData.value.processId = selectedProcess.value
  }
  if (val && selectedParam.value) {
    uploadData.value.paramId = selectedParam.value
  }
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

@media (max-width: 600px) {
  .filter-bar { flex-direction: column; align-items: stretch; }
  .stat-grid { grid-template-columns: repeat(2, 1fr); }
  .form-row { flex-direction: column; }
  .chart-mini-stats { flex-wrap: wrap; gap: 8px; }
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
}
.limit-title { font-size: 14px; font-weight: 700; color: var(--text-primary); }
.limit-hint { font-size: 11px; color: var(--text-tertiary); }

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
</style>
