<template>
  <div class="spc-data-import">
    <div class="import-actions-bar">
      <button class="btn-upload" @click="requireAuth(() => showUploadForm = true)" v-if="canWriteData">填写数据</button>
      <button class="btn-import" @click="requireAuth(showImportDialog)" v-if="canWriteData && selectedParam && selectedProduct">导入数据</button>
      <button class="btn-export" @click="exportDataReport" v-if="canExportReport && selectedParam && selectedProduct">导出报告</button>
      <button class="btn-template" @click="downloadTemplate" v-if="canWriteData">下载模板</button>
    </div>

    <!-- 数据导入对话框 -->
    <div class="import-modal" v-if="showImport">
      <div class="modal-content">
        <h3>导入SPC数据</h3>
        <div class="import-form">
          <div class="form-row">
            <div class="form-field">
              <label>产品</label>
              <select v-model.number="importForm.productId" class="form-input" disabled>
                <option :value="selectedProduct">{{ getProductName(selectedProduct) }}</option>
              </select>
            </div>
            <div class="form-field">
              <label>工序 (可选)</label>
              <select v-model.number="importForm.processId" class="form-input">
                <option :value="null">不指定</option>
                <option v-for="p in processes" :key="p.id" :value="p.id">{{ p.processName }}</option>
              </select>
            </div>
          </div>
          <div class="form-row">
            <div class="form-field full">
              <label>设备 (可选)</label>
              <select v-model.number="importForm.equipmentId" class="form-input">
                <option :value="null">不指定</option>
                <option v-for="eq in equipmentList" :key="eq.id" :value="eq.id">{{ eq.name }}</option>
              </select>
            </div>
          </div>
          <div class="form-row">
            <div class="form-field full">
              <label>选择文件 <span class="req">*</span></label>
              <input type="file" ref="fileInput" accept=".csv,.xlsx,.xls" @change="onFileChange"
                     class="file-input" />
              <span class="field-hint">支持 CSV 格式，请先下载模板填写</span>
            </div>
          </div>
          <div class="import-preview" v-if="importPreview.length > 0">
            <strong>预览 (前5条):</strong>
            <table class="preview-table">
              <thead>
                <tr>
                  <th v-for="(header, idx) in importHeaders" :key="idx">{{ header }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in importPreview.slice(0, 5)" :key="idx">
                  <td v-for="(header, hidx) in importHeaders" :key="hidx">{{ row[header] || '-' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="import-result" v-if="importResult">
            <span :class="importResult.success ? 'success' : 'error'">{{ importResult.message }}</span>
          </div>
        </div>
        <div class="form-actions">
          <button class="btn-cancel" @click="showImport = false">取消</button>
          <button class="btn-submit" @click="submitImport" :disabled="!importFile || importing">
            {{ importing ? '导入中...' : '开始导入' }}
          </button>
        </div>
      </div>
    </div>

    <!-- 填写数据表单 -->
    <div class="upload-modal" v-if="showUploadForm">
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

          <div class="form-row" v-if="uploadSelectedParamIds.length > 0">
            <div class="form-field full">
              <label>设备 <span class="req">*</span></label>
              <select v-model.number="uploadData.equipmentId" class="form-input" :class="{ 'input-error': submitAttempted && !uploadData.equipmentId }">
                <option :value="null">请选择设备 (必选)</option>
                <option v-for="eq in uploadEquipmentList" :key="eq.id" :value="eq.id">{{ eq.name }}</option>
              </select>
              <span class="field-error" v-if="submitAttempted && !uploadData.equipmentId">请选择设备</span>
            </div>
          </div>

          <div class="multi-param-section" v-if="uploadParams.length > 0">
            <label class="multi-param-label">选择工艺参数 (可多选) <span class="req">*</span></label>
            <div class="param-checkbox-grid">
              <label v-for="p in uploadParams" :key="p.id"
                     class="param-checkbox-item"
                     :class="{ checked: uploadSelectedParamIds.includes(p.id) }">
                <input type="checkbox" :value="p.id" v-model="uploadSelectedParamIds"
                       @change="onMultiParamChange(p)" />
                <span class="param-info">
                  <strong>{{ p.paramName }}</strong>
                  <small>{{ p.paramCode }} <span v-if="p.unit">[{{ p.unit }}]</span></small>
                </span>
              </label>
            </div>
          </div>

          <div class="multi-value-section" v-if="uploadSelectedParamIds.length > 0">
            <div class="batch-table-header">
              <label class="multi-param-label">测量数据 <span class="req">*</span></label>
              <div class="row-actions">
                <button type="button" class="btn-add-row" @click="addRow" :disabled="uploadRows.length >= MAX_ROWS" title="添加一行">+ 添加一行</button>
                <span class="row-count-hint">{{ uploadRows.length }}/{{ MAX_ROWS }} 行</span>
              </div>
            </div>
            <div class="batch-table-wrap">
              <table class="batch-table">
                <thead>
                  <tr>
                    <th class="col-row-num">#</th>
                    <th v-for="pid in uploadSelectedParamIds" :key="'h-' + pid">
                      <div class="th-param-main">
                        {{ getParamName(pid) }}
                        <span v-if="getParamUnit(pid)" class="th-unit">[{{ getParamUnit(pid) }}]</span>
                        <span class="th-type-badge" :class="'type-' + (getParamDataType(pid) || 'continuous')">{{ getParamDataTypeLabel(pid) }}</span>
                      </div>
                      <div class="th-limit-hint" v-if="uploadParamVersionMap[pid]">
                        <span class="lim-spec">±{{ uploadParamVersionMap[pid].target ?? '-' }}</span>
                        <span class="lim-range">[{{ uploadParamVersionMap[pid].lsl ?? '?' }} ~ {{ uploadParamVersionMap[pid].usl ?? '?' }}]</span>
                      </div>
                      <div class="th-limit-hint th-loading" v-else-if="uploadLoadingVersions.has(pid)">加载标准中...</div>
                    </th>
                    <th class="col-action"></th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(row, ri) in uploadRows" :key="'r-' + ri" :class="{ 'row-empty': !hasRowValue(ri) }">
                    <td class="cell-row-num">{{ ri + 1 }}</td>
                    <td v-for="pid in uploadSelectedParamIds" :key="'c-' + ri + '-' + pid" class="cell-value"
                        :class="getCellLimitClass(ri, pid)">
                      <div class="cell-input-wrap">
                        <input :model-value="getRowValue(ri, pid)"
                               @input="setRowValue(ri, pid, $event.target.valueAsNumber || null)"
                               type="number" :step="getInputStep(pid)"
                               class="form-input value-input cell-input"
                               :placeholder="'.' + '0'.repeat(getParamDecimalPlaces(pid) || 2)"
                               :class="{ 'input-empty': submitAttempted && (getRowValue(ri, pid) === null || getRowValue(ri, pid) === undefined || getRowValue(ri, pid) === ''),
                                         'input-oos': isValueOOS(ri, pid),
                                         'input-warning': isValueWarning(ri, pid) }" />
                        <span class="cell-status-dot" v-if="getRowValue(ri, pid) != null && getRowValue(ri, pid) !== ''"
                              :class="isValueOOS(ri, pid) ? 'dot-danger' : (isValueWarning(ri, pid) ? 'dot-warn' : 'dot-ok')"
                              :title="getCellStatusText(ri, pid)"></span>
                      </div>
                      <div class="cell-sample-wrap" v-if="getParamDataType(pid) === 'COUNT'">
                        <input :model-value="getSampleSize(ri, pid)"
                               @input="setSampleSize(ri, pid, $event.target.valueAsNumber || null)"
                               type="number" min="1" step="1"
                               class="form-input sample-input cell-input"
                               placeholder="样本量n" />
                      </div>
                    </td>
                    <td class="cell-action">
                      <button type="button" class="btn-del-row" @click="removeRow(ri)" :disabled="uploadRows.length <= 1" title="删除此行">✕</button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div class="param-limit-bar enhanced" v-if="uploadSelectedParamIds.some(_pid => uploadParamVersionMap[_pid])">
              <div v-for="pid in uploadSelectedParamIds" :key="'lim-' + pid" class="limit-card" v-if="uploadParamVersionMap[pid]">
                <div class="limit-card-header">
                  <strong>{{ getParamName(pid) }}</strong>
                  <span class="limit-card-type" :class="'type-' + (getParamDataType(pid) || 'continuous')">{{ getParamDataTypeLabel(pid) }}</span>
                </div>
                <div class="limit-card-body">
                  <div class="limit-item spec-limits">
                    <span class="li-label">规格限</span>
                    <span class="li-values">
                      <em>LSL</em><strong>{{ uploadParamVersionMap[pid].lsl ?? '-' }}</strong>
                      <em>Tgt</em><strong>{{ uploadParamVersionMap[pid].target ?? '-' }}</strong>
                      <em>USL</em><strong>{{ uploadParamVersionMap[pid].usl ?? '-' }}</strong>
                    </span>
                  </div>
                  <div class="limit-item ctrl-limits">
                    <span class="li-label">控制限</span>
                    <span class="li-values">
                      <em>LCL</em><strong>{{ uploadParamVersionMap[pid].lcl ?? '-' }}</strong>
                      <em>CL</em><strong>{{ uploadParamVersionMap[pid].cl ?? '-' }}</strong>
                      <em>UCL</em><strong>{{ uploadParamVersionMap[pid].ucl ?? '-' }}</strong>
                    </span>
                  </div>
                </div>
              </div>
            </div>
            <div class="param-limit-info no-version-hint" v-if="uploadSelectedParamIds.length > 0 && !uploadSelectedParamIds.some(_p => uploadParamVersionMap[_p]) && !Array.from(uploadLoadingVersions.value).some(_p2 => uploadSelectedParamIds.includes(_p2))">
              暂无版本标准，提交时将自动创建
            </div>
          </div>

          <div class="form-row">
            <div class="form-field">
              <label>批次号</label>
              <div class="batch-input-group">
                <input v-model="uploadData.batchId" type="text" class="form-input" placeholder="批次号(可留空)" ref="batchInputRef" />
                <button class="btn-scan" @click="toggleScanner" :title="scannerActive ? '关闭扫描' : '扫码录入'">
                  {{ scannerActive ? '✕' : '📷' }}
                </button>
              </div>
            </div>
          </div>
          <div class="scanner-container" v-if="scannerActive">
            <div class="scanner-header">
              <span>扫码录入批次号</span>
              <button class="btn-close-scanner" @click="toggleScanner">✕ 关闭</button>
            </div>
            <div id="qr-reader" class="qr-reader"></div>
            <div class="scanner-hint">将条形码/二维码对准摄像头</div>
          </div>
          <div class="form-row">
            <div class="form-field full">
              <label>填写时间</label>
              <div class="datetime-row">
                <input v-model="uploadData.fillTime" type="datetime-local" class="form-input" />
                <button class="btn-now" @click="setNowTime">当前时间</button>
              </div>
              <span class="field-hint">不选择则默认为当前时间</span>
            </div>
          </div>
          <div class="form-actions">
            <button class="btn-cancel" @click="showUploadForm = false">取消</button>
            <button class="btn-submit" @click="submitData" :disabled="uploading || uploadSelectedParamIds.length === 0 || !hasAnyValue">
              {{ uploading ? '提交中...' : `提交 (${uploadRows.length}行 × ${uploadSelectedParamIds.length}项)` }}
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
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts'
import { spcApi, getToken, getUser } from '@/utils/api'
import { decryptResponse, isEncryptionEnabled } from '@/utils/crypto.js'

const props = defineProps({
  isLoggedIn: Boolean,
  selectedProduct: Number,
  selectedProcess: Number,
  selectedParam: Number,
  products: Array,
  processes: Array,
  equipmentList: Array,
  params: Array,
  dataLimit: Number,
  timeRange: String,
  userInfo: { type: Object, default: null }
})

const emit = defineEmits(['require-login', 'refresh', 'data-imported'])

const isViewer = computed(() => ((props.userInfo?.role || getUser()?.role || '').toUpperCase() === 'VIEWER'))
const canWriteData = computed(() => props.isLoggedIn && !isViewer.value)
const canExportReport = computed(() => props.isLoggedIn && !isViewer.value)

const showImport = ref(false)
const importForm = ref({ productId: null, processId: null, equipmentId: null })
const importFile = ref(null)
const importing = ref(false)
const importResult = ref(null)
const importPreview = ref([])
const importHeaders = ref([])
const fileInput = ref(null)

const showUploadForm = ref(false)
const uploading = ref(false)
const uploadResult = ref(null)
const submitAttempted = ref(false)

const uploadProcesses = ref([])
const uploadParams = ref([])
const uploadEquipmentList = ref([])

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

const scannerActive = ref(false)
let html5QrCodeScanner = null
const batchInputRef = ref(null)

const uploadSelectedParamIds = ref([])
const uploadRows = ref([{}])
const uploadSampleSizes = ref([{}])
const uploadParamVersionMap = reactive({})
const uploadLoadingVersions = ref(new Set())

const MAX_ROWS = 5

function getRowValues(rowIndex) { return uploadRows.value[rowIndex] || {} }
function setRowValue(rowIndex, pid, val) {
  if (!uploadRows.value[rowIndex]) uploadRows.value[rowIndex] = {}
  uploadRows.value[rowIndex][pid] = val
}
function getRowValue(rowIndex, pid) { return (uploadRows.value[rowIndex] || {})[pid] }
function getSampleSize(rowIndex, pid) { return (uploadSampleSizes.value[rowIndex] || {})[pid] }
function setSampleSize(rowIndex, pid, val) {
  if (!uploadSampleSizes.value[rowIndex]) uploadSampleSizes.value[rowIndex] = {}
  uploadSampleSizes.value[rowIndex][pid] = val
}

const hasAnyValue = computed(() => {
  return uploadRows.value.some(row => row && Object.values(row).some(v => v !== null && v !== undefined && v !== ''))
})

function addRow() {
  if (uploadRows.value.length < MAX_ROWS) {
    uploadRows.value.push({})
    uploadSampleSizes.value.push({})
  }
}

function removeRow(index) {
  if (uploadRows.value.length > 1) {
    uploadRows.value.splice(index, 1)
    uploadSampleSizes.value.splice(index, 1)
  }
}

function hasRowValue(rowIndex) {
  const row = uploadRows.value[rowIndex]
  return row && Object.values(row).some(v => v !== null && v !== undefined && v !== '')
}

function requireAuth(fn) {
  if (!props.isLoggedIn) {
    emit('require-login')
    return
  }
  fn()
}

function getProductName(id) {
  const p = props.products?.find(x => x.id === id)
  return p ? p.productName : '-'
}

function getParamName(id) {
  const p = uploadParams.value.find(p => p.id === id)
  return p ? p.paramName : '-'
}

function getParamUnit(id) {
  const p = uploadParams.value.find(p => p.id === id)
  return p ? (p.unit || '') : ''
}

function getParamDataType(id) {
  const p = uploadParams.value.find(p => p.id === id)
  return p ? (p.dataType || 'CONTINUOUS') : 'CONTINUOUS'
}

function getParamDataTypeLabel(id) {
  const t = getParamDataType(id)
  const map = { CONTINUOUS: '连续', DISCRETE: '离散', COUNT: '计数' }
  return map[t] || t
}

function getParamDecimalPlaces(id) {
  const p = uploadParams.value.find(p => p.id === id)
  return p ? (p.decimalPlaces || 2) : 2
}

function getInputStep(pid) {
  const dp = getParamDecimalPlaces(pid)
  if (dp <= 0) return '1'
  return Number('0.' + '0'.repeat(dp - 1) + '1').toFixed(dp)
}

function isValueOOS(ri, pid) {
  const val = getRowValue(ri, pid)
  if (val == null || val === '') return false
  const ver = uploadParamVersionMap[pid]
  if (!ver) return false
  const numVal = Number(val)
  if (ver.usl != null && numVal > Number(ver.usl)) return true
  if (ver.lsl != null && numVal < Number(ver.lsl)) return true
  return false
}

function isValueWarning(ri, pid) {
  if (isValueOOS(ri, pid)) return false
  const val = getRowValue(ri, pid)
  if (val == null || val === '') return false
  const ver = uploadParamVersionMap[pid]
  if (!ver || !ver.ucl || !ver.lcl) return false
  const numVal = Number(val)
  const ucl = Number(ver.ucl), lcl = Number(ver.lcl)
  const range = ucl - lcl
  if (range <= 0) return false
  const warnInner = range * 0.1
  return numVal > ucl - warnInner || numVal < lcl + warnInner
}

function getCellLimitClass(ri, pid) {
  if (isValueOOS(ri, pid)) return 'cell-oos'
  if (isValueWarning(ri, pid)) return 'cell-warning'
  return ''
}

function getCellStatusText(ri, pid) {
  const val = getRowValue(ri, pid)
  if (val == null || val === '') return ''
  const ver = uploadParamVersionMap[pid]
  if (!ver) return '暂无标准'
  const numVal = Number(val)
  if (ver.usl != null && numVal > Number(ver.usl)) return `⚠ 超规格上限 USL=${ver.usl}`
  if (ver.lsl != null && numVal < Number(ver.lsl)) return `⚠ 超规格下限 LSL=${ver.lsl}`
  if (ver.ucl != null && numVal > Number(ver.ucl)) return `接近控制上限 UCL=${ver.ucl}`
  if (ver.lcl != null && numVal < Number(ver.lcl)) return `接近控制下限 LCL=${ver.lcl}`
  return '✓ 在规格范围内'
}

function showImportDialog() {
  importForm.value = {
    productId: props.selectedProduct,
    processId: props.selectedProcess || null,
    equipmentId: null
  }
  importFile.value = null
  importResult.value = null
  importPreview.value = []
  importHeaders.value = []
  showImport.value = true
}

function onFileChange(event) {
  const file = event.target.files[0]
  if (!file) return

  importFile.value = file
  importResult.value = null

  if (!file.name.endsWith('.csv')) {
    parseCSVFile(file)
  } else {
    parseCSVFile(file)
  }
}

function parseCSVFile(file) {
  const reader = new FileReader()
  reader.onload = (e) => {
    try {
      const text = e.target.result
      const lines = text.split('\n').filter(l => l.trim())

      if (lines.length < 2) {
        importResult.value = { success: false, message: '文件格式错误或无数据' }
        return
      }

      const headers = lines[0].split(',').map(h => h.trim().replace(/^\uFEFF/, ''))
      importHeaders.value = headers

      const data = []
      for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',')
        const row = {}
        headers.forEach((h, idx) => {
          row[h] = values[idx] ? values[idx].trim() : ''
        })
        data.push(row)
      }
      importPreview.value = data

    } catch (err) {
      importResult.value = { success: false, message: '解析文件失败: ' + err.message }
    }
  }
  reader.readAsText(file, 'UTF-8')
}

async function submitImport() {
  if (!importFile.value) return

  if (!props.selectedParam || props.selectedParam <= 0) {
    importResult.value = { success: false, message: '请先选择工艺参数后再导入数据' }
    return
  }
  if (!importForm.value.productId || importForm.value.productId <= 0) {
    importResult.value = { success: false, message: '请先选择产品后再导入数据' }
    return
  }

  importing.value = true
  importResult.value = null

  try {
    const formData = new FormData()
    formData.append('file', importFile.value)
    formData.append('paramId', String(props.selectedParam))
    formData.append('productId', String(importForm.value.productId))
    if (importForm.value.processId) formData.append('processId', String(importForm.value.processId))
    if (importForm.value.equipmentId) formData.append('equipmentId', String(importForm.value.equipmentId))

    const res = await fetch('/api/spc/data/import', {
      method: 'POST',
      body: formData,
      headers: {
        'Authorization': `Bearer ${getToken()}`,
        'X-Encrypted': 'true'
      }
    })
    let result = await res.json()
    // 文件上传走 FormData 绕过了 api.js, 需手动解密响应
    if (isEncryptionEnabled() && result.encrypted && result.data) {
      result = decryptResponse(result)
    }

    if (result.code === 200) {
      importResult.value = { success: true, message: `导入完成: 成功${result.data.successCount}条, 失败${result.data.failCount}条` }
      setTimeout(() => {
        showImport.value = false
        emit('data-imported')
        emit('refresh')
      }, 1500)
    } else {
      importResult.value = { success: false, message: result.msg || '导入失败' }
    }
  } catch (e) {
    importResult.value = { success: false, message: '网络错误' }
  } finally {
    importing.value = false
  }
}

async function downloadTemplate() {
  try {
    const res = await fetch(spcApi.downloadTemplate(), {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${getToken()}`,
        'X-Encrypted': 'true'
      }
    })

    if (!res.ok) throw new Error('下载模板失败')

    const blob = await res.blob()
    const contentDisposition = res.headers.get('Content-Disposition')
    let filename = 'SPC数据导入模板.xlsx'
    if (contentDisposition) {
      const match = contentDisposition.match(/filename\*?=(?:UTF-8''|"?)([^";]+)/i)
      if (match) filename = decodeURIComponent(match[1])
    }

    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    link.click()
    window.URL.revokeObjectURL(url)
  } catch (e) {
    console.error('下载模板失败:', e)
    alert('下载模板失败，请重试')
  }
}

async function exportDataReport() {
  if (!props.selectedParam || !props.selectedProduct) {
    alert('请先选择产品和工艺参数')
    return
  }

  const exportBtn = document.querySelector('.btn-export')
  const originalText = exportBtn ? exportBtn.textContent : ''
  if (exportBtn) {
    exportBtn.disabled = true
    exportBtn.textContent = '生成报告中...'
  }

  try {
    // 1. 拉取控制图数据(含统计量、限值、原始 values)
    const limit = props.dataLimit || 500
    const chartParams = {
      paramId: props.selectedParam,
      productId: props.selectedProduct,
      limit
    }
    if (props.selectedEquipment) chartParams.equipmentId = props.selectedEquipment
    if (props.timeRange) {
      const endTime = new Date()
      const startTime = new Date()
      startTime.setDate(startTime.getDate() - parseInt(props.timeRange))
      chartParams.startTime = formatDateTime(startTime)
      chartParams.endTime = formatDateTime(endTime)
    }

    const chartRes = await spcApi.getControlChart(chartParams)
    const chartData = chartRes?.data || {}
    const values = (chartData.values || []).map(v => Number(v))
    const timeSeries = chartData.timeSeries || []
    const limits = chartData.limits || {}
    const chartType = (chartData.chartType || 'I_MR').toUpperCase()
    const subgroupSize = chartData.subgroupSize || 1

    if (!values.length) {
      alert('所选范围内无 SPC 数据')
      return
    }

    // 2. 用 ECharts 在隐藏 div 渲染各图并截图
    const charts = {}
    charts.controlChartImr = renderControlChart(values, timeSeries, limits, chartType)
    if (chartType === 'XBAR_R' || chartType === 'XBAR_S') {
      const xbarR = renderXbarRChart(values, subgroupSize, limits)
      charts.controlChartXbar = xbarR.xbar
      charts.controlChartR = xbarR.r
    }
    charts.capabilityHistogram = renderHistogram(values, limits)
    charts.normalProbabilityPlot = renderNormalProbability(values)
    charts.trendChart = renderTrendChart(values, timeSeries)

    // 调试: 打印各图表 base64 长度,便于排查
    console.log('[Export Report] charts 状态:', {
      controlChartImr: charts.controlChartImr ? `OK(${charts.controlChartImr.length} chars)` : 'NULL',
      controlChartXbar: charts.controlChartXbar ? `OK(${charts.controlChartXbar.length} chars)` : 'NULL',
      controlChartR: charts.controlChartR ? `OK(${charts.controlChartR.length} chars)` : 'NULL',
      capabilityHistogram: charts.capabilityHistogram ? `OK(${charts.capabilityHistogram.length} chars)` : 'NULL',
      normalProbabilityPlot: charts.normalProbabilityPlot ? `OK(${charts.normalProbabilityPlot.length} chars)` : 'NULL',
      trendChart: charts.trendChart ? `OK(${charts.trendChart.length} chars)` : 'NULL',
      valuesCount: values.length
    })

    // 3. 上送截图 + 参数,生成 PDF
    const formData = new FormData()
    formData.append('paramId', String(props.selectedParam))
    formData.append('productId', String(props.selectedProduct))
    formData.append('limit', String(limit))
    if (props.selectedEquipment) formData.append('equipmentId', String(props.selectedEquipment))
    if (chartParams.startTime) formData.append('startTime', chartParams.startTime)
    if (chartParams.endTime) formData.append('endTime', chartParams.endTime)
    if (charts.controlChartImr) formData.append('controlChartImr', charts.controlChartImr)
    if (charts.controlChartXbar) formData.append('controlChartXbar', charts.controlChartXbar)
    if (charts.controlChartR) formData.append('controlChartR', charts.controlChartR)
    if (charts.capabilityHistogram) formData.append('capabilityHistogram', charts.capabilityHistogram)
    if (charts.normalProbabilityPlot) formData.append('normalProbabilityPlot', charts.normalProbabilityPlot)
    if (charts.trendChart) formData.append('trendChart', charts.trendChart)

    const res = await fetch('/api/spc/data/export/pdf-report', {
      method: 'POST',
      body: formData,
      headers: {
        'Authorization': `Bearer ${getToken()}`,
        'X-Encrypted': 'true'
      }
    })

    if (!res.ok) {
      const errText = await res.text().catch(() => '')
      throw new Error(`导出失败: ${res.status} ${errText}`)
    }

    const blob = await res.blob()
    const contentDisposition = res.headers.get('Content-Disposition')
    let filename = `SPC统计过程控制分析报告_${formatDate(new Date())}.pdf`
    if (contentDisposition) {
      const match = contentDisposition.match(/filename\*?=(?:UTF-8''|"?)([^";]+)/i)
      if (match) filename = decodeURIComponent(match[1])
    }

    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    link.click()
    window.URL.revokeObjectURL(url)
  } catch (e) {
    console.error('SPC 报告导出失败:', e)
    alert('SPC 报告导出失败: ' + (e.message || '请重试'))
  } finally {
    if (exportBtn) {
      exportBtn.disabled = false
      exportBtn.textContent = originalText
    }
  }
}

/** 在内存中创建隐藏 div + ECharts 实例,渲染后返回 base64 截图,最后销毁 */
function renderHiddenChart(option, width = 800, height = 400) {
  const div = document.createElement('div')
  div.style.width = width + 'px'
  div.style.height = height + 'px'
  div.style.position = 'fixed'
  div.style.left = '0'
  div.style.top = '0'
  div.style.zIndex = '-1'
  div.style.visibility = 'hidden'
  div.style.pointerEvents = 'none'
  document.body.appendChild(div)
  try {
    // 关闭动画，确保 setOption 后 canvas 立即完成渲染
    option.animation = false
    const inst = echarts.init(div, null, { width, height, renderer: 'canvas' })
    inst.setOption(option, true)
    // 强制刷新一次，确保 canvas 绘制完成
    inst.resize()
    const dataUrl = inst.getDataURL({
      type: 'png',
      pixelRatio: 1.5,
      backgroundColor: '#fff'
    })
    inst.dispose()
    return dataUrl
  } finally {
    document.body.removeChild(div)
  }
}

/** 单值-移动极差(I-MR)控制图 */
function renderControlChart(values, timeSeries, limits, chartType) {
  if (!values.length) return null
  const mrValues = values.slice(1).map((v, i) => Math.abs(v - values[i]))
  // X 轴用数据点序号 (1, 2, ..., n)，与网页版 SpcControlChart 保持一致
  const categories = values.map((_, i) => i + 1)

  const markLines = []
  if (limits.ucl != null) markLines.push({ yAxis: Number(limits.ucl), name: 'UCL' })
  if (limits.cl != null) markLines.push({ yAxis: Number(limits.cl), name: 'CL' })
  if (limits.lcl != null) markLines.push({ yAxis: Number(limits.lcl), name: 'LCL' })
  if (limits.usl != null) markLines.push({ yAxis: Number(limits.usl), name: 'USL', lineStyle: { color: '#fa541c' } })
  if (limits.lsl != null) markLines.push({ yAxis: Number(limits.lsl), name: 'LSL', lineStyle: { color: '#fa541c' } })

  const option = {
    title: { text: '单值-移动极差控制图 (I-MR)', left: 'center', textStyle: { fontSize: 14 } },
    grid: [
      { left: '10%', right: '5%', top: '15%', height: '35%' },
      { left: '10%', right: '5%', top: '60%', height: '30%' }
    ],
    xAxis: [
      { type: 'category', data: categories, gridIndex: 0, name: '数据点序号', nameTextStyle: { fontSize: 10 }, axisLabel: { show: false } },
      { type: 'category', data: categories, gridIndex: 1, name: '数据点序号', nameTextStyle: { fontSize: 10 }, axisLabel: { fontSize: 9 } }
    ],
    yAxis: [
      { type: 'value', gridIndex: 0, name: '单值', nameTextStyle: { fontSize: 11 }, scale: true },
      { type: 'value', gridIndex: 1, name: 'MR', nameTextStyle: { fontSize: 11 }, scale: true }
    ],
    series: [
      {
        name: '单值', type: 'line', xAxisIndex: 0, yAxisIndex: 0,
        data: values, symbol: 'circle', symbolSize: 6,
        itemStyle: { color: '#1890ff' },
        markLine: {
          symbol: 'none', silent: true,
          data: markLines.map(m => ({ yAxis: m.yAxis, name: m.name, lineStyle: m.lineStyle || { color: '#888', type: 'dashed' }, label: { formatter: m.name, fontSize: 9 } }))
        }
      },
      {
        name: '移动极差', type: 'line', xAxisIndex: 1, yAxisIndex: 1,
        data: mrValues, symbol: 'circle', symbolSize: 5,
        itemStyle: { color: '#52c41a' }
      }
    ],
    tooltip: { show: false }
  }
  return renderHiddenChart(option)
}

/** X-bar 和 R 控制图(仅 XBAR_R/XBAR_S 时渲染) */
function renderXbarRChart(values, subgroupSize, limits) {
  if (!values.length || subgroupSize < 2) return { xbar: null, r: null }
  const numGroups = Math.floor(values.length / subgroupSize)
  if (numGroups < 2) return { xbar: null, r: null }

  const xbarData = [], rData = [], categories = []
  for (let i = 0; i < numGroups; i++) {
    const grp = values.slice(i * subgroupSize, (i + 1) * subgroupSize)
    const sum = grp.reduce((a, b) => a + b, 0)
    const mean = sum / grp.length
    const range = Math.max(...grp) - Math.min(...grp)
    xbarData.push(Number(mean.toFixed(4)))
    rData.push(Number(range.toFixed(4)))
    categories.push(i + 1)
  }

  const xbarOption = {
    title: { text: 'X-bar 控制图 - 过程均值监控', left: 'center', textStyle: { fontSize: 14 } },
    grid: { left: '10%', right: '5%', top: '15%', bottom: '15%' },
    xAxis: { type: 'category', data: categories, name: '子组序号', nameTextStyle: { fontSize: 11 }, axisLabel: { fontSize: 10 } },
    yAxis: { type: 'value', name: 'X̄', nameTextStyle: { fontSize: 11 }, scale: true },
    series: [{
      name: 'X̄', type: 'line', data: xbarData, symbol: 'circle', symbolSize: 6,
      itemStyle: { color: '#1890ff' }
    }],
    tooltip: { show: false }
  }
  const rOption = {
    title: { text: 'R 控制图 - 过程变异监控', left: 'center', textStyle: { fontSize: 14 } },
    grid: { left: '10%', right: '5%', top: '15%', bottom: '15%' },
    xAxis: { type: 'category', data: categories, name: '子组序号', nameTextStyle: { fontSize: 11 }, axisLabel: { fontSize: 10 } },
    yAxis: { type: 'value', name: 'R', nameTextStyle: { fontSize: 11 }, scale: true },
    series: [{
      name: 'R', type: 'line', data: rData, symbol: 'circle', symbolSize: 6,
      itemStyle: { color: '#52c41a' }
    }],
    tooltip: { show: false }
  }
  return {
    xbar: renderHiddenChart(xbarOption),
    r: renderHiddenChart(rOption)
  }
}

/** 直方图 + 正态分布曲线 + 规格限 */
function renderHistogram(values, limits) {
  if (!values.length) return null
  const sorted = [...values].sort((a, b) => a - b)
  const min = sorted[0], max = sorted[sorted.length - 1]
  const binCount = Math.min(20, Math.max(5, Math.ceil(Math.sqrt(values.length))))
  const binWidth = (max - min) / binCount || 1
  const bins = new Array(binCount).fill(0)
  for (const v of values) {
    let idx = Math.floor((v - min) / binWidth)
    if (idx >= binCount) idx = binCount - 1
    if (idx < 0) idx = 0
    bins[idx]++
  }
  const binLabels = []
  for (let i = 0; i < binCount; i++) {
    binLabels.push((min + i * binWidth).toFixed(2))
  }

  // 计算正态曲线(基于均值+样本标准差)
  const n = values.length
  const mean = values.reduce((a, b) => a + b, 0) / n
  const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1 || 1)
  const sigma = Math.sqrt(variance)
  const curveData = []
  const steps = 100
  const xMin = min, xMax = max
  for (let i = 0; i <= steps; i++) {
    const x = xMin + (xMax - xMin) * i / steps
    const y = (1 / (sigma * Math.sqrt(2 * Math.PI))) * Math.exp(-((x - mean) ** 2) / (2 * sigma ** 2)) * n * binWidth
    curveData.push([Number(x.toFixed(4)), Number(y.toFixed(4))])
  }

  const markLines = []
  if (limits.usl != null) markLines.push({ xAxis: Number(limits.usl), name: 'USL', lineStyle: { color: '#fa541c' } })
  if (limits.lsl != null) markLines.push({ xAxis: Number(limits.lsl), name: 'LSL', lineStyle: { color: '#fa541c' } })
  if (limits.target != null) markLines.push({ xAxis: Number(limits.target), name: 'T', lineStyle: { color: '#888' } })

  const option = {
    title: { text: '直方图 - 数据分布分析', left: 'center', textStyle: { fontSize: 14 } },
    grid: { left: '8%', right: '5%', top: '15%', bottom: '12%' },
    xAxis: { type: 'category', data: binLabels, name: '测量值' },
    yAxis: { type: 'value', name: '频次' },
    series: [
      {
        name: '频次', type: 'bar', data: bins,
        itemStyle: { color: '#5470c6' },
        markLine: {
          symbol: 'none', silent: true,
          data: markLines.map(m => ({ xAxis: m.xAxis, name: m.name, lineStyle: m.lineStyle || { type: 'dashed' }, label: { formatter: m.name, fontSize: 9 } }))
        }
      },
      {
        name: '正态曲线', type: 'line', data: curveData,
        smooth: true, symbol: 'none',
        itemStyle: { color: '#ee6666' },
        lineStyle: { width: 2 }
      }
    ],
    tooltip: { show: false }
  }
  return renderHiddenChart(option)
}

/** 正态概率图(Q-Q 图) */
function renderNormalProbability(values) {
  if (!values.length) return null
  const sorted = [...values].sort((a, b) => a - b)
  const n = sorted.length
  // 经验分位数 vs 理论分位数
  const points = []
  const mean = sorted.reduce((a, b) => a + b, 0) / n
  const variance = sorted.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1 || 1)
  const sigma = Math.sqrt(variance)
  for (let i = 0; i < n; i++) {
    const p = (i + 0.5) / n
    const z = invNorm(p)
    points.push([Number(z.toFixed(4)), Number(sorted[i].toFixed(4))])
  }
  // 参考线
  const refLine = [
    [-3, mean - 3 * sigma],
    [3, mean + 3 * sigma]
  ]
  const option = {
    title: { text: '正态概率图 - 正态性检验', left: 'center', textStyle: { fontSize: 14 } },
    grid: { left: '8%', right: '5%', top: '15%', bottom: '12%' },
    xAxis: { type: 'value', name: '理论分位数 Z' },
    yAxis: { type: 'value', name: '实测值' },
    series: [
      {
        name: '数据点', type: 'scatter', data: points,
        symbolSize: 5, itemStyle: { color: '#1890ff' }
      },
      {
        name: '参考线', type: 'line', data: refLine,
        symbol: 'none', lineStyle: { color: '#fa541c', type: 'dashed', width: 2 }
      }
    ],
    tooltip: { show: false }
  }
  return renderHiddenChart(option)
}

/** 趋势图 */
function renderTrendChart(values, timeSeries) {
  if (!values.length) return null
  // X 轴用数据点序号，与网页版 SpcControlChart 保持一致
  const categories = values.map((_, i) => i + 1)
  const option = {
    title: { text: '趋势图 - 数据趋势分析', left: 'center', textStyle: { fontSize: 14 } },
    grid: { left: '10%', right: '5%', top: '15%', bottom: '15%' },
    xAxis: { type: 'category', data: categories, name: '数据点序号', nameTextStyle: { fontSize: 11 }, axisLabel: { fontSize: 10 } },
    yAxis: { type: 'value', name: '测量值', nameTextStyle: { fontSize: 11 }, scale: true },
    series: [{
      name: '测量值', type: 'line', data: values,
      symbol: 'circle', symbolSize: 5,
      itemStyle: { color: '#1890ff' },
      lineStyle: { width: 2 }
    }],
    tooltip: { show: false }
  }
  return renderHiddenChart(option)
}

/** 标准正态分布反函数(用于 Q-Q 图理论分位数) */
function invNorm(p) {
  if (p <= 0) return -3
  if (p >= 1) return 3
  const a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
    1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
  const b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
    6.680131188771972e+01, -1.328068155288572e+01]
  const c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
    -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
  const d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00]
  const pLow = 0.02425, pHigh = 1 - pLow
  let q, r
  if (p < pLow) {
    q = Math.sqrt(-2 * Math.log(p))
    return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
  } else if (p <= pHigh) {
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5]) * q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
  }
  q = Math.sqrt(-2 * Math.log(1 - p))
  return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
}

function formatDate(d) {
  return d.toISOString().slice(0, 10)
}

function formatDateTime(d) {
  const pad = n => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

async function onUploadProductChange() {
  uploadData.value.processId = null
  uploadData.value.paramId = null
  uploadProcesses.value = []
  uploadParams.value = []
  uploadEquipmentList.value = []

  if (!uploadData.value.productId) return

  try {
    const res = await spcApi.getProcessPage({ current: 1, size: 100 })
    if (res.code === 200) uploadProcesses.value = res.data.records
  } catch (e) {
    console.error('加载工序失败', e)
  }
}

async function onUploadProcessChange() {
  uploadData.value.paramId = null
  uploadParams.value = []
  uploadSelectedParamIds.value = []
  uploadRows.value = [{}]
  uploadSampleSizes.value = [{}]
  uploadEquipmentList.value = []
  Object.keys(uploadParamVersionMap).forEach(k => delete uploadParamVersionMap[k])
  uploadLoadingVersions.value = new Set()

  if (!uploadData.value.processId) return

  try {
    const res = await spcApi.getParamPage({ current: 1, size: 100 })
    if (res.code === 200) uploadParams.value = res.data.records.filter(p => p.processId === uploadData.value.processId)
  } catch (e) {
    console.error('加载参数失败', e)
  }

  try {
    const equipRes = await spcApi.getProcessEquipment(uploadData.value.processId)
    if (equipRes.code === 200 && equipRes.data) {
      uploadEquipmentList.value = equipRes.data.map(eq => ({
        id: eq.id,
        name: eq.equipName || eq.equipCode,
        code: eq.equipCode
      }))
    }
  } catch (e) {
    console.error('加载设备失败', e)
  }
}

function onMultiParamChange(param) {
  const isChecked = uploadSelectedParamIds.value.includes(param.id)

  if (!isChecked) {
    delete uploadParamVersionMap[param.id]
    for (const row of uploadRows.value) {
      if (row) delete row[param.id]
    }
    for (const row of uploadSampleSizes.value) {
      if (row) delete row[param.id]
    }
    return
  }

  loadParamVersion(param.id)
}

async function loadParamVersion(paramId) {
  if (!uploadData.value.productId) return
  uploadLoadingVersions.value = new Set([...uploadLoadingVersions.value, paramId])

  try {
    const res = await spcApi.getParamVersionCurrent({
      paramId: paramId,
      productId: uploadData.value.productId
    })
    if (res.code === 200 && res.data) {
      uploadParamVersionMap[paramId] = res.data
    }
  } catch (e) {
    console.error('加载版本失败', e)
  } finally {
    const s = new Set(uploadLoadingVersions.value)
    s.delete(paramId)
    uploadLoadingVersions.value = s
  }
}

function toggleScanner() {
  scannerActive.value = !scannerActive.value
  if (scannerActive.value) {
    initScanner()
  } else {
    stopScanner()
  }
}

function initScanner() {
  if (typeof Html5Qrcode === 'undefined') {
    alert('扫码功能加载中，请稍后再试')
    scannerActive.value = false
    return
  }

  html5QrCodeScanner = new Html5Qrcode('qr-reader')

  html5QrCodeScanner.start(
    { facingMode: 'environment' },
    { fps: 10, qrbox: { width: 250, height: 250 } },
    (decodedText) => {
      uploadData.value.batchId = decodedText.trim()
      stopScanner()
      scannerActive.value = false
    },
    () => {}
  ).catch(err => {
    console.error('摄像头启动失败:', err)
    alert('无法访问摄像头，请检查权限设置')
    scannerActive.value = false
  })
}

function stopScanner() {
  if (html5QrCodeScanner) {
    html5QrCodeScanner.stop().then(() => {
      html5QrCodeScanner = null
    }).catch(() => {})
  }
}

function setNowTime() {
  const now = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  uploadData.value.fillTime = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:00`
}

async function submitData() {
  submitAttempted.value = true

  if (!uploadData.value.productId || uploadData.value.productId <= 0) {
    uploadResult.value = { success: false, message: '请先选择产品后再提交数据' }
    return
  }
  if (!uploadData.value.processId || uploadData.value.processId <= 0) {
    uploadResult.value = { success: false, message: '请先选择工序后再提交数据' }
    return
  }
  if (uploadSelectedParamIds.value.length === 0) {
    uploadResult.value = { success: false, message: '请至少选择一个工艺参数' }
    return
  }

  if (!uploadData.value.equipmentId || uploadData.value.equipmentId <= 0) {
    uploadResult.value = { success: false, message: '请选择设备' }
    return
  }

  const emptyCells = []
  for (let ri = 0; ri < uploadRows.value.length; ri++) {
    for (const pid of uploadSelectedParamIds.value) {
      const val = getRowValue(ri, pid)
      if (val === null || val === undefined || val === '') emptyCells.push(`第${ri + 1}行-${getParamName(pid)}`)
    }
  }

  if (emptyCells.length > 0) {
    uploadResult.value = { success: false, message: `以下单元格不能为空: ${emptyCells.slice(0, 5).join(', ')}${emptyCells.length > 5 ? ` 等${emptyCells.length}处` : ''}` }
    return
  }

  // 计数型图数据校验: 样本量必填
  const missingSampleSizes = []
  for (let ri = 0; ri < uploadRows.value.length; ri++) {
    for (const pid of uploadSelectedParamIds.value) {
      if (getParamDataType(pid) === 'COUNT') {
        const val = getRowValue(ri, pid)
        if (val !== null && val !== undefined && val !== '') {
          const sz = getSampleSize(ri, pid)
          if (!sz || sz <= 0) missingSampleSizes.push(`第${ri + 1}行-${getParamName(pid)}`)
        }
      }
    }
  }
  if (missingSampleSizes.length > 0) {
    uploadResult.value = { success: false, message: `计数型数据需填写样本量: ${missingSampleSizes.slice(0, 5).join(', ')}${missingSampleSizes.length > 5 ? ` 等${missingSampleSizes.length}处` : ''}` }
    return
  }

  uploading.value = true
  uploadResult.value = null

  try {
    const records = []
    const normalizedFillTime = (function(t) {
      if (!t) return new Date().toISOString().replace('T', ' ').slice(0, 19)
      t = t.replace('T', ' ')
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(t)) return t + ':00'
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(t)) return t
      return t
    })(uploadData.value.fillTime)

    for (let ri = 0; ri < uploadRows.value.length; ri++) {
      for (const pid of uploadSelectedParamIds.value) {
        records.push({
          paramId: pid,
          productId: uploadData.value.productId,
          processId: uploadData.value.processId,
          equipmentId: uploadData.value.equipmentId,
          batchId: uploadData.value.batchId || null,
          measuredValue: getRowValue(ri, pid),
          sampleSize: getParamDataType(pid) === 'COUNT' ? (getSampleSize(ri, pid) || null) : null,
          fillTime: normalizedFillTime
        })
      }
    }
    // TODO 需要完善数据留痕，方便后期查找对应负责人
    const res = await spcApi.batchUploadData(records)

    let okCount = 0
    let failCount = 0
    if (res.code === 200 && Array.isArray(res.data)) {
      okCount = res.data.length
    } else {
      failCount = records.length
    }

    if (failCount === 0) {
      uploadResult.value = { success: true, message: `成功提交 ${okCount} 条数据！` }
      setTimeout(() => {
        showUploadForm.value = false
        emit('refresh')
      }, 1200)
    } else if (okCount > 0) {
      uploadResult.value = { success: true, message: `成功 ${okCount} 条，失败 ${failCount} 条` }
    } else {
      uploadResult.value = { success: false, message: '全部提交失败，请检查网络或数据格式' }
    }
  } catch (e) {
    const errMsg = e.response?.data?.msg || e.message || '提交异常'
    uploadResult.value = { success: false, message: errMsg }
  } finally {
    uploading.value = false
  }
}

onUnmounted(() => {
  stopScanner()
})
</script>

<style scoped>
.spc-data-import {
  margin-bottom: 12px;
}

.import-actions-bar {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  align-items: center;
}

.btn-upload, .btn-import, .btn-export, .btn-template {
  padding: 8px 16px;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  border: none;
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.btn-upload {
  background: linear-gradient(135deg, #1890ff, #096dd9);
  color: white;
}
.btn-upload:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(24, 144, 255, 0.35);
}

.btn-import {
  background: linear-gradient(135deg, #722ed1, #531dab);
  color: white;
}
.btn-import:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(114, 46, 209, 0.35);
}

.btn-export {
  background: linear-gradient(135deg, #52c41a, #389e0d);
  color: white;
}
.btn-export:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(82, 196, 26, 0.35);
}

.btn-template {
  background: linear-gradient(135deg, #fa8c16, #d46b08);
  color: white;
}
.btn-template:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(250, 140, 22, 0.35);
}

.import-modal, .upload-modal {
  position: fixed;
  inset: 0;
  z-index: 10000;
  background: rgba(0, 0, 0, 0.45);
  display: flex;
  align-items: center;
  justify-content: center;
  backdrop-filter: blur(2px);
}

.modal-content {
  background: var(--bg-modal);
  border-radius: 16px;
  padding: 28px;
  width: 92%;
  max-width: 720px;
  max-height: 88vh;
  overflow-y: auto;
  box-shadow: var(--shadow-lg);
}

.modal-content h3 {
  margin: 0 0 22px 0;
  font-size: 19px;
  color: var(--text-primary);
  font-weight: 700;
  letter-spacing: -0.02em;
}

.upload-form, .import-form {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.form-row {
  display: flex;
  gap: 14px;
  flex-wrap: wrap;
}

.form-field {
  flex: 1;
  min-width: 180px;
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.form-field.full {
  min-width: 100%;
}

.form-field label {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
}

.req {
  color: #f5222d;
}

.form-input {
  padding: 9px 12px;
  border: 1.5px solid var(--border-input);
  border-radius: 8px;
  font-size: 13.5px;
  outline: none;
  transition: all 0.25s;
  background: var(--bg-input);
  color: var(--text-primary);
}
.form-input:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px var(--accent-light);
  background: var(--bg-input);
}
.form-input.input-error {
  border-color: #f5222d;
  background: rgba(245,34,45,0.06);
}

.file-input {
  padding: 10px;
  border: 2px dashed var(--border-input);
  border-radius: 8px;
  cursor: pointer;
  font-size: 13px;
  background: var(--bg-tertiary);
  color: var(--text-primary);
}
.file-input:hover {
  border-color: var(--accent-primary);
  background: var(--accent-lighter);
}

.field-hint {
  font-size: 11.5px;
  color: var(--text-tertiary);
  line-height: 1.4;
}

.field-error {
  font-size: 11.5px;
  color: #f5222d;
  font-weight: 500;
}

.import-preview {
  margin-top: 8px;
  max-height: 220px;
  overflow: auto;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 12px;
  background: var(--bg-tertiary);
}

.preview-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11.5px;
}
.preview-table th, .preview-table td {
  padding: 6px 8px;
  border: 1px solid var(--border-color);
  text-align: left;
  color: var(--text-primary);
}
.preview-table th {
  background: var(--bg-primary);
  font-weight: 600;
  position: sticky;
  top: 0;
}

.import-result, .upload-result {
  margin-top: 8px;
  padding: 10px 14px;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 500;
}
.import-result .success, .upload-result .success {
  background: #f6ffed;
  color: #389e0d;
  border: 1px solid #b7eb8f;
}
.import-result .error, .upload-result .error {
  background: #fff2f0;
  color: #cf1322;
  border: 1px solid #ffa39e;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 18px;
  padding-top: 16px;
  border-top: 1px solid var(--border-color);
}

.btn-cancel {
  padding: 9px 22px;
  border: 1.5px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-secondary);
  cursor: pointer;
  font-size: 13.5px;
  font-weight: 500;
  color: var(--text-secondary);
  transition: all 0.2s;
}
.btn-cancel:hover {
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

.btn-submit {
  padding: 9px 26px;
  border: none;
  border-radius: 8px;
  background: linear-gradient(135deg, #1890ff, #096dd9);
  color: white;
  cursor: pointer;
  font-size: 13.5px;
  font-weight: 600;
  transition: all 0.2s;
}
.btn-submit:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: var(--shadow-md);
}
.btn-submit:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.multi-param-section {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.multi-param-label {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
}

.param-checkbox-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
  gap: 8px;
}

.param-checkbox-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border: 2px solid var(--border-color);
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s;
  background: var(--bg-tertiary);
}
.param-checkbox-item:hover {
  border-color: var(--accent-primary);
  background: var(--accent-lighter);
}
.param-checkbox-item.checked {
  border-color: var(--accent-primary);
  background: var(--accent-lighter);
}
.param-checkbox-item input[type='checkbox'] {
  width: 17px;
  height: 17px;
  accent-color: var(--accent-primary);
}

.param-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.param-info strong {
  font-size: 13px;
  color: var(--text-primary);
}
.param-info small {
  font-size: 11px;
  color: var(--text-tertiary);
}

.multi-value-section {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 6px;
}

.batch-table-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.row-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.btn-add-row {
  padding: 5px 12px;
  border: 1.5px dashed var(--accent-primary);
  border-radius: 8px;
  background: var(--accent-lighter);
  color: var(--accent-primary);
  cursor: pointer;
  font-size: 12px;
  font-weight: 500;
  transition: all 0.2s;
}
.btn-add-row:hover:not(:disabled) { background: var(--accent-primary); color: var(--text-on-accent); }
.btn-add-row:disabled { opacity: 0.4; cursor: not-allowed; }

.row-count-hint { font-size: 11px; color: var(--text-tertiary); }

.batch-table-wrap {
  overflow-x: auto;
  border: 1px solid var(--border-color);
  border-radius: 10px;
  background: var(--bg-tertiary);
}

.batch-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.batch-table thead th {
  background: var(--accent-lighter);
  padding: 9px 8px;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-primary);
  white-space: nowrap;
  border-bottom: 2px solid var(--border-color);
  text-align: center;
}

.th-unit { font-weight: 400; color: var(--text-tertiary); font-size: 10px; }

.col-row-num, .col-action { width: 42px; min-width: 42px; }
.col-row-num { background: var(--bg-primary); }

.batch-table tbody tr { transition: background 0.15s; }
.batch-table tbody tr:hover { background: var(--accent-light); }
.row-empty td { color: var(--text-tertiary); }

.cell-row-num {
  text-align: center;
  font-weight: 600;
  font-size: 12px;
  color: var(--accent-primary);
  padding: 8px 4px;
  vertical-align: middle;
}

.cell-value { padding: 4px 3px; vertical-align: middle; }

.cell-input {
  width: 100% !important;
  max-width: none !important;
  min-width: 70px;
  height: 32px;
  text-align: center;
  font-size: 13px;
  padding: 4px 6px !important;
  background: var(--bg-input) !important;
  color: var(--text-primary) !important;
  border: 1.5px solid var(--border-input) !important;
}
.cell-input.input-empty { border-color: #ff4d4f !important; background: rgba(255,77,79,0.08) !important; }
.cell-input:focus { border-color: var(--accent-primary) !important; box-shadow: 0 0 0 2px var(--accent-light); outline: none; }

.cell-action { text-align: center; vertical-align: middle; padding: 4px 2px; }

.btn-del-row {
  width: 26px; height: 26px;
  border: 1px solid var(--border-input);
  border-radius: 50%;
  background: var(--bg-secondary);
  color: var(--text-tertiary);
  cursor: pointer;
  font-size: 13px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s;
}
.btn-del-row:hover:not(:disabled) { border-color: #ff4d4f; color: #ff4d4f; background: rgba(255,77,79,0.08); }
.btn-del-row:disabled { opacity: 0.3; cursor: not-allowed; }

.param-limit-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding: 8px 10px;
  background: var(--bg-primary);
  border-radius: 8px;
  border: 1px solid var(--border-color);
}

.limit-tag {
  font-size: 11px;
  color: var(--text-secondary);
  line-height: 1.6;
}
.limit-tag strong { color: var(--text-primary); }

.th-param-main {
  display: flex; align-items: center; justify-content: center; gap: 4px;
  white-space: nowrap;
}

.th-type-badge {
  display: inline-block; font-size: 10px; font-weight: 500;
  padding: 1px 6px; border-radius: 8px; line-height: 1.5;
  letter-spacing: 0.3px;
}
.th-type-badge.type-CONTINUOUS { background: #e6f4ff; color: #1677ff; }
.th-type-badge.type-DISCRETE { background: #f6ffed; color: #52c41a; }
.th-type-badge.type-COUNT { background: #fff7e6; color: #fa8c16; }

.th-limit-hint {
  display: flex; align-items: center; gap: 6px;
  margin-top: 3px; font-size: 9.5px; color: var(--text-tertiary);
  justify-content: center;
}
.th-limit-hint .lim-spec { color: var(--accent-primary); font-weight: 500; }
.th-limit-hint .lim-range { color: #8c8c8c; }
.th-limit-hint.th-loading { color: #bfbfbf; font-style: italic; }

.cell-input-wrap { position: relative; display: flex; align-items: center; }
.cell-input-wrap .cell-input { padding-right: 22px !important; }
.cell-sample-wrap { margin-top: 4px; }
.cell-sample-wrap .sample-input { font-size: 12px; color: #fa8c16; border-color: #ffd591; }

.cell-status-dot {
  position: absolute; right: 4px; top: 50%; transform: translateY(-50%);
  width: 7px; height: 7px; border-radius: 50%; pointer-events: none;
  transition: all 0.2s ease;
}
.cell-status-dot.dot-ok { background: #52c41a; box-shadow: 0 0 4px rgba(82,196,26,0.35); }
.cell-status-dot.dot-warn { background: #faad14; box-shadow: 0 0 4px rgba(250,173,20,0.35); animation: pulse-warn 1.5s infinite; }
.cell-status-dot.dot-danger { background: #ff4d4f; box-shadow: 0 0 4px rgba(255,77,79,0.4); animation: pulse-danger 1s infinite; }

@keyframes pulse-warn {
  0%, 100% { transform: translateY(-50%) scale(1); opacity: 1; }
  50% { transform: translateY(-50%) scale(1.35); opacity: 0.7; }
}
@keyframes pulse-danger {
  0%, 100% { transform: translateY(-50%) scale(1); }
  50% { transform: translateY(-50%) scale(1.45); }
}

.cell-input.input-oos {
  border-color: #ff4d4f !important;
  background: linear-gradient(135deg, rgba(255,77,79,0.06), rgba(255,77,79,0.02)) !important;
  box-shadow: 0 0 0 3px rgba(255,77,79,0.08) !important;
}
.cell-input.input-warning {
  border-color: #faad14 !important;
  background: linear-gradient(135deg, rgba(250,173,20,0.05), rgba(250,173,20,0.01)) !important;
}

.cell-oos { background: rgba(255,77,79,0.04) !important; }
.cell-warning { background: rgba(250,173,20,0.03) !important; transition: background 0.15s; }

.param-limit-bar.enhanced {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 10px;
  padding: 12px;
  background: linear-gradient(to bottom, #fafbfc, #fff);
  border: 1px solid #e2e8f0;
  border-radius: 10px;
}

.limit-card {
  background: #fff;
  border: 1px solid #e8ecf0;
  border-radius: 8px;
  overflow: hidden;
  transition: box-shadow 0.2s;
}
.limit-card:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.06); }

.limit-card-header {
  display: flex; align-items: center; gap: 8px;
  padding: 8px 12px;
  background: #f8fafc;
  border-bottom: 1px solid #edf2f7;
  font-size: 13px;
}
.limit-card-header strong { color: #334155; font-weight: 600; }

.limit-card-type {
  display: inline-block; font-size: 10px; font-weight: 500;
  padding: 1px 7px; border-radius: 8px; line-height: 1.5;
}
.limit-card-type.type-CONTINUOUS { background: #dbeafe; color: #2563eb; }
.limit-card-type.type-DISCRETE { background: #dcfce7; color: #16a34a; }
.limit-card-type.type-COUNT { background: #fef3c7; color: #d97706; }

.limit-card-body { padding: 8px 12px; }

.limit-item {
  display: flex; align-items: center; gap: 10px;
  padding: 4px 0;
  font-size: 11.5px;
}
.li-label {
  min-width: 40px; font-weight: 500; color: #64748b;
  font-size: 10.5px; text-transform: uppercase; letter-spacing: 0.5px;
}
.li-values { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.li-values em {
  font-style: normal; font-size: 9.5px; color: #94a3b8;
  font-weight: 500; letter-spacing: 0.3px;
}
.li-values strong {
  font-size: 12px; color: #334155; font-weight: 600;
  font-family: 'SF Mono', 'Cascadia Code', Consolas, monospace;
}
.spec-limits .li-values strong { color: #dc2626; }
.ctrl-limits .li-values strong { color: #ea580c; }

.param-limit-info {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  font-size: 11px;
  color: var(--text-secondary);
  padding: 6px 10px;
  background: var(--bg-secondary);
  border-radius: 6px;
  border: 1px solid var(--border-color);
}

.limit-item {
  white-space: nowrap;
}

.limit-sep {
  color: var(--text-tertiary);
}

.loading-hint {
  color: #fa8c16 !important;
  font-style: italic;
}

.no-version-hint {
  color: var(--text-tertiary) !important;
}

.batch-input-group {
  display: flex;
  gap: 8px;
  align-items: center;
}

.btn-scan {
  padding: 9px 14px;
  border: 1.5px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-secondary);
  cursor: pointer;
  font-size: 16px;
  transition: all 0.2s;
}
.btn-scan:hover {
  border-color: var(--accent-primary);
  background: var(--accent-lighter);
}

.scanner-container {
  margin-top: 10px;
  border: 2px solid var(--accent-primary);
  border-radius: 12px;
  overflow: hidden;
  background: black;
}

.scanner-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 14px;
  background: linear-gradient(135deg, #1890ff, #096dd9);
  color: white;
  font-size: 13.5px;
  font-weight: 600;
}

.btn-close-scanner {
  padding: 4px 12px;
  border: 1px solid rgba(255,255,255,0.5);
  border-radius: 6px;
  background: transparent;
  color: white;
  cursor: pointer;
  font-size: 12px;
  transition: all 0.2s;
}
.btn-close-scanner:hover {
  background: rgba(255,255,255,0.15);
}

.qr-reader {
  width: 100%;
  min-height: 260px;
  border: none;
}

.scanner-hint {
  padding: 10px;
  text-align: center;
  font-size: 12px;
  color: var(--text-tertiary);
  background: var(--bg-tertiary);
}

.datetime-row {
  display: flex;
  gap: 8px;
  align-items: center;
}

.btn-now {
  padding: 9px 14px;
  border: 1.5px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-secondary);
  cursor: pointer;
  font-size: 12px;
  font-weight: 500;
  color: var(--text-secondary);
  transition: all 0.2s;
  white-space: nowrap;
}
.btn-now:hover {
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

@media (max-width: 768px) {
  .import-actions-bar {
    flex-wrap: wrap;
    gap: 8px;
    justify-content: center;
  }
  .btn-upload, .btn-import, .btn-export, .btn-template {
    font-size: 12px;
    padding: 7px 14px;
  }
}

@media (max-width: 640px) {
  .modal-content {
    padding: 20px;
    width: 96%;
    max-height: 92vh;
  }

  .import-actions-bar {
    flex-direction: column;
    gap: 6px;
  }
  .btn-upload, .btn-import, .btn-export, .btn-template {
    width: 100%;
    text-align: center;
    font-size: 13px;
    padding: 10px 16px;
  }

  .param-checkbox-grid {
    grid-template-columns: 1fr;
  }

  .batch-table-wrap {
    max-width: 100%;
    overflow-x: auto;
  }
  .cell-input { min-width: 55px; font-size: 12px; height: 28px; }
}
</style>
