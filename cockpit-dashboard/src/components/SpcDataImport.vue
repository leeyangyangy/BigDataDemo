<template>
  <div class="spc-data-import">
    <div class="import-actions-bar">
      <button class="btn-upload" @click="requireAuth(() => showUploadForm = true)" v-if="isLoggedIn">填写数据</button>
      <button class="btn-import" @click="requireAuth(showImportDialog)" v-if="isLoggedIn && selectedProduct">导入数据</button>
      <button class="btn-export" @click="exportDataReport" v-if="selectedParam && selectedProduct">导出报告</button>
      <button class="btn-template" @click="downloadTemplate" v-if="isLoggedIn">下载模板</button>
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
                <option v-for="eq in equipmentList" :key="eq.id" :value="eq.id">{{ eq.name }}</option>
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
            <div class="multi-value-row" v-for="pid in uploadSelectedParamIds" :key="'val-' + pid">
              <div class="param-col-left">
                <div class="param-badge">{{ getParamName(pid) }}</div>
                <span class="unit-tag">{{ getParamUnit(pid) }}</span>
              </div>
              <div class="param-col-right">
                <input v-model.number="uploadMultiValues[pid]" type="number" step="0.000001"
                       class="form-input value-input" placeholder="测量值 (必填)" />
                <div class="param-limit-info" v-if="uploadParamVersionMap[pid]">
                  <span class="limit-item">USL={{ uploadParamVersionMap[pid].usl ?? '-' }}</span>
                  <span class="limit-item">LSL={{ uploadParamVersionMap[pid].lsl ?? '-' }}</span>
                  <span class="limit-item">T={{ uploadParamVersionMap[pid].target ?? '-' }}</span>
                  <span class="limit-sep">|</span>
                  <span class="limit-item">UCL={{ uploadParamVersionMap[pid].ucl ?? '-' }}</span>
                  <span class="limit-item">LCL={{ uploadParamVersionMap[pid].lcl ?? '-' }}</span>
                </div>
                <div class="param-limit-info loading-hint" v-else-if="uploadLoadingVersions.has(pid)">
                  加载标准中...
                </div>
                <div class="param-limit-info no-version-hint" v-else>
                  暂无版本，提交时将自动创建
                </div>
              </div>
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
              {{ uploading ? '提交中...' : `批量提交 (${uploadSelectedParamIds.length} 项)` }}
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
import { spcApi, getToken } from '../utils/api'

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
  timeRange: String
})

const emit = defineEmits(['require-login', 'refresh', 'data-imported'])

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
const uploadMultiValues = reactive({})
const uploadParamVersionMap = reactive({})
const uploadLoadingVersions = ref(new Set())

const hasAnyValue = computed(() => {
  return Object.values(uploadMultiValues).some(v => v !== null && v !== undefined && v !== '')
})

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

  importing.value = true
  importResult.value = null

  try {
    const formData = new FormData()
    formData.append('file', importFile.value)
    formData.append('productId', importForm.value.productId)
    if (importForm.value.processId) formData.append('processId', importForm.value.processId)
    if (importForm.value.equipmentId) formData.append('equipmentId', importForm.value.equipmentId)

    const res = await fetch('/api/spc/data/import', {
      method: 'POST',
      body: formData,
      headers: {
        'Authorization': `Bearer ${getToken()}`
      }
    })
    const result = await res.json()

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

function downloadTemplate() {
  const link = document.createElement('a')
  link.href = spcApi.downloadTemplate()
  link.download = 'SPC数据导入模板.csv'
  link.click()
}

async function exportDataReport() {
  if (!props.selectedParam || !props.selectedProduct) return

  try {
    const params = new URLSearchParams({
      paramId: props.selectedParam,
      productId: props.selectedProduct,
      limit: props.dataLimit || 500
    })

    if (props.selectedEquipment) {
      params.append('equipmentId', props.selectedEquipment)
    }

    if (props.timeRange) {
      const endTime = new Date()
      const startTime = new Date()
      startTime.setDate(startTime.getDate() - parseInt(props.timeRange))
      params.append('startTime', formatDateTime(startTime))
      params.append('endTime', formatDateTime(endTime))
    }

    const res = await fetch(`/api/spc/data/export/report?${params.toString()}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${getToken()}`
      }
    })

    if (!res.ok) throw new Error('导出失败')

    const blob = await res.blob()
    const contentDisposition = res.headers.get('Content-Disposition')
    let filename = `SPC数据报告_${formatDate(new Date())}.csv`
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
    console.error('导出失败:', e)
    alert('导出失败，请重试')
  }
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
  Object.keys(uploadMultiValues).forEach(k => delete uploadMultiValues[k])
  Object.keys(uploadParamVersionMap).forEach(k => delete uploadParamVersionMap[k])
  uploadLoadingVersions.value = new Set()

  if (!uploadData.value.processId) return

  try {
    const res = await spcApi.getParamPage({ current: 1, size: 100 })
    if (res.code === 200) uploadParams.value = res.data.records.filter(p => p.processId === uploadData.value.processId)
  } catch (e) {
    console.error('加载参数失败', e)
  }
}

function onMultiParamChange(param) {
  const isChecked = uploadSelectedParamIds.value.includes(param.id)

  if (!isChecked) {
    delete uploadMultiValues[param.id]
    delete uploadParamVersionMap[param.id]
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
  uploadData.value.fillTime = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}T${pad(now.getHours())}:${pad(now.getMinutes())}`
}

async function submitData() {
  submitAttempted.value = true

  if (uploadSelectedParamIds.value.length === 0) {
    uploadResult.value = { success: false, message: '请至少选择一个工艺参数' }
    return
  }

  if (!uploadData.value.equipmentId) {
    uploadResult.value = { success: false, message: '请选择设备' }
    return
  }

  const emptyParams = []
  for (const pid of uploadSelectedParamIds.value) {
    const val = uploadMultiValues[pid]
    if (val === null || val === undefined || val === '') emptyParams.push(getParamName(pid))
  }

  if (emptyParams.length > 0) {
    uploadResult.value = { success: false, message: `以下参数的测量值不能为空: ${emptyParams.join(', ')}` }
    return
  }

  uploading.value = true
  uploadResult.value = null

  try {
    const records = []

    for (const pid of uploadSelectedParamIds.value) {
      records.push({
        paramId: pid,
        productId: uploadData.value.productId,
        processId: uploadData.value.processId,
        equipmentId: uploadData.value.equipmentId,
        batchId: uploadData.value.batchId || null,
        measuredValue: uploadMultiValues[pid],
        fillTime: uploadData.value.fillTime || new Date().toISOString()
      })
    }

    const results = await Promise.allSettled(
      records.map(r => spcApi.uploadData(r))
    )

    let okCount = 0
    let failCount = 0
    results.forEach(r => {
      if (r.status === 'fulfilled' && r.value?.code === 200) okCount++
      else failCount++
    })

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
    uploadResult.value = { success: false, message: '提交异常: ' + e.message }
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
  background: white;
  border-radius: 16px;
  padding: 28px;
  width: 92%;
  max-width: 720px;
  max-height: 88vh;
  overflow-y: auto;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.18);
}

.modal-content h3 {
  margin: 0 0 22px 0;
  font-size: 19px;
  color: #1a1a1a;
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
  color: #333;
}

.req {
  color: #f5222d;
}

.form-input {
  padding: 9px 12px;
  border: 1.5px solid #d9d9d9;
  border-radius: 8px;
  font-size: 13.5px;
  outline: none;
  transition: all 0.25s;
  background: #fafafa;
}
.form-input:focus {
  border-color: #1890ff;
  box-shadow: 0 0 0 3px rgba(24, 144, 255, 0.1);
  background: white;
}
.form-input.input-error {
  border-color: #f5222d;
  background: #fff1f0;
}

.file-input {
  padding: 10px;
  border: 2px dashed #d9d9d9;
  border-radius: 8px;
  cursor: pointer;
  font-size: 13px;
  background: #fafafa;
}
.file-input:hover {
  border-color: #1890ff;
  background: #e6f7ff;
}

.field-hint {
  font-size: 11.5px;
  color: #999;
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
  border: 1px solid #e8e8e8;
  border-radius: 8px;
  padding: 12px;
  background: #fafbfc;
}

.preview-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11.5px;
}
.preview-table th, .preview-table td {
  padding: 6px 8px;
  border: 1px solid #eee;
  text-align: left;
}
.preview-table th {
  background: #f0f0f0;
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
  border-top: 1px solid #f0f0f0;
}

.btn-cancel {
  padding: 9px 22px;
  border: 1.5px solid #d9d9d9;
  border-radius: 8px;
  background: white;
  cursor: pointer;
  font-size: 13.5px;
  font-weight: 500;
  color: #555;
  transition: all 0.2s;
}
.btn-cancel:hover {
  border-color: #1890ff;
  color: #1890ff;
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
  box-shadow: 0 4px 12px rgba(24, 144, 255, 0.35);
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
  color: #333;
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
  border: 2px solid #e8e8e8;
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s;
  background: #fafafa;
}
.param-checkbox-item:hover {
  border-color: #bae7ff;
  background: #e6f7ff;
}
.param-checkbox-item.checked {
  border-color: #1890ff;
  background: #e6f7ff;
}
.param-checkbox-item input[type='checkbox'] {
  width: 17px;
  height: 17px;
  accent-color: #1890ff;
}

.param-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.param-info strong {
  font-size: 13px;
  color: #333;
}
.param-info small {
  font-size: 11px;
  color: #888;
}

.multi-value-section {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 6px;
}

.multi-value-row {
  display: flex;
  gap: 12px;
  align-items: flex-start;
  padding: 12px;
  background: #fafbfc;
  border: 1px solid #e8e8e8;
  border-radius: 10px;
}

.param-col-left {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 90px;
}

.param-badge {
  background: linear-gradient(135deg, #1890ff, #096dd9);
  color: white;
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
}

.unit-tag {
  font-size: 11px;
  color: #888;
  font-style: italic;
}

.param-col-right {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.value-input {
  width: 100% !important;
  max-width: 280px;
}

.param-limit-info {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  font-size: 11px;
  color: #666;
  padding: 6px 10px;
  background: white;
  border-radius: 6px;
  border: 1px solid #f0f0f0;
}

.limit-item {
  white-space: nowrap;
}

.limit-sep {
  color: #ccc;
}

.loading-hint {
  color: #fa8c16 !important;
  font-style: italic;
}

.no-version-hint {
  color: #999 !important;
}

.batch-input-group {
  display: flex;
  gap: 8px;
  align-items: center;
}

.btn-scan {
  padding: 9px 14px;
  border: 1.5px solid #d9d9d9;
  border-radius: 8px;
  background: white;
  cursor: pointer;
  font-size: 16px;
  transition: all 0.2s;
}
.btn-scan:hover {
  border-color: #1890ff;
  background: #e6f7ff;
}

.scanner-container {
  margin-top: 10px;
  border: 2px solid #1890ff;
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
  color: #999;
  background: #fafafa;
}

.datetime-row {
  display: flex;
  gap: 8px;
  align-items: center;
}

.btn-now {
  padding: 9px 14px;
  border: 1.5px solid #d9d9d9;
  border-radius: 8px;
  background: white;
  cursor: pointer;
  font-size: 12px;
  font-weight: 500;
  color: #555;
  transition: all 0.2s;
  white-space: nowrap;
}
.btn-now:hover {
  border-color: #1890ff;
  color: #1890ff;
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

  .multi-value-row {
    flex-direction: column;
  }
}
</style>
