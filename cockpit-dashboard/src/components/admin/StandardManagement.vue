<template>
  <div class="mgmt-section">
    <div class="toolbar">
      <div class="search-bar">
        <select v-model="filterProductId" class="filter-select-sm" @change="onProductChange">
          <option value="">全部产品</option>
          <option v-for="p in productList" :key="p.id" :value="p.id">{{ p.productName }} ({{ p.productCode }})</option>
        </select>
        <select v-model="filterProcessId" class="filter-select-sm" @change="loadData">
          <option value="">全部工序</option>
          <option v-for="p in filteredProcessList" :key="p.id" :value="p.id">{{ p.processName }} ({{ p.processCode }})</option>
        </select>
        <input v-model="keyword" type="text" class="search-input" placeholder="搜索参数编码/名称..." @keyup.enter="loadData" />
        <button class="btn-search" @click="loadData">查询</button>
      </div>
      <div class="toolbar-actions">
        <button class="btn-batch-add" @click="openBatchAdd">+ 批量添加工艺参数</button>
      </div>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="list.length > 0">
        <thead>
          <tr>
            <th>参数编码</th>
            <th>参数名称</th>
            <th>测量单位</th>
            <th>数据类型</th>
            <th>小数位数</th>
            <th>所属工序</th>
            <th>当前版本</th>
            <th>上下限</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id">
            <td><strong>{{ item.paramCode }}</strong></td>
            <td>{{ item.paramName }}</td>
            <td><span class="unit-tag">{{ item.unit || '-' }}</span></td>
            <td>{{ item.dataType || '连续型' }}</td>
            <td>{{ item.decimalPlaces ?? 3 }}</td>
            <td><span class="process-tag">{{ getProcessName(item.processId) }}</span></td>
            <td>{{ getVersionNo(item.id) || '-' }}</td>
            <td class="limits-cell">
              <template v-if="getVersionInfo(item.id)">
                USL={{ getVersionInfo(item.id).usl ?? '-' }}
                LSL={{ getVersionInfo(item.id).lsl ?? '-' }}
              </template>
              <span v-else>-</span>
            </td>
            <td>
              <span class="status-tag" :class="item.status === 1 ? 'on' : 'off'">{{ item.status === 1 ? '启用' : '停用' }}</span>
            </td>
            <td class="actions">
              <button class="btn-action btn-copy" @click="handleDuplicate(item)">复制</button>
              <button class="btn-action btn-edit" @click="openEdit(item)">编辑</button>
              <button class="btn-action btn-version" @click="openVersionManage(item)">版本</button>
              <button class="btn-action btn-del" @click="handleDelete(item)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">📏</span>
        <p>暂无工艺参数数据，请先选择工序后批量添加工艺参数</p>
      </div>
    </div>

    <div class="pagination" v-if="total > pageSize">
      <button class="page-btn" :disabled="current <= 1" @click="current--; loadData()">上一页</button>
      <span class="page-info">{{ current }} / {{ totalPages }}</span>
      <button class="page-btn" :disabled="current >= totalPages" @click="current++; loadData()">下一页</button>
    </div>

    <!-- 批量添加弹窗 -->
    <div class="modal-overlay" v-if="showBatchForm" @click.self="closeBatchForm">
      <div class="modal-card modal-lg">
        <h3 class="modal-title">批量添加工艺参数</h3>

        <div class="batch-form-section">
          <div class="form-field full">
            <label>选择工序 <span class="req">*</span></label>
            <select v-model.number="batchForm.processId" class="form-input" @change="onBatchProcessChange">
              <option :value="null">请先选择工序</option>
              <option v-for="p in processList" :key="p.id" :value="p.id">{{ p.processName }} ({{ p.processCode }})</option>
            </select>
          </div>

          <div class="batch-items-label">
            <span>标准工艺参数列表 (可一次添加多条)</span>
            <button class="btn-add-row" @click="addBatchRow">+ 添加一行</button>
          </div>

          <div class="batch-table-wrap">
            <table class="batch-table">
              <thead>
                <tr>
                  <th style="width:40px">#</th>
                  <th>参数编码 <span class="req">*</span></th>
                  <th>参数名称 <span class="req">*</span></th>
                  <th>单位</th>
                  <th>数据类型</th>
                  <th>小数位</th>
                  <th style="width:50px"></th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in batchForm.rows" :key="idx">
                  <td class="row-num">{{ idx + 1 }}</td>
                  <td><input v-model="row.paramCode" type="text" class="form-input-sm" placeholder="如: CHIP_PROTRUDE" /></td>
                  <td><input v-model="row.paramName" type="text" class="form-input-sm" placeholder="如: 芯片凸出值" /></td>
                  <td><input v-model="row.unit" type="text" class="form-input-sm" placeholder="mm/μm/V" /></td>
                  <td>
                    <select v-model="row.dataType" class="form-input-sm">
                      <option value="">连续型</option>
                      <option value="离散型">离散型</option>
                      <option value="计数型">计数型</option>
                    </select>
                  </td>
                  <td><input v-model.number="row.decimalPlaces" type="number" class="form-input-sm" placeholder="3" min="0" max="6" /></td>
                  <td><button class="btn-remove-row" @click="removeBatchRow(idx)" :disabled="batchForm.rows.length <= 1">×</button></td>
                </tr>
              </tbody>
            </table>
          </div>

          <div class="batch-example" v-if="batchForm.processId">
            <span class="example-label">示例：</span>
            <span class="example-text">
              工序「{{ getProcessName(batchForm.processId) }}」下可添加：
              芯片凸出值(mm)、热沉突出值(μm)、键合强度(N)、空洞率(%)
            </span>
          </div>
        </div>

        <div class="form-msg" v-if="formMsg" :class="{ error: formMsgType === 'error', success: formMsgType === 'success' }">{{ formMsg }}</div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeBatchForm">取消</button>
          <button class="btn-submit" @click="submitBatchAdd" :disabled="submitting">
            {{ submitting ? '提交中...' : `确认添加 (${validRowCount} 条)` }}
          </button>
        </div>
      </div>
    </div>

    <!-- 编辑弹窗 -->
    <div class="modal-overlay" v-if="showEditForm" @click.self="closeEditForm">
      <div class="modal-card">
        <h3 class="modal-title">编辑标准参数</h3>
        <div class="form-grid">
          <div class="form-field full">
            <label>参数编码 <span class="req">*</span></label>
            <input v-model="editForm.paramCode" type="text" class="form-input" disabled />
          </div>
          <div class="form-field full">
            <label>参数名称 <span class="req">*</span></label>
            <input v-model="editForm.paramName" type="text" class="form-input" placeholder="参数名称" />
          </div>
          <div class="form-field">
            <label>测量单位</label>
            <input v-model="editForm.unit" type="text" class="form-input" placeholder="如 mm/μm/V/%/N" />
          </div>
          <div class="form-field">
            <label>数据类型</label>
            <select v-model="editForm.dataType" class="form-input">
              <option value="">连续型</option>
              <option value="离散型">离散型</option>
              <option value="计数型">计数型</option>
            </select>
          </div>
          <div class="form-field">
            <label>小数位数</label>
            <input v-model.number="editForm.decimalPlaces" type="number" class="form-input" placeholder="3" min="0" max="6" />
          </div>
          <div class="form-field">
            <label>状态</label>
            <select v-model="editForm.status" class="form-input">
              <option :value="1">启用</option>
              <option :value="0">停用</option>
            </select>
          </div>
        </div>
        <div class="form-msg" v-if="formMsg" :class="{ error: formMsgType === 'error', success: formMsgType === 'success' }">{{ formMsg }}</div>
        <div class="modal-actions">
          <button class="btn-cancel" @click="closeEditForm">取消</button>
          <button class="btn-submit" @click="submitEdit" :disabled="submitting">{{ submitting ? '保存中...' : '保存' }}</button>
        </div>
      </div>
    </div>

    <!-- 版本管理弹窗 -->
    <div class="modal-overlay" v-if="showVersionForm" @click.self="closeVersionForm">
      <div class="modal-card modal-lg">
        <h3 class="modal-title">版本管理 - {{ versionItem?.paramName }}</h3>

        <div class="version-create-section">
          <h4>新建版本 (设置上下限)</h4>
          <div class="form-grid form-grid-4">
            <div class="form-field">
              <label>USL (规格上限)</label>
              <input v-model.number="newVersion.usl" type="number" step="0.000001" class="form-input" placeholder="规格上限" />
            </div>
            <div class="form-field">
              <label>LSL (规格下限)</label>
              <input v-model.number="newVersion.lsl" type="number" step="0.000001" class="form-input" placeholder="规格下限" />
            </div>
            <div class="form-field">
              <label>Target (目标值)</label>
              <input v-model.number="newVersion.target" type="number" step="0.000001" class="form-input" placeholder="目标值" />
            </div>
            <div class="form-field">
              <label>图表类型</label>
              <select v-model="newVersion.chartType" class="form-input">
                <option value="XBAR_R">Xbar-R</option>
                <option value="XBAR_S">Xbar-S</option>
                <option value="I_MR">I-MR</option>
                <option value="P">P图</option>
              </select>
            </div>
            <div class="form-field">
              <label>UCL (控制上限)</label>
              <input v-model.number="newVersion.ucl" type="number" step="0.000001" class="form-input" placeholder="留空自动计算" />
            </div>
            <div class="form-field">
              <label>LCL (控制下限)</label>
              <input v-model.number="newVersion.lcl" type="number" step="0.000001" class="form-input" placeholder="留空自动计算" />
            </div>
            <div class="form-field">
              <label>变更原因</label>
              <input v-model="newVersion.changeReason" type="text" class="form-input" placeholder="变更原因说明" />
            </div>
            <div class="form-field">
              <label>子组大小</label>
              <input v-model.number="newVersion.subgroupSize" type="number" class="form-input" placeholder="5" min="1" />
            </div>
          </div>
          <div class="version-actions-inline">
            <button class="btn-submit btn-sm" @click="submitNewVersion" :disabled="versionSubmitting">{{ versionSubmitting ? '提交中...' : '发布新版本' }}</button>
          </div>
        </div>

        <div class="version-divider"></div>

        <h4>历史版本</h4>
        <div class="version-list" v-if="versionHistoryList.length > 0">
          <div v-for="v in versionHistoryList" :key="v.id" class="version-item" :class="{ current: v.isCurrent === 1, disabled: v.status === 0 }">
            <div class="version-item-header">
              <span class="v-no">V{{ v.versionNo }}</span>
              <span class="v-status" :class="v.isCurrent === 1 ? 'current-v' : ''">{{ v.isCurrent === 1 ? '● 当前生效' : '已归档' }}</span>
              <span class="v-status-tag" :class="v.status === 1 ? 'status-on' : 'status-off'">{{ v.status === 1 ? '启用' : '停用' }}</span>
              <span class="v-time">{{ v.effectiveFrom }}</span>
            </div>
            <div class="version-item-body">
              <span>USL={{ v.usl ?? '-' }} LSL={{ v.lsl ?? '-' }} Target={{ v.target ?? '-' }}</span>
              <span>| UCL={{ v.ucl ?? '-' }} LCL={{ v.lcl ?? '-' }}</span>
              <span v-if="v.changeReason" class="v-reason">原因: {{ v.changeReason }}</span>
            </div>
            <div class="version-item-actions">
              <button class="btn-action btn-edit" @click="handleEditVersion(v)">编辑</button>
              <button v-if="v.status === 0"
                      class="btn-action btn-enable" @click="handleEnableVersion(v)">启用</button>
              <button v-if="v.status === 1"
                      class="btn-action btn-disable" @click="handleDisableVersion(v)">停用</button>
              <button v-if="v.isCurrent !== 1"
                      class="btn-action btn-del" @click="handleDeleteVersion(v)">删除</button>
            </div>

            <div v-if="editingVersionId === v.id" class="version-edit-form">
              <div class="form-grid form-grid-4">
                <div class="form-field"><label>USL</label><input v-model.number="editVersionForm.usl" type="number" step="0.000001" class="form-input form-input-sm" /></div>
                <div class="form-field"><label>LSL</label><input v-model.number="editVersionForm.lsl" type="number" step="0.000001" class="form-input form-input-sm" /></div>
                <div class="form-field"><label>Target</label><input v-model.number="editVersionForm.target" type="number" step="0.000001" class="form-input form-input-sm" /></div>
                <div class="form-field"><label>UCL</label><input v-model.number="editVersionForm.ucl" type="number" step="0.000001" class="form-input form-input-sm" /></div>
                <div class="form-field"><label>LCL</label><input v-model.number="editVersionForm.lcl" type="number" step="0.000001" class="form-input form-input-sm" /></div>
                <div class="form-field"><label>CL</label><input v-model.number="editVersionForm.cl" type="number" step="0.000001" class="form-input form-input-sm" /></div>
                <div class="form-field"><label>图表类型</label>
                  <select v-model="editVersionForm.chartType" class="form-input form-input-sm">
                    <option value="XBAR_R">Xbar-R</option>
                    <option value="XBAR_S">Xbar-S</option>
                    <option value="I_MR">I-MR</option>
                    <option value="P">P图</option>
                  </select>
                </div>
                <div class="form-field"><label>子组大小</label><input v-model.number="editVersionForm.subgroupSize" type="number" class="form-input form-input-sm" min="1" /></div>
              </div>
              <div class="version-edit-actions">
                <button class="btn-submit btn-sm" @click="submitEditVersion" :disabled="versionSubmitting">{{ versionSubmitting ? '保存中...' : '保存修改' }}</button>
                <button class="btn-cancel btn-sm" @click="cancelEditVersion">取消</button>
              </div>
            </div>
          </div>
        </div>
        <div class="empty-state" v-else><p>暂无版本记录</p></div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeVersionForm">关闭</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { adminApi, spcApi } from '@/utils/api.js'

const list = ref([])
const total = ref(0)
const current = ref(1)
const pageSize = ref(15)
const loading = ref(false)

const keyword = ref('')
const filterProductId = ref('')
const filterProcessId = ref('')

const productList = ref([])
const processList = ref([])
const versionMap = ref({})

const filteredProcessList = computed(() => {
  if (!filterProductId.value) return processList.value
  return processList.value.filter(p => p.productId === Number(filterProductId.value))
})

const showBatchForm = ref(false)
const showEditForm = ref(false)
const showVersionForm = ref(false)

const submitting = ref(false)
const versionSubmitting = ref(false)
const formMsg = ref('')
const formMsgType = ref('')

const editId = ref(null)
const versionItem = ref(null)
const versionHistoryList = ref([])

const batchForm = ref({
  processId: null,
  rows: [
    { paramCode: '', paramName: '', unit: '', dataType: '', decimalPlaces: 3 }
  ]
})

const editForm = ref({
  paramCode: '', paramName: '', unit: '',
  dataType: '', decimalPlaces: 3, status: 1
})

const newVersion = ref({
  usl: null, lsl: null, target: null,
  ucl: null, lcl: null,
  chartType: 'XBAR_R',
  subgroupSize: 5,
  changeReason: ''
})

const editingVersionId = ref(null)
const editVersionForm = ref({
  usl: null, lsl: null, target: null,
  ucl: null, lcl: null, cl: null,
  chartType: 'XBAR_R', subgroupSize: 5
})

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

const validRowCount = computed(() =>
  batchForm.value.rows.filter(r => r.paramCode?.trim() && r.paramName?.trim()).length
)

function getProcessName(processId) {
  if (!processId) return '-'
  const p = processList.value.find(x => x.id === processId)
  return p ? p.processName : '-'
}

function getVersionNo(paramId) {
  return versionMap.value[paramId]?.versionNo || null
}

function getVersionInfo(paramId) {
  return versionMap.value[paramId] || null
}

async function loadProcesses() {
  try {
    const res = await adminApi.process.getPage({ current: 1, size: 200 })
    if (res.code === 200 && res.data) {
      processList.value = res.data.records || []
    }
  } catch (e) { console.error('加载工序失败:', e) }
}

async function loadProducts() {
  try {
    const res = await adminApi.product.getPage({ current: 1, size: 100 })
    if (res.code === 200 && res.data) {
      productList.value = res.data.records || []
    }
  } catch (e) { console.error('加载产品失败:', e) }
}

function onProductChange() {
  filterProcessId.value = ''
  loadData()
}

async function loadVersionsForList() {
  if (!list.value.length) return
  try {
    for (const item of list.value) {
      if (item.productId) continue
      try {
        const res = await spcApi.getParamVersionCurrent({
          paramId: item.id,
          productId: filterProductId.value || undefined
        })
        if (res.code === 200 && res.data) {
          versionMap.value[item.id] = res.data
        }
      } catch (e) { }
    }
  } catch (e) { console.error('加载版本信息失败:', e) }
}

async function loadData() {
  loading.value = true
  versionMap.value = {}
  try {
    const params = {
      current: current.value,
      size: pageSize.value,
      keyword: keyword.value || undefined
    }
    const res = await adminApi.standard.getPage(params)
    if (res.code === 200 && res.data) {
      list.value = res.data.records || []
      total.value = res.data.total || 0
      if (filterProcessId.value) {
        list.value = list.value.filter(p => p.processId === Number(filterProcessId.value))
      }
      await loadVersionsForList()
    }
  } catch (e) {
    console.error('加载标准列表失败:', e)
  } finally {
    loading.value = false
  }
}

function openBatchAdd() {
  batchForm.value = {
    processId: filterProcessId.value ? Number(filterProcessId.value) : null,
    rows: [{ paramCode: '', paramName: '', unit: '', dataType: '', decimalPlaces: 3 }]
  }
  formMsg.value = ''
  showBatchForm.value = true
}

function closeBatchForm() {
  showBatchForm.value = false
}

function addBatchRow() {
  batchForm.value.rows.push({ paramCode: '', paramName: '', unit: '', dataType: '', decimalPlaces: 3 })
}

function removeBatchRow(idx) {
  if (batchForm.value.rows.length <= 1) return
  batchForm.value.rows.splice(idx, 1)
}

function onBatchProcessChange() {
  batchForm.value.rows.forEach(row => {
    row.paramCode = ''
    row.paramName = ''
  })
}

async function submitBatchAdd() {
  const validRows = batchForm.value.rows.filter(r => r.paramCode?.trim() && r.paramName?.trim())
  if (!validRows.length) { formMsg.value = '至少填写一条有效记录'; formMsgType.value = 'error'; return }
  if (!batchForm.value.processId) { formMsg.value = '请选择工序'; formMsgType.value = 'error'; return }

  submitting.value = true
  formMsg.value = ''

  try {
    const payloads = validRows.map(row => ({
      ...row,
      processId: batchForm.value.processId,
      status: 1
    }))

    const results = []
    for (const payload of payloads) {
      try {
        const res = await adminApi.standard.create(payload)
        results.push(res)
      } catch (e) {
        results.push({ code: 500, msg: e.message })
      }
    }

    const successCount = results.filter(r => r.code === 200).length
    const failCount = results.length - successCount

    if (failCount === 0) {
      formMsg.value = `成功添加 ${successCount} 条标准`
      formMsgType.value = 'success'
      setTimeout(() => { closeBatchForm(); loadData() }, 1200)
    } else {
      formMsg.value = `成功 ${successCount} 条，失败 ${failCount} 条`
      formMsgType.value = 'error'
    }
  } catch (e) {
    formMsg.value = e.message || '网络错误'
    formMsgType.value = 'error'
  } finally {
    submitting.value = false
  }
}

function openEdit(item) {
  editId.value = item.id
  editForm.value = {
    paramCode: item.paramCode,
    paramName: item.paramName,
    unit: item.unit || '',
    dataType: item.dataType || '',
    decimalPlaces: item.decimalPlaces ?? 3,
    status: item.status
  }
  formMsg.value = ''
  showEditForm.value = true
}

function closeEditForm() {
  showEditForm.value = false
}

async function submitEdit() {
  if (!editForm.value.paramName?.trim()) { formMsg.value = '参数名称为必填项'; formMsgType.value = 'error'; return }

  submitting.value = true
  formMsg.value = ''

  try {
    const res = await adminApi.standard.update(editId.value, editForm.value)
    if (res.code === 200) {
      formMsg.value = '更新成功'
      formMsgType.value = 'success'
      setTimeout(() => { closeEditForm(); loadData() }, 1000)
    } else {
      formMsg.value = res.msg || '更新失败'
      formMsgType.value = 'error'
    }
  } catch (e) {
    formMsg.value = e.message || '网络错误'
    formMsgType.value = 'error'
  } finally {
    submitting.value = false
  }
}

async function openVersionManage(item) {
  versionItem.value = item
  versionHistoryList.value = []
  newVersion.value = {
    usl: null, lsl: null, target: null,
    ucl: null, lcl: null,
    chartType: 'XBAR_R',
    subgroupSize: 5,
    changeReason: ''
  }
  formMsg.value = ''
  showVersionForm.value = true

  try {
    const res = await spcApi.getParamVersionHistory({
      paramId: item.id,
      productId: filterProductId.value || undefined
    })
    if (res.code === 200 && Array.isArray(res.data)) {
      versionHistoryList.value = res.data
    }
  } catch (e) { console.error('加载版本历史失败:', e) }
}

function closeVersionForm() {
  showVersionForm.value = false
}

async function submitNewVersion() {
  if (!versionItem.value) return

  versionSubmitting.value = true
  try {
    const payload = {
      ...newVersion.value,
      paramId: versionItem.value.id,
      productId: filterProductId.value || undefined,
      changeType: 'LIMIT_ADJUST',
      isCurrent: 1
    }

    const res = await adminApi.paramVersion.create(payload)
    if (res.code === 200) {
      await openVersionManage(versionItem.value)
    } else {
      formMsg.value = res.msg || '创建版本失败'
      formMsgType.value = 'error'
    }
  } catch (e) {
    formMsg.value = e.message || '网络错误'
    formMsgType.value = 'error'
  } finally {
    versionSubmitting.value = false
  }
}

async function handleDuplicate(item) {
  if (!confirm(`确定复制工艺参数 "${item.paramName}" (${item.paramCode}) 吗？\n复制后将生成新的参数编码和名称。`)) return

  try {
    const res = await adminApi.standard.duplicate(item.id)
    if (res.code === 200) {
      loadData()
    }
  } catch (e) {
    console.error('复制失败:', e)
  }
}

async function handleDelete(item) {
  if (!confirm(`确定删除标准 "${item.paramName}" (${item.paramCode}) 吗？\n关联的测量数据不会删除。`)) return

  try {
    const res = await adminApi.standard.delete(item.id)
    if (res.code === 200) {
      loadData()
    }
  } catch (e) {
    console.error('删除失败:', e)
  }
}

async function handleEnableVersion(v) {
  try {
    const res = await adminApi.paramVersion.enable(v.id)
    if (res.code === 200) {
      await openVersionManage(versionItem.value)
    }
  } catch (e) { console.error('启用版本失败:', e) }
}

async function handleDisableVersion(v) {
  if (!confirm(`确定停用版本 V${v.versionNo} 吗？`)) return
  try {
    const res = await adminApi.paramVersion.disable(v.id)
    if (res.code === 200) {
      await openVersionManage(versionItem.value)
    }
  } catch (e) { alert(e.message || '停用失败: ' + (e.response?.data?.msg || '')) }
}

async function handleDeleteVersion(v) {
  if (!confirm(`确定删除版本 V${v.versionNo} 吗？此操作不可恢复！`)) return
  try {
    const res = await adminApi.paramVersion.delete(v.id)
    if (res.code === 200) {
      await openVersionManage(versionItem.value)
    }
  } catch (e) { alert(e.message || '删除失败: ' + (e.response?.data?.msg || '')) }
}

function handleEditVersion(v) {
  editingVersionId.value = v.id
  editVersionForm.value = {
    usl: v.usl, lsl: v.lsl, target: v.target,
    ucl: v.ucl, lcl: v.lcl, cl: v.cl,
    chartType: v.chartType || 'XBAR_R', subgroupSize: v.subgroupSize
  }
}

function cancelEditVersion() {
  editingVersionId.value = null
}

async function submitEditVersion() {
  if (!editingVersionId.value) return
  versionSubmitting.value = true
  try {
    const res = await adminApi.paramVersion.update(editingVersionId.value, editVersionForm.value)
    if (res.code === 200) {
      editingVersionId.value = null
      await openVersionManage(versionItem.value)
    }
  } catch (e) { alert(e.message || '保存失败: ' + (e.response?.data?.msg || '')) } finally {
    versionSubmitting.value = false
  }
}

function hasEnabledSibling(v) {
  return versionHistoryList.value.some(sib =>
    sib.id !== v.id && sib.status === 1
  )
}

onMounted(async () => {
  await Promise.all([loadProducts(), loadProcesses()])
  await loadData()
})
</script>

<style scoped>
.mgmt-section {
  background: var(--bg-secondary);
  border-radius: 16px;
  padding: 20px;
  border: 1px solid var(--border-color);
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  flex-wrap: wrap;
  gap: 10px;
}

.search-bar {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}

.toolbar-actions {
  display: flex;
  gap: 8px;
}

.search-input {
  padding: 8px 14px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary); font-size: 13px; width: 220px; outline: none;
}
.search-input:focus { border-color: var(--accent-primary); box-shadow: 0 0 0 3px rgba(var(--accent-rgb), 0.12); }

.filter-select-sm {
  padding: 8px 10px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary); font-size: 13px; cursor: pointer;
}

.btn-search {
  padding: 8px 18px; background: var(--accent-primary); color: white;
  border: none; border-radius: 8px; font-size: 13px; font-weight: 500; cursor: pointer;
}
.btn-search:hover { opacity: 0.9; }

.btn-batch-add {
  padding: 8px 18px;
  background: linear-gradient(135deg, #722ed1, #531dab);
  color: white; border: none; border-radius: 8px;
  font-size: 13px; font-weight: 600; cursor: pointer;
  transition: all 0.25s;
}
.btn-batch-add:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(114,46,209,0.35); }

.table-wrap { overflow-x: auto; }

.data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.data-table th {
  background: var(--bg-tertiary); color: var(--text-secondary); font-weight: 600;
  padding: 10px 12px; text-align: left; white-space: nowrap;
  border-bottom: 2px solid var(--border-color);
}
.data-table td {
  padding: 10px 12px; border-bottom: 1px solid var(--border-color);
  color: var(--text-primary); white-space: nowrap;
}
.data-table tr:hover td { background: rgba(var(--accent-rgb), 0.04); }

.limits-cell { font-family: monospace; font-size: 11px; color: var(--text-tertiary); }

.unit-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  font-size: 11px; background: rgba(82,196,26,0.1); color: #52c41a; font-weight: 600;
}

.process-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  font-size: 11px; background: rgba(114,46,209,0.08); color: #722ed1;
}

.status-tag {
  padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600;
}
.status-tag.on { background: #f6ffed; color: #389e0d; }
.status-tag.off { background: #f5f5f5; color: #999; }

.actions { white-space: nowrap; }

.btn-action {
  padding: 4px 10px; border: none; border-radius: 6px;
  font-size: 12px; cursor: pointer; margin-right: 3px; transition: all 0.2s;
}
.btn-edit { background: #e6f7ff; color: #1890ff; }
.btn-edit:hover { background: #bae7ff; }
.btn-copy { background: #e6f7ff; color: #096dd9; }
.btn-copy:hover { background: #91d5ff; }

.btn-version { background: #fff7e6; color: #fa8c16; }
.btn-version:hover { background: #ffe7ba; }
.btn-del { background: #fff1f0; color: #cf1322; }
.btn-del:hover { background: #ffa39e; }

.empty-state { text-align: center; padding: 50px 20px; color: var(--text-tertiary); }
.empty-icon { font-size: 40px; display: block; margin-bottom: 10px; }

.pagination {
  display: flex; justify-content: center; align-items: center; gap: 12px;
  margin-top: 16px; padding-top: 16px; border-top: 1px solid var(--border-color);
}
.page-btn {
  padding: 6px 16px; border: 1px solid var(--border-color); border-radius: 8px;
  background: var(--bg-secondary); color: var(--text-primary); font-size: 13px; cursor: pointer;
}
.page-btn:hover:not(:disabled) { border-color: var(--accent-primary); color: var(--accent-primary); }
.page-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.page-info { font-size: 13px; color: var(--text-secondary); }

.modal-overlay {
  position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.45); backdrop-filter: blur(4px);
  display: flex; align-items: center; justify-content: center;
  z-index: 2000;
  padding-bottom: env(safe-area-inset-bottom);
}

.modal-card {
  background: var(--bg-modal); border-radius: 16px; padding: 24px;
  width: 520px; max-width: 90vw; max-height: 85vh; overflow-y: auto;
  box-shadow: 0 20px 60px rgba(0,0,0,0.3); border: 1px solid var(--border-color);
}

.modal-lg { width: 720px; }

.modal-title { font-size: 18px; font-weight: 700; color: var(--text-primary); margin-bottom: 18px; }

.form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.form-grid .full { grid-column: 1 / -1; }
.form-grid-4 { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }

.form-field { display: flex; flex-direction: column; gap: 4px; }
.form-field label { font-size: 12px; font-weight: 600; color: var(--text-secondary); }
.req { color: #cf1322; }

.form-input {
  padding: 8px 12px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary); font-size: 13px; outline: none;
}
.form-input:focus { border-color: var(--accent-primary); box-shadow: 0 0 0 3px rgba(var(--accent-rgb), 0.12); }
.form-input:disabled { opacity: 0.6; cursor: not-allowed; }

.form-input-sm {
  padding: 6px 8px; border: 1px solid var(--border-input); border-radius: 6px;
  background: var(--bg-input); color: var(--text-primary); font-size: 12px; outline: none; width: 100%;
}
.form-input-sm:focus { border-color: var(--accent-primary); }

.form-msg {
  margin-top: 12px; padding: 8px 12px; border-radius: 8px;
  font-size: 13px; text-align: center;
}
.form-msg.error { background: #fff1f0; color: #cf1322; }
.form-msg.success { background: #f6ffed; color: #389e0d; }

.modal-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 18px; }

.btn-cancel {
  padding: 8px 20px; border: 1px solid var(--border-color); border-radius: 8px;
  background: transparent; color: var(--text-secondary); font-size: 13px; cursor: pointer;
}
.btn-cancel:hover { border-color: var(--text-secondary); }

.btn-submit {
  padding: 8px 24px; border: none; border-radius: 8px;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 85%, white));
  color: white; font-size: 13px; font-weight: 600; cursor: pointer;
}
.btn-submit:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(var(--accent-rgb), 0.35); }
.btn-submit:disabled { opacity: 0.6; cursor: not-allowed; }
.btn-sm { padding: 6px 16px; font-size: 12px; }

.batch-form-section { display: flex; flex-direction: column; gap: 14px; }

.batch-items-label {
  display: flex; justify-content: space-between; align-items: center;
  font-size: 13px; font-weight: 600; color: var(--text-primary);
}

.btn-add-row {
  padding: 4px 12px; background: #e6f7ff; color: #1890ff; border: 1px solid #91d5ff;
  border-radius: 6px; font-size: 12px; cursor: pointer;
}
.btn-add-row:hover { background: #bae7ff; }

.batch-table-wrap { overflow-x: auto; }

.batch-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.batch-table th {
  background: var(--bg-tertiary); color: var(--text-secondary); font-weight: 600;
  padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border-color);
}
.batch-table td { padding: 6px 8px; border-bottom: 1px solid var(--border-color); }
.row-num { color: var(--text-tertiary); font-size: 11px; text-align: center; }

.btn-remove-row {
  width: 24px; height: 24px; border: 1px solid #cf1322; border-radius: 50%;
  background: #fff1f0; color: #cf1322; font-size: 14px; cursor: pointer;
  line-height: 1; display: inline-flex; align-items: center; justify-content: center;
}
.btn-remove-row:hover:not(:disabled) { background: #cf1322; color: white; }
.btn-remove-row:disabled { opacity: 0.3; cursor: not-allowed; }

.batch-example {
  display: flex; gap: 8px; padding: 10px 14px;
  background: rgba(114,46,209,0.05); border: 1px dashed rgba(114,46,209,0.25);
  border-radius: 8px; font-size: 12px; flex-wrap: wrap;
}
.example-label { font-weight: 600; color: #722ed1; white-space: nowrap; }
.example-text { color: var(--text-secondary); }

.version-create-section { margin-bottom: 8px; }
.version-create-section h4 { font-size: 14px; color: var(--text-primary); margin-bottom: 12px; }
.version-actions-inline { margin-top: 10px; }

.version-divider {
  height: 1px; background: var(--border-color); margin: 20px 0;
}

.version-list { display: flex; flex-direction: column; gap: 8px; max-height: 300px; overflow-y: auto; }

.version-item {
  padding: 10px 14px; border-radius: 8px; border: 1px solid var(--border-color);
  background: var(--bg-tertiary);
}
.version-item.current { border-color: #52c41a; background: rgba(82,196,26,0.04); }
.version-item.disabled { opacity: 0.65; }

.version-item-header { display: flex; align-items: center; gap: 10px; margin-bottom: 4px; }
.v-no {
  background: var(--accent-primary); color: white; padding: 2px 8px;
  border-radius: 4px; font-size: 11px; font-weight: 600;
}
.v-status { font-size: 11px; }
.v-status.current-v { color: #52c41a; font-weight: 600; }
.v-status-tag {
  font-size: 10px; padding: 1px 6px; border-radius: 3px; font-weight: 600;
}
.v-status-tag.status-on { background: rgba(82,196,26,0.12); color: #52c41a; }
.v-status-tag.status-off { background: rgba(255,77,79,0.1); color: #ff4d4f; }
.v-time { font-size: 11px; color: var(--text-tertiary); margin-left: auto; }

.version-item-body {
  font-size: 12px; color: var(--text-secondary); font-family: monospace;
  display: flex; gap: 12px; flex-wrap: wrap;
}
.v-reason { color: #faad14 !important; font-family: inherit !important; }

.version-item-actions {
  display: flex; gap: 8px; margin-top: 8px;
  padding-top: 8px; border-top: 1px dashed var(--border-color);
}
.version-item-actions .btn-action {
  padding: 3px 12px; font-size: 11px; border-radius: 5px;
}
.version-item-actions .btn-enable {
  background: rgba(82,196,26,0.08); color: #52c41a; border: 1px solid rgba(82,196,26,0.3);
}
.version-item-actions .btn-enable:hover { background: rgba(82,196,26,0.16); }
.version-item-actions .btn-disable {
  background: rgba(255,77,79,0.06); color: #ff4d4f; border: 1px solid rgba(255,77,79,0.25);
}
.version-item-actions .btn-disable:hover { background: rgba(255,77,79,0.12); }

.version-edit-form {
  margin-top: 10px;
  padding: 14px;
  background: rgba(24,144,255,0.03);
  border: 1px solid rgba(24,144,255,0.15);
  border-radius: 8px;
}
.version-edit-form .form-grid-4 {
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
  margin-bottom: 10px;
}
.version-edit-form .form-field label {
  font-size: 11px;
  color: var(--text-secondary);
}
.form-input-sm {
  padding: 5px 8px;
  font-size: 12px;
}
.version-edit-actions {
  display: flex;
  gap: 8px;
}

@media (max-width: 768px) {
  .toolbar { flex-direction: column; align-items: stretch; gap: 8px; }
  .search-bar { flex-direction: column; gap: 6px; }
  .search-input { width: 100%; box-sizing: border-box; }
  .filter-select-sm { width: 100%; box-sizing: border-box; }
  .btn-search, .btn-create { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .btn-search { order: 3; }
  .form-grid, .form-grid-4 { grid-template-columns: 1fr; }
  .data-table { font-size: 12px; overflow-x: auto; display: block; white-space: nowrap; }
  .data-table th, .data-table td { padding: 8px 10px; min-width: 80px; }
  .actions { display: flex; gap: 4px; flex-wrap: wrap; }
  .btn-action { padding: 4px 10px; font-size: 11px; flex: 1; text-align: center; min-width: 60px; }
  .pagination { justify-content: center; flex-wrap: wrap; gap: 6px; }
  .page-btn { padding: 6px 14px; font-size: 12px; }
  .modal-actions { flex-direction: column-reverse; width: 100%; }
  .btn-cancel, .btn-submit { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .modal-card { width: 95vw; padding: 16px; padding-bottom: 100px; max-height: calc(100vh - 40px); overflow-y: auto; }
  .batch-table th, .batch-table td { padding: 5px 6px; min-width: 60px; }
}
</style>