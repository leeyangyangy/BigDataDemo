<template>
  <div class="mgmt-section">
    <div class="toolbar">
      <div class="search-bar">
        <input v-model="keyword" type="text" class="search-input" placeholder="搜索产品编码/名称..." @keyup.enter="loadData" />
        <select v-model="filterStatus" class="filter-select-sm">
          <option value="">全部状态</option>
          <option :value="1">启用</option>
          <option :value="0">停用</option>
        </select>
        <button class="btn-search" @click="loadData">查询</button>
      </div>
      <button class="btn-create" @click="openCreate">+ 新增产品</button>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="list.length > 0">
        <thead>
          <tr>
            <th>产品编码</th>
            <th>产品名称</th>
            <th>类型</th>
            <th>规格</th>
            <th>状态</th>
            <th>创建时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id">
            <td><strong>{{ item.productCode }}</strong></td>
            <td>{{ item.productName }}</td>
            <td><span class="type-tag">{{ item.productType || '-' }}</span></td>
            <td class="text-muted">{{ item.specification || '-' }}</td>
            <td>
              <span class="status-tag" :class="item.status === 1 ? 'on' : 'off'">{{ item.status === 1 ? '启用' : '停用' }}</span>
            </td>
            <td class="text-muted text-sm">{{ formatTime(item.createdAt) }}</td>
            <td class="actions">
              <button class="btn-action btn-bind" @click="openBindProcess(item)">绑定工序</button>
              <button class="btn-action btn-edit" @click="openEdit(item)">编辑</button>
              <button class="btn-action btn-del" @click="handleDelete(item)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">📦</span>
        <p>暂无产品数据</p>
      </div>
    </div>

    <div class="pagination" v-if="total > pageSize">
      <button class="page-btn" :disabled="current <= 1" @click="current--; loadData()">上一页</button>
      <span class="page-info">{{ current }} / {{ totalPages }}</span>
      <button class="page-btn" :disabled="current >= totalPages" @click="current++; loadData()">下一页</button>
    </div>

    <!-- 弹窗 -->
    <div class="modal-overlay" v-if="showForm">
      <div class="modal-card">
        <h3 class="modal-title">{{ isEdit ? '编辑产品' : '新增产品' }}</h3>

        <div class="form-grid">
          <div class="form-field full">
            <label>产品编码 <span class="req">*</span></label>
            <input v-model="form.productCode" type="text" class="form-input" :disabled="isEdit" placeholder="如: P001" />
          </div>
          <div class="form-field full">
            <label>产品名称 <span class="req">*</span></label>
            <input v-model="form.productName" type="text" class="form-input" placeholder="请输入产品名称" />
          </div>
          <div class="form-field">
            <label>产品类型</label>
            <input v-model="form.productType" type="text" class="form-input" placeholder="如: 封测/光刻" />
          </div>
          <div class="form-field">
            <label>规格说明</label>
            <input v-model="form.specification" type="text" class="form-input" placeholder="规格参数描述" />
          </div>
        </div>

        <div class="form-msg" v-if="formMsg" :class="{ error: formMsgType === 'error', success: formMsgType === 'success' }">{{ formMsg }}</div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeForm">取消</button>
          <button class="btn-submit" @click="handleSubmit" :disabled="submitting">{{ submitting ? '提交中...' : (isEdit ? '保存' : '创建') }}</button>
        </div>
      </div>
    </div>

    <!-- 工序绑定弹窗 -->
    <div class="modal-overlay" v-if="showBindProcess">
      <div class="modal-card bind-modal">
        <h3 class="modal-title">绑定工序 - {{ bindProductName }}</h3>

        <div class="bind-hint">
          <span>选择该产品需要经过的工序（可多选，支持复用）</span>
        </div>

        <div class="bind-list" v-if="!bindLoading">
          <label
            v-for="p in allProcesses"
            :key="p.id"
            class="bind-item"
            :class="{ checked: selectedProcessIds.includes(p.id) }"
          >
            <input
              type="checkbox"
              :value="p.id"
              v-model="selectedProcessIds"
              class="bind-checkbox"
            />
            <div class="bind-info">
              <span class="bind-name">{{ p.processName }}</span>
              <span class="bind-code">{{ p.processCode }}</span>
              <span class="bind-type" v-if="p.processType">{{ p.processType }}</span>
            </div>
            <div class="bind-sort" v-if="selectedProcessIds.includes(p.id)">
              <label class="sort-label">排序</label>
              <input type="number" v-model.number="processSortMap[p.id]" class="sort-input" min="0" />
            </div>
          </label>
          <div class="bind-empty" v-if="allProcesses.length === 0">
            暂无可用工序，请先在工序管理中创建
          </div>
        </div>
        <div class="bind-loading" v-else>
          <span>加载中...</span>
        </div>

        <div class="form-msg" v-if="bindMsg" :class="{ error: bindMsgType === 'error', success: bindMsgType === 'success' }">{{ bindMsg }}</div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="showBindProcess = false">取消</button>
          <button class="btn-submit" @click="handleBindSubmit" :disabled="bindSubmitting">{{ bindSubmitting ? '保存中...' : '保存绑定' }}</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { adminApi, spcApi } from '@/utils/api.js'
import { StatusCode } from '@/utils/statusCode'
import { StatusMsg } from '@/utils/statusMsg'

const list = ref([])
const total = ref(0)
const current = ref(1)
const pageSize = ref(10)
const loading = ref(false)

const keyword = ref('')
const filterStatus = ref('')

const showForm = ref(false)
const isEdit = ref(false)
const editId = ref(null)
const submitting = ref(false)
const formMsg = ref('')
const formMsgType = ref('')

const form = ref({
  productCode: '', productName: '', productType: '', specification: '', status: 1
})

const showBindProcess = ref(false)
const bindProductId = ref(null)
const bindProductName = ref('')
const allProcesses = ref([])
const selectedProcessIds = ref([])
const processSortMap = ref({})
const bindLoading = ref(false)
const bindSubmitting = ref(false)
const bindMsg = ref('')
const bindMsgType = ref('')

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

async function loadData() {
  loading.value = true
  try {
    const res = await adminApi.product.getPage({
      current: current.value,
      size: pageSize.value,
      keyword: keyword.value,
      status: filterStatus.value !== '' ? Number(filterStatus.value) : undefined
    })
    if (res.code === StatusCode.SUCCESS && res.data) {
      list.value = res.data.records || []
      total.value = res.data.total || 0
    }
  } catch (e) {
    console.error('加载产品列表失败:', e)
  } finally {
    loading.value = false
  }
}

function openCreate() {
  isEdit.value = false
  editId.value = null
  form.value = { productCode: '', productName: '', productType: '', specification: '', status: 1 }
  formMsg.value = ''
  showForm.value = true
}

function openEdit(item) {
  isEdit.value = true
  editId.value = item.id
  form.value = {
    productCode: item.productCode,
    productName: item.productName,
    productType: item.productType || '',
    specification: item.specification || '',
    status: item.status
  }
  formMsg.value = ''
  showForm.value = true
}

function closeForm() {
  showForm.value = false
}

async function handleSubmit() {
  if (!form.value.productCode?.trim()) { formMsg.value = '产品编码为必填项'; formMsgType.value = 'error'; return }
  if (!form.value.productName?.trim()) { formMsg.value = '产品名称为必填项'; formMsgType.value = 'error'; return }

  submitting.value = true
  formMsg.value = ''

  try {
    let res
    if (isEdit.value) {
      res = await adminApi.product.update(editId.value, form.value)
    } else {
      res = await adminApi.product.create(form.value)
    }

    if (res.code === StatusCode.SUCCESS) {
      formMsg.value = isEdit.value ? StatusMsg.UPDATE_SUCCESS : StatusMsg.CREATE_SUCCESS
      formMsgType.value = 'success'
      setTimeout(() => { closeForm(); loadData() }, 1000)
    } else {
      formMsg.value = res.msg || StatusMsg.OPERATION_FAILED
      formMsgType.value = 'error'
    }
  } catch (e) {
    formMsg.value = e.message || '网络错误'
    formMsgType.value = 'error'
  } finally {
    submitting.value = false
  }
}

async function handleDelete(item) {
  if (!confirm(`确定删除产品 "${item.productName}" (${item.productCode}) 吗？`)) return

  try {
    const res = await adminApi.product.delete(item.id)
    if (res.code === StatusCode.SUCCESS) {
      loadData()
    }
  } catch (e) {
    console.error('删除失败:', e)
  }
}

async function openBindProcess(item) {
  bindProductId.value = item.id
  bindProductName.value = item.productName
  selectedProcessIds.value = []
  processSortMap.value = {}
  bindMsg.value = ''
  showBindProcess.value = true
  bindLoading.value = true

  try {
    const [processRes, boundRes] = await Promise.all([
      adminApi.process.getPage({ current: 1, size: 200 }),
      spcApi.getProductProcesses(item.id)
    ])

    if (processRes.code === StatusCode.SUCCESS) {
      allProcesses.value = processRes.data.records || []
    }

    if (boundRes.code === StatusCode.SUCCESS && boundRes.data) {
      selectedProcessIds.value = boundRes.data.map(b => b.processId)
      boundRes.data.forEach(b => {
        processSortMap.value[b.processId] = b.sortOrder || 0
      })
    }
  } catch (e) {
    console.error('加载工序数据失败:', e)
    bindMsg.value = '加载失败'
    bindMsgType.value = 'error'
  } finally {
    bindLoading.value = false
  }
}

async function handleBindSubmit() {
  bindSubmitting.value = true
  bindMsg.value = ''

  try {
    const items = selectedProcessIds.value.map((pid, idx) => ({
      processId: pid,
      isRequired: 1,
      sortOrder: processSortMap.value[pid] ?? idx
    }))

    const res = await spcApi.bindProductProcesses(bindProductId.value, items)

    if (res.code === StatusCode.SUCCESS) {
      bindMsg.value = StatusMsg.BIND_SUCCESS
      bindMsgType.value = 'success'
      setTimeout(() => { showBindProcess.value = false }, 1000)
    } else {
      bindMsg.value = res.msg || StatusMsg.BIND_FAILED
      bindMsgType.value = 'error'
    }
  } catch (e) {
    bindMsg.value = e.message || '网络错误'
    bindMsgType.value = 'error'
  } finally {
    bindSubmitting.value = false
  }
}

function formatTime(t) {
  if (!t) return ''
  return t.replace('T', ' ').substring(0, 16)
}

onMounted(() => {
  loadData()
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

.search-input {
  padding: 8px 14px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 13px;
  width: 220px;
  outline: none;
  transition: border-color 0.2s;
}
.search-input:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(var(--accent-rgb), 0.12);
}

.filter-select-sm {
  padding: 8px 10px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 13px;
  outline: none;
  cursor: pointer;
}

.btn-search {
  padding: 8px 18px;
  background: var(--accent-primary);
  color: white;
  border: none;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
}
.btn-search:hover { opacity: 0.9; transform: translateY(-1px); }

.btn-create {
  padding: 8px 18px;
  background: linear-gradient(135deg, #52c41a, #389e0d);
  color: white;
  border: none;
  border-radius: 8px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.25s;
}
.btn-create:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(82,196,26,0.35); }

.table-wrap { overflow-x: auto; }

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}
.data-table th {
  background: var(--bg-tertiary);
  color: var(--text-secondary);
  font-weight: 600;
  padding: 10px 14px;
  text-align: left;
  white-space: nowrap;
  border-bottom: 2px solid var(--border-color);
}
.data-table td {
  padding: 10px 14px;
  border-bottom: 1px solid var(--border-color);
  color: var(--text-primary);
  white-space: nowrap;
}
.data-table tr:hover td { background: rgba(var(--accent-rgb), 0.04); }

.text-muted { color: var(--text-tertiary); }
.text-sm { font-size: 12px; }

.type-tag {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 6px;
  font-size: 11px;
  background: rgba(var(--accent-rgb), 0.08);
  color: var(--accent-primary);
}

.status-tag {
  padding: 3px 12px;
  border-radius: 12px;
  font-size: 11px;
  font-weight: 600;
}
.status-tag.on { background: #f6ffed; color: #389e0d; }
.status-tag.off { background: #f5f5f5; color: #999; }

.actions { white-space: nowrap; }

.btn-action {
  padding: 4px 12px;
  border: none;
  border-radius: 6px;
  font-size: 12px;
  cursor: pointer;
  margin-right: 4px;
  transition: all 0.2s;
}
.btn-edit { background: #e6f7ff; color: #1890ff; }
.btn-edit:hover { background: #bae7ff; }
.btn-del { background: #fff1f0; color: #cf1322; }
.btn-del:hover { background: #ffa39e; }
.btn-bind { background: #f0f5ff; color: #2f54eb; }
.btn-bind:hover { background: #d6e4ff; }

.empty-state {
  text-align: center;
  padding: 50px 20px;
  color: var(--text-tertiary);
}
.empty-icon { font-size: 40px; display: block; margin-bottom: 10px; }

.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 12px;
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid var(--border-color);
}

.page-btn {
  padding: 6px 16px;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--bg-secondary);
  color: var(--text-primary);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
}
.page-btn:hover:not(:disabled) { border-color: var(--accent-primary); color: var(--accent-primary); }
.page-btn:disabled { opacity: 0.4; cursor: not-allowed; }

.page-info { font-size: 13px; color: var(--text-secondary); }

.modal-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.45);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 2000;
  padding-bottom: env(safe-area-inset-bottom);
}

.modal-card {
  background: var(--bg-modal);
  border-radius: 16px;
  padding: 24px;
  width: 500px;
  max-width: 90vw;
  box-shadow: 0 20px 60px rgba(0,0,0,0.3);
  border: 1px solid var(--border-color);
}

.modal-title {
  font-size: 18px;
  font-weight: 700;
  color: var(--text-primary);
  margin-bottom: 18px;
}

.form-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 14px;
}

.form-grid .full { grid-column: 1 / -1; }

.form-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.form-field label {
  font-size: 12px;
  font-weight: 600;
  color: var(--text-secondary);
}

.req { color: #cf1322; }

.form-input {
  padding: 8px 12px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 13px;
  outline: none;
  transition: border-color 0.2s;
}
.form-input:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(var(--accent-rgb), 0.12);
}
.form-input:disabled { opacity: 0.6; cursor: not-allowed; }

.form-msg {
  margin-top: 12px;
  padding: 8px 12px;
  border-radius: 8px;
  font-size: 13px;
  text-align: center;
}
.form-msg.error { background: #fff1f0; color: #cf1322; }
.form-msg.success { background: #f6ffed; color: #389e0d; }

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 18px;
}

.btn-cancel {
  padding: 8px 20px;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: transparent;
  color: var(--text-secondary);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
}
.btn-cancel:hover { border-color: var(--text-secondary); }

.btn-submit {
  padding: 8px 24px;
  border: none;
  border-radius: 8px;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 85%, white));
  color: white;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.25s;
}
.btn-submit:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(var(--accent-rgb), 0.35); }
.btn-submit:disabled { opacity: 0.6; cursor: not-allowed; }

.bind-modal { width: 560px; max-height: 80vh; overflow-y: auto; }

.bind-hint {
  font-size: 12px;
  color: var(--text-tertiary);
  margin-bottom: 14px;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  border-radius: 8px;
}

.bind-list {
  max-height: 360px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.bind-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border: 1px solid var(--border-color);
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s;
  background: var(--bg-secondary);
}
.bind-item:hover { border-color: var(--accent-primary); }
.bind-item.checked {
  border-color: var(--accent-primary);
  background: rgba(var(--accent-rgb), 0.06);
}

.bind-checkbox {
  width: 18px;
  height: 18px;
  accent-color: var(--accent-primary);
  cursor: pointer;
  flex-shrink: 0;
}

.bind-info {
  flex: 1;
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.bind-name {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
}

.bind-code {
  font-size: 11px;
  color: var(--text-tertiary);
  background: var(--bg-tertiary);
  padding: 1px 7px;
  border-radius: 4px;
}

.bind-type {
  font-size: 11px;
  color: var(--accent-primary);
  background: rgba(var(--accent-rgb), 0.08);
  padding: 1px 7px;
  border-radius: 4px;
}

.bind-sort {
  display: flex;
  align-items: center;
  gap: 4px;
  flex-shrink: 0;
}

.sort-label {
  font-size: 11px;
  color: var(--text-secondary);
  white-space: nowrap;
}

.sort-input {
  width: 50px;
  padding: 4px 6px;
  border: 1px solid var(--border-input);
  border-radius: 6px;
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 12px;
  text-align: center;
  outline: none;
}
.sort-input:focus { border-color: var(--accent-primary); }

.bind-empty {
  text-align: center;
  padding: 30px;
  color: var(--text-tertiary);
  font-size: 13px;
}

.bind-loading {
  text-align: center;
  padding: 40px;
  color: var(--text-tertiary);
}

@media (max-width: 768px) {
  .toolbar { flex-direction: column; align-items: stretch; gap: 8px; }
  .search-bar { flex-direction: column; gap: 6px; }
  .search-input { width: 100%; box-sizing: border-box; }
  .filter-select-sm { width: 100%; box-sizing: border-box; }
  .btn-search, .btn-create { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .btn-search { order: 3; }
  .form-grid { grid-template-columns: 1fr; }
  .data-table { font-size: 12px; overflow-x: auto; display: block; white-space: nowrap; }
  .data-table th, .data-table td { padding: 8px 10px; min-width: 80px; }
  .actions { display: flex; gap: 4px; flex-wrap: wrap; }
  .btn-action { padding: 4px 10px; font-size: 11px; flex: 1; text-align: center; min-width: 60px; }
  .pagination { justify-content: center; flex-wrap: wrap; gap: 6px; }
  .page-btn { padding: 6px 14px; font-size: 12px; }
  .page-info { font-size: 12px; }
  .modal-actions { flex-direction: column-reverse; width: 100%; }
  .btn-cancel, .btn-submit { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .modal-card { width: 95vw; padding: 16px; padding-bottom: 100px; max-height: calc(100vh - 40px); overflow-y: auto; }
}
</style>
