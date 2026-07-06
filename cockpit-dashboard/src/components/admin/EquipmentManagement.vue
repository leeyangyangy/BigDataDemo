<template>
  <div class="mgmt-section">
    <div class="toolbar">
      <div class="search-bar">
        <select v-model="filterProcessId" class="filter-select-sm" @change="loadData">
          <option value="">全部工序</option>
          <option v-for="p in processList" :key="p.id" :value="p.id">{{ p.processName }} ({{ p.processCode }})</option>
        </select>
        <input v-model="keyword" type="text" class="search-input" placeholder="搜索设备编码/名称/型号..." @keyup.enter="loadData" />
        <button class="btn-search" @click="loadData">查询</button>
      </div>
      <div class="toolbar-actions">
        <button class="btn-batch-add" @click="openAdd">+ 新增设备</button>
      </div>
    </div>

    <div class="table-wrap" v-if="list.length > 0">
<!--      TODO 样式带变更-->
      <table class="data-table">
        <thead>
          <tr>
            <th>设备编码</th>
            <th>设备名称</th>
            <th>设备类型</th>
            <th>型号</th>
            <th>所属工序</th>
            <th>位置</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id">
            <td><strong>{{ item.equipCode }}</strong></td>
            <td>{{ item.equipName }}</td>
            <td><span class="type-tag">{{ item.equipType || '-' }}</span></td>
            <td>{{ item.equipModel || '-' }}</td>
            <td><span class="process-tag">{{ getProcessName(item.processId) }}</span></td>
            <td>{{ item.location || '-' }}</td>
            <td>
              <span class="status-badge" :class="getStatusClass(item.status)">{{ item.status || '正常' }}</span>
            </td>
            <td class="action-cell">
              <button class="btn-action edit" @click="openEdit(item)">编辑</button>
              <button class="btn-action delete" @click="handleDelete(item)" :disabled="deleting">删除</button>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="pagination-bar" v-if="total > pageSize">
        <span class="total-info">共 {{ total }} 条</span>
        <div class="page-btns">
          <button @click="goPage(currentPage - 1)" :disabled="currentPage <= 1">上一页</button>
          <span class="page-num">{{ currentPage }} / {{ totalPages }}</span>
          <button @click="goPage(currentPage + 1)" :disabled="currentPage >= totalPages">下一页</button>
        </div>
      </div>
    </div>

    <div class="empty-state" v-else-if="!loading">
      <p>暂无设备数据，点击「新增设备」添加</p>
    </div>

    <div class="loading-overlay" v-if="loading">
      <div class="spinner"></div>
    </div>

    <!-- 新增/编辑弹窗 -->
    <div class="modal-overlay" v-if="showForm">
      <div class="modal-card form-modal">
        <h3>{{ isEdit ? '编辑设备' : '新增设备' }}</h3>

        <div class="form-grid-4">
          <div class="form-field">
            <label>设备编码 <span class="req">*</span></label>
            <input v-model.trim="form.equipCode" type="text" class="form-input" placeholder="如: EQ-001" :disabled="isEdit" />
          </div>
          <div class="form-field">
            <label>设备名称 <span class="req">*</span></label>
            <input v-model.trim="form.equipName" type="text" class="form-input" placeholder="如: 贴片机A" />
          </div>
          <div class="form-field">
            <label>设备类型</label>
            <select v-model="form.equipType" class="form-input">
              <option value="">请选择</option>
              <option value="贴片机">贴片机</option>
              <option value="键合机">键合机</option>
              <option value="塑封机">塑封机</option>
              <option value="测试仪">测试仪</option>
              <option value="其他">其他</option>
            </select>
          </div>
          <div class="form-field">
            <label>型号</label>
            <input v-model.trim="form.equipModel" type="text" class="form-input" placeholder="如: ASM-Eagle60" />
          </div>
          <div class="form-field">
            <label>所属工序</label>
            <select v-model.number="form.processId" class="form-input">
              <option :value="null">请选择</option>
              <option v-for="p in processList" :key="p.id" :value="p.id">{{ p.processName }}</option>
            </select>
          </div>
          <div class="form-field">
            <label>位置</label>
            <input v-model.trim="form.location" type="text" class="form-input" placeholder="如: A区3号工位" />
          </div>
          <div class="form-field">
            <label>状态</label>
            <select v-model="form.status" class="form-input">
              <option value="正常">正常</option>
              <option value="维修中">维修中</option>
              <option value="停用">停用</option>
              <option value="报废">报废</option>
            </select>
          </div>
          <div class="form-field">
            <label>备注</label>
            <input v-model.trim="form.remark" type="text" class="form-input" placeholder="选填" />
          </div>
        </div>

        <div class="form-actions">
          <button class="btn-cancel" @click="closeForm">取消</button>
          <button class="btn-submit" @click="handleSubmit" :disabled="submitting">
            {{ submitting ? '提交中...' : (isEdit ? '保存修改' : '创建') }}
          </button>
        </div>
        <div class="form-error" v-if="errorMsg">{{ errorMsg }}</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { adminApi, spcApi } from '../../utils/api.js'
import { StatusCode } from '../../utils/statusCode.js'
import { StatusMsg } from '../../utils/statusMsg.js'

const list = ref([])
const processList = ref([])
const keyword = ref('')
const filterProcessId = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const total = ref(0)
const loading = ref(false)

const showForm = ref(false)
const isEdit = ref(false)
const submitting = ref(false)
const deleting = ref(false)
const errorMsg = ref('')

const form = ref({
  equipCode: '',
  equipName: '',
  equipType: '',
  equipModel: '',
  processId: null,
  location: '',
  status: '正常',
  remark: ''
})
let editingId = null

function resetForm() {
  form.value = {
    equipCode: '', equipName: '', equipType: '',
    equipModel: '', processId: null, location: '', status: '正常', remark: ''
  }
  isEdit.value = false
  editingId = null
  errorMsg.value = ''
}

function openAdd() {
  resetForm()
  showForm.value = true
}

function openEdit(item) {
  resetForm()
  isEdit.value = true
  editingId = item.id
  form.value = {
    equipCode: item.equipCode,
    equipName: item.equipName,
    equipType: item.equipType || '',
    equipModel: item.equipModel || '',
    processId: item.processId,
    location: item.location || '',
    status: item.status || '正常',
    remark: item.remark || ''
  }
  showForm.value = true
}

function closeForm() {
  showForm.value = false
}

async function handleSubmit() {
  if (!form.value.equipCode || !form.value.equipName) {
    errorMsg.value = '设备编码和名称不能为空'
    return
  }
  errorMsg.value = ''
  submitting.value = true

  try {
    let res
    if (isEdit.value && editingId) {
      res = await adminApi.equipment.update(editingId, form.value)
    } else {
      res = await adminApi.equipment.create(form.value)
    }
    if (res.code === StatusCode.SUCCESS) {
      closeForm()
      loadData()
    } else {
      errorMsg.value = res.message || (isEdit.value ? StatusMsg.UPDATE_FAILED : StatusMsg.CREATE_FAILED)
    }
  } catch (e) {
    errorMsg.value = e.message || StatusMsg.REQUEST_FAILED
  } finally {
    submitting.value = false
  }
}

async function handleDelete(item) {
  // TODO 删除功能需要和后端一块联动，确认删除后需要更新前端列表
  if (!confirm(`确认删除设备 "${item.equipName}" 吗？`)) return
  deleting.value = true
  try {
    const res = await adminApi.equipment.delete(item.id)
    if (res.code === StatusCode.SUCCESS) loadData()
    else alert(res.message || StatusMsg.DELETE_FAILED)
  } catch (e) { alert(e.message) }
  finally { deleting.value = false }
}

async function loadProcesses() {
  try {
    const res = await spcApi.getProcessPage({ current: 1, size: 100 })
    if (res.code === StatusCode.SUCCESS) processList.value = res.data.records
  } catch (e) {}
}

async function loadData() {
  loading.value = true
  try {
    const params = {
      current: currentPage.value,
      size: pageSize.value,
      keyword: keyword.value || undefined,
      processId: filterProcessId.value || undefined
    }
    const res = await adminApi.equipment.getPage(params)
    if (res.code === StatusCode.SUCCESS) {
      list.value = res.data.records
      total.value = res.data.total
    }
  } catch (e) { console.error('加载设备列表失败', e) }
  finally { loading.value = false }
}

function goPage(page) {
  if (page < 1 || page > totalPages.value) return
  currentPage.value = page
  loadData()
}

const totalPages = computed(() => Math.max(1, Math.ceil(total.value / pageSize.value)))

function getProcessName(processId) {
  if (!processId) return '-'
  const p = processList.value.find(x => x.id === processId)
  return p ? p.processName : `ID:${processId}`
}

function getStatusClass(status) {
  switch (status) {
    case '正常': return 'status-ok'
    case '维修中': return 'status-warn'
    case '停用': return 'status-disabled'
    case '报废': return 'status-error'
    default: return ''
  }
}

import { computed } from 'vue'

onMounted(() => {
  loadProcesses()
  loadData()
})
</script>

<style scoped>
.mgmt-section { padding: 20px; }

.toolbar {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 18px; flex-wrap: wrap; gap: 12px;
}
.search-bar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.filter-select-sm {
  padding: 7px 10px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary);
  font-size: 13px; min-width: 140px; outline: none;
}
.filter-select-sm:focus { border-color: var(--accent-primary); }

.search-input {
  padding: 7px 14px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary);
  font-size: 13px; width: 220px; outline: none; transition: border-color .2s;
}
.search-input:focus { border-color: var(--accent-primary); box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.08); }

.btn-search {
  padding: 7px 16px; border: none; border-radius: 8px;
  background: linear-gradient(135deg, #1890ff, #096dd9); color: white;
  font-size: 13px; font-weight: 600; cursor: pointer;
  transition: all .2s;
}
.btn-search:hover { transform: translateY(-1px); box-shadow: 0 3px 10px rgba(24,144,255,0.35); }

.toolbar-actions { display: flex; gap: 8px; }
.btn-batch-add {
  padding: 7px 18px; border: none; border-radius: 8px;
  background: linear-gradient(135deg, #52c41a, #389e0d); color: white;
  font-size: 13px; font-weight: 600; cursor: pointer;
  transition: all .2s;
}
.btn-batch-add:hover { transform: translateY(-1px); box-shadow: 0 3px 10px rgba(82,196,26,0.35); }

.table-wrap { overflow-x: auto; border-radius: 12px; border: 1px solid var(--border-color); }
.data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.data-table thead th {
  padding: 11px 14px; text-align: left; font-weight: 600;
  background: linear-gradient(135deg, rgba(24,144,255,0.06), rgba(114,46,209,0.04));
  color: var(--text-secondary); white-space: nowrap; border-bottom: 2px solid var(--border-color);
}
.data-table tbody td {
  padding: 10px 14px; border-bottom: 1px solid var(--border-light);
  color: var(--text-primary); vertical-align: middle;
}
.data-table tbody tr:hover { background: rgba(24,144,255,0.03); }
.data-table tbody tr:last-child td { border-bottom: none; }

.type-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  background: rgba(24,144,255,0.08); color: #1890ff; font-size: 12px; font-weight: 500;
}
.process-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  background: rgba(82,196,26,0.08); color: #389e0d; font-size: 12px; font-weight: 500;
}

.status-badge {
  display: inline-block; padding: 2px 10px; border-radius: 10px;
  font-size: 11px; font-weight: 600; letter-spacing: .5px;
}
.status-ok { background: rgba(82,196,26,0.1); color: #389e0d; }
.status-warn { background: rgba(250,173,20,0.1); color: #d48806; }
.status-disabled { background: rgba(140,140,140,0.1); color: #8c8c8c; }
.status-error { background: rgba(245,34,45,0.1); color: #cf1322; }

.action-cell { white-space: nowrap; }
.btn-action {
  padding: 5px 12px; border: 1px solid var(--border-input); border-radius: 6px;
  background: transparent; cursor: pointer; font-size: 12px; margin-right: 6px;
  transition: all .15s;
}
.btn-action.edit { color: #1890ff; border-color: rgba(24,144,255,0.3); }
.btn-action.edit:hover { background: rgba(24,144,255,0.06); }
.btn-action.delete { color: #ff4d4f; border-color: rgba(255,77,79,0.3); }
.btn-action.delete:hover { background: rgba(255,77,79,0.06); }
.btn-action:disabled { opacity: .4; cursor: not-allowed; }

.pagination-bar {
  display: flex; justify-content: space-between; align-items: center;
  padding: 14px 18px; border-top: 1px solid var(--border-light);
}
.total-info { font-size: 13px; color: var(--text-secondary); }
.page-btns { display: flex; align-items: center; gap: 8px; }
.page-btns button {
  padding: 5px 14px; border: 1px solid var(--border-input); border-radius: 6px;
  background: var(--bg-card); color: var(--text-primary); cursor: pointer; font-size: 12px;
}
.page-btns button:disabled { opacity: .4; cursor: not-allowed; }
.page-num { font-size: 13px; color: var(--text-secondary); min-width: 50px; text-align: center; }

.empty-state {
  text-align: center; padding: 60px 20px; color: var(--text-tertiary);
  font-size: 14px; border-radius: 12px; border: 1px dashed var(--border-color);
}

.loading-overlay {
  position: absolute; inset: 0; z-index: 10;
  background: var(--bg-primary); opacity: .55; backdrop-filter: blur(2px);
  display: flex; align-items: center; justify-content: center; border-radius: 12px;
}
.spinner {
  width: 36px; height: 36px; border: 3px solid var(--border-input);
  border-top-color: var(--accent-primary); border-radius: 50%;
  animation: spin .65s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

.modal-overlay {
  position: fixed; inset: 0; z-index: 2000;
  background: rgba(0,0,0,.45); backdrop-filter: blur(4px);
  display: flex; align-items: center; justify-content: center;
  animation: fadeIn .2s ease-out;
  padding-bottom: env(safe-area-inset-bottom);
}
@keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }

.form-modal {
  width: 640px; max-width: 95vw; max-height: 90vh; overflow-y: auto;
  padding: 28px; border-radius: 16px; background: var(--bg-modal);
  box-shadow: var(--shadow-lg);
  animation: slideUp .28s ease-out;
}
@keyframes slideUp { from { transform: translateY(30px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }

.form-modal h3 {
  font-size: 17px; font-weight: 700; color: var(--text-primary);
  margin-bottom: 22px; padding-bottom: 14px; border-bottom: 1px solid var(--border-light);
}

.form-grid-4 {
  display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px;
}
.form-grid-4 .form-field { display: flex; flex-direction: column; gap: 5px; }
.form-grid-4 label {
  font-size: 12px; font-weight: 600; color: var(--text-secondary);
}
.req { color: #ff4d4f; }
.form-input {
  padding: 9px 12px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary);
  font-size: 13px; outline: none; transition: border-color .2s, box-shadow .2s;
}
.form-input:focus { border-color: var(--accent-primary); box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.1); }
.form-input:disabled { background: var(--bg-tertiary); color: var(--text-tertiary); cursor: not-allowed; }

.form-actions {
  display: flex; justify-content: flex-end; gap: 10px; margin-top: 22px;
  padding-top: 16px; border-top: 1px solid var(--border-light);
}
.btn-cancel {
  padding: 9px 22px; border: 1px solid var(--border-input); border-radius: 8px;
  background: transparent; color: var(--text-secondary); cursor: pointer; font-size: 13px;
}
.btn-cancel:hover { border-color: var(--accent-primary); color: var(--accent-primary); }
.btn-submit {
  padding: 9px 26px; border: none; border-radius: 8px;
  background: linear-gradient(135deg, #1890ff, #096dd9); color: white;
  cursor: pointer; font-size: 13px; font-weight: 600;
  transition: all .2s;
}
.btn-submit:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 4px 14px rgba(24,144,255,0.35); }
.btn-submit:disabled { opacity: .5; cursor: not-allowed; }

.form-error {
  margin-top: 10px; padding: 8px 12px; border-radius: 6px;
  background: rgba(255,77,79,0.06); color: #cf1322; font-size: 12px;
}

@media (max-width: 768px) {
  .toolbar { flex-direction: column; align-items: stretch; gap: 8px; }
  .search-bar { flex-direction: column; gap: 6px; }
  .search-input { width: 100%; box-sizing: border-box; }
  .filter-select-sm { width: 100%; box-sizing: border-box; }
  .btn-search, .btn-create { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .btn-search { order: 3; }
  .form-grid-4 { grid-template-columns: 1fr; }
  .data-table { font-size: 12px; overflow-x: auto; display: block; white-space: nowrap; }
  .data-table th, .data-table td { padding: 8px 10px; min-width: 80px; }
  .actions { display: flex; gap: 4px; flex-wrap: wrap; }
  .btn-action { padding: 4px 10px; font-size: 11px; flex: 1; text-align: center; min-width: 60px; }
  .pagination { justify-content: center; flex-wrap: wrap; gap: 6px; }
  .page-btn { padding: 6px 14px; font-size: 12px; }
  .form-actions { flex-direction: column-reverse; width: 100%; }
  .btn-cancel, .btn-submit { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .form-modal { width: 95vw; padding: 16px; padding-bottom: 100px; max-height: calc(100vh - 40px); }
}
</style>
