<template>
  <div class="mgmt-section">
    <div class="toolbar">
      <div class="search-bar">
        <input v-model="keyword" type="text" class="search-input" placeholder="搜索工号/姓名/手机号..." @keyup.enter="loadData" />
        <select v-model="filterRole" class="filter-select-sm">
          <option value="">全部角色</option>
          <option value="ADMIN">管理员</option>
          <option value="ENGINEER">工程师</option>
          <option value="OPERATOR">操作员</option>
          <option value="VIEWER">观察者</option>
        </select>
        <select v-model="filterStatus" class="filter-select-sm">
          <option value="">全部状态</option>
          <option :value="1">启用</option>
          <option :value="0">停用</option>
        </select>
        <button class="btn-search" @click="loadData">查询</button>
      </div>
      <button class="btn-create" @click="openCreate">+ 新增用户</button>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="list.length > 0">
        <thead>
          <tr>
            <th>工号</th>
            <th>姓名</th>
            <th>邮箱</th>
            <th>手机号</th>
            <th>角色</th>
            <th>主车间</th>
            <th>状态</th>
            <th>最后登录</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id">
            <td><strong>{{ item.empNo }}</strong></td>
            <td>{{ item.username }}</td>
            <td class="text-muted">{{ item.email || '-' }}</td>
            <td>{{ item.phone || '-' }}</td>
            <td><span class="role-tag" :class="'role-' + item.role">{{ roleMap[item.role] || item.role }}</span></td>
            <td>{{ getPrimaryWorkshopName(item) }}</td>
            <td>
              <span class="status-dot" :class="item.status === 1 ? 'on' : 'off'" @click="toggleStatus(item)"></span>
              {{ item.status === 1 ? '启用' : '停用' }}
            </td>
            <td class="text-muted text-sm">{{ formatTime(item.lastLoginAt) || '-' }}</td>
            <td class="actions">
              <button class="btn-action btn-edit" @click="openEdit(item)">编辑</button>
              <button class="btn-action btn-perm" @click="openWorkshopBinding(item)">车间权限</button>
              <button class="btn-action btn-del" @click="handleDelete(item)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">👥</span>
        <p>暂无用户数据</p>
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
        <h3 class="modal-title">{{ isEdit ? '编辑用户' : '新增用户' }}</h3>

        <div class="form-grid">
          <div class="form-field">
            <label>工号 <span class="req">*</span></label>
            <input v-model="form.empNo" type="text" class="form-input" :disabled="isEdit" placeholder="请输入工号" />
          </div>
          <div class="form-field">
            <label>姓名 <span class="req">*</span></label>
            <input v-model="form.username" type="text" class="form-input" placeholder="请输入姓名" />
          </div>
          <div class="form-field">
            <label>{{ isEdit ? '新密码(不填则不变)' : '密码' }} <span class="req" v-if="!isEdit">*</span></label>
            <input v-model="form.password" type="password" class="form-input" :placeholder="isEdit ? '留空则不修改密码' : '请输入密码'" />
          </div>
          <div class="form-field">
            <label>邮箱</label>
            <input v-model="form.email" type="email" class="form-input" placeholder="请输入邮箱" />
          </div>
          <div class="form-field">
            <label>手机号</label>
            <input v-model="form.phone" type="text" class="form-input" placeholder="请输入手机号" />
          </div>
          <div class="form-field">
            <label>角色</label>
            <select v-model="form.role" class="form-input">
              <option value="ADMIN">管理员</option>
              <option value="ENGINEER">工程师</option>
              <option value="OPERATOR">操作员</option>
              <option value="VIEWER">观察者</option>
            </select>
          </div>
          <div class="form-field">
            <label>状态</label>
            <select v-model="form.status" class="form-input">
              <option :value="1">启用</option>
              <option :value="0">停用</option>
            </select>
          </div>
        </div>
        <div class="form-msg" v-if="formMsg" :class="{ error: formMsgType === 'error', success: formMsgType === 'success' }">{{ formMsg }}</div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeForm">取消</button>
          <button class="btn-submit" @click="handleSubmit" :disabled="submitting">{{ submitting ? '提交中...' : (isEdit ? '保存' : '创建') }}</button>
        </div>
      </div>
    </div>

    <!-- 车间权限绑定弹窗 -->
    <div class="modal-overlay" v-if="showWorkshopForm">
      <div class="modal-card">
        <h3 class="modal-title">车间权限 - {{ editingUser.username }} ({{ editingUser.empNo }})</h3>

        <div class="form-field">
          <label>生产车间 (可多选, 单选主车间)</label>
          <div class="multi-workshop">
            <div v-for="w in productionWorkshops" :key="w.id" class="workshop-check">
              <label>
                <input type="checkbox" :value="w.id" v-model="workshopForm.workshopIds" />
                <span>{{ w.workshopName }}（{{ w.workshopCode }}）</span>
                <input
                  v-if="workshopForm.workshopIds.includes(w.id)"
                  type="radio"
                  name="primaryWorkshop"
                  :value="w.id"
                  v-model="workshopForm.primaryWorkshopId"
                  class="primary-radio"
                  title="设为主车间"
                />
                <span v-if="workshopForm.workshopIds.includes(w.id)" class="primary-label">主</span>
              </label>
            </div>
            <div v-if="productionWorkshops.length === 0" class="text-muted text-sm">暂无可绑定的生产车间</div>
          </div>
        </div>

        <div class="form-field">
          <label>测试站 (可多选)</label>
          <div class="multi-workshop">
            <div v-for="w in testStationWorkshops" :key="w.id" class="workshop-check">
              <label>
                <input type="checkbox" :value="w.id" v-model="workshopForm.testStationIds" />
                <span>{{ w.workshopName }}（{{ w.workshopCode }}）</span>
              </label>
            </div>
            <div v-if="testStationWorkshops.length === 0" class="text-muted text-sm">暂无可绑定的测试站</div>
          </div>
        </div>

        <div class="form-msg" v-if="workshopFormMsg" :class="{ error: workshopFormMsgType === 'error', success: workshopFormMsgType === 'success' }">{{ workshopFormMsg }}</div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeWorkshopForm">取消</button>
          <button class="btn-submit" @click="saveWorkshopBinding" :disabled="workshopSubmitting">{{ workshopSubmitting ? '保存中...' : '保存' }}</button>
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { adminApi } from '../../utils/api.js'
import { StatusCode } from '../../utils/statusCode.js'
import { StatusMsg } from '../../utils/statusMsg.js'

const list = ref([])
const total = ref(0)
const current = ref(1)
const pageSize = ref(10)
const loading = ref(false)

const keyword = ref('')
const filterRole = ref('')
const filterStatus = ref('')

const workshopList = ref([])
const userBindingsMap = ref({})

const showForm = ref(false)
const isEdit = ref(false)
const editId = ref(null)
const submitting = ref(false)
const formMsg = ref('')
const formMsgType = ref('')

const form = ref({
  empNo: '', username: '', password: '',
  email: '', phone: '', role: 'OPERATOR', status: 1
})

// 车间权限绑定状态
const showWorkshopForm = ref(false)
const editingUser = ref({})
const workshopSubmitting = ref(false)
const workshopFormMsg = ref('')
const workshopFormMsgType = ref('')
const workshopForm = ref({
  workshopIds: [],
  primaryWorkshopId: null,
  testStationIds: []
})

const productionWorkshops = computed(() =>
  workshopList.value.filter(w => w.workshopType !== '测试车间' && w.workshopType !== 'TEST')
)
const testStationWorkshops = computed(() =>
  workshopList.value.filter(w => w.workshopType === '测试车间' || w.workshopType === 'TEST')
)

// TODO 动态查询获取
const roleMap = {
  ADMIN: '管理员',
  ENGINEER: '工程师',
  OPERATOR: '操作员',
  VIEWER: '观察者'
}

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

async function loadData() {
  loading.value = true
  try {
    const [res, bindingsRes] = await Promise.all([
      adminApi.user.getPage({
        current: current.value,
        size: pageSize.value,
        keyword: keyword.value,
        role: filterRole.value,
        status: filterStatus.value !== '' ? Number(filterStatus.value) : undefined
      }),
      adminApi.userWorkshop.listUsers()
    ])
    if (res.code === StatusCode.SUCCESS && res.data) {
      list.value = res.data.records || []
      total.value = res.data.total || 0
    }
    if (bindingsRes.code === StatusCode.SUCCESS && bindingsRes.data) {
      const map = {}
      for (const u of bindingsRes.data) {
        map[u.id] = u
      }
      userBindingsMap.value = map
    }
  } catch (e) {
    console.error('加载用户列表失败:', e)
  } finally {
    loading.value = false
  }
}

function openCreate() {
  isEdit.value = false
  editId.value = null
  form.value = { empNo: '', username: '', password: '', email: '', phone: '', role: 'OPERATOR', status: 1 }
  formMsg.value = ''
  showForm.value = true
}

async function openEdit(item) {
  isEdit.value = true
  editId.value = item.id
  form.value = {
    empNo: item.empNo,
    username: item.username,
    password: '',
    email: item.email || '',
    phone: item.phone || '',
    role: item.role || 'OPERATOR',
    status: item.status
  }
  formMsg.value = ''
  showForm.value = true
}

function closeForm() {
  showForm.value = false
}

async function handleSubmit() {
  if (!form.value.empNo?.trim()) { formMsg.value = '工号为必填项'; formMsgType.value = 'error'; return }
  if (!form.value.username?.trim()) { formMsg.value = '姓名为必填项'; formMsgType.value = 'error'; return }
  if (!isEdit.value && !form.value.password) { formMsg.value = '密码为必填项'; formMsgType.value = 'error'; return }

  const payload = { ...form.value }
  if (isEdit.value && !payload.password) delete payload.password

  submitting.value = true
  formMsg.value = ''

  try {
    let res
    if (isEdit.value) {
      res = await adminApi.user.update(editId.value, payload)
    } else {
      res = await adminApi.user.create(payload)
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

async function toggleStatus(item) {
  try {
    const newStatus = item.status === 1 ? 0 : 1
    await adminApi.user.toggleStatus(item.id, newStatus)
    item.status = newStatus
  } catch (e) {
    console.error('切换状态失败:', e)
  }
}

async function handleDelete(item) {
  if (!confirm(`确定删除用户 "${item.username}" (${item.empNo}) 吗？`)) return

  try {
    const res = await adminApi.user.delete(item.id)
    if (res.code === StatusCode.SUCCESS) {
      loadData()
    }
  } catch (e) {
    console.error('删除失败:', e)
  }
}

function formatTime(t) {
  if (!t) return ''
  return t.replace('T', ' ').substring(0, 16)
}

function getWorkshopName(workshopId) {
  if (!workshopId) return '-'
  const w = workshopList.value.find(item => item.id === workshopId)
  return w ? w.workshopName : '-'
}

function getPrimaryWorkshopName(item) {
  const binding = userBindingsMap.value[item.id]
  if (binding && binding.primaryWorkshopId) {
    return getWorkshopName(binding.primaryWorkshopId)
  }
  return getWorkshopName(item.workshopId)
}

// 车间权限绑定
async function openWorkshopBinding(user) {
  editingUser.value = user
  workshopFormMsg.value = ''
  workshopForm.value = { workshopIds: [], primaryWorkshopId: null, testStationIds: [] }
  showWorkshopForm.value = true
  try {
    const res = await adminApi.userWorkshop.getBindings(user.id)
    if (res.code === StatusCode.SUCCESS && res.data) {
      workshopForm.value.workshopIds = res.data.workshopIds || []
      workshopForm.value.primaryWorkshopId = res.data.primaryWorkshopId || null
      workshopForm.value.testStationIds = res.data.testStationIds || []
    }
  } catch (e) {
    console.error('加载用户车间绑定失败:', e)
  }
}

function closeWorkshopForm() {
  showWorkshopForm.value = false
  editingUser.value = {}
  workshopForm.value = { workshopIds: [], primaryWorkshopId: null, testStationIds: [] }
}

async function saveWorkshopBinding() {
  workshopSubmitting.value = true
  workshopFormMsg.value = ''
  try {
    if (workshopForm.value.primaryWorkshopId && !workshopForm.value.workshopIds.includes(workshopForm.value.primaryWorkshopId)) {
      workshopFormMsg.value = '主车间必须在所选车间列表中，请重新指定'
      workshopFormMsgType.value = 'error'
      workshopSubmitting.value = false
      return
    }
    const res = await adminApi.userWorkshop.rebind(editingUser.value.id, workshopForm.value)
    if (res.code === StatusCode.SUCCESS) {
      workshopFormMsg.value = '保存成功'
      workshopFormMsgType.value = 'success'
      await loadData()
      setTimeout(() => closeWorkshopForm(), 500)
    } else {
      workshopFormMsg.value = res.msg || StatusMsg.SAVE_FAILED
      workshopFormMsgType.value = 'error'
    }
  } catch (e) {
    workshopFormMsg.value = e.message || '网络错误'
    workshopFormMsgType.value = 'error'
  } finally {
    workshopSubmitting.value = false
  }
}

onMounted(() => {
  loadData()
  // 加载所有车间 (含生产车间和测试车间, 统一在所属车间列表中展示)
  adminApi.workshop.listAll().then(res => {
    if (res.code === StatusCode.SUCCESS && res.data) workshopList.value = res.data
  }).catch(() => {})
})
</script>

<style scoped>
.mgmt-section {
  background: var(--bg-secondary);
  border-radius: 16px;
  padding: 20px;
  border: 1px solid var(--border-color);
}

/* 多选车间/测试站 */
.multi-workshop {
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 140px;
  overflow-y: auto;
  padding: 8px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-input);
}
.workshop-check label {
  display: flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  font-size: 13px;
}
.workshop-check input[type="checkbox"] { margin: 0; }
.workshop-check .primary-radio { margin-left: 8px; }
.workshop-check .primary-label {
  font-size: 11px;
  color: #2563eb;
  background: rgba(37, 99, 235, 0.1);
  padding: 1px 6px;
  border-radius: 8px;
}
.workshop-type-tag {
  font-size: 10px;
  color: #6b7280;
  background: rgba(107, 114, 128, 0.1);
  padding: 0 4px;
  border-radius: 4px;
  margin-left: 4px;
}

.form-tip {
  margin-top: 4px;
  padding: 8px 12px;
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.2);
  border-radius: 6px;
  font-size: 12px;
  color: #3b82f6;
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

.btn-search:hover {
  opacity: 0.9;
  transform: translateY(-1px);
}

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

.btn-create:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(82, 196, 26, 0.35);
}

.table-wrap {
  overflow-x: auto;
}

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

.data-table tr:hover td {
  background: rgba(var(--accent-rgb), 0.04);
}

.text-muted { color: var(--text-tertiary); }
.text-sm { font-size: 12px; }

.role-tag {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 10px;
  font-size: 11px;
  font-weight: 600;
}
.role-ADMIN { background: #fff7e6; color: #d48806; }
.role-ENGINEER { background: #e6f7ff; color: #096dd9; }
.role-OPERATOR { background: #f6ffed; color: #389e0d; }
.role-VIEWER { background: #f9f0ff; color: #722ed1; }

.status-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-right: 4px;
  cursor: pointer;
  vertical-align: middle;
  transition: transform 0.2s;
}
.status-dot:hover { transform: scale(1.4); }
.status-dot.on { background: #52c41a; box-shadow: 0 0 6px rgba(82,196,26,0.5); }
.status-dot.off { background: #999; }

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
.btn-perm { background: #f9f0ff; color: #722ed1; }
.btn-perm:hover { background: #efdbff; }
.btn-del { background: #fff1f0; color: #cf1322; }
.btn-del:hover { background: #ffa39e; }

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

.page-btn:hover:not(:disabled) {
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

.page-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.page-info {
  font-size: 13px;
  color: var(--text-secondary);
}

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
  width: 520px;
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

.form-input:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

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

.btn-cancel:hover {
  border-color: var(--text-secondary);
}

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

.btn-submit:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(var(--accent-rgb), 0.35);
}

.btn-submit:disabled {
  opacity: 0.6;
  cursor: not-allowed;
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
  .modal-actions { flex-direction: column-reverse; width: 100%; }
  .btn-cancel, .btn-submit { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .modal-card { width: 95vw; padding: 16px; padding-bottom: 100px; max-height: calc(100vh - 40px); overflow-y: auto; }
}

</style>
