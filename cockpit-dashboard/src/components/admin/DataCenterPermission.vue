<template>
  <!--
    数据中心权限管理 - 用户-车间绑定
    - 列出所有用户, 显示其车间绑定状态
    - 点击编辑可重新绑定用户的车间 (生产车间 + 测试站) + 主车间
    - 车间列表包含 workshopType (区分生产车间/测试站)
    - 与 SysUserWorkshopController 对接
  -->
  <div class="mgmt-section">
    <div class="toolbar">
      <h3>数据中心 - 用户车间权限</h3>
      <div class="search-bar">
        <input v-model="keyword" type="text" class="search-input" placeholder="搜索工号/姓名..." @keyup.enter="loadData" />
        <button class="btn-search" @click="loadData">查询</button>
      </div>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="users.length > 0">
        <thead>
          <tr>
            <th>工号</th>
            <th>姓名</th>
            <th>角色</th>
            <th>绑定车间</th>
            <th>主车间</th>
            <th>测试站</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="u in filteredUsers" :key="u.id">
            <td><strong>{{ u.empNo }}</strong></td>
            <td>{{ u.username }}</td>
            <td><span class="role-tag" :class="u.role">{{ u.role }}</span></td>
            <td>
              <span v-if="u.workshopNames && u.workshopNames.length" class="workshop-list">
                {{ u.workshopNames.join('、') }}
              </span>
              <span v-else class="text-muted">未绑定</span>
            </td>
            <td>{{ u.primaryWorkshopName || '-' }}</td>
            <td>
              <span v-if="u.testStationNames && u.testStationNames.length">
                {{ u.testStationNames.join('、') }}
              </span>
              <span v-else class="text-muted">-</span>
            </td>
            <td>
              <span class="status-tag" :class="u.status === 1 ? 'on' : 'off'">{{ u.status === 1 ? '启用' : '停用' }}</span>
            </td>
            <td>
              <button class="btn-action btn-edit" @click="openEdit(u)">编辑绑定</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">👤</span>
        <p>暂无用户数据</p>
      </div>
    </div>

    <!-- 绑定编辑弹窗 -->
    <div class="modal-overlay" v-if="showForm">
      <div class="modal-card">
        <h3 class="modal-title">编辑车间权限 - {{ editing.username }} ({{ editing.empNo }})</h3>

        <div class="form-field">
          <label>生产车间 (可多选, 单选主车间)</label>
          <div class="multi-workshop">
            <div v-for="w in productionWorkshops" :key="w.id" class="workshop-check">
              <label>
                <input type="checkbox" :value="w.id" v-model="form.workshopIds" />
                <span>{{ w.workshopName }}（{{ w.workshopCode }}）</span>
                <input
                  v-if="form.workshopIds.includes(w.id)"
                  type="radio"
                  name="primaryWorkshop"
                  :value="w.id"
                  v-model="form.primaryWorkshopId"
                  class="primary-radio"
                  title="设为主车间"
                />
                <span v-if="form.workshopIds.includes(w.id)" class="primary-label">主</span>
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
                <input type="checkbox" :value="w.id" v-model="form.testStationIds" />
                <span>{{ w.workshopName }}（{{ w.workshopCode }}）</span>
              </label>
            </div>
            <div v-if="testStationWorkshops.length === 0" class="text-muted text-sm">暂无可绑定的测试站</div>
          </div>
        </div>

        <div class="form-msg" v-if="formMsg" :class="{ error: formMsgType === 'error', success: formMsgType === 'success' }">{{ formMsg }}</div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeForm">取消</button>
          <button class="btn-submit" @click="saveForm" :disabled="submitting">{{ submitting ? '保存中...' : '保存' }}</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { adminApi } from '../../utils/api.js'

const users = ref([])
const loading = ref(false)
const keyword = ref('')

const allWorkshops = ref([])

const showForm = ref(false)
const editing = ref({})
const submitting = ref(false)
const formMsg = ref('')
const formMsgType = ref('')

const form = ref({
  workshopIds: [],
  primaryWorkshopId: null,
  testStationIds: []
})

const productionWorkshops = computed(() =>
  allWorkshops.value.filter(w => w.workshopType !== '测试车间' && w.workshopType !== 'TEST')
)

const testStationWorkshops = computed(() =>
  allWorkshops.value.filter(w => w.workshopType === '测试车间' || w.workshopType === 'TEST')
)

const filteredUsers = computed(() => {
  if (!keyword.value.trim()) return users.value
  const k = keyword.value.trim().toLowerCase()
  return users.value.filter(u =>
    (u.empNo && u.empNo.toLowerCase().includes(k)) ||
    (u.username && u.username.toLowerCase().includes(k))
  )
})

async function loadData() {
  loading.value = true
  try {
    const [usersRes, wsRes] = await Promise.all([
      adminApi.userWorkshop.listUsers(),
      adminApi.userWorkshop.listWorkshops()
    ])
    if (usersRes.code === 200) {
      users.value = usersRes.data || []
    }
    if (wsRes.code === 200) {
      allWorkshops.value = wsRes.data || []
    }
  } catch (e) {
    console.error('加载用户车间权限失败:', e)
  } finally {
    loading.value = false
  }
}

async function openEdit(user) {
  editing.value = user
  formMsg.value = ''
  form.value = {
    workshopIds: [],
    primaryWorkshopId: null,
    testStationIds: []
  }
  showForm.value = true
  try {
    const res = await adminApi.userWorkshop.getBindings(user.id)
    if (res.code === 200 && res.data) {
      form.value.workshopIds = res.data.workshopIds || []
      form.value.primaryWorkshopId = res.data.primaryWorkshopId || null
      form.value.testStationIds = res.data.testStationIds || []
    }
  } catch (e) {
    console.error('加载用户绑定失败:', e)
  }
}

function closeForm() {
  showForm.value = false
  editing.value = {}
  form.value = { workshopIds: [], primaryWorkshopId: null, testStationIds: [] }
}

async function saveForm() {
  submitting.value = true
  formMsg.value = ''
  try {
    // 校验主车间必须在 workshopIds 中
    if (form.value.primaryWorkshopId && !form.value.workshopIds.includes(form.value.primaryWorkshopId)) {
      formMsg.value = '主车间必须在所选车间列表中'
      formMsgType.value = 'error'
      submitting.value = false
      return
    }
    const res = await adminApi.userWorkshop.rebind(editing.value.id, form.value)
    if (res.code === 200) {
      formMsg.value = '保存成功'
      formMsgType.value = 'success'
      // 刷新用户列表
      await loadData()
      setTimeout(() => closeForm(), 500)
    } else {
      formMsg.value = res.msg || '保存失败'
      formMsgType.value = 'error'
    }
  } catch (e) {
    formMsg.value = e.message || '网络错误'
    formMsgType.value = 'error'
  } finally {
    submitting.value = false
  }
}

onMounted(loadData)
</script>

<style scoped>
.mgmt-section {
  background: var(--bg-secondary);
  border-radius: 12px;
  padding: 20px;
  border: 1px solid var(--border-color);
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  gap: 12px;
}

.toolbar h3 { margin: 0; font-size: 18px; }

.search-bar {
  display: flex;
  gap: 8px;
}

.search-input {
  padding: 6px 10px;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  background: var(--bg-input);
  color: var(--text-primary);
}

.btn-search {
  padding: 6px 12px;
  border-radius: 6px;
  border: none;
  background: rgba(59, 130, 246, 0.12);
  color: #3b82f6;
  cursor: pointer;
}

.table-wrap { overflow-x: auto; }

.data-table { width: 100%; border-collapse: collapse; }
.data-table th, .data-table td {
  padding: 10px 12px;
  text-align: left;
  border-bottom: 1px solid var(--border-color);
}
.data-table th {
  font-weight: 600;
  background: var(--bg-tertiary, rgba(0,0,0,0.03));
}

.role-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
}
.role-tag.ADMIN { background: rgba(239, 68, 68, 0.12); color: #ef4444; }
.role-tag.ENGINEER { background: rgba(59, 130, 246, 0.12); color: #3b82f6; }
.role-tag.OPERATOR { background: rgba(16, 185, 129, 0.12); color: #10b981; }
.role-tag.VIEWER { background: rgba(156, 163, 175, 0.12); color: #9ca3af; }

.workshop-list {
  display: inline-block;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.status-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
}
.status-tag.on { background: rgba(16, 185, 129, 0.12); color: #10b981; }
.status-tag.off { background: rgba(156, 163, 175, 0.12); color: #9ca3af; }

.text-muted { color: var(--text-muted); }
.text-sm { font-size: 13px; }

.btn-action {
  padding: 4px 10px;
  border-radius: 6px;
  border: none;
  cursor: pointer;
  font-size: 13px;
}
.btn-edit { background: rgba(59, 130, 246, 0.12); color: #3b82f6; }

.empty-state {
  text-align: center;
  padding: 40px;
  color: var(--text-muted);
}
.empty-icon {
  font-size: 40px;
  display: block;
  margin-bottom: 8px;
}

.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-card {
  background: var(--bg-secondary);
  border-radius: 12px;
  padding: 20px;
  width: 90%;
  max-width: 600px;
  max-height: 80vh;
  overflow-y: auto;
}

.modal-title {
  margin: 0 0 16px;
  font-size: 16px;
}

.form-field {
  margin-bottom: 16px;
}

.form-field > label {
  display: block;
  font-size: 14px;
  margin-bottom: 6px;
  font-weight: 500;
}

.multi-workshop {
  max-height: 240px;
  overflow-y: auto;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 8px;
}

.workshop-check {
  padding: 6px 0;
  border-bottom: 1px dashed var(--border-color);
}

.workshop-check:last-child { border-bottom: none; }

.workshop-check label {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  font-size: 14px;
}

.primary-radio {
  margin-left: auto;
}

.primary-label {
  display: inline-block;
  padding: 1px 6px;
  background: rgba(245, 158, 11, 0.18);
  color: #d97706;
  border-radius: 8px;
  font-size: 11px;
  margin-left: 4px;
}

.form-msg {
  margin-top: 12px;
  font-size: 13px;
}
.form-msg.error { color: #ef4444; }
.form-msg.success { color: #10b981; }

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 16px;
}

.btn-cancel, .btn-submit {
  padding: 8px 16px;
  border-radius: 6px;
  border: none;
  cursor: pointer;
}
.btn-cancel {
  background: var(--bg-tertiary, rgba(0,0,0,0.05));
  color: var(--text-muted);
}
.btn-submit {
  background: rgba(59, 130, 246, 0.12);
  color: #3b82f6;
}
.btn-submit:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
</style>
