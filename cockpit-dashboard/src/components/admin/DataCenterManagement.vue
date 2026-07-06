<template>
  <!--
    数据中心后台管理 - 组件关联
    - 列出所有标记为 data_center_visible=1 的车间
    - 点击车间编辑其关联的组件 (多选 + 排序 + 启用)
    - 可用组件清单从后端获取 (须与前端 registry.js 对齐)
    - 车间标记 (data_center_visible) 的开关在车间管理页完成
  -->
  <div class="mgmt-section">
    <div class="toolbar">
      <h3>数据中心 - 车间组件管理</h3>
      <button class="btn-refresh" @click="loadData">↻ 刷新</button>
    </div>

    <div class="hint-text">
      <p>仅显示标记为数据中心可见的车间。如需新增车间到数据中心, 请到车间管理页开启"数据中心可见"开关。</p>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="workshops.length > 0">
        <thead>
          <tr>
            <th>车间编码</th>
            <th>车间名称</th>
            <th>类型</th>
            <th>关联组件数</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="w in workshops" :key="w.id">
            <td><strong>{{ w.workshopCode }}</strong></td>
            <td>{{ w.workshopName }}</td>
            <td><span class="type-tag">{{ w.workshopType || '-' }}</span></td>
            <td>{{ componentCounts[w.id] ?? 0 }}</td>
            <td>
              <button class="btn-action btn-edit" @click="openEdit(w)">管理组件</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">🏭</span>
        <p>暂无数据中心车间</p>
        <p class="hint">请先在车间管理页开启"数据中心可见"开关</p>
      </div>
    </div>

    <!-- 组件关联编辑弹窗 -->
    <div class="modal-overlay" v-if="showForm">
      <div class="modal-card">
        <h3 class="modal-title">管理组件关联 - {{ editing.workshopName }}</h3>

        <div class="component-list">
          <div class="component-row" v-for="comp in editingComponents" :key="comp.componentKey">
            <label class="comp-check">
              <input type="checkbox" v-model="comp.enabled" :true-value="1" :false-value="0" />
              <span class="comp-name">{{ getCompName(comp.componentKey) }}</span>
              <span class="comp-desc">{{ getCompDesc(comp.componentKey) }}</span>
            </label>
            <div class="comp-order">
              <label>顺序</label>
              <input type="number" v-model.number="comp.sortOrder" min="0" class="order-input" />
            </div>
          </div>
          <div class="empty-state-sm" v-if="editingComponents.length === 0">
            <p>暂未关联任何组件</p>
          </div>
        </div>

        <div class="add-component" v-if="availableToAdd.length > 0">
          <h4>添加组件</h4>
          <div class="add-row">
            <select v-model="addComponentKey" class="form-input">
              <option value="">请选择组件</option>
              <option v-for="a in availableToAdd" :key="a.key" :value="a.key">{{ a.name }} - {{ a.description }}</option>
            </select>
            <button class="btn-add" @click="addComponent" :disabled="!addComponentKey">+ 添加</button>
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
import { StatusCode } from '../../utils/statusCode.js'
import { StatusMsg } from '../../utils/statusMsg.js'

const workshops = ref([])
const loading = ref(false)
const componentCounts = ref({})

// 可用组件清单 (前端注册表 + 后端校验)
const availableComponents = ref([])

// 编辑弹窗
const showForm = ref(false)
const editing = ref({})
const editingComponents = ref([])
const addComponentKey = ref('')
const submitting = ref(false)
const formMsg = ref('')
const formMsgType = ref('')

const availableToAdd = computed(() => {
  if (!availableComponents.value.length) return []
  const used = new Set(editingComponents.value.map(c => c.componentKey))
  return availableComponents.value.filter(a => !used.has(a.key))
})

function getCompName(key) {
  const a = availableComponents.value.find(x => x.key === key)
  return a ? a.name : key
}

function getCompDesc(key) {
  const a = availableComponents.value.find(x => x.key === key)
  return a ? a.description : ''
}

async function loadData() {
  loading.value = true
  try {
    // 并行加载车间列表 + 可用组件清单
    const [wsRes, acRes] = await Promise.all([
      adminApi.dataCenter.listWorkshops(),
      adminApi.dataCenter.listAvailableComponents()
    ])
    if (wsRes.code === StatusCode.SUCCESS) {
      workshops.value = wsRes.data || []
      // 批量加载每个车间的组件数
      await Promise.all(workshops.value.map(async w => {
        const r = await adminApi.dataCenter.listComponents(w.id)
        if (r.code === StatusCode.SUCCESS) {
          componentCounts.value[w.id] = (r.data || []).length
        }
      }))
    }
    if (acRes.code === StatusCode.SUCCESS) {
      availableComponents.value = acRes.data || []
    }
  } catch (e) {
    console.error('加载数据中心车间失败:', e)
  } finally {
    loading.value = false
  }
}

async function openEdit(workshop) {
  editing.value = workshop
  editingComponents.value = []
  addComponentKey.value = ''
  formMsg.value = ''
  showForm.value = true
  try {
    const res = await adminApi.dataCenter.listComponents(workshop.id)
    if (res.code === StatusCode.SUCCESS) {
      // 映射为可编辑对象
      editingComponents.value = (res.data || []).map(c => ({
        componentKey: c.componentKey,
        sortOrder: c.sortOrder ?? 0,
        enabled: c.enabled ?? 1
      }))
    }
  } catch (e) {
    console.error('加载组件关联失败:', e)
  }
}

function closeForm() {
  showForm.value = false
  editing.value = {}
  editingComponents.value = []
}

function addComponent() {
  if (!addComponentKey.value) return
  editingComponents.value.push({
    componentKey: addComponentKey.value,
    sortOrder: editingComponents.value.length,
    enabled: 1
  })
  addComponentKey.value = ''
}

function removeComponent(idx) {
  editingComponents.value.splice(idx, 1)
}

async function saveForm() {
  submitting.value = true
  formMsg.value = ''
  try {
    const payload = editingComponents.value.map(c => ({
      componentKey: c.componentKey,
      sortOrder: c.sortOrder ?? 0,
      enabled: c.enabled ?? 1
    }))
    const res = await adminApi.dataCenter.updateComponents(editing.value.id, payload)
    if (res.code === StatusCode.SUCCESS) {
      formMsg.value = '保存成功'
      formMsgType.value = 'success'
      componentCounts.value[editing.value.id] = (res.data || []).length
      setTimeout(() => { closeForm() }, 500)
    } else {
      formMsg.value = res.msg || StatusMsg.SAVE_FAILED
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
}

.toolbar h3 {
  margin: 0;
  font-size: 18px;
}

.hint-text {
  margin-bottom: 16px;
  padding: 10px 14px;
  background: var(--bg-tertiary, rgba(0,0,0,0.03));
  border-radius: 8px;
  font-size: 13px;
  color: var(--text-muted);
}

.table-wrap {
  overflow-x: auto;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
}

.data-table th, .data-table td {
  padding: 10px 12px;
  text-align: left;
  border-bottom: 1px solid var(--border-color);
}

.data-table th {
  font-weight: 600;
  background: var(--bg-tertiary, rgba(0,0,0,0.03));
}

.type-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  background: rgba(59, 130, 246, 0.12);
  color: #3b82f6;
}

.actions {
  display: flex;
  gap: 6px;
}

.btn-action {
  padding: 4px 10px;
  border-radius: 6px;
  border: none;
  cursor: pointer;
  font-size: 13px;
}

.btn-edit { background: rgba(59, 130, 246, 0.12); color: #3b82f6; }
.btn-refresh {
  padding: 6px 12px;
  border-radius: 6px;
  border: 1px solid var(--border-color);
  background: var(--bg-secondary);
  cursor: pointer;
}

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

.hint {
  font-size: 13px;
  margin-top: 4px;
  color: var(--text-muted);
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

.component-list {
  margin-bottom: 16px;
}

.component-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px;
  border-bottom: 1px solid var(--border-color);
}

.comp-check {
  display: flex;
  flex-direction: column;
  gap: 2px;
  cursor: pointer;
}

.comp-name {
  font-weight: 500;
}

.comp-desc {
  font-size: 12px;
  color: var(--text-muted);
}

.comp-order {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
}

.order-input {
  width: 60px;
  padding: 4px 6px;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--bg-input);
  color: var(--text-primary);
}

.add-component {
  border-top: 1px dashed var(--border-color);
  padding-top: 12px;
  margin-top: 8px;
}

.add-component h4 {
  margin: 0 0 8px;
  font-size: 14px;
}

.add-row {
  display: flex;
  gap: 8px;
}

.form-input {
  flex: 1;
  padding: 6px 8px;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  background: var(--bg-input);
  color: var(--text-primary);
}

.btn-add {
  padding: 6px 12px;
  border-radius: 6px;
  border: none;
  background: rgba(16, 185, 129, 0.12);
  color: #10b981;
  cursor: pointer;
}

.btn-add:disabled {
  opacity: 0.5;
  cursor: not-allowed;
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

.empty-state-sm {
  padding: 20px;
  text-align: center;
  color: var(--text-muted);
  font-size: 13px;
}
</style>
