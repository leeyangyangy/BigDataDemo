<template>
  <div class="mgmt-section">
    <div class="toolbar">
      <div class="search-bar">
        <input v-model="keyword" type="text" class="search-input" placeholder="搜索工序编码/名称..." @keyup.enter="loadData" />
        <select v-model="filterStatus" class="filter-select-sm">
          <option value="">全部状态</option>
          <option :value="1">启用</option>
          <option :value="0">停用</option>
        </select>
        <button class="btn-search" @click="loadData">查询</button>
      </div>
      <button class="btn-create" @click="openCreate">+ 新增工序</button>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="list.length > 0">
        <thead>
          <tr>
            <th>工序编码</th>
            <th>工序名称</th>
            <th>类型</th>
            <th>所属车间</th>
            <th>绑定设备</th>
            <th>描述</th>
            <th>排序</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id">
            <td><strong>{{ item.processCode }}</strong></td>
            <td>{{ item.processName }}</td>
            <td><span class="type-tag">{{ item.processType || '-' }}</span></td>
            <td><span class="process-tag">{{ getWorkshopName(item.workshopId) }}</span></td>
            <td><span class="equip-count" @click="openEquipModal(item)">{{ getEquipCount(item.id) }} 台</span></td>
            <td class="text-muted text-truncate" style="max-width:150px">{{ item.description || '-' }}</td>
            <td>{{ item.sortOrder ?? 0 }}</td>
            <td>
              <span class="status-tag" :class="item.status === 1 ? 'on' : 'off'">{{ item.status === 1 ? '启用' : '停用' }}</span>
            </td>
            <td class="actions">
              <button class="btn-action btn-copy" @click="handleDuplicate(item)">复制</button>
              <button class="btn-action btn-param" @click="openParamBind(item)">参数</button>
              <button class="btn-action btn-edit" @click="openEdit(item)">编辑</button>
              <button class="btn-action btn-equip" @click="openEquipModal(item)">设备</button>
              <button class="btn-action btn-del" @click="handleDelete(item)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">⚙️</span>
        <p>暂无工序数据</p>
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
        <h3 class="modal-title">{{ isEdit ? '编辑工序' : '新增工序' }}</h3>

        <div class="form-grid">
          <div class="form-field full">
            <label>工序编码 <span class="req">*</span></label>
            <input v-model="form.processCode" type="text" class="form-input" :disabled="isEdit" placeholder="如: PROC001" />
          </div>
          <div class="form-field full">
            <label>工序名称 <span class="req">*</span></label>
            <input v-model="form.processName" type="text" class="form-input" placeholder="请输入工序名称" />
          </div>
          <div class="form-field">
            <label>工序类型</label>
            <input v-model="form.processType" type="text" class="form-input" placeholder="如: 封测/镀膜/光刻" />
          </div>
          <div class="form-field">
            <label>所属车间</label>
            <select v-model.number="form.workshopId" class="form-input">
              <option :value="null">未绑定</option>
              <option v-for="w in workshopList" :key="w.id" :value="w.id">{{ w.workshopName }} ({{ w.workshopCode }})</option>
            </select>
          </div>
          <div class="form-field full">
            <label>描述</label>
            <textarea v-model="form.description" class="form-textarea" rows="2" placeholder="工序描述"></textarea>
          </div>
          <div class="form-field">
            <label>排序值</label>
            <input v-model.number="form.sortOrder" type="number" class="form-input" placeholder="数字越小越靠前" />
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

    <!-- 工序设备管理弹窗 -->
    <div class="modal-overlay" v-if="showEquipModal">
      <div class="modal-card modal-lg">
        <h3 class="modal-title">工序设备管理 - {{ currentProcess?.processName }}</h3>
        <div class="toolbar-sm">
          <button class="btn-create btn-sm" @click="openEquipSelector">+ 添加设备</button>
        </div>
        <div class="table-wrap" style="margin-top:12px">
          <table class="data-table" v-if="processEquipList.length > 0">
            <thead>
              <tr><th>设备编码</th><th>设备名称</th><th>型号</th><th>类型</th><th>位置</th><th>状态</th><th>操作</th></tr>
            </thead>
            <tbody>
              <tr v-for="eq in processEquipList" :key="eq.id">
                <td><strong>{{ eq.equipCode }}</strong></td>
                <td>{{ eq.equipName }}</td>
                <td>{{ eq.equipModel || '-' }}</td>
                <td>{{ eq.equipType || '-' }}</td>
                <td>{{ eq.location || '-' }}</td>
                <td><span class="status-tag" :class="eq.status === '正常' ? 'on' : 'off'">{{ eq.status || '-' }}</span></td>
                <td class="actions">
                  <button class="btn-action btn-edit" @click="openEquipForm(eq)">编辑</button>
                  <button class="btn-action btn-del" @click="handleUnbindEquip(eq)">解绑</button>
                </td>
              </tr>
            </tbody>
          </table>
          <div class="empty-state" v-else><p>该工序暂无绑定设备，请点击上方按钮添加</p></div>
        </div>

        <!-- 设备选择器弹窗 -->
        <div class="modal-overlay-inner" v-if="showEquipSelector">
          <div class="modal-card modal-lg">
            <h3 class="modal-title">选择要绑定的设备</h3>
            <div class="toolbar-sm" style="margin-bottom:12px">
              <input v-model="equipSearchKeyword" type="text" class="search-input" placeholder="搜索设备编码/名称..." @keyup.enter="searchAvailableEquip" style="width:250px" />
              <button class="btn-search btn-sm" @click="searchAvailableEquip">搜索</button>
              <button class="btn-create btn-sm" @click="openEquipForm(null)" style="margin-left:auto">+ 手动新增设备</button>
            </div>
            <div class="table-wrap">
              <table class="data-table" v-if="availableEquipList.length > 0">
                <thead>
                  <tr><th>设备编码</th><th>设备名称</th><th>型号</th><th>类型</th><th>位置</th><th>当前绑定工序</th><th>操作</th></tr>
                </thead>
                <tbody>
                  <tr v-for="eq in availableEquipList" :key="eq.id">
                    <td><strong>{{ eq.equipCode }}</strong></td>
                    <td>{{ eq.equipName }}</td>
                    <td>{{ eq.equipModel || '-' }}</td>
                    <td>{{ eq.equipType || '-' }}</td>
                    <td>{{ eq.location || '-' }}</td>
                    <td><span class="process-tag">{{ eq.processId ? (getProcessNameById(eq.processId) || '已绑定') : '未绑定' }}</span></td>
                    <td class="actions">
                      <button class="btn-action btn-edit" @click="bindEquipment(eq)">绑定到本工序</button>
                    </td>
                  </tr>
                </tbody>
              </table>
              <div class="empty-state" v-else>
                <p v-if="!equipSearching">暂无可选设备，可点击上方按钮手动新增</p>
                <p v-else>搜索中...</p>
              </div>
            </div>
            <div class="modal-actions">
              <button class="btn-cancel" @click="showEquipSelector = false">关闭</button>
            </div>
          </div>
        </div>

        <!-- 设备添加/编辑子弹窗 -->
        <div class="modal-overlay-inner" v-if="showEquipForm">
          <div class="modal-card">
            <h3 class="modal-title">{{ equipEditId ? '编辑设备' : '添加设备到工序' }}</h3>
            <div class="form-grid">
              <div class="form-field full">
                <label>设备编码 <span class="req">*</span></label>
                <input v-model="equipForm.equipCode" type="text" class="form-input" :disabled="!!equipEditId" placeholder="如: EQ001" />
              </div>
              <div class="form-field full">
                <label>设备名称 <span class="req">*</span></label>
                <input v-model="equipForm.equipName" type="text" class="form-input" placeholder="请输入设备名称" />
              </div>
              <div class="form-field">
                <label>设备型号</label>
                <input v-model="equipForm.equipModel" type="text" class="form-input" placeholder="型号" />
              </div>
              <div class="form-field">
                <label>设备类型</label>
                <select v-model="equipForm.equipType" class="form-input">
                  <option value="">请选择</option>
                  <option value="检测设备">检测设备</option>
                  <option value="生产设备">生产设备</option>
                  <option value="辅助设备">辅助设备</option>
                </select>
              </div>
              <div class="form-field">
                <label>位置</label>
                <input v-model="equipForm.location" type="text" class="form-input" placeholder="位置信息" />
              </div>
              <div class="form-field">
                <label>状态</label>
                <select v-model="equipForm.status" class="form-input">
                  <option value="正常">正常</option>
                  <option value="维修中">维修中</option>
                  <option value="停用">停用</option>
                </select>
              </div>
              <div class="form-field full">
                <label>备注</label>
                <input v-model="equipForm.remark" type="text" class="form-input" placeholder="备注信息" />
              </div>
            </div>
            <div class="form-msg" v-if="equipMsg" :class="{ error: equipMsgType === 'error', success: equipMsgType === 'success' }">{{ equipMsg }}</div>
            <div class="modal-actions">
              <button class="btn-cancel" @click="showEquipForm = false; equipMsg = ''">取消</button>
              <button class="btn-submit" @click="submitEquip" :disabled="equipSubmitting">{{ equipSubmitting ? '提交中...' : (equipEditId ? '保存' : '添加') }}</button>
            </div>
          </div>
        </div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="showEquipModal = false">关闭</button>
        </div>
      </div>
    </div>

    <!-- 工序参数绑定弹窗 -->
    <div class="modal-overlay" v-if="showParamBind">
      <div class="modal-card modal-lg">
        <h3 class="modal-title">绑定参数 - {{ currentBindProcess?.processName }}</h3>
        <p style="color:#888;font-size:13px;margin-bottom:12px">选择该工序需要检测的工艺参数（同一参数可被多个工序复用）</p>
        <div class="toolbar-sm" style="margin-bottom:12px">
          <input v-model="paramSearchKeyword" type="text" class="search-input" placeholder="搜索参数编码/名称..." @keyup.enter="searchAvailableParams" style="width:250px" />
          <button class="btn-search btn-sm" @click="searchAvailableParams">搜索</button>
        </div>

        <!-- 已绑定参数 -->
        <div class="bind-panel">
          <div class="bind-head">
            <span class="bind-label">已绑定参数</span>
            <span class="bind-count">{{ boundParamList.length }}</span>
            <button v-if="boundParamList.length > 0" class="btn-action" :class="'btn-del'" @click="clearAllParams">清空</button>
          </div>
          <div class="table-wrap" v-if="boundParamList.length > 0">
            <table class="data-table bind-table">
              <thead>
                <tr><th>参数编码</th><th>参数名称</th><th>类型</th><th>单位</th><th>数据类型</th><th style="width:56px"></th></tr>
              </thead>
              <tbody>
                <tr v-for="(bp, idx) in boundParamList" :key="bp.id" class="row-bind">
                  <td><code>{{ bp.paramCode }}</code></td>
                  <td>{{ bp.paramName }}</td>
                  <td>{{ bp.paramType || '-' }}</td>
                  <td>{{ bp.unit || '-' }}</td>
                  <td>{{ formatDataType(bp.dataType) }}</td>
                  <td class="actions">
                    <button
                        class="btn-action"
                        :class="'btn-del'"
                        @click="removeBoundParam(bp)">移除</button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="bind-empty" v-else>
            <p>暂无已绑定参数，从下方表格中添加</p>
          </div>
        </div>

        <!-- 可选参数列表 -->
        <div class="avail-panel">
          <div class="avail-head">
            <span class="avail-label">可选参数</span>
            <span class="avail-count">{{ availableParamList.length }}</span>
          </div>
          <div class="table-wrap">
            <table class="data-table avail-table" v-if="availableParamList.length > 0">
              <thead>
                <tr><th>参数编码</th><th>参数名称</th><th>类型</th><th>单位</th><th>数据类型</th><th style="width:56px"></th></tr>
              </thead>
              <tbody>
                <tr v-for="p in availableParamList" :key="p.id" :class="{ 'row-selected': isParamBound(p.id) }">
                  <td><code>{{ p.paramCode }}</code></td>
                  <td>{{ p.paramName }}</td>
                  <td>{{ p.paramType || '-' }}</td>
                  <td>{{ p.unit || '-' }}</td>
                  <td>{{ formatDataType(p.dataType) }}</td>
                  <td class="actions">
                    <button
                      class="btn-action"
                      :class="isParamBound(p.id) ? 'btn-del' : 'btn-edit'"
                      @click="toggleParam(p)">
                      {{ isParamBound(p.id) ? '取消' : '添加' }}
                    </button>
                  </td>
                </tr>
              </tbody>
            </table>
            <div class="empty-state" v-else>
              <p v-if="!paramSearching">{{ paramSearchKeyword ? '未找到匹配的参数' : '暂无可选参数' }}</p>
              <p v-else>搜索中...</p>
            </div>
          </div>
        </div>

        <div class="form-msg" v-if="paramMsg" :class="{ error: paramMsgType === 'error', success: paramMsgType === 'success' }">{{ paramMsg }}</div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="showParamBind = false; paramMsg = ''">取消</button>
          <button class="btn-submit" @click="submitParamBind" :disabled="paramSubmitting">{{ paramSubmitting ? '保存中...' : '保存绑定' }}</button>
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

const DATA_TYPE_LABELS = { CONTINUOUS: '连续型', DISCRETE: '离散型', COUNT: '计数型' }
function formatDataType(val) {
  if (!val) return '连续型'
  return DATA_TYPE_LABELS[val] || val
}

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
  processCode: '', processName: '', processType: '',
  workshopId: null, description: '', status: 1, sortOrder: 0
})

const workshopList = ref([])

const showEquipModal = ref(false)
const currentProcess = ref(null)
const processEquipList = ref([])
const showEquipForm = ref(false)
const equipEditId = ref(null)
const equipSubmitting = ref(false)
const equipMsg = ref('')
const equipMsgType = ref('')
const equipForm = ref({ equipCode: '', equipName: '', equipModel: '', equipType: '检测设备', location: '', status: '正常', remark: '' })
const processEquipCountMap = ref({})
const showEquipSelector = ref(false)
const availableEquipList = ref([])
const equipSearchKeyword = ref('')
const equipSearching = ref(false)

function getWorkshopName(workshopId) {
  if (!workshopId) return '未绑定'
  const w = workshopList.value.find(x => x.id === workshopId)
  return w ? w.workshopName : '-'
}

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

async function loadData() {
  loading.value = true
  try {
    const res = await adminApi.process.getPage({
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
    console.error('加载工序列表失败:', e)
  } finally {
    loading.value = false
  }
  await loadEquipCounts()
}

function openCreate() {
  isEdit.value = false
  editId.value = null
  form.value = { processCode: '', processName: '', processType: '', workshopId: null, description: '', status: 1, sortOrder: 0 }
  formMsg.value = ''
  showForm.value = true
}

function openEdit(item) {
  isEdit.value = true
  editId.value = item.id
  form.value = {
    processCode: item.processCode,
    processName: item.processName,
    processType: item.processType || '',
    workshopId: item.workshopId,
    description: item.description || '',
    status: item.status,
    sortOrder: item.sortOrder ?? 0
  }
  formMsg.value = ''
  showForm.value = true
}

function closeForm() {
  showForm.value = false
}

async function handleSubmit() {
  if (!form.value.processCode?.trim()) { formMsg.value = '工序编码为必填项'; formMsgType.value = 'error'; return }
  if (!form.value.processName?.trim()) { formMsg.value = '工序名称为必填项'; formMsgType.value = 'error'; return }

  submitting.value = true
  formMsg.value = ''

  try {
    let res
    if (isEdit.value) {
      res = await adminApi.process.update(editId.value, form.value)
    } else {
      res = await adminApi.process.create(form.value)
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

async function handleDuplicate(item) {
  if (!confirm(`确定复制工序 "${item.processName}" (${item.processCode}) 吗？\n复制后将生成新的工序编码和名称。`)) return

  try {
    const res = await adminApi.process.duplicate(item.id)
    if (res.code === StatusCode.SUCCESS) {
      loadData()
    }
  } catch (e) {
    console.error('复制失败:', e)
  }
}

async function handleDelete(item) {
  if (!confirm(`确定删除工序 "${item.processName}" (${item.processCode}) 吗？`)) return

  try {
    const res = await adminApi.process.delete(item.id)
    if (res.code === StatusCode.SUCCESS) {
      loadData()
    }
  } catch (e) {
    console.error('删除失败:', e)
  }
}

onMounted(() => {
  loadData()
  loadWorkshopList()
})

async function loadWorkshopList() {
  try {
    const res = await adminApi.workshop.listAll()
    if (res.code === StatusCode.SUCCESS) workshopList.value = res.data || []
  } catch (e) { console.error('加载车间列表失败', e) }
}

function getEquipCount(processId) {
  return processEquipCountMap.value[processId] ?? 0
}

async function openEquipModal(item) {
  currentProcess.value = item
  showEquipModal.value = true
  showEquipForm.value = false
  await loadProcessEquipData(item.id)
}

async function loadProcessEquipData(processId) {
  try {
    const res = await spcApi.getProcessEquipment(processId)
    processEquipList.value = (res.code === StatusCode.SUCCESS && Array.isArray(res.data)) ? res.data : []
  } catch (e) { console.error('加载工序设备失败:', e); processEquipList.value = [] }
}

function getProcessNameById(processId) {
  if (!processId || !list.value.length) return null
  const p = list.value.find(x => x.id === processId)
  return p ? p.processName : null
}

async function openEquipSelector() {
  showEquipSelector.value = true
  equipSearchKeyword.value = ''
  await searchAvailableEquip()
}

async function searchAvailableEquip() {
  equipSearching.value = true
  try {
    const res = await adminApi.equipment.getPage({
      keyword: equipSearchKeyword.value || undefined,
      size: 50
    })
    if (res.code === StatusCode.SUCCESS && res.data?.records) {
      availableEquipList.value = res.data.records.filter(eq => eq.processId !== currentProcess.value?.id)
    } else {
      availableEquipList.value = []
    }
  } catch (e) {
    console.error('查询可用设备失败:', e)
    availableEquipList.value = []
  } finally {
    equipSearching.value = false
  }
}

async function bindEquipment(eq) {
  if (!confirm(`确定将设备 "${eq.equipName}" (${eq.equipCode}) 绑定到当前工序吗？`)) return

  try {
    const res = await adminApi.equipment.update(eq.id, {
      ...eq,
      processId: currentProcess.value.id
    })
    if (res.code === StatusCode.SUCCESS) {
      showEquipSelector.value = false
      await loadProcessEquipData(currentProcess.value.id)
      await loadEquipCounts()
    }
  } catch (e) {
    console.error('绑定设备失败:', e)
  }
}

function openEquipForm(eq) {
  equipEditId.value = eq ? eq.id : null
  equipMsg.value = ''
  if (eq) {
    equipForm.value = { equipCode: eq.equipCode, equipName: eq.equipName, equipModel: eq.equipModel || '', equipType: eq.equipType || '检测设备', location: eq.location || '', status: eq.status || '正常', remark: eq.remark || '' }
  } else {
    equipForm.value = { equipCode: '', equipName: '', equipModel: '', equipType: '检测设备', location: '', status: '正常', remark: '' }
  }
  showEquipForm.value = true
}

async function submitEquip() {
  if (!equipForm.value.equipCode?.trim()) { equipMsg.value = '设备编码为必填项'; equipMsgType.value = 'error'; return }
  if (!equipForm.value.equipName?.trim()) { equipMsg.value = '设备名称为必填项'; equipMsgType.value = 'error'; return }

  equipSubmitting.value = true
  equipMsg.value = ''

  try {
    let res
    const payload = { ...equipForm.value, processId: currentProcess.value.id }
    if (equipEditId.value) {
      res = await adminApi.equipment.update(equipEditId.value, payload)
    } else {
      res = await adminApi.equipment.create(payload)
    }

    if (res.code === StatusCode.SUCCESS) {
      equipMsg.value = equipEditId.value ? StatusMsg.UPDATE_SUCCESS : StatusMsg.ADD_SUCCESS
      equipMsgType.value = 'success'
      setTimeout(() => { showEquipForm.value = false; loadProcessEquipData(currentProcess.value.id) }, 800)
    } else {
      equipMsg.value = res.msg || StatusMsg.OPERATION_FAILED
      equipMsgType.value = 'error'
    }
  } catch (e) {
    equipMsg.value = e.message || '网络错误'
    equipMsgType.value = 'error'
  } finally {
    equipSubmitting.value = false
  }
}

async function handleUnbindEquip(eq) {
  if (!confirm(`确定解绑设备 "${eq.equipName}" (${eq.equipCode}) 吗？`)) return

  try {
    const res = await adminApi.equipment.update(eq.id, { clearProcessId: true })
    if (res.code === StatusCode.SUCCESS) {
      await loadProcessEquipData(currentProcess.value.id)
      await loadEquipCounts()
    }
  } catch (e) { console.error('解绑失败:', e) }
}

async function loadEquipCounts() {
  const map = {}
  for (const p of list.value) {
    try {
      const res = await spcApi.getProcessEquipment(p.id)
      map[p.id] = (res.code === StatusCode.SUCCESS && Array.isArray(res.data)) ? res.data.length : 0
    } catch (e) { map[p.id] = 0 }
  }
  processEquipCountMap.value = map
}

const showParamBind = ref(false)
const currentBindProcess = ref(null)
const boundParamList = ref([])
const availableParamList = ref([])
const paramSearchKeyword = ref('')
const paramSearching = ref(false)
const paramSubmitting = ref(false)
const paramMsg = ref('')
const paramMsgType = ref('')

async function openParamBind(item) {
  currentBindProcess.value = item
  paramMsg.value = ''
  paramSearchKeyword.value = ''
  showParamBind.value = true
  await loadBoundParams(item.id)
  await searchAvailableParams()
}

async function loadBoundParams(processId) {
  try {
    const [bindRes, allParamRes] = await Promise.all([
      adminApi.process.getParams(processId),
      spcApi.getParamPage({ current: 1, size: 200 })
    ])
    if (bindRes.code === StatusCode.SUCCESS && bindRes.data && allParamRes.code === StatusCode.SUCCESS) {
      const boundIds = bindRes.data.map(b => b.paramId)
      boundParamList.value = allParamRes.data.records.filter(p => boundIds.includes(p.id))
    } else {
      boundParamList.value = []
    }
  } catch (e) { console.error('加载已绑定参数失败:', e); boundParamList.value = [] }
}

async function searchAvailableParams() {
  paramSearching.value = true
  try {
    const res = await spcApi.getParamPage({
      keyword: paramSearchKeyword.value || undefined,
      size: 100
    })
    if (res.code === StatusCode.SUCCESS && res.data?.records) {
      const boundIds = boundParamList.value.map(p => p.id)
      availableParamList.value = res.data.records.filter(p => !boundIds.includes(p.id))
    } else {
      availableParamList.value = []
    }
  } catch (e) { console.error('搜索参数失败:', e); availableParamList.value = [] }
  finally { paramSearching.value = false }
}

function isParamBound(paramId) {
  return boundParamList.value.some(p => p.id === paramId)
}

function toggleParam(p) {
  if (isParamBound(p.id)) {
    boundParamList.value = boundParamList.value.filter(x => x.id !== p.id)
    availableParamList.value.unshift(p)
  } else {
    boundParamList.value.push(p)
    availableParamList.value = availableParamList.value.filter(x => x.id !== p.id)
  }
}

function removeBoundParam(bp) {
  boundParamList.value = boundParamList.value.filter(x => x.id !== bp.id)
  availableParamList.value.unshift(bp)
}

function clearAllParams() {
  if (!confirm(`确定清空全部 ${boundParamList.value.length} 个已绑定参数吗？`)) return
  availableParamList.value.unshift(...boundParamList.value)
  boundParamList.value = []
}

async function submitParamBind() {
  paramSubmitting.value = true
  paramMsg.value = ''
  try {
    const items = boundParamList.value.map((p) => ({ paramId: p.id }))
    const res = await adminApi.process.bindParams(currentBindProcess.value.id, items)
    if (res.code === StatusCode.SUCCESS) {
      paramMsg.value = StatusMsg.BIND_SUCCESS
      paramMsgType.value = 'success'
      setTimeout(() => { showParamBind.value = false }, 800)
    } else {
      paramMsg.value = res.msg || StatusMsg.BIND_FAILED
      paramMsgType.value = 'error'
    }
  } catch (e) {
    paramMsg.value = e.message || '网络错误'
    paramMsgType.value = 'error'
  } finally {
    paramSubmitting.value = false
  }
}
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
.text-truncate { overflow: hidden; text-overflow: ellipsis; }

.type-tag {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 6px;
  font-size: 11px;
  background: rgba(114,46,209,0.08);
  color: #722ed1;
}

.process-tag {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 6px;
  font-size: 11px;
  background: rgba(24,144,255,0.08);
  color: #1890ff;
}

.equip-count {
  display: inline-block;
  padding: 3px 12px;
  border-radius: 10px;
  font-size: 11px;
  font-weight: 600;
  background: rgba(250,173,20,0.1);
  color: #ad6800;
  cursor: pointer;
  transition: all 0.2s;
}
.equip-count:hover { background: rgba(250,173,20,0.22); transform: scale(1.05); }

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

.btn-param { background: #f6ffed; color: #389e0d; }
.btn-param:hover { background: #b7eb8f; }

.btn-copy { background: #e6f7ff; color: #096dd9; }
.btn-copy:hover { background: #91d5ff; }

.btn-equip { background: #fff7e6; color: #d46b08; }
.btn-equip:hover { background: #ffd591; }

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
  width: 520px;
  max-width: 90vw;
  box-shadow: 0 20px 60px rgba(0,0,0,0.3);
  border: 1px solid var(--border-color);
}

.modal-card.modal-lg {
  width: 760px;
}

.toolbar-sm {
  display: flex;
  justify-content: flex-end;
  margin-bottom: 8px;
}
.btn-sm { padding: 6px 14px; font-size: 12px; }

.modal-overlay-inner {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.35);
  backdrop-filter: blur(3px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1300;
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

.form-textarea {
  padding: 8px 12px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 13px;
  outline: none;
  resize: vertical;
  font-family: inherit;
  transition: border-color 0.2s;
}
.form-textarea:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(var(--accent-rgb), 0.12);
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
.btn-cancel:hover { border-color: var(--text-secondary); }

.btn-submit {
  padding: 8px 24px;
  border: none;
  border-radius: 8px;
  background: linear-gradient(135deg, #1890ff, #096dd9);
  color: white;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.25s;
}
.btn-submit:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(24,144,255,0.35); }
.btn-submit:disabled { opacity: 0.6; cursor: not-allowed; }

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

  .bind-panel {
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    margin-bottom: 24px;
    overflow: hidden;
  }

  .bind-head {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 14px;
    background: #f8fafc;
    border-bottom: 1px solid #e2e8f0;
  }
  .bind-label { font-size: 13px; font-weight: 600; color: #334155; }
  .bind-count {
    display: inline-flex; align-items: center; justify-content: center;
    min-width: 20px; height: 20px; padding: 0 6px;
    background: var(--accent-primary); color: #fff;
    border-radius: 10px; font-size: 11px; font-weight: 700;
  }
  .bind-table { border: none; border-radius: 0; }
  .bind-table thead th { background: #fff; color: #64748b; font-size: 11px; padding: 7px 12px; border-bottom: 1px solid #f1f5f9; }
  .bind-table td { padding: 7px 12px; font-size: 13px; }
  .bind-table code {
    font-family: 'SF Mono', 'Cascadia Code', Consolas, monospace;
    font-size: 12px; font-weight: 600; color: var(--accent-primary);
    background: #eff6ff; padding: 1px 5px; border-radius: 3px;
  }
  .bind-table tr.row-bind { transition: background 0.12s; }
  .bind-table tr.row-bind:nth-child(even) { background: #fafbfc; }
  .bind-table tr.row-bind:hover { background: #fef2f2; }

  .bind-empty { padding: 18px 14px; text-align: center; color: #94a3b8; font-size: 13px; }

  .avail-panel { margin-top: 4px; }
  .avail-head {
    display: flex; align-items: center; gap: 8px;
    margin-bottom: 8px; padding: 0 2px;
  }
  .avail-label { font-size: 13px; font-weight: 600; color: #475569; }
  .avail-count {
    display: inline-flex; align-items: center; justify-content: center;
    min-width: 20px; height: 20px; padding: 0 6px;
    background: #e2e8f0; color: #64748b;
    border-radius: 10px; font-size: 11px; font-weight: 600;
  }

  .avail-table code {
    font-family: 'SF Mono', 'Cascadia Code', Consolas, monospace;
    font-size: 12px; font-weight: 600; color: #334155;
    background: #f1f5f9; padding: 1px 5px; border-radius: 3px;
  }

  .row-selected { background-color: rgba(var(--accent-rgb), 0.05); }
}
</style>
