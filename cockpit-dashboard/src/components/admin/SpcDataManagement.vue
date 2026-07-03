<template>
  <div class="mgmt-section spc-data-mgmt">
    <!-- 顶部标题与统计 -->
    <div class="page-header">
      <div class="page-title">
        <h2>SPC 采集数据管理</h2>
      </div>
      <div class="stat-cards">
        <div class="stat-card stat-total">
          <div class="stat-value">{{ total }}</div>
          <div class="stat-label">数据总数</div>
        </div>
        <div class="stat-card stat-ooc">
          <div class="stat-value">{{ pageOocCount }}</div>
          <div class="stat-label">本页 OOC</div>
        </div>
        <div class="stat-card stat-oos">
          <div class="stat-value">{{ pageOosCount }}</div>
          <div class="stat-label">本页 OOS</div>
        </div>
      </div>
    </div>

    <!-- 筛选工具栏 -->
    <div class="toolbar">
      <div class="search-bar filter-row">
        <select v-model="filter.productId" class="filter-select-sm" @change="onProductChange">
          <option value="">全部产品</option>
          <option v-for="p in productList" :key="p.id" :value="p.id">{{ p.productName }} ({{ p.productCode }})</option>
        </select>
        <select v-model="filter.processId" class="filter-select-sm" :disabled="!filter.productId" @change="onProcessChange">
          <option value="">{{ filter.productId ? '全部工序' : '请先选择产品' }}</option>
          <option v-for="p in processOptions" :key="p.id" :value="p.id">{{ p.processName }} ({{ p.processCode }})</option>
        </select>
        <select v-model="filter.equipmentId" class="filter-select-sm" :disabled="!filter.processId" @change="onFilterChange">
          <option value="">{{ filter.processId ? '全部设备' : '请先选择工序' }}</option>
          <option v-for="e in equipmentOptions" :key="e.id" :value="e.id">{{ e.equipName }} ({{ e.equipCode }})</option>
        </select>
        <select v-model="filter.isOoc" class="filter-select-sm" @change="onFilterChange">
          <option value="">OOC状态</option>
          <option value="0">正常</option>
          <option value="1">超出控制限</option>
        </select>
        <select v-model="filter.isOos" class="filter-select-sm" @change="onFilterChange">
          <option value="">OOS状态</option>
          <option value="0">正常</option>
          <option value="1">超出规格限</option>
        </select>
        <select v-model="filter.dataSource" class="filter-select-sm" @change="onFilterChange">
          <option value="">数据来源</option>
          <option value="MANUAL">手动录入</option>
          <option value="AUTO">自动采集</option>
          <option value="IMPORT">导入</option>
        </select>
        <input v-model="filter.batchId" type="text" class="search-input" placeholder="批次号..." @keyup.enter="loadData" />
        <input v-model="filter.startTime" type="datetime-local" class="filter-select-sm" />
        <input v-model="filter.endTime" type="datetime-local" class="filter-select-sm" />
        <button class="btn-search" @click="loadData">查询</button>
        <button class="btn-reset" @click="resetFilter">重置</button>
      </div>
      <div class="toolbar-actions">
        <button class="btn-batch-delete" @click="handleBatchDelete" :disabled="selectedIds.length === 0 || deleting">
          批量删除 ({{ selectedIds.length }})
        </button>
      </div>
    </div>

    <!-- 数据表格 -->
    <div class="table-wrap" v-if="list.length > 0">
      <table class="data-table spc-table">
        <thead>
          <tr>
            <th class="check-col"><input type="checkbox" :checked="allSelected" @change="toggleSelectAll" /></th>
            <th>采集时间</th>
            <th>测量值</th>
            <th>产品</th>
            <th>工序</th>
            <th>参数</th>
            <th>设备</th>
            <th>批次号</th>
            <th>版本/图类</th>
            <th>状态</th>
            <th>σ/区域</th>
            <th>来源</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id" :class="{ 'row-ooc': item.isOoc === 1, 'row-oos': item.isOos === 1 && item.isOoc !== 1 }">
            <td class="check-col"><input type="checkbox" :value="item.id" v-model="selectedIds" /></td>
            <td class="cell-time">{{ formatTime(item.collectTime) }}</td>
            <td class="cell-value">
              <div class="value-main">{{ formatValue(item.measuredValue) }}</div>
              <div class="value-dev" :class="{ 'neg': (item.deviation || 0) < 0, 'pos': (item.deviation || 0) > 0 }">
                偏差 {{ formatValue(item.deviation) }}
              </div>
            </td>
            <td>
              <div class="cell-name">{{ item.productName || '-' }}</div>
              <div class="cell-sub">{{ item.productCode || '' }}</div>
            </td>
            <td>
              <div class="cell-name">{{ item.processName || '-' }}</div>
              <div class="cell-sub">{{ item.processCode || '' }}</div>
            </td>
            <td>
              <div class="cell-name">{{ item.paramName || '-' }}</div>
              <div class="cell-sub" v-if="item.paramUnit || item.paramCode">{{ [item.paramCode, item.paramUnit].filter(Boolean).join(' · ') }}</div>
            </td>
            <td>
              <div class="cell-name">{{ item.equipName || '-' }}</div>
              <div class="cell-sub">{{ item.equipCode || '' }}</div>
            </td>
            <td>{{ item.batchId || '-' }}</td>
            <td>
              <div class="cell-name">v{{ item.versionNo || '?' }}</div>
              <div class="cell-sub">{{ chartTypeText(item.chartType) }}</div>
            </td>
            <td class="cell-status">
              <span class="status-badge" :class="item.isOoc === 1 ? 'status-error' : 'status-ok'">
                {{ item.isOoc === 1 ? 'OOC' : '正常' }}
              </span>
              <span class="status-badge" :class="item.isOos === 1 ? 'status-warn' : 'status-ok'">
                {{ item.isOos === 1 ? 'OOS' : '正常' }}
              </span>
            </td>
            <td>
              <div class="sigma-val">{{ item.sigmaLevel != null ? item.sigmaLevel.toFixed(2) + 'σ' : '-' }}</div>
              <div class="zone-tag" :class="zoneClass(item.zone)">{{ zoneText(item.zone) }}</div>
            </td>
            <td><span class="type-tag">{{ sourceText(item.dataSource) }}</span></td>
            <td class="action-cell">
              <button class="btn-action detail" @click="openDetail(item)" title="查看规格/控制限">详情</button>
              <button class="btn-action delete" @click="handleDelete(item)" :disabled="deleting">删除</button>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="pagination-bar" v-if="total > 0">
        <div class="page-left">
          <span class="total-info">共 {{ total }} 条</span>
          <select v-model.number="pageSize" class="size-select" @change="onPageSizeChange">
            <option v-for="s in sizeOptions" :key="s" :value="s">{{ s }} 条/页</option>
          </select>
        </div>
        <div class="page-btns">
          <button @click="goPage(currentPage - 1)" :disabled="currentPage <= 1">上一页</button>
          <span class="page-num">{{ currentPage }} / {{ totalPages }}</span>
          <button @click="goPage(currentPage + 1)" :disabled="currentPage >= totalPages">下一页</button>
        </div>
      </div>
    </div>

    <div class="empty-state" v-else-if="!loading">
      <p>暂无SPC采集数据</p>
    </div>

    <div class="loading-overlay" v-if="loading">
      <div class="spinner"></div>
    </div>

    <!-- 详情弹窗：规格/控制限 -->
    <div class="modal-overlay" v-if="detailItem">
      <div class="modal-card detail-modal">
        <h3>数据详情 #{{ detailItem.id }}</h3>
        <div class="detail-grid">
          <div class="detail-item">
            <label>采集时间</label>
            <span>{{ formatTime(detailItem.collectTime) }}</span>
          </div>
          <div class="detail-item">
            <label>测量值</label>
            <span class="value-main">{{ formatValue(detailItem.measuredValue) }}</span>
          </div>
          <div class="detail-item">
            <label>偏差</label>
            <span :class="{ 'neg': (detailItem.deviation || 0) < 0, 'pos': (detailItem.deviation || 0) > 0 }">{{ formatValue(detailItem.deviation) }}</span>
          </div>
          <div class="detail-item">
            <label>产品</label>
            <span>{{ detailItem.productName }} ({{ detailItem.productCode }})</span>
          </div>
          <div class="detail-item">
            <label>工序</label>
            <span>{{ detailItem.processName }} ({{ detailItem.processCode }})</span>
          </div>
          <div class="detail-item">
            <label>参数</label>
            <span>{{ detailItem.paramName }}{{ detailItem.paramUnit ? ' / ' + detailItem.paramUnit : '' }}</span>
          </div>
          <div class="detail-item">
            <label>设备</label>
            <span>{{ detailItem.equipName }} ({{ detailItem.equipCode }})</span>
          </div>
          <div class="detail-item">
            <label>批次号</label>
            <span>{{ detailItem.batchId || '-' }}</span>
          </div>
          <div class="detail-item">
            <label>版本/图类</label>
            <span>v{{ detailItem.versionNo }} · {{ chartTypeText(detailItem.chartType) }}</span>
          </div>
          <div class="detail-item">
            <label>目标值</label>
            <span>{{ formatValue(detailItem.target) }}</span>
          </div>
          <div class="detail-item">
            <label>规格限 USL/LSL</label>
            <span>{{ formatValue(detailItem.usl) }} / {{ formatValue(detailItem.lsl) }}</span>
          </div>
          <div class="detail-item">
            <label>控制限 UCL/LCL</label>
            <span>{{ formatValue(detailItem.ucl) }} / {{ formatValue(detailItem.lcl) }}</span>
          </div>
          <div class="detail-item">
            <label>σ层级/区域</label>
            <span>{{ detailItem.sigmaLevel != null ? detailItem.sigmaLevel.toFixed(2) + 'σ' : '-' }} · {{ zoneText(detailItem.zone) }}</span>
          </div>
          <div class="detail-item">
            <label>SPC标记</label>
            <span>{{ detailItem.spcFlags || '-' }}</span>
          </div>
          <div class="detail-item">
            <label>数据来源</label>
            <span>{{ sourceText(detailItem.dataSource) }}</span>
          </div>
        </div>
        <div class="modal-actions">
          <button class="btn-action" @click="detailItem = null">关闭</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { adminApi, spcApi } from '../../utils/api.js'
import { useUiStore } from '../../stores/ui.js'

const list = ref([])
const productList = ref([])
// 全量工序列表，仅用于按 processId 查找名称（ProcessBindingVO 只返回 processId）
const processList = ref([])
// 当前产品绑定的工序列表（下拉选项）
const processOptions = ref([])
// 当前工序绑定的设备列表（下拉选项）
const equipmentOptions = ref([])
const currentPage = ref(1)
const sizeOptions = [10, 20, 50, 100]
const uiStore = useUiStore()
// 默认页大小从 Pinia store 读取（由 pinia-plugin-persistedstate 从 cookie 水合），非法值回退 20
const initialPageSize = sizeOptions.includes(uiStore.spcAdminPageSize) ? uiStore.spcAdminPageSize : 20
const pageSize = ref(initialPageSize)
const total = ref(0)
const loading = ref(false)
const deleting = ref(false)
const selectedIds = ref([])
const detailItem = ref(null)

const filter = ref({
  productId: '',
  processId: '',
  equipmentId: '',
  batchId: '',
  isOoc: '',
  isOos: '',
  dataSource: '',
  startTime: '',
  endTime: ''
})

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))
const allSelected = computed(() => list.value.length > 0 && selectedIds.value.length === list.value.length)
const pageOocCount = computed(() => list.value.filter(d => d.isOoc === 1).length)
const pageOosCount = computed(() => list.value.filter(d => d.isOos === 1).length)

function formatTime(t) {
  if (!t) return '-'
  return typeof t === 'string' ? t.replace('T', ' ') : '-'
}

function formatValue(v) {
  if (v == null) return '-'
  return Number(v).toFixed(4)
}

function zoneText(z) {
  if (z == null) return '-'
  const map = { 1: 'A区', 2: 'B区', 3: 'C区' }
  return map[z] || '-'
}

function zoneClass(z) {
  if (z == null) return ''
  if (z === 1) return 'zone-a'
  if (z === 2) return 'zone-b'
  return 'zone-c'
}

function chartTypeText(c) {
  if (!c) return '-'
  const map = { I_MR: 'I-MR', XBAR_R: 'Xbar-R', XBAR_S: 'Xbar-S', P: 'P图' }
  return map[c] || c
}

function sourceText(s) {
  if (!s) return '-'
  const map = { MANUAL: '手动', AUTO: '自动', IMPORT: '导入' }
  return map[s] || s
}

function toggleSelectAll(e) {
  if (e.target.checked) {
    selectedIds.value = list.value.map(d => d.id)
  } else {
    selectedIds.value = []
  }
}

function onFilterChange() {
  currentPage.value = 1
  loadData()
}

// 产品变更：加载该产品绑定的工序，重置工序/设备级联
async function onProductChange() {
  filter.value.processId = ''
  filter.value.equipmentId = ''
  processOptions.value = []
  equipmentOptions.value = []
  if (filter.value.productId) {
    await loadProcessesForProduct(filter.value.productId)
  }
  onFilterChange()
}

// 工序变更：加载该工序绑定的设备，重置设备级联
async function onProcessChange() {
  filter.value.equipmentId = ''
  equipmentOptions.value = []
  if (filter.value.processId) {
    await loadEquipmentForProcess(filter.value.processId)
  }
  onFilterChange()
}

// 加载产品绑定的工序：ProcessBindingVO 仅含 processId，需与全量 processList 做名称关联
// 注意：路径为 /api/product/{id}/processes，使用 spcApi.getProductProcesses（adminApi.product.getProcesses 路径错误）
async function loadProcessesForProduct(productId) {
  try {
    const res = await spcApi.getProductProcesses(productId)
    const bindings = res.data || []
    const boundIds = new Set(bindings.map(b => b.processId))
    processOptions.value = processList.value.filter(p => boundIds.has(p.id))
  } catch (e) {
    console.error('加载产品工序失败:', e)
    processOptions.value = []
  }
}

// 加载工序绑定的设备：EquipmentVO 已含 equipName/equipCode
async function loadEquipmentForProcess(processId) {
  try {
    const res = await spcApi.getProcessEquipment(processId)
    equipmentOptions.value = res.data || []
  } catch (e) {
    console.error('加载工序设备失败:', e)
    equipmentOptions.value = []
  }
}

function resetFilter() {
  filter.value = {
    productId: '', processId: '', equipmentId: '',
    batchId: '', isOoc: '', isOos: '', dataSource: '',
    startTime: '', endTime: ''
  }
  processOptions.value = []
  equipmentOptions.value = []
  currentPage.value = 1
  loadData()
}

function goPage(p) {
  if (p < 1 || p > totalPages.value) return
  currentPage.value = p
  loadData()
}

function onPageSizeChange() {
  // 同步到 Pinia store，插件自动持久化到 cookie
  uiStore.spcAdminPageSize = pageSize.value
  currentPage.value = 1
  loadData()
}

function openDetail(item) {
  detailItem.value = item
}

async function loadData() {
  loading.value = true
  selectedIds.value = []
  try {
    const params = {
      current: currentPage.value,
      size: pageSize.value
    }
    if (filter.value.productId) params.productId = filter.value.productId
    if (filter.value.processId) params.processId = filter.value.processId
    if (filter.value.equipmentId) params.equipmentId = filter.value.equipmentId
    if (filter.value.batchId) params.batchId = filter.value.batchId
    if (filter.value.isOoc !== '') params.isOoc = filter.value.isOoc
    if (filter.value.isOos !== '') params.isOos = filter.value.isOos
    if (filter.value.dataSource) params.dataSource = filter.value.dataSource
    if (filter.value.startTime) params.startTime = filter.value.startTime.replace('T', ' ') + ':00'
    if (filter.value.endTime) params.endTime = filter.value.endTime.replace('T', ' ') + ':00'

    const res = await adminApi.spcData.getPage(params)
    list.value = res.data?.records || []
    total.value = res.data?.total || 0
  } catch (e) {
    console.error('加载SPC数据失败:', e)
  } finally {
    loading.value = false
  }
}

async function loadFilterOptions() {
  try {
    // 仅加载产品全量 + 工序全量（工序全量用于按 processId 关联名称）
    // 设备按「产品→工序→设备」级联，选工序时动态加载
    const [productRes, processRes] = await Promise.all([
      adminApi.product.getPage({ current: 1, size: 100 }),
      adminApi.process.getPage({ current: 1, size: 100 })
    ])
    productList.value = productRes.data?.records || []
    processList.value = processRes.data?.records || []
  } catch (e) {
    console.error('加载筛选选项失败:', e)
  }
}

async function handleDelete(item) {
  if (!confirm(`确认删除该条采集数据？(ID: ${item.id}, 采集时间: ${formatTime(item.collectTime)})`)) return
  deleting.value = true
  try {
    await adminApi.spcData.delete(item.id)
    await loadData()
  } catch (e) {
    console.error('删除失败:', e)
  } finally {
    deleting.value = false
  }
}

async function handleBatchDelete() {
  if (selectedIds.value.length === 0) return
  if (!confirm(`确认批量删除 ${selectedIds.value.length} 条采集数据？`)) return
  deleting.value = true
  try {
    await adminApi.spcData.batchDelete(selectedIds.value)
    selectedIds.value = []
    await loadData()
  } catch (e) {
    console.error('批量删除失败:', e)
  } finally {
    deleting.value = false
  }
}

onMounted(() => {
  loadFilterOptions()
  loadData()
})
</script>

<style scoped>
.mgmt-section { padding: 20px; }

/* 顶部标题与统计卡片 */
.page-header {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 18px; gap: 16px; flex-wrap: wrap;
}
.page-title h2 {
  margin: 0; font-size: 17px; font-weight: 700; color: var(--text-primary);
}
.subtitle { font-size: 12px; color: var(--text-tertiary); margin-left: 8px; }
.stat-cards { display: flex; gap: 10px; }
.stat-card {
  min-width: 92px; padding: 8px 14px; border-radius: 10px;
  background: var(--bg-card); border: 1px solid var(--border-color);
}
.stat-value { font-size: 18px; font-weight: 700; line-height: 1.2; }
.stat-label { font-size: 11px; color: var(--text-secondary); margin-top: 2px; }
.stat-total .stat-value { color: #1890ff; }
.stat-ooc .stat-value { color: #cf1322; }
.stat-oos .stat-value { color: #d48806; }

/* 工具栏 */
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
.filter-select-sm:disabled { background: var(--bg-tertiary); color: var(--text-tertiary); cursor: not-allowed; }
.search-input {
  padding: 7px 14px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary);
  font-size: 13px; width: 180px; outline: none; transition: border-color .2s;
}
.search-input:focus { border-color: var(--accent-primary); box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.08); }
.btn-search {
  padding: 7px 16px; border: none; border-radius: 8px;
  background: linear-gradient(135deg, #1890ff, #096dd9); color: white;
  font-size: 13px; font-weight: 600; cursor: pointer; transition: all .2s;
}
.btn-search:hover { transform: translateY(-1px); box-shadow: 0 3px 10px rgba(24,144,255,0.35); }
.btn-reset {
  padding: 7px 16px; border: 1px solid var(--border-input); border-radius: 8px;
  background: transparent; color: var(--text-secondary); cursor: pointer; font-size: 13px;
  transition: all .15s;
}
.btn-reset:hover { border-color: var(--accent-primary); color: var(--accent-primary); }
.toolbar-actions { display: flex; gap: 8px; }
.btn-batch-delete {
  padding: 7px 18px; border: none; border-radius: 8px;
  background: linear-gradient(135deg, #ff4d4f, #cf1322); color: white;
  font-size: 13px; font-weight: 600; cursor: pointer; transition: all .2s;
}
.btn-batch-delete:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 3px 10px rgba(255,77,79,0.35); }
.btn-batch-delete:disabled { opacity: .4; cursor: not-allowed; }

/* 表格 */
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

/* 行状态高亮 */
.row-ooc { background: rgba(245,34,45,0.04); }
.row-ooc:hover { background: rgba(245,34,45,0.07); }
.row-oos { background: rgba(250,173,20,0.04); }
.row-oos:hover { background: rgba(250,173,20,0.07); }
.check-col { width: 36px; text-align: center; }
.neg { color: #cf1322; }
.pos { color: #389e0d; }

/* 单元格内容 */
.cell-time { white-space: nowrap; color: var(--text-secondary); font-size: 13px; }
.cell-value .value-main { font-weight: 700; color: var(--text-primary); }
.cell-value .value-dev { font-size: 11px; margin-top: 2px; }
.cell-name { font-weight: 600; color: var(--text-primary); }
.cell-sub { font-size: 11px; color: var(--text-tertiary); margin-top: 2px; }
.cell-status { display: flex; flex-direction: column; gap: 3px; }

/* 标签与徽章（与其它组件一致） */
.type-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  background: rgba(24,144,255,0.08); color: #1890ff; font-size: 12px; font-weight: 500;
}
.status-badge {
  display: inline-block; padding: 2px 10px; border-radius: 10px;
  font-size: 11px; font-weight: 600; letter-spacing: .5px; text-align: center; min-width: 44px;
}
.status-ok { background: rgba(82,196,26,0.1); color: #389e0d; }
.status-warn { background: rgba(250,173,20,0.1); color: #d48806; }
.status-error { background: rgba(245,34,45,0.1); color: #cf1322; }

/* σ 与区域 */
.sigma-val { font-weight: 600; color: var(--text-primary); font-size: 13px; }
.zone-tag {
  display: inline-block; font-size: 11px; padding: 1px 8px;
  border-radius: 4px; margin-top: 2px; font-weight: 500;
}
.zone-a { background: rgba(82,196,26,0.1); color: #389e0d; }
.zone-b { background: rgba(250,173,20,0.1); color: #d48806; }
.zone-c { background: rgba(245,34,45,0.1); color: #cf1322; }

/* 操作按钮（与其它组件一致） */
.action-cell { white-space: nowrap; }
.btn-action {
  padding: 5px 12px; border: 1px solid var(--border-input); border-radius: 6px;
  background: transparent; cursor: pointer; font-size: 12px; margin-right: 6px;
  transition: all .15s;
}
.btn-action.detail { color: #1890ff; border-color: rgba(24,144,255,0.3); }
.btn-action.detail:hover { background: rgba(24,144,255,0.06); }
.btn-action.delete { color: #ff4d4f; border-color: rgba(255,77,79,0.3); }
.btn-action.delete:hover { background: rgba(255,77,79,0.06); }
.btn-action:disabled { opacity: .4; cursor: not-allowed; }

/* 分页 */
.pagination-bar {
  display: flex; justify-content: space-between; align-items: center;
  padding: 14px 18px; border-top: 1px solid var(--border-light);
}
.page-left { display: flex; align-items: center; gap: 12px; }
.total-info { font-size: 13px; color: var(--text-secondary); }
.size-select {
  padding: 5px 10px; border: 1px solid var(--border-input); border-radius: 6px;
  background: var(--bg-card); color: var(--text-primary); font-size: 12px; cursor: pointer; outline: none;
}
.size-select:focus { border-color: var(--accent-primary); }
.page-btns { display: flex; align-items: center; gap: 8px; }
.page-btns button {
  padding: 5px 14px; border: 1px solid var(--border-input); border-radius: 6px;
  background: var(--bg-card); color: var(--text-primary); cursor: pointer; font-size: 12px;
}
.page-btns button:disabled { opacity: .4; cursor: not-allowed; }
.page-num { font-size: 13px; color: var(--text-secondary); min-width: 50px; text-align: center; }

/* 空状态与加载 */
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

/* 详情弹窗（与 form-modal 风格一致） */
.modal-overlay {
  position: fixed; inset: 0; z-index: 2000;
  background: rgba(0,0,0,.45); backdrop-filter: blur(4px);
  display: flex; align-items: center; justify-content: center;
  animation: fadeIn .2s ease-out;
}
@keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
.detail-modal {
  width: 580px; max-width: 95vw; max-height: 90vh; overflow-y: auto;
  padding: 28px; border-radius: 16px; background: var(--bg-modal);
  box-shadow: var(--shadow-lg); animation: slideUp .28s ease-out;
}
@keyframes slideUp { from { transform: translateY(30px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
.detail-modal h3 {
  font-size: 17px; font-weight: 700; color: var(--text-primary);
  margin-bottom: 22px; padding-bottom: 14px; border-bottom: 1px solid var(--border-light);
}
.detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px 20px; }
.detail-item { display: flex; flex-direction: column; gap: 5px; }
.detail-item label { font-size: 12px; font-weight: 600; color: var(--text-secondary); }
.detail-item span { font-size: 13px; color: var(--text-primary); }
.modal-actions {
  display: flex; justify-content: flex-end; gap: 10px; margin-top: 22px;
  padding-top: 16px; border-top: 1px solid var(--border-light);
}
.modal-actions .btn-action {
  padding: 9px 22px; border: 1px solid var(--border-input); border-radius: 8px;
  background: transparent; color: var(--text-secondary); cursor: pointer; font-size: 13px;
}
.modal-actions .btn-action:hover { border-color: var(--accent-primary); color: var(--accent-primary); }

@media (max-width: 768px) {
  .toolbar { flex-direction: column; align-items: stretch; gap: 8px; }
  .search-bar { flex-direction: column; gap: 6px; }
  .search-input { width: 100%; box-sizing: border-box; }
  .filter-select-sm { width: 100%; box-sizing: border-box; }
  .btn-search, .btn-reset, .btn-batch-delete { width: 100%; text-align: center; padding: 10px 16px; font-size: 13px; }
  .data-table { font-size: 12px; overflow-x: auto; display: block; white-space: nowrap; }
  .data-table th, .data-table td { padding: 8px 10px; min-width: 80px; }
  .detail-grid { grid-template-columns: 1fr; }
  .detail-modal { width: 95vw; padding: 16px; }
}
</style>
