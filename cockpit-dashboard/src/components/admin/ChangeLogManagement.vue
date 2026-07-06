<template>
  <div class="mgmt-section">
    <div class="toolbar">
      <div class="search-bar">
        <select v-model="filterParamId" class="filter-select-sm" @change="loadData">
          <option value="">全部参数</option>
          <option v-for="p in paramList" :key="p.id" :value="p.id">{{ p.paramName }} ({{ p.paramCode }})</option>
        </select>
        <select v-model="filterChangeType" class="filter-select-sm" @change="loadData">
          <option value="">全部类型</option>
          <option value="NEW">新建版本</option>
          <option value="LIMIT_ADJUST">规格限调整</option>
          <option value="CHART_TYPE_CHANGE">控制图变更</option>
          <option value="VERSION_SWITCH">版本切换</option>
          <option value="VERSION_UPDATE">版本更新</option>
          <option value="VERSION_DISABLE">版本停用</option>
        </select>
        <input v-model="startDate" type="date" class="date-input-sm" />
        <span class="date-sep">~</span>
        <input v-model="endDate" type="date" class="date-input-sm" />
        <button class="btn-search" @click="loadData">查询</button>
      </div>
      <div class="toolbar-actions">
        <button class="btn-refresh" @click="loadData">刷新</button>
      </div>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="list.length > 0">
        <thead>
          <tr>
            <th>时间</th>
            <th>变更类型</th>
            <th>参数</th>
            <th>版本号</th>
            <th>USL 变更</th>
            <th>LSL 变更</th>
            <th>Target 变更</th>
            <th>变更原因</th>
            <th>SPC重算</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id">
            <td><span class="time-cell">{{ formatTime(item.createdAt) }}</span></td>
            <td>
              <span class="type-tag" :class="typeClass(item.changeType)">{{ typeLabel(item.changeType) }}</span>
            </td>
            <td>{{ getParamName(item.paramId) }}</td>
            <td>
              <template v-if="item.oldVersionNo != null && item.newVersionNo != null">
                V{{ item.oldVersionNo }} → V{{ item.newVersionNo }}
              </template>
              <template v-else-if="item.newVersionNo != null">
                V{{ item.newVersionNo }}
              </template>
              <span v-else>-</span>
            </td>
            <td class="diff-cell">
              <template v-if="item.oldUsl != null || item.newUsl != null">
                <span class="old-val">{{ fmt(item.oldUsl) }}</span>
                →
                <span class="new-val">{{ fmt(item.newUsl) }}</span>
              </template>
              <span v-else>-</span>
            </td>
            <td class="diff-cell">
              <template v-if="item.oldLsl != null || item.newLsl != null">
                <span class="old-val">{{ fmt(item.oldLsl) }}</span>
                →
                <span class="new-val">{{ fmt(item.newLsl) }}</span>
              </template>
              <span v-else>-</span>
            </td>
            <td class="diff-cell">
              <template v-if="item.oldTarget != null || item.newTarget != null">
                <span class="old-val">{{ fmt(item.oldTarget) }}</span>
                →
                <span class="new-val">{{ fmt(item.newTarget) }}</span>
              </template>
              <span v-else>-</span>
            </td>
            <td class="reason-cell">
              <span :title="item.changeReason">{{ truncate(item.changeReason, 24) }}</span>
            </td>
            <td>
              <span class="regen-tag" :class="regenClass(item.regenerateStatus)">
                {{ regenLabel(item.regenerateStatus) }}
              </span>
            </td>
            <td class="actions">
              <button class="btn-action btn-detail" @click="openDetail(item)">详情</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">📋</span>
        <p>暂无变更日志记录</p>
      </div>
    </div>

    <div class="pagination" v-if="total > pageSize">
      <button class="page-btn" :disabled="current <= 1" @click="current--; loadData()">上一页</button>
      <span class="page-info">{{ current }} / {{ totalPages }}</span>
      <button class="page-btn" :disabled="current >= totalPages" @click="current++; loadData()">下一页</button>
    </div>

    <!-- 详情弹窗 -->
    <div class="modal-overlay" v-if="showDetail" @click.self="closeDetail">
      <div class="modal-card modal-lg">
        <h3 class="modal-title">变更日志详情</h3>

        <div class="detail-grid">
          <div class="detail-field full">
            <label>变更时间</label>
            <span>{{ formatTime(detailItem?.createdAt) }}</span>
          </div>
          <div class="detail-field">
            <label>变更类型</label>
            <span class="type-tag" :class="typeClass(detailItem?.changeType)">{{ typeLabel(detailItem?.changeType) }}</span>
          </div>
          <div class="detail-field">
            <label>变更原因</label>
            <span class="reason-text">{{ detailItem?.changeReason || '-' }}</span>
          </div>
        </div>

        <div class="version-divider"></div>

        <h4 class="section-title">规格限对比</h4>
        <table class="detail-table">
          <thead>
            <tr>
              <th>字段</th>
              <th>旧值</th>
              <th>新值</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="field-label">USL (上规格限)</td>
              <td class="old-val">{{ fmt(detailItem?.oldUsl) }}</td>
              <td class="new-val">{{ fmt(detailItem?.newUsl) }}</td>
            </tr>
            <tr>
              <td class="field-label">LSL (下规格限)</td>
              <td class="old-val">{{ fmt(detailItem?.oldLsl) }}</td>
              <td class="new-val">{{ fmt(detailItem?.newLsl) }}</td>
            </tr>
            <tr>
              <td class="field-label">Target (目标值)</td>
              <td class="old-val">{{ fmt(detailItem?.oldTarget) }}</td>
              <td class="new-val">{{ fmt(detailItem?.newTarget) }}</td>
            </tr>
            <tr>
              <td class="field-label">UCL (上控制限)</td>
              <td class="old-val">{{ fmt(detailItem?.oldUcl) }}</td>
              <td class="new-val">{{ fmt(detailItem?.newUcl) }}</td>
            </tr>
            <tr>
              <td class="field-label">LCL (下控制限)</td>
              <td class="old-val">{{ fmt(detailItem?.oldLcl) }}</td>
              <td class="new-val">{{ fmt(detailItem?.newLcl) }}</td>
            </tr>
          </tbody>
        </table>

        <div class="version-divider"></div>

        <h4 class="section-title">SPC 重算状态</h4>
        <div class="regen-info">
          <div class="regen-row">
            <span class="regen-label">是否触发重算：</span>
            <span>{{ detailItem?.regenerateSpc === 1 ? '是' : '否' }}</span>
          </div>
          <div class="regen-row" v-if="detailItem?.regenerateStatus">
            <span class="regen-label">重算状态：</span>
            <span class="regen-tag" :class="regenClass(detailItem.regenerateStatus)">{{ regenLabel(detailItem.regenerateStatus) }}</span>
          </div>
          <div class="regen-row" v-if="detailItem?.regenerateStartedAt">
            <span class="regen-label">重算开始：</span>
            <span>{{ formatTime(detailItem.regenerateStartedAt) }}</span>
          </div>
          <div class="regen-row" v-if="detailItem?.regenerateFinishedAt">
            <span class="regen-label">重算完成：</span>
            <span>{{ formatTime(detailItem.regenerateFinishedAt) }}</span>
          </div>
        </div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeDetail">关闭</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { adminApi } from '@/utils/api.js'
import { StatusCode } from '@/utils/statusCode'

const list = ref([])
const total = ref(0)
const current = ref(1)
const pageSize = ref(20)
const loading = ref(false)

const filterParamId = ref('')
const filterChangeType = ref('')
const startDate = ref('')
const endDate = ref('')

const paramList = ref([])
const paramMap = ref({})

const showDetail = ref(false)
const detailItem = ref(null)

const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

function getParamName(paramId) {
  if (!paramId) return '-'
  return paramMap.value[paramId] || '-'
}

function formatTime(val) {
  if (!val) return '-'
  const d = new Date(val)
  const pad = n => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`
}

function fmt(val) {
  if (val == null) return '-'
  return Number(val).toFixed(2)
}

function truncate(str, len) {
  if (!str) return '-'
  return str.length > len ? str.slice(0, len) + '...' : str
}

function typeLabel(type) {
  const map = {
    'NEW': '新建版本',
    'LIMIT_ADJUST': '规格限调整',
    'CHART_TYPE_CHANGE': '控制图变更',
    'VERSION_SWITCH': '版本切换',
    'VERSION_UPDATE': '版本更新',
    'VERSION_DISABLE': '版本停用'
  }
  return map[type] || type || '-'
}

function typeClass(type) {
  const map = {
    'NEW': 'type-new',
    'LIMIT_ADJUST': 'type-limit',
    'CHART_TYPE_CHANGE': 'type-chart',
    'VERSION_SWITCH': 'type-switch',
    'VERSION_UPDATE': 'type-update',
    'VERSION_DISABLE': 'type-disable'
  }
  return map[type] || ''
}

function regenLabel(status) {
  const map = {
    'PENDING': '待执行',
    'RUNNING': '运行中',
    'COMPLETED': '已完成',
    'FAILED': '失败'
  }
  return map[status] || status || '-'
}

function regenClass(status) {
  const map = {
    'PENDING': 'regen-pending',
    'RUNNING': 'regen-running',
    'COMPLETED': 'regen-completed',
    'FAILED': 'regen-failed'
  }
  return map[status] || ''
}

async function loadParams() {
  try {
    const res = await adminApi.standard.getPage({ current: 1, size: 500 })
    if (res.code === StatusCode.SUCCESS && res.data) {
      const records = res.data.records || []
      paramList.value = records
      const m = {}
      records.forEach(p => { m[p.id] = p.paramName })
      paramMap.value = m
    }
  } catch (e) { console.error('加载参数列表失败:', e) }
}

async function loadData() {
  loading.value = true
  try {
    const params = {
      current: current.value,
      size: pageSize.value,
      paramId: filterParamId.value || undefined,
      changeType: filterChangeType.value || undefined,
      startDate: startDate.value || undefined,
      endDate: endDate.value || undefined
    }
    const res = await adminApi.changeLog.getPage(params)
    if (res.code === StatusCode.SUCCESS && res.data) {
      list.value = res.data.records || []
      total.value = res.data.total || 0
    }
  } catch (e) {
    console.error('加载变更日志失败:', e)
  } finally {
    loading.value = false
  }
}

function openDetail(item) {
  detailItem.value = item
  showDetail.value = true
}

function closeDetail() {
  showDetail.value = false
  detailItem.value = null
}

// handleDelete 已移除 (等保三级要求: 审计日志不可删除)

onMounted(async () => {
  await loadParams()
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

.filter-select-sm {
  padding: 8px 10px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary); font-size: 13px; cursor: pointer;
}

.date-input-sm {
  padding: 7px 10px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary); font-size: 13px; outline: none;
}
.date-input-sm:focus { border-color: var(--accent-primary); }

.date-sep { color: var(--text-tertiary); font-size: 13px; }

.btn-search {
  padding: 8px 18px; background: var(--accent-primary); color: white;
  border: none; border-radius: 8px; font-size: 13px; font-weight: 500; cursor: pointer;
}
.btn-search:hover { opacity: 0.9; }

.btn-refresh {
  padding: 8px 16px; background: rgba(var(--accent-rgb), 0.08); color: var(--accent-primary);
  border: 1px solid rgba(var(--accent-rgb), 0.25); border-radius: 8px;
  font-size: 13px; cursor: pointer;
}
.btn-refresh:hover { background: rgba(var(--accent-rgb), 0.14); }

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

.time-cell { font-size: 12px; color: var(--text-tertiary); font-family: monospace; }

.type-tag {
  display: inline-block; padding: 3px 10px; border-radius: 12px;
  font-size: 11px; font-weight: 600;
}
.type-new { background: #e6f7ff; color: #1890ff; }
.type-limit { background: #fff7e6; color: #fa8c16; }
.type-chart { background: #f9f0ff; color: #722ed1; }
.type-switch { background: #f6ffed; color: #52c41a; }
.type-update { background: #fffbe6; color: #faad14; }
.type-disable { background: #f5f5f5; color: #999; }

.diff-cell { font-family: monospace; font-size: 12px; }
.old-val { color: #cf1322; text-decoration: line-through; opacity: 0.7; margin-right: 4px; }
.new-val { color: #389e0d; font-weight: 600; margin-left: 4px; }

.reason-cell { max-width: 180px; overflow: hidden; text-overflow: ellipsis; font-size: 12px; color: var(--text-secondary); }

.regen-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  font-size: 11px; font-weight: 600;
}
.regen-pending { background: #fffbe6; color: #d48806; }
.regen-running { background: #e6f7ff; color: #1890ff; animation: pulse 1.5s infinite; }
.regen-completed { background: #f6ffed; color: #389e0d; }
.regen-failed { background: #fff1f0; color: #cf1322; }

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.55; }
}

.actions { white-space: nowrap; }

.btn-action {
  padding: 4px 10px; border: none; border-radius: 6px;
  font-size: 12px; cursor: pointer; margin-right: 3px; transition: all 0.2s;
}
.btn-detail { background: #e6f7ff; color: #1890ff; }
.btn-detail:hover { background: #bae7ff; }
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
  width: 600px; max-width: 90vw; max-height: 85vh; overflow-y: auto;
  box-shadow: 0 20px 60px rgba(0,0,0,0.3); border: 1px solid var(--border-color);
}
.modal-lg { width: 650px; }

.modal-title { font-size: 18px; font-weight: 700; color: var(--text-primary); margin-bottom: 18px; }

.detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.detail-grid .full { grid-column: 1 / -1; }
.detail-field { display: flex; flex-direction: column; gap: 4px; }
.detail-field label { font-size: 12px; font-weight: 600; color: var(--text-secondary); }
.detail-field span { font-size: 13px; color: var(--text-primary); }
.reason-text { word-break: break-all; line-height: 1.5; }

.version-divider { height: 1px; background: var(--border-color); margin: 20px 0; }

.section-title { font-size: 15px; font-weight: 600; color: var(--text-primary); margin-bottom: 12px; }

.detail-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.detail-table th {
  background: var(--bg-tertiary); color: var(--text-secondary); font-weight: 600;
  padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border-color);
}
.detail-table td {
  padding: 8px 12px; border-bottom: 1px solid var(--border-color);
  color: var(--text-primary); font-family: monospace; font-size: 12px;
}
.field-label { font-family: inherit !important; font-weight: 600; color: var(--text-secondary) !important; }

.regen-info { display: flex; flex-direction: column; gap: 8px; }
.regen-row { display: flex; align-items: center; gap: 8px; font-size: 13px; }
.regen-label { color: var(--text-secondary); min-width: 100px; }

.modal-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 18px; }

.btn-cancel {
  padding: 8px 20px; border: 1px solid var(--border-color); border-radius: 8px;
  background: transparent; color: var(--text-secondary); font-size: 13px; cursor: pointer;
}
.btn-cancel:hover { border-color: var(--text-secondary); }
</style>
