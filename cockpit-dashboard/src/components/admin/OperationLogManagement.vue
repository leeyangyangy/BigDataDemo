<template>
  <div class="mgmt-section">
    <div class="toolbar">
      <div class="search-bar">
        <select v-model="filterModule" class="filter-select-sm" @change="loadData">
          <option value="">全部模块</option>
          <option value="USER">用户</option>
          <option value="EQUIPMENT">设备</option>
          <option value="PROCESS">工序</option>
          <option value="PARAM">参数</option>
          <option value="DATA">数据</option>
          <option value="ALERT">报警</option>
          <option value="SYSTEM">系统</option>
        </select>
        <select v-model="filterAction" class="filter-select-sm" @change="loadData">
          <option value="">全部操作</option>
          <option value="LOGIN">登录</option>
          <option value="LOGOUT">登出</option>
          <option value="CREATE">新建</option>
          <option value="UPDATE">更新</option>
          <option value="DELETE">删除</option>
          <option value="ERROR">错误</option>
          <option value="SECURITY">安全</option>
        </select>
        <select v-model="filterResult" class="filter-select-sm" @change="loadData">
          <option value="">全部结果</option>
          <option value="SUCCESS">成功</option>
          <option value="FAIL">失败</option>
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

    <div class="stat-bar" v-if="list.length > 0">
      <span class="stat-item stat-total">共 {{ total }} 条记录</span>
      <span class="stat-item stat-success">成功: {{ successCount }}</span>
      <span class="stat-item stat-fail">失败: {{ failCount }}</span>
    </div>

    <div class="table-wrap">
      <table class="data-table" v-if="list.length > 0">
        <thead>
          <tr>
            <th>时间</th>
            <th>模块</th>
            <th>操作</th>
            <th>操作人</th>
            <th>结果</th>
            <th>IP地址</th>
            <th>内容/错误</th>
            <th>耗时</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in list" :key="item.id" :class="{ 'row-error': item.result === 'FAIL' }">
            <td><span class="time-cell">{{ formatTime(item.createdAt) }}</span></td>
            <td>
              <span class="mod-tag" :class="modClass(item.module)">{{ modLabel(item.module) }}</span>
            </td>
            <td>
              <span class="action-tag">{{ actionLabel(item.action) }}</span>
            </td>
            <td>{{ item.operatorName || '-' }}</td>
            <td>
              <span class="result-badge" :class="item.result === 'SUCCESS' ? 'ok' : 'fail'">
                {{ item.result === 'SUCCESS' ? '✓ 成功' : '✗ 失败' }}
              </span>
            </td>
            <td class="ip-cell">{{ item.ipAddress || '-' }}</td>
            <td class="content-cell">
              <span v-if="item.errorMsg" class="error-text" :title="item.errorMsg">{{ truncate(item.errorMsg, 30) }}</span>
              <span v-else-if="item.content" :title="item.content">{{ truncate(item.content, 30) }}</span>
              <span v-else>-</span>
            </td>
            <td>{{ item.durationMs != null ? item.durationMs + 'ms' : '-' }}</td>
            <td class="actions">
              <button class="btn-action btn-detail" @click="openDetail(item)">详情</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="empty-state" v-else-if="!loading">
        <span class="empty-icon">📝</span>
        <p>暂无操作日志记录</p>
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
        <h3 class="modal-title">操作日志详情</h3>

        <div class="detail-grid">
          <div class="detail-field full">
            <label>时间</label>
            <span class="time-cell-lg">{{ formatTime(detailItem?.createdAt) }}</span>
          </div>
          <div class="detail-field">
            <label>模块</label>
            <span class="mod-tag" :class="modClass(detailItem?.module)">{{ modLabel(detailItem?.module) }}</span>
          </div>
          <div class="detail-field">
            <label>操作</label>
            <span class="action-tag">{{ actionLabel(detailItem?.action) }}</span>
          </div>
          <div class="detail-field">
            <label>结果</label>
            <span class="result-badge" :class="detailItem?.result === 'SUCCESS' ? 'ok' : 'fail'">
              {{ detailItem?.result === 'SUCCESS' ? '✓ 成功' : '✗ 失败' }}
            </span>
          </div>
          <div class="detail-field">
            <label>操作人ID</label>
            <span>{{ detailItem?.operatorId || '-' }}</span>
          </div>
          <div class="detail-field">
            <label>操作人姓名</label>
            <span>{{ detailItem?.operatorName || '-' }}</span>
          </div>
          <div class="detail-field">
            <label>目标对象</label>
            <span>{{ detailItem?.targetType || '-' }} #{{ detailItem?.targetId || '-' }}</span>
          </div>
          <div class="detail-field">
            <label>耗时</label>
            <span>{{ detailItem?.durationMs != null ? detailItem.durationMs + ' ms' : '-' }}</span>
          </div>
          <div class="detail-field full">
            <label>IP地址</label>
            <span class="ip-cell">{{ detailItem?.ipAddress || '-' }}</span>
          </div>
          <div class="detail-field full">
            <label>User-Agent</label>
            <span class="ua-cell">{{ detailItem?.userAgent || '-' }}</span>
          </div>
        </div>

        <div class="version-divider" v-if="detailItem?.content"></div>
        <div v-if="detailItem?.content">
          <h4 class="section-title">操作内容</h4>
          <pre class="json-block">{{ formatContent(detailItem.content) }}</pre>
        </div>

        <div class="version-divider" v-if="detailItem?.errorMsg"></div>
        <div v-if="detailItem?.errorMsg">
          <h4 class="section-title error-title">错误信息</h4>
          <pre class="error-block">{{ detailItem.errorMsg }}</pre>
        </div>

        <div class="modal-actions">
          <button class="btn-cancel" @click="closeDetail">关闭</button>
        </div>
      </div>
    </div>

    <!-- 清理弹窗已移除 (等保三级要求: 审计日志不可删除) -->
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

const filterModule = ref('')
const filterAction = ref('')
const filterResult = ref('')
const startDate = ref('')
const endDate = ref('')

const showDetail = ref(false)
const detailItem = ref(null)

const successCount = computed(() => list.value.filter(i => i.result === 'SUCCESS').length)
const failCount = computed(() => list.value.filter(i => i.result === 'FAIL').length)
const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

function formatTime(val) {
  if (!val) return '-'
  const d = new Date(val)
  const pad = n => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

function truncate(str, len) {
  if (!str) return '-'
  return str.length > len ? str.slice(0, len) + '...' : str
}

function formatContent(content) {
  if (!content) return '-'
  try {
    const obj = JSON.parse(content)
    return JSON.stringify(obj, null, 2)
  } catch {
    return content
  }
}

function modLabel(m) {
  const map = {
    USER: '用户', EQUIPMENT: '设备', PROCESS: '工序',
    PARAM: '参数', DATA: '数据', ALERT: '报警', SYSTEM: '系统'
  }
  return map[m] || m || '-'
}
function modClass(m) {
  const map = {
    USER: 'mod-user', EQUIPMENT: 'mod-equip', PROCESS: 'mod-process',
    PARAM: 'mod-param', DATA: 'mod-data', ALERT: 'mod-alert', SYSTEM: 'mod-system'
  }
  return map[m] || ''
}

function actionLabel(a) {
  const map = {
    LOGIN: '登录', LOGOUT: '登出', CREATE: '新建',
    UPDATE: '更新', DELETE: '删除', ERROR: '异常', SECURITY: '安全'
  }
  return map[a] || a || '-'
}

async function loadData() {
  loading.value = true
  try {
    const params = {
      current: current.value,
      size: pageSize.value,
      module: filterModule.value || undefined,
      action: filterAction.value || undefined,
      result: filterResult.value || undefined,
      startDate: startDate.value || undefined,
      endDate: endDate.value || undefined
    }
    const res = await adminApi.operationLog.getPage(params)
    if (res.code === StatusCode.SUCCESS && res.data) {
      list.value = res.data.records || []
      total.value = res.data.total || 0
    }
  } catch (e) {
    console.error('加载操作日志失败:', e)
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

// handleDelete/handleClean 已移除 (等保三级要求: 审计日志不可删除)

onMounted(async () => {
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
  margin-bottom: 12px;
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

.btn-clean {
  padding: 8px 16px; background: rgba(207,19,34,0.06); color: #cf1322;
  border: 1px solid rgba(207,19,34,0.2); border-radius: 8px;
  font-size: 13px; cursor: pointer;
}
.btn-clean:hover { background: rgba(207,19,34,0.12); }

.stat-bar {
  display: flex; gap: 16px; margin-bottom: 12px; padding: 8px 14px;
  background: var(--bg-tertiary); border-radius: 8px; font-size: 12px;
}
.stat-item { color: var(--text-secondary); }
.stat-total { font-weight: 600; color: var(--text-primary); }
.stat-success { color: #389e0d; }
.stat-fail { color: #cf1322; }

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
.row-error td { background: rgba(255,77,79,0.03); }

.time-cell { font-size: 11px; color: var(--text-tertiary); font-family: monospace; }
.time-cell-lg { font-size: 14px; color: var(--text-primary); font-family: monospace; font-weight: 500; }

.mod-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  font-size: 11px; font-weight: 600;
}
.mod-user { background: #e6f7ff; color: #1890ff; }
.mod-equip { background: #fff7e6; color: #fa8c16; }
.mod-process { background: #f6ffed; color: #52c41a; }
.mod-param { background: #f9f0ff; color: #722ed1; }
.mod-data { background: #fffbe6; color: #faad14; }
.mod-alert { background: #fff1f0; color: #cf1322; }
.mod-system { background: #f5f5f5; color: #666; }

.action-tag {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  background: rgba(var(--accent-rgb), 0.08); color: var(--accent-primary);
  font-size: 11px; font-weight: 600;
}

.result-badge {
  display: inline-block; padding: 2px 10px; border-radius: 10px;
  font-size: 11px; font-weight: 600;
}
.result-badge.ok { background: #f6ffed; color: #389e0d; }
.result-badge.fail { background: #fff1f0; color: #cf1322; }

.ip-cell { max-width: 180px; overflow: hidden; text-overflow: ellipsis; font-size: 12px; }

.content-cell { max-width: 180px; overflow: hidden; text-overflow: ellipsis; font-size: 12px; }
.error-text { color: #cf1322; }
.ua-cell { word-break: break-all; font-size: 11px; color: var(--text-tertiary); line-height: 1.4; }

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
  z-index: 2000; padding-bottom: env(safe-area-inset-bottom);
}

.modal-card {
  background: var(--bg-modal); border-radius: 16px; padding: 24px;
  width: 600px; max-width: 90vw; max-height: 85vh; overflow-y: auto;
  box-shadow: 0 20px 60px rgba(0,0,0,0.3); border: 1px solid var(--border-color);
}
.modal-lg { width: 680px; }

.modal-title { font-size: 18px; font-weight: 700; color: var(--text-primary); margin-bottom: 18px; }

.detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.detail-grid .full { grid-column: 1 / -1; }
.detail-field { display: flex; flex-direction: column; gap: 4px; }
.detail-field label { font-size: 12px; font-weight: 600; color: var(--text-secondary); }
.detail-field span { font-size: 13px; color: var(--text-primary); }

.version-divider { height: 1px; background: var(--border-color); margin: 20px 0; }

.section-title { font-size: 15px; font-weight: 600; color: var(--text-primary); margin-bottom: 10px; }
.error-title { color: #cf1322; }

.json-block, .error-block {
  padding: 14px; border-radius: 8px; font-size: 12px; font-family: monospace;
  line-height: 1.6; white-space: pre-wrap; word-break: break-all; max-height: 300px; overflow-y: auto;
}
.json-block { background: rgba(114,46,209,0.04); border: 1px solid rgba(114,46,209,0.15); color: var(--text-secondary); }
.error-block { background: #fff1f0; border: 1px solid rgba(207,19,34,0.2); color: #cf1322; }

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
.btn-danger {
  background: linear-gradient(135deg, #cf1322, #a8071a);
}
.btn-danger:hover:not(:disabled) { box-shadow: 0 4px 12px rgba(207,19,34,0.35); }

.clean-form { display: flex; flex-direction: column; gap: 10px; margin-bottom: 8px; }
.clean-form label { font-size: 13px; font-weight: 600; color: var(--text-secondary); }
.clean-form .form-input {
  padding: 10px 14px; border: 1px solid var(--border-input); border-radius: 8px;
  background: var(--bg-input); color: var(--text-primary); font-size: 14px; outline: none;
}
.clean-warning { font-size: 12px; color: #faad14; margin: 0; }
</style>
