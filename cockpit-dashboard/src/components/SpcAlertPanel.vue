<template>
  <div class="spc-alert-panel">
    <div class="alert-header">
      <div class="alert-header-left">
        <span class="alert-title">⚠ 异常检测</span>
        <div class="active-rules-indicator" v-if="enabledRules.length > 0" :title="'当前生效: ' + enabledRules.map(id => allRules.find(r => r.id === id)?.code || 'N' + id).join(', ')">
          <span v-for="id in enabledRules.slice(0, 6)" :key="id"
                class="rule-dot" :class="'dot-' + (allRules.find(r => r.id === id)?.severity || 'warning')">
            {{ allRules.find(r => r.id === id)?.code || id }}
          </span>
          <span v-if="enabledRules.length > 6" class="rule-more">+{{ enabledRules.length - 6 }}</span>
        </div>
        <span class="alert-count" v-if="alerts.length > 0" :class="{ 'has-alert': alerts.length > 0 }">
          {{ alerts.length }} 条
        </span>
      </div>
      <div class="alert-header-right">
        <button class="btn-toggle-rules" :class="{ active: showRules }" @click="showRules = !showRules">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>
          规则设置
        </button>
      </div>
    </div>

    <!-- 预设模板 + 规则配置 -->
    <transition name="slide-down">
      <div class="rules-config" v-show="showRules">
        <!-- 预设模板 -->
        <div class="template-section">
          <div class="section-label">预设检测模板</div>
          <div class="template-grid">
            <button
              v-for="tpl in templates"
              :key="tpl.id"
              class="template-btn"
              :class="{ active: currentTemplate === tpl.id, [tpl.id]: true }"
              @click="applyTemplate(tpl)">
              <span class="tpl-icon">{{ tpl.icon }}</span>
              <span class="tpl-name">{{ tpl.name }}</span>
              <span class="tpl-desc">{{ tpl.desc }}</span>
              <span class="tpl-rule-count">{{ tpl.rules.length }}条规则</span>
            </button>
          </div>
        </div>

        <!-- σ 区域图例 -->
        <div class="zone-legend">
          <div class="zone-label">控制图 σ 区域参考</div>
          <div class="zone-diagram">
            <div class="zone-row zone-a-above">
              <span class="zone-mark">+3σ</span><span class="zone-bar bar-a"></span><span class="zone-name">Zone A+</span>
            </div>
            <div class="zone-row zone-b-above">
              <span class="zone-mark">+2σ</span><span class="zone-bar bar-b"></span><span class="zone-name">Zone B+</span>
            </div>
            <div class="zone-row zone-c-above">
              <span class="zone-mark">+1σ</span><span class="zone-bar bar-c"></span><span class="zone-name">Zone C+</span>
            </div>
            <div class="zone-row zone-cl">
              <span class="zone-mark">CL</span><span class="zone-bar bar-cl"></span><span class="zone-name center">中心线</span>
            </div>
            <div class="zone-row zone-c-below">
              <span class="zone-mark">-1σ</span><span class="zone-bar bar-c"></span><span class="zone-name">Zone C-</span>
            </div>
            <div class="zone-row zone-b-below">
              <span class="zone-mark">-2σ</span><span class="zone-bar bar-b"></span><span class="zone-name">Zone B-</span>
            </div>
            <div class="zone-row zone-a-below">
              <span class="zone-mark">-3σ</span><span class="zone-bar bar-a"></span><span class="zone-name">Zone A-</span>
            </div>
          </div>
        </div>

        <!-- 详细规则列表 -->
        <div class="rules-detail-section">
          <div class="section-label">
            判异规则明细
            <span class="rule-toggle-hint">已启用 {{ enabledRules.length }}/8 条</span>
          </div>
          <div class="rules-list">
            <label v-for="rule in allRules" :key="rule.code"
                   class="rule-card" :class="[`sev-${rule.severity}`, { active: enabledRules.includes(rule.id) }]">
              <input type="checkbox" :value="rule.id" v-model="enabledRules" />
              <div class="rule-card-body">
                <div class="rule-card-top">
                  <span class="rule-badge" :class="'badge-' + rule.severity">{{ rule.code }}</span>
                  <strong class="rule-title">{{ rule.name }}</strong>
                  <span class="rule-severity-tag" :class="'tag-' + rule.severity">{{ severityLabel(rule.severity) }}</span>
                </div>
                <p class="rule-full-desc">{{ rule.fullDesc }}</p>
                <div class="rule-pattern" v-if="rule.pattern">
                  <span class="pattern-label">模式示意:</span>
                  <pre class="pattern-art">{{ rule.pattern }}</pre>
                </div>
                <div class="rule-meta">
                  <span class="meta-item">最小样本: <strong>{{ rule.minSamples }}</strong></span>
                  <span class="meta-item">检出力: <strong>{{ rule.sensitivity }}</strong></span>
                  <span class="meta-item" v-if="rule.implication">含义: <em>{{ rule.implication }}</em></span>
                </div>
              </div>
            </label>
          </div>
        </div>

        <div class="rules-actions">
          <button class="btn-action btn-select-all" @click="selectAllRules">全选</button>
          <button class="btn-action btn-select-none" @click="enabledRules = []">全不选</button>
          <div class="actions-spacer"></div>
          <button class="btn-action btn-apply" @click="applyRules"
                  :disabled="enabledRules.length === 0 || applying"
                  :class="{ 'applying': applying }">
            <span v-if="applying" class="apply-spinner"></span>
            {{ applying ? '应用中...' : `应用 (${enabledRules.length}条)` }}
          </button>
        </div>

        <!-- 应用反馈 Toast -->
        <transition name="fade-up">
          <div v-if="applyFeedback" class="apply-toast" :class="{ 'toast-loading': applying, 'toast-success': !applying }">
            <span v-if="applying" class="toast-spin"></span>
            <span v-else class="toast-check">✓</span>
            {{ applyFeedback }}
          </div>
        </transition>
      </div>
    </transition>

    <!-- 统计摘要 -->
    <div class="stats-bar" v-if="alerts.length > 0 && !showRules">
      <div class="stat-chip" v-for="s in alertStats" :key="s.ruleCode" :class="'stat-' + s.severity">
        <span class="stat-rule">{{ s.ruleName }}</span>
        <span class="stat-count">{{ s.count }}次</span>
      </div>
    </div>

    <!-- 报警列表 -->
    <div class="alerts-list" v-if="alerts.length > 0">
      <div class="alert-item" v-for="(alert, index) in sortedAlerts" :key="index"
           :class="[`severity-${alert.severity || 'warning'}`]">
        <div class="alert-left">
          <div class="alert-severity-ring" :class="'ring-' + (alert.severity || 'warning')"></div>
        </div>
        <div class="alert-content">
          <div class="alert-main">
            <div class="alert-main-left">
              <span class="alert-rule-badge" :class="'badge-' + (alert.severity || 'warning')">
                {{ alert.ruleCode || ('N' + alert.ruleId) }}
              </span>
              <span class="alert-rule-name">{{ alert.ruleName }}</span>
            </div>
            <span class="alert-time">{{ formatTime(alert.time) }}</span>
          </div>
          <div class="alert-detail-row">
            <span class="detail-value">
              测量值: <strong>{{ alert.value }}</strong>
            </span>
            <span class="detail-index" v-if="alert.index != null">
              第 <strong>{{ alert.index + 1 }}</strong> 个数据点
            </span>
          </div>
          <p class="alert-message" v-if="alert.message">{{ alert.message }}</p>
        </div>
      </div>
    </div>

    <div class="no-alerts" v-else-if="!loading">
      <div class="no-alert-icon-wrap">
        <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="#52c41a" stroke-width="1.5">
          <path d="M22 11.08V12a10 10 0 11-5.93-9.14"/>
          <polyline points="22,4 12,14.01 9,11.01"/>
        </svg>
      </div>
      <div class="no-alert-text">
        <strong>未检测到异常</strong>
        <small>当前规则配置下，所有数据点均符合统计受控状态</small>
      </div>
    </div>

    <div class="loading-alerts" v-if="loading">
      <div class="loading-spinner"></div>
      <span>正在执行判异分析...</span>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { sanitizeRuleIds } from '../utils/spcRules'

const props = defineProps({
  alerts: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false }
})

const emit = defineEmits(['rules-change'])

const showRules = ref(false)
const enabledRules = ref([1, 2, 3, 4, 5, 6, 7, 8])
const currentTemplate = ref('standard')
const applying = ref(false)
const applyFeedback = ref('')
let feedbackTimer = null

const templates = [
  {
    id: 'strict',
    icon: '🔴',
    name: '严格模式',
    desc: '全部8条规则，适用于关键工序',
    rules: [1, 2, 3, 4, 5, 6, 7, 8]
  },
  {
    id: 'standard',
    icon: '🟡',
    name: '标准模式',
    desc: 'Western Electric 标准规则集',
    rules: [1, 2, 3, 4, 5, 6]
  },
  {
    id: 'relaxed',
    icon: '🟢',
    name: '宽松模式',
    desc: '仅基础超限和趋势检测',
    rules: [1, 2, 3]
  },
  {
    id: 'custom',
    icon: '⚙',
    name: '自定义',
    desc: '手动选择需要的规则',
    rules: []
  }
]

const allRules = [
  {
    id: 1,
    code: 'N1',
    name: '超出3σ控制限',
    fullDesc: '任一数据点落在中心线±3σ范围之外（超出UCL或低于LCL）。这是最严重的异常信号，表明过程可能存在特殊原因变异。',
    pattern: '         UCL ─────────●\n             |     ↑ 超出\n         +2σ │\n         +1σ │\n    CL ──────┼──────────\n         -1σ │\n         -2σ │\n         LCL ────────────',
    minSamples: 1,
    sensitivity: '极高',
    implication: '过程失控，需立即调查原因',
    severity: 'critical'
  },
  {
    id: 2,
    code: 'N2',
    name: '连续9点在中心线同侧',
    fullDesc: '连续9个或更多数据点全部位于中心线的同一侧。表明过程均值发生了偏移（shift）。',
    pattern: '    UCL ─────────────\n        │ ● ●\n    +2σ │ ● ●\n    +1σ │ ● ●\n CL ──┼────────────\n        │\n    -1σ │\n    -2σ │\n    LCL ─────────────',
    minSamples: 9,
    sensitivity: '高',
    implication: '过程均值偏移',
    severity: 'major'
  },
  {
    id: 3,
    code: 'N3',
    name: '连续6点单调趋势',
    fullDesc: '连续6个数据点持续递增或持续递减。表明存在趋势性变化（trend），如工具磨损、温度漂移等。',
    pattern: '    UCL ──────── ● ←\n        │       ╱\n    +2σ │     ╱\n    +1sigma │   ╱\n CL ──┼─ ●\n        │╱\n    -1σ ●\n    -2σ │\n    LCL ────────────',
    minSamples: 6,
    sensitivity: '高',
    implication: '过程存在趋势性漂移',
    severity: 'major'
  },
  {
    id: 4,
    code: 'N4',
    name: '连续14点上下交替',
    fullDesc: '连续14个或更多数据点呈现持续的高低交替模式。表明可能存在系统性振荡或过度调整（over-control）。',
    pattern: '    UCL ──── ●   ●\n        │   ╲ ╱   ╲\n    +2σ │  ●   ●   ●\n    +1σ │ ╲     ╲ ╱\n CL ──┼──●─────●──\n        │ ╱     ╱ ╲\n    -1σ │●       ●  ●\n    -2σ │  ╲   ╱   ╲\n    LCL ────●   ●',
    minSamples: 14,
    sensitivity: '中',
    implication: '过度调整或系统振荡',
    severity: 'warning'
  },
  {
    id: 5,
    code: 'N5',
    name: '3点中有2点超出2σ(同侧)',
    fullDesc: '在连续3个数据点中，有2个或更多点落在中心线同一侧的±2σ之外（Zone A 或更远）。提示过程变异增大。',
    pattern: '    UCL ────────────\n        │\n    +2σ │ ●   ●  ← 2/3在此区外\n    +1σ │   ●\n CL ──┼────────────\n        │\n    -1σ │\n    -2σ │\n    LCL ────────────',
    minSamples: 3,
    sensitivity: '高',
    implication: '过程标准差增大',
    severity: 'warning'
  },
  {
    id: 6,
    code: 'N6',
    name: '5点中有4点超出1σ(同侧)',
    fullDesc: '在连续5个数据点中，有4个或更多点落在中心线同一侧的±1σ之外。是过程偏移的早期预警信号。',
    pattern: '    UCL ────────────\n        │\n    +2σ │\n    +1σ │ ● ● ● ●  ← 4/5在此区外\n CL ──┼────●────────\n        │\n    -1σ │\n    -2σ │\n    LCL ────────────',
    minSamples: 5,
    sensitivity: '中高',
    implication: '过程均值开始偏移(早期)',
    severity: 'warning'
  },
  {
    id: 7,
    code: 'N7',
    name: '连续15点在1σ内(分层)',
    fullDesc: '连续15个或更多数据点全部落在中心线±1σ范围内。表明数据可能来自多个来源的混合（stratification），导致变差异常小。',
    pattern: '    UCL ────────────\n        │\n    +2σ │\n    +1σ │ ┌─────────┐\n        │ │●●●●●●●●●│← 全部挤在这里\n CL ──┼─│─────────│─\n        │ │●●●●●●●●●│\n    -1σ │ └─────────┘\n    -2σ │\n    LCL ────────────',
    minSamples: 15,
    sensitivity: '低',
    implication: '分层/混合来源(假性稳定)',
    severity: 'info'
  },
  {
    id: 8,
    code: 'N8',
    name: '连续8点在1σ外(混合)',
    fullDesc: '连续8个或更多数据点全部落在中心线±1σ范围之外（两侧都有）。表明可能存在两个或以上不同分布的数据混合在一起。',
    pattern: '    UCL ────●   ●\n        │       ╲\n    +2σ │  ●       ●\n    +1σ │●           ●\n CL ──┼────────────\n        │●           ●\n    -1σ │  ●       ●\n    -2σ │   ●     ╱\n    LCL ────●   ●',
    minSamples: 8,
    sensitivity: '低',
    implication: '多来源混合(双峰等)',
    severity: 'info'
  }
]

const sortedAlerts = computed(() => {
  return [...props.alerts].sort((a, b) => {
    const order = { critical: 0, major: 1, warning: 2, info: 3 }
    return (order[a.severity] || 99) - (order[b.severity] || 99)
  })
})

const alertStats = computed(() => {
  const map = {}
  for (const a of props.alerts) {
    const key = a.ruleCode || a.ruleName || '未知'
    if (!map[key]) {
      map[key] = { ruleCode: key, ruleName: a.ruleName || key, count: 0, severity: a.severity || 'warning' }
    }
    map[key].count++
  }
  return Object.values(map).sort((a, b) => b.count - a.count)
})

function applyTemplate(tpl) {
  if (tpl.id === 'custom') return
  currentTemplate.value = tpl.id
  enabledRules.value = [...tpl.rules]
}

function selectAllRules() {
  enabledRules.value = allRules.map(r => r.id)
  currentTemplate.value = 'custom'
}

function sanitizeRules(raw) {
  return sanitizeRuleIds(raw)
}

function applyRules() {
  enabledRules.value = sanitizeRules(enabledRules.value)
  if (enabledRules.value.length === 0) return
  currentTemplate.value = 'custom'
  applying.value = true
  applyFeedback.value = `已应用 ${enabledRules.value.length} 条规则，正在重新检测...`
  if (feedbackTimer) clearTimeout(feedbackTimer)
  emit('rules-change', [...enabledRules.value])
  setTimeout(() => {
    applying.value = false
    applyFeedback.value = `✓ 规则已生效: ${enabledRules.value.map(id => allRules.find(r => r.id === id)?.code || id).join(', ')}`
    feedbackTimer = setTimeout(() => { applyFeedback.value = '' }, 3500)
  }, 600)
}

function formatTime(t) {
  if (!t) return ''
  return t.replace('T', ' ').substring(0, 19)
}

function severityLabel(s) {
  const m = { critical: '严重', major: '重要', warning: '警告', info: '提示' }
  return m[s] || s
}

watch(enabledRules, (val) => {
  const match = templates.find(t =>
    t.id !== 'custom' && t.rules.length === val.length &&
    t.rules.every(r => val.includes(r))
  )
  if (match) currentTemplate.value = match.id
  else if (val.length > 0) currentTemplate.value = 'custom'
}, { deep: true })

defineExpose({ getEnabledRules: () => [...enabledRules.value] })
</script>

<style scoped>
.spc-alert-panel {
  margin-top: 16px;
  background: var(--bg-secondary);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

/* ===== Header ===== */
.alert-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  background: linear-gradient(135deg, #fff7e6 0%, #fffbe6 100%);
  border-bottom: 1px solid #ffe58f;
}
.alert-header-left { display: flex; align-items: center; gap: 10px; }
.alert-header-right { display: flex; align-items: center; }

.alert-title {
  font-size: 14px; font-weight: 600; color: #d48806;
}
.alert-count {
  font-size: 11px; padding: 2px 9px; border-radius: 10px;
  background: #f5f5f5; color: #999; font-weight: 500;
}
.alert-count.has-alert {
  background: #fff2f0; color: #cf1322; border: 1px solid #ffa39e;
}

.active-rules-indicator {
  display: flex; align-items: center; gap: 3px;
  padding: 1px 6px; border-radius: 6px;
  background: rgba(255,255,255,0.7); cursor: default;
}
.rule-dot {
  display: inline-flex; align-items: center; justify-content: center;
  width: 20px; height: 18px; border-radius: 4px;
  font-size: 9px; font-weight: 700; font-family: 'SF Mono', Consolas, monospace;
}
.dot-critical { background: #fef2f2; color: #dc2626; }
.dot-major { background: #fffbeb; color: #d97706; }
.dot-warning { background: #eff6ff; color: #2563eb; }
.dot-info { background: #f0fdf4; color: #16a34a; }
.rule-more {
  font-size: 10px; font-weight: 600; color: #94a3b8; padding-left: 2px;
}

.btn-toggle-rules {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 5px 12px; border: 1px solid #d48806; border-radius: 6px;
  background: transparent; color: #d48806; font-size: 12px; font-weight: 500;
  cursor: pointer; transition: all 0.2s;
}
.btn-toggle-rules:hover { background: #d48806; color: white; }
.btn-toggle-rules.active { background: #d48806; color: white; }

/* ===== Rules Config Panel ===== */
.rules-config {
  padding: 18px 18px 14px;
  background: linear-gradient(to bottom, #fafbfc, #fff);
  border-bottom: 1px solid #e8ecf0;
}
.section-label {
  font-size: 12px; font-weight: 600; color: #475569;
  margin-bottom: 10px; display: flex; align-items: center; gap: 8px;
}
.rule-toggle-hint {
  font-weight: 400; color: #94a3b8; font-size: 11px;
}

/* Templates */
.template-section { margin-bottom: 18px; }
.template-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 10px;
}
.template-btn {
  display: flex; flex-direction: column; align-items: center; gap: 4px;
  padding: 12px 8px; border: 2px solid #e2e8f0; border-radius: 10px;
  background: white; cursor: pointer; transition: all 0.2s;
}
.template-btn:hover { border-color: #93c5fd; box-shadow: 0 2px 8px rgba(59,130,246,0.1); }
.template-btn.active { border-color: #3b82f6; background: #eff6ff; }
.template-btn.strict.active { border-color: #ef4444; background: #fef2f2; }
.template-btn.standard.active { border-color: #f59e0b; background: #fffbeb; }
.template-btn.relaxed.active { border-color: #22c55e; background: #f0fdf4; }

.tpl-icon { font-size: 20px; line-height: 1; }
.tpl-name { font-size: 13px; font-weight: 600; color: #334155; }
.tpl-desc { font-size: 10px; color: #94a3b8; text-align: center; line-height: 1.35; }
.tpl-rule-count {
  margin-top: 2px; font-size: 10px; font-weight: 500;
  padding: 1px 8px; border-radius: 8px; background: #f1f5f9; color: #64748b;
}

/* Zone Legend */
.zone-legend {
  margin-bottom: 18px;
  padding: 12px; background: white; border: 1px solid #e2e8f0; border-radius: 8px;
}
.zone-diagram { display: flex; flex-direction: column; gap: 1px; }
.zone-row {
  display: flex; align-items: center; gap: 8px;
  font-size: 10.5px; font-family: 'SF Mono', Consolas, monospace;
}
.zone-mark {
  width: 32px; text-align: right; font-weight: 600; color: #64748b;
  flex-shrink: 0;
}
.zone-bar { height: 14px; border-radius: 2px; flex: 1; max-width: 200px; transition: width 0.3s; }
.bar-a { background: linear-gradient(90deg, #fecaca, #fca5a5); }
.bar-b { background: linear-gradient(90deg, #fed7aa, #fdba74); }
.bar-c { background: linear-gradient(90deg, #bfdbfe, #93c5fd); }
.bar-cl { height: 2px; background: #52c41a; max-width: 160px; }
.zone-name {
  width: 56px; font-size: 9.5px; color: #94a3b8; font-family: inherit;
  flex-shrink: 0;
}
.zone-name.center { color: #52c41a; font-weight: 600; }

/* Rules Detail */
.rules-detail-section { margin-bottom: 14px; }
.rules-list {
  display: grid;
  grid-template-columns: 1fr;
  gap: 8px;
  max-height: 420px;
  overflow-y: auto;
  padding-right: 4px;
}

.rule-card {
  display: flex; gap: 10px; padding: 12px;
  border: 1.5px solid #e2e8f0; border-radius: 10px;
  background: white; cursor: pointer; transition: all 0.2s;
}
.rule-card:hover { border-color: #93c5fd; box-shadow: 0 2px 6px rgba(0,0,0,0.04); }
.rule-card.active { border-color: #3b82f6; background: #f8fafc; }
.rule-card.sev-critical.active { border-color: #ef4444; background: #fef2f2; }
.rule-card.sev-major.active { border-color: #f59e0b; background: #fffbeb; }

.rule-card input[type="checkbox"] {
  margin-top: 4px; cursor: pointer; width: 16px; height: 16px; accent-color: #3b82f6;
}
.rule-card-body { flex: 1; min-width: 0; }

.rule-card-top {
  display: flex; align-items: center; gap: 8px; margin-bottom: 4px;
}
.rule-badge {
  display: inline-block; padding: 1px 7px; border-radius: 4px;
  font-size: 10.5px; font-weight: 700; font-family: 'SF Mono', Consolas, monospace;
  letter-spacing: 0.3px;
}
.badge-critical { background: #fef2f2; color: #dc2626; }
.badge-major { background: #fffbeb; color: #d97706; }
.badge-warning { background: #fffbeb; color: #b45309; }
.badge-info { background: #f0fdf4; color: #16a34a; }

.rule-title { font-size: 13px; color: #1e293b; font-weight: 600; }
.rule-severity-tag {
  margin-left: auto; font-size: 10px; padding: 1px 7px; border-radius: 8px; font-weight: 500;
}
.tag-critical { background: #fee2e2; color: #dc2626; }
.tag-major { background: #fef3c7; color: #d97706; }
.tag-warning { background: #fef3c7; color: #b45309; }
.tag-info { background: #dcfce7; color: #16a34a; }

.rule-full-desc {
  font-size: 11.5px; color: #475569; line-height: 1.55; margin: 0 0 6px;
}

.rule-pattern {
  display: flex; gap: 6px; align-items: flex-start;
}
.pattern-label { font-size: 10px; color: #94a3b8; white-space: nowrap; padding-top: 2px; }
.pattern-art {
  margin: 0; font-size: 9px; line-height: 1.25; color: #64748b;
  background: #f8fafc; padding: 6px 10px; border-radius: 6px;
  border: 1px solid #f1f5f9; overflow-x: auto; font-family: 'SF Mono', Consolas, monospace;
  white-space: pre; max-width: 320px;
}

.rule-meta {
  display: flex; gap: 14px; flex-wrap: wrap; margin-top: 6px;
}
.meta-item { font-size: 10px; color: #94a3b8; }
.meta-item strong { color: #475569; }
.meta-item em { color: #64748b; font-style: normal; }

/* Actions */
.rules-actions {
  display: flex; align-items: center; gap: 8px; padding-top: 10px;
  border-top: 1px solid #f1f5f9;
}
.actions-spacer { flex: 1; }
.btn-action {
  padding: 6px 16px; border: 1px solid #d9d9d9; border-radius: 6px;
  background: white; font-size: 12px; cursor: pointer; transition: all 0.2s;
  font-weight: 500;
}
.btn-action:hover:not(:disabled) { border-color: #3b82f6; color: #3b82f6; }
.btn-action:disabled { opacity: 0.4; cursor: not-allowed; }
.btn-apply {
  background: #3b82f6; color: white; border-color: #3b82f6;
}
.btn-apply:hover:not(:disabled) { background: #2563eb; }
.btn-apply.applying {
  opacity: 0.85; cursor: wait;
  animation: pulse-bg 1s ease-in-out infinite;
}
.btn-apply.applying .apply-spinner {
  display: inline-block; width: 12px; height: 12px;
  border: 2px solid rgba(255,255,255,.3); border-top-color: white;
  border-radius: 50%; animation: spin 0.7s linear infinite;
  vertical-align: middle; margin-right: 4px;
}
@keyframes pulse-bg {
  0%, 100% { box-shadow: 0 0 0 0 rgba(59,130,246,0); }
  50% { box-shadow: 0 0 0 6px rgba(59,130,246,0.15); }
}

/* Apply Toast */
.apply-toast {
  display: flex; align-items: center; gap: 8px;
  margin-top: 10px; padding: 10px 14px; border-radius: 8px;
  font-size: 12.5px; font-weight: 500;
}
.toast-loading {
  background: linear-gradient(135deg, #eff6ff, #dbeafe);
  color: #2563eb; border: 1px solid #bfdbfe;
}
.toast-success {
  background: linear-gradient(135deg, #f0fdf4, #dcfce7);
  color: #16a34a; border: 1px solid #bbf7d0;
}
.toast-spin {
  display: inline-block; width: 14px; height: 14px;
  border: 2px solid #93c5fd; border-top-color: #2563eb;
  border-radius: 50%; animation: spin 0.7s linear infinite;
}
.toast-check {
  font-size: 14px; font-weight: 700;
}

/* Fade up transition */
.fade-up-enter-active { animation: fadeUpIn 0.3s ease-out; }
.fade-up-leave-active { animation: fadeUpOut 0.25s ease-in; }
@keyframes fadeUpIn {
  from { opacity: 0; transform: translateY(6px); }
  to { opacity: 1; transform: translateY(0); }
}
@keyframes fadeUpOut {
  from { opacity: 1; transform: translateY(0); }
  to { opacity: 0; transform: translateY(-4px); }
}

/* Stats Bar */
.stats-bar {
  display: flex; gap: 8px; padding: 10px 16px;
  background: #fafbfc; border-bottom: 1px solid #f1f5f9;
  flex-wrap: wrap;
}
.stat-chip {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 3px 10px; border-radius: 6px; font-size: 11px;
  background: white; border: 1px solid #e2e8f0;
}
.stat-stat-critical { border-color: #fecaca; }
.stat-stat-major { border-color: #fed7aa; }
.stat-stat-warning { border-color: #bfdbfe; }
.stat-stat-info { border-color: #bbf7d0; }
.stat-rule { color: #64748b; font-weight: 500; }
.stat-count { font-weight: 700; }
.stat-critical .stat-count { color: #dc2626; }
.stat-major .stat-count { color: #d97706; }
.stat-warning .stat-count { color: #2563eb; }
.stat-info .stat-count { color: #16a34a; }

/* Alerts List */
.alerts-list { max-height: 420px; overflow-y: auto; }

.alert-item {
  display: flex; gap: 12px; padding: 12px 16px;
  border-bottom: 1px solid #f5f5f5; transition: background 0.15s;
}
.alert-item:hover { background: #fafbfc; }
.alert-item:last-child { border-bottom: none; }

.alert-left { display: flex; align-items: flex-start; padding-top: 4px; }
.alert-severity-ring {
  width: 10px; height: 10px; border-radius: 50%; border: 2px solid; flex-shrink: 0;
}
.ring-critical { border-color: #ef4444; box-shadow: 0 0 0 2px rgba(239,68,68,0.15); }
.ring-major { border-color: #f59e0b; box-shadow: 0 0 0 2px rgba(245,158,11,0.15); }
.ring-warning { border-color: #3b82f6; box-shadow: 0 0 0 2px rgba(59,130,246,0.15); }
.ring-info { border-color: #22c55e; box-shadow: 0 0 0 2px rgba(34,197,94,0.15); }

.alert-content { flex: 1; min-width: 0; }
.alert-main { display: flex; align-items: center; justify-content: space-between; margin-bottom: 4px; }
.alert-main-left { display: flex; align-items: center; gap: 8px; }

.alert-rule-badge {
  display: inline-block; padding: 1px 7px; border-radius: 4px;
  font-size: 10px; font-weight: 700; font-family: 'SF Mono', Consolas, monospace;
}
.badge-critical { background: #fef2f2; color: #dc2626; }
.badge-major { background: #fffbeb; color: #d97706; }
.badge-warning { background: #eff6ff; color: #2563eb; }
.badge-info { background: #f0fdf4; color: #16a34a; }

.alert-rule-name { font-size: 13px; font-weight: 600; color: #1e293b; }
.alert-time { font-size: 10.5px; color: #94a3b8; white-space: nowrap; }

.alert-detail-row {
  display: flex; gap: 16px; font-size: 11.5px; color: #64748b; flex-wrap: wrap;
}
.detail-value strong { color: #1e293b; }
.detail-index strong { color: #1e293b; }

.alert-message {
  margin: 4px 0 0; font-size: 11px; color: #94a3b8;
  line-height: 1.45; font-style: italic; padding: 4px 8px;
  background: #f8fafc; border-radius: 4px; border-left: 2px solid #e2e8f0;
}

/* Severity backgrounds */
.severity-critical { background: linear-gradient(90deg, #fef2f0 0%, white 25%); border-left: 3px solid #ef4444; }
.severity-major { background: linear-gradient(90deg, #fffbeb 0%, white 25%); border-left: 3px solid #f59e0b; }
.severity-warning { background: linear-gradient(90deg, #eff6ff 0%, white 25%); border-left: 3px solid #3b82f6; }
.severity-info { background: linear-gradient(90deg, #f0fdf4 0%, white 25%); border-left: 3px solid #22c55e; }

/* No Alerts */
.no-alerts {
  display: flex; flex-direction: column; align-items: center; gap: 10px;
  padding: 28px 16px; color: #52c41a;
}
.no-alert-icon-wrap { opacity: 0.8; }
.no-alert-text { text-align: center; }
.no-alert-text strong { display: block; font-size: 14px; color: #16a34a; margin-bottom: 2px; }
.no-alert-text small { font-size: 11.5px; color: #94a3b8; }

/* Loading */
.loading-alerts {
  display: flex; align-items: center; justify-content: center; gap: 10px;
  padding: 28px 16px; color: #94a3b8; font-size: 13px;
}
.loading-spinner {
  width: 18px; height: 18px; border: 2px solid #e2e8f0; border-top-color: #3b82f6;
  border-radius: 50%; animation: spin 0.7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* Transitions */
.slide-down-enter-active { animation: slideDown 0.25s ease-out; }
.slide-down-leave-active { animation: slideDown 0.2s ease-in reverse; }
@keyframes slideDown {
  from { opacity: 0; transform: translateY(-8px); max-height: 0; }
  to { opacity: 1; transform: translateY(0); max-height: 1200px; }
}

/* Scrollbar */
.rules-list::-webkit-scrollbar { width: 4px; }
.rules-list::-webkit-scrollbar-track { background: transparent; }
.rules-list::-webkit-scrollbar-thumb { background: #d1d5db; border-radius: 2px; }
.alerts-list::-webkit-scrollbar { width: 4px; }
.alerts-list::-webkit-scrollbar-track { background: transparent; }
.alerts-list::-webkit-scrollbar-thumb { background: #d1d5db; border-radius: 2px; }

@media (max-width: 768px) {
  .template-grid { grid-template-columns: repeat(2, 1fr); }
  .rule-pattern { flex-direction: column; }
  .pattern-art { max-width: 100%; }
}
</style>
