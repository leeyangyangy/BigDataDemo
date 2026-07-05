<template>
  <div class="spc-chart-container">
    <div class="chart-toolbar" v-if="showToolbar">
      <div class="chart-type-selector">
        <button
          v-for="type in chartTypes"
          :key="type.value"
          :class="['type-btn', { active: currentChartType === type.value }]"
          @click="switchChartType(type.value)"
          :title="type.label"
        >
          {{ type.label }}
        </button>
      </div>
    </div>

    <div ref="chartRef" class="spc-control-chart"></div>

    <div v-if="capabilityInfo" class="spc-capability-panel">
      <div class="cap-row">
        <span class="cap-label">Cp</span><strong :class="capClass(capabilityInfo.cp)">{{ capabilityInfo.cp ?? '-' }}</strong>
        <span class="cap-label">Cpk</span><strong :class="capClass(capabilityInfo.cpk)">{{ capabilityInfo.cpk ?? '-' }}</strong>
        <span class="cap-label">Pp</span><strong>{{ capabilityInfo.pp ?? '-' }}</strong>
        <span class="cap-label">Ppk</span><strong :class="capClass(capabilityInfo.ppk)">{{ capabilityInfo.ppk ?? '-' }}</strong>
        <span class="cap-label">Cpm</span><strong :class="capClass(capabilityInfo.cpm)">{{ capabilityInfo.cpm ?? '-' }}</strong>
      </div>
      <div class="cap-row">
        <span class="cap-label">均值</span><strong>{{ capabilityInfo.mean ?? '-' }}</strong>
        <span class="cap-label">σ_within</span><strong>{{ capabilityInfo.stdDev ?? '-' }}</strong>
        <span class="cap-label">σ_overall</span><strong>{{ capabilityInfo.stdDevOverall ?? '-' }}</strong>
        <span class="cap-label">样本N</span><strong>{{ capabilityInfo.sampleCount ?? 0 }}</strong>
        <span class="cap-label cap-highlight" v-if="capabilityInfo.passRate != null">
          合格率 <strong :class="capabilityInfo.passRate >= 95 ? 'pass' : 'fail'">{{ capabilityInfo.passRate }}%</strong>
          <small>({{ capabilityInfo.passCount }}/{{ capabilityInfo.passCount + capabilityInfo.failCount }})</small>
        </span>
      </div>
      <div class="cap-row">
        <span class="cap-label">最小值</span><strong>{{ capabilityInfo.min ?? '-' }}</strong>
        <span class="cap-label">最大值</span><strong>{{ capabilityInfo.max ?? '-' }}</strong>
        <span class="cap-label">中位数</span><strong>{{ capabilityInfo.median ?? '-' }}</strong>
        <span class="cap-label">极差</span><strong>{{ capabilityInfo.range ?? '-' }}</strong>
      </div>
      <div class="cap-row" v-if="capabilityInfo.normalityPValue != null">
        <span class="cap-label">正态性 W</span><strong>{{ capabilityInfo.normalityW }}</strong>
        <span class="cap-label">p-value</span><strong :class="capabilityInfo.isNormal ? 'pass' : 'fail'">{{ capabilityInfo.normalityPValue }}</strong>
        <span class="cap-tag" :class="capabilityInfo.isNormal ? 'tag-normal' : 'tag-non-normal'">
          {{ capabilityInfo.isNormal ? '✓ 正态分布' : '✗ 非正态' }}
        </span>
      </div>
    </div>

    <div v-if="boxplotStats && currentChartType === 'boxplot'" class="spc-boxplot-stats">
      <div class="stat-item">样本数: <strong>{{ boxplotStats.n }}</strong></div>
      <div class="stat-divider"></div>
      <div class="stat-item">中位数: <strong>{{ boxplotStats.median }}</strong></div>
      <div class="stat-divider"></div>
      <div class="stat-item">Q1: <strong>{{ boxplotStats.q1 }}</strong> | Q3: <strong>{{ boxplotStats.q3 }}</strong></div>
      <div class="stat-divider"></div>
      <div class="stat-item">IQR: <strong>{{ boxplotStats.iqr.toFixed(4) }}</strong></div>
      <div class="stat-divider"></div>
      <div class="stat-item" :class="boxplotStats.outliers.length > 0 ? 'stat-danger' : 'stat-safe'">
        异常值: <strong>{{ boxplotStats.outliers.length }}</strong>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  chartData: { type: Object, default: null },
  height: { type: String, default: '580px' },
  chartType: { type: String, default: '' },
  showToolbar: { type: Boolean, default: true }
})

const emit = defineEmits(['chart-type-change'])

const chartRef = ref(null)
let chartInstance = null
let subChartInstance = null

function normalizeChartType(raw) {
  if (!raw) return 'imr'
  const map = {
    'I_MR': 'imr', 'IMR': 'imr',
    'XBAR_R': 'xbar_r', 'XBARR': 'xbar_r',
    'XBAR_S': 'xbar_r', 'XBARS': 'xbar_r',
    'P': 'pchart', 'P_CHART': 'pchart',
    'NP': 'pchart', 'NP_CHART': 'pchart',
    'C': 'pchart', 'C_CHART': 'pchart',
    'U': 'pchart', 'U_CHART': 'pchart'
  }
  const normalized = raw.replace(/[-_]/g, '').toUpperCase()
  for (const [k, v] of Object.entries(map)) {
    if (normalized === k.replace(/[-_]/g, '')) return v
  }
  return 'imr'
}

// Xbar-R 控制图系数表（按子组大小 n=2..10）
const CONTROL_CHART_COEFFICIENTS = {
  2:  { A2: 1.880, D3: 0,     D4: 3.267, d2: 1.128 },
  3:  { A2: 1.023, D3: 0,     D4: 2.574, d2: 1.693 },
  4:  { A2: 0.729, D3: 0,     D4: 2.282, d2: 2.059 },
  5:  { A2: 0.577, D3: 0,     D4: 2.114, d2: 2.326 },
  6:  { A2: 0.483, D3: 0,     D4: 2.004, d2: 2.534 },
  7:  { A2: 0.419, D3: 0,     D4: 1.924, d2: 2.704 },
  8:  { A2: 0.373, D3: 0.076, D4: 1.864, d2: 2.847 },
  9:  { A2: 0.337, D3: 0.184, D4: 1.816, d2: 2.970 },
  10: { A2: 0.308, D3: 0.223, D4: 1.777, d2: 3.078 }
}

function getCoefficients(subgroupSize) {
  const sg = Math.max(2, Math.min(10, subgroupSize || 5))
  return CONTROL_CHART_COEFFICIENTS[sg] || CONTROL_CHART_COEFFICIENTS[5]
}

const currentChartType = ref(normalizeChartType(props.chartType))
const boxplotStats = ref(null)

const chartTypes = [
  { value: 'imr', label: 'I-MR图' },
  { value: 'xbar_r', label: 'Xbar-R图' },
  { value: 'pchart', label: '计数图' },
  { value: 'histogram', label: '直方图' },
  { value: 'boxplot', label: '箱线图' },
  { value: 'scatter', label: '散点图' }
]

const capabilityInfo = computed(() => {
  if (!props.chartData?.capability) return null
  const c = props.chartData.capability
  return {
    cp: c.cp, cpk: c.cpk, pp: c.pp, ppk: c.ppk, cpm: c.cpm,
    mean: c.mean, stdDev: c.stdDev, stdDevOverall: c.stdDevOverall,
    min: c.min, max: c.max, median: c.median, range: c.range,
    sampleCount: c.sampleCount,
    passRate: props.chartData.passRate != null ? Number(props.chartData.passRate) : null,
    passCount: props.chartData.passCount,
    failCount: props.chartData.failCount,
    normalityW: props.chartData.normalityW,
    normalityPValue: props.chartData.normalityPValue,
    isNormal: props.chartData.isNormal
  }
})

onMounted(() => {
  initChart()
})

onUnmounted(() => {
  disposeCharts()
})

watch(() => [props.chartData, currentChartType.value], ([newData]) => {
  if (newData) nextTick(() => updateChart(newData))
}, { deep: true })

watch(() => props.chartType, (val) => {
  const normalized = normalizeChartType(val)
  if (normalized !== currentChartType.value) {
    currentChartType.value = normalized
    if (props.chartData) nextTick(() => updateChart(props.chartData))
  }
})

function switchChartType(type) {
  currentChartType.value = type
  emit('chart-type-change', type)
  if (props.chartData) nextTick(() => updateChart(props.chartData))
}

function initChart() {
  if (!chartRef.value) return
  const container = chartRef.value
  container.style.height = props.height
  container.innerHTML = ''

  const mainDiv = document.createElement('div')
  mainDiv.id = `main-chart-${Math.random().toString(36).substr(2, 9)}`
  mainDiv.style.cssText = currentChartType.value === 'imr' || currentChartType.value === 'xbar_r'
    ? 'width:100%;height:65%' : 'width:100%;height:100%'
  container.appendChild(mainDiv)
  chartInstance = echarts.init(mainDiv)

  if (currentChartType.value === 'imr' || currentChartType.value === 'xbar_r') {
    const subDiv = document.createElement('div')
    subDiv.style.cssText = 'width:100%;height:35%'
    container.appendChild(subDiv)
    subChartInstance = echarts.init(subDiv)
  }

  window.addEventListener('resize', handleResize)

  if (props.chartData) updateChart(props.chartData)
}

function handleResize() {
  chartInstance?.resize()
  subChartInstance?.resize()
}

function disposeCharts() {
  window.removeEventListener('resize', handleResize)
  chartInstance?.dispose()
  chartInstance = null
  subChartInstance?.dispose()
  subChartInstance = null
}

/** 标线 label 统一样式: 加粗+阴影+不透明白底增强可读性, position 由 assignLabelPositions 统一分配 */
function makeLabel(text, color, position) {
  return {
    show: true,
    position: position || 'end',
    formatter: text,
    fontSize: 11,
    fontWeight: 'bold',
    color,
    backgroundColor: 'rgba(255,255,255,0.95)',
    padding: [4, 6],
    borderRadius: 4,
    borderColor: color,
    borderWidth: 1,
    shadowColor: 'rgba(0,0,0,0.25)',
    shadowBlur: 4,
    shadowOffsetY: 1
  }
}

/** 按标线值排序后统一分配 label 位置为 'end'(线末端, 即图表最右侧, 与 X 轴名称同列),
 *  相邻标线通过 offset 上下交替偏移避免重叠 */
function assignLabelPositions(lines) {
  if (!lines || lines.length === 0) return lines
  // 复制并按值从大到小排序(上方线在前)
  const sorted = [...lines].map((line, idx) => {
    const v = line.yAxis != null ? line.yAxis : (line.xAxis != null ? line.xAxis : null)
    return { line, value: v, origIdx: idx }
  }).filter(item => item.value != null).sort((a, b) => b.value - a.value)
  // 统一放最右侧 end 位置, 相邻标线交替上下偏移避免 label 重叠
  sorted.forEach((item, i) => {
    if (item.line.label) {
      item.line.label.position = 'end'
      // 偶数(上方线) label 上偏, 奇数(下方线) label 下偏
      item.line.label.offset = i % 2 === 0 ? [0, -8] : [0, 10]
    }
  })
  return lines
}

function buildMarkLines(limits) {
  const lines = []
  // 所有 label 统一放右边: 上方线 insideEndTop, 下方线 insideEndBottom
  // CL 实线最易遮挡数据, opacity 调低(0.55); UCL/LCL 虚线 0.75
  if (limits.ucl != null) lines.push({ yAxis: Number(limits.ucl), name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('UCL: ' + limits.ucl, '#f5222d', 'insideEndTop') })
  if (limits.lcl != null) lines.push({ yAxis: Number(limits.lcl), name: 'LCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('LCL: ' + limits.lcl, '#f5222d', 'insideEndBottom') })
  if (limits.cl != null) lines.push({ yAxis: Number(limits.cl), name: 'CL', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5, opacity: 0.55 }, label: makeLabel('CL: ' + limits.cl, '#52c41a', 'insideEndTop') })
  return lines
}

function buildSpecLines(limits) {
  const lines = []
  // USL/LSL 全部放右边, 与 UCL/LCL 上下错开
  if (limits.usl != null) lines.push({ yAxis: Number(limits.usl), name: 'USL', lineStyle: { color: '#fa8c16', type: 'dotted', width: 1.5, opacity: 0.75 }, label: makeLabel('USL: ' + limits.usl, '#fa8c16', 'insideEndTop') })
  if (limits.lsl != null) lines.push({ yAxis: Number(limits.lsl), name: 'LSL', lineStyle: { color: '#fa8c16', type: 'dotted', width: 1.5, opacity: 0.75 }, label: makeLabel('LSL: ' + limits.lsl, '#fa8c16', 'insideEndBottom') })
  return lines
}

/** 构建目标值 Target 标线 */
function buildTargetLine(limits) {
  if (limits.target == null) return []
  // Target 放右边
  return [{ yAxis: Number(limits.target), name: 'Target', lineStyle: { color: '#1677ff', type: 'dashdot', width: 1.5, opacity: 0.75 }, label: makeLabel('Target: ' + limits.target, '#1677ff', 'insideEndTop') }]
}

/** 构建图例辅助 series：data:[null] 让 series 有效但不可见(无数据点不画线)，仅用于 legend 显示标线颜色含义
 *  注意: 不能设置 opacity:0, 否则 legend 图标也会变透明显示为灰色
 *  legend 线条加粗(width:3) 增强高亮显示效果
 *  categories: 数组，元素为 { name, color, type } */
function buildLegendSeries(categories) {
  return categories.map(c => ({
    name: c.name, type: 'line', data: [null],
    lineStyle: { color: c.color, type: c.type, width: 3 },
    itemStyle: { color: c.color },
    showSymbol: false,
    silent: true
  }))
}

/** 监听 legend 点击, 联动控制主 series 的 markLine 显示/隐藏
 *  chart: 图表实例
 *  legendMap: { legendName: [markLineName, ...] } 标线 legend 名称到 markLine.name 的映射
 *  mainSeriesName: 主数据 series 的 name(包含 markLine)
 *  allMarkLines: 完整的 markLine 数据数组(不会被修改) */
function setupMarkLineLegendToggle(chart, legendMap, mainSeriesName, allMarkLines) {
  if (!chart || !allMarkLines || allMarkLines.length === 0) return
  chart.off('legendselectchanged')
  chart.on('legendselectchanged', function(params) {
    const selected = (params && params.selected) || {}
    // 过滤 markLine: 只保留对应 legend 项被选中的标线
    const visibleMarkLines = allMarkLines.filter(line => {
      for (const [legendName, lineNames] of Object.entries(legendMap)) {
        if (lineNames.includes(line.name)) {
          return selected[legendName] !== false
        }
      }
      return true  // 没有对应 legend 的标线默认显示
    })
    chart.setOption({
      series: [{ name: mainSeriesName, markLine: { silent: true, symbol: 'none', data: visibleMarkLines } }]
    })
  })
}

function getPointColors(values, oocFlags, oosFlags, zones) {
  return values.map((v, i) => {
    if (oosFlags && oosFlags[i] === 1) return '#cf1322'
    if (oocFlags && oocFlags[i] === 1) return '#f5222d'
    if (zones && zones[i] === 2) return '#faad14'
    if (zones && zones[i] === 3) return '#90CAF9'
    return '#1890ff'
  })
}

function getPointSymbols(values, oocFlags, oosFlags, zones) {
  return values.map((v, i) => {
    if (oosFlags && oosFlags[i] === 1) return 'diamond'
    if (oocFlags && oocFlags[i] === 1) return 'triangle'
    if (zones && zones[i] >= 2) return 'circle'
    return 'circle'
  })
}

function getPointSizes(values, oocFlags, oosFlags, zones) {
  return values.map((v, i) => {
    if (oosFlags && oosFlags[i] === 1) return 11
    if (oocFlags && oocFlags[i] === 1) return 9
    return 6
  })
}

function computeOosFlags(values, limits) {
  if (!values || !limits) return null
  const { usl, lsl } = limits
  if (usl == null && lsl == null) return null
  return values.map(v => {
    if (v == null) return 0
    if (usl != null && v > usl) return 1
    if (lsl != null && v < lsl) return 1
    return 0
  })
}

function computeOocFlags(values, limits) {
  if (!values || !limits) return null
  const { ucl, lcl } = limits
  if (ucl == null && lcl == null) return null
  return values.map(v => {
    if (v == null) return 0
    if (ucl != null && v > ucl) return 1
    if (lcl != null && v < lcl) return 1
    return 0
  })
}

function mergeOocFlags(backendOoc, clientOoc, length) {
  if (!backendOoc && !clientOoc) return null
  const result = new Array(length).fill(0)
  if (backendOoc) {
    for (let i = 0; i < Math.min(backendOoc.length, length); i++) {
      if (backendOoc[i]) result[i] = 1
    }
  }
  if (clientOoc) {
    for (let i = 0; i < Math.min(clientOoc.length, length); i++) {
      if (clientOoc[i]) result[i] = 1
    }
  }
  return result
}

function formatTimeLabel(val) {
  if (!val) return ''
  if (val.includes('T')) {
    return val.replace('T', ' ').substring(0, 19)
  }
  if (val.length > 19) return val.substring(0, 19)
  return val
}

// 生成子组序号数组 [1, 2, ..., n]，用于 X 轴刻度
function indexLabels(n) {
  return Array.from({ length: n }, (_, i) => i + 1)
}

function renderIMR(data) {
  if (!chartInstance || !subChartInstance || !data) return

  const { timeSeries, values, zones, oocFlags, limits } = data
  if (!values || !values.length) return

  // 统一颜色方案: 控制限红、中心线绿、规格限橙、目标值蓝
  const COLOR_CTRL = '#f5222d'
  const COLOR_CL = '#52c41a'
  const COLOR_SPEC = '#fa8c16'
  const COLOR_TARGET = '#1677ff'

  const allMarkLines = [...buildMarkLines(limits), ...buildSpecLines(limits), ...buildTargetLine(limits)]
  assignLabelPositions(allMarkLines)
  const oosFlags = computeOosFlags(values, limits)
  const clientOocFlags = computeOocFlags(values, limits)
  const effectiveOocFlags = mergeOocFlags(oocFlags, clientOocFlags, values.length)
  const pointColors = getPointColors(values, effectiveOocFlags, oosFlags, zones)
  const pointSymbols = getPointSymbols(values, effectiveOocFlags, oosFlags, zones)
  const pointSizes = getPointSizes(values, effectiveOocFlags, oosFlags, zones)

  // I 图图例 categories: 主系列 + 按实际存在的标线类型
  const iLegendCats = [{ name: '测量值', color: '#1890ff', type: 'solid' }]
  if (limits.usl != null || limits.lsl != null) iLegendCats.push({ name: '规格限 USL/LSL', color: COLOR_SPEC, type: 'dotted' })
  if (limits.target != null) iLegendCats.push({ name: '目标值 Target', color: COLOR_TARGET, type: 'dashdot' })
  if (limits.ucl != null || limits.lcl != null) iLegendCats.push({ name: '控制限 UCL/LCL', color: COLOR_CTRL, type: 'dashed' })
  if (limits.cl != null) iLegendCats.push({ name: '中心线 CL', color: COLOR_CL, type: 'solid' })

  const iOption = {
    title: {
      text: `I 图 - 单值控制图`,
      left: 'center',
      textStyle: { fontSize: 14, fontWeight: 600 },
      top: 8
    },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, confine: true, extraCssText: 'z-index:999', formatter(params) {
      if (!Array.isArray(params)) params = [params]
      const idx = params[0]?.dataIndex ?? 0
      const val = values[idx]
      let html = `<strong>数据点 ${idx + 1}</strong>`
      if (timeSeries && timeSeries[idx]) html += `<br/><small style="color:#8c8c8c">${formatTimeLabel(timeSeries[idx])}</small>`
      html += `<br/>${params[0].marker}${params[0].seriesName}: <strong>${val}</strong>`

      const isOOS = oosFlags && oosFlags[idx] === 1
      const isOOC = effectiveOocFlags && effectiveOocFlags[idx] === 1

      if (isOOS) {
        html += `<br/><span style="color:#cf1322;font-weight:600">⚠ 超规格限 (OOS)</span>`
        if (limits) html += `<br/><small style="color:#8c8c8c">USL=${limits.usl ?? '-'} LSL=${limits.lsl ?? '-'}</small>`
      } else if (isOOC) {
        html += `<br/><span style="color:#f5222d;font-weight:600">⚡ 超控制限 (OOC)</span>`
        if (limits) html += `<br/><small style="color:#8c8c8c">UCL=${limits.ucl ?? '-'} LCL=${limits.lcl ?? '-'}</small>`
      } else if (limits) {
        const inSpec = (limits.usl == null || val <= limits.usl) && (limits.lsl == null || val >= limits.lsl)
        const inCtrl = (limits.ucl == null || val <= limits.ucl) && (limits.lcl == null || val >= limits.lcl)
        if (inSpec && inCtrl) {
          html += `<br/><span style="color:#52c41a">✓ 正常</span>`
        } else if (inSpec && !inCtrl) {
          html += `<br/><span style="color:#faad14;font-weight:500">⚠ 接近控制限</span>`
          if (limits.ucl != null && limits.lcl != null) {
            const range = limits.ucl - limits.lcl
            const dist = Math.min(Math.abs(val - limits.ucl), Math.abs(val - limits.lcl))
            html += `<br/><small style="color:#8c8c8c">距控限 ${(dist / range * 100).toFixed(1)}%</small>`
          }
        } else if (!inSpec && inCtrl) {
          html += `<br/><span style="color:#faad14;font-weight:500">⚠ 偏离规格中心</span>`
        } else {
          html += `<br/><span style="color:#8c8c8c">待评估</span>`
        }
      }
      return html
    } },
    legend: {
      data: iLegendCats.map(c => c.name),
      top: 32, itemGap: 14, textStyle: { fontSize: 11, fontWeight: 'bold' }
    },
    grid: { left: 80, right: 120, top: 60, bottom: 55, containLabel: true },
    xAxis: { type: 'category', data: indexLabels(values.length), name: '数据点序号', nameTextStyle: { fontSize: 11 }, nameGap: 28, axisLabel: { fontSize: 10, interval: Math.floor(values.length / 15) || 0, margin: 8 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', scale: true, name: '测量值', nameTextStyle: { fontSize: 11 }, nameGap: 16, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [
      {
        name: '测量值', type: 'line', data: values,
        symbol: (val, params) => pointSymbols[params.dataIndex] || 'circle',
        symbolSize: (val, params) => pointSizes[params.dataIndex] || 6,
        lineStyle: { color: '#1890ff', width: 1.5 },
        itemStyle: { color(params) { return pointColors[params.dataIndex] || '#1890ff' } },
        markLine: { silent: true, symbol: 'none', data: allMarkLines }
      },
      ...buildLegendSeries(iLegendCats.filter(c => c.name !== '测量值'))
    ]
  }

  const mrValues = values.map((v, i) => i === 0 ? null : Math.abs(v - values[i - 1]))
  const validMr = mrValues.filter(v => v != null && !isNaN(v))

  let mrUcl = null, mrCl = null, mrLcl = null
  if (validMr.length > 0 && limits.cl != null) {
    const d2 = 1.128
    mrCl = validMr.reduce((a, b) => a + b, 0) / validMr.length
    const sigma = (limits.ucl != null && limits.cl != null ? (Number(limits.ucl) - Number(limits.cl)) / 3 : mrCl / d2)
    mrUcl = mrCl * 3.267
    mrLcl = 0
  }

  const mrMarkLines = []
  if (mrUcl != null) mrMarkLines.push({ yAxis: Number(mrUcl), name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('MR-UCL: ' + mrUcl.toFixed(4), '#f5222d', 'insideEndTop') })
  if (mrCl != null) mrMarkLines.push({ yAxis: Number(mrCl), name: 'CL', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5, opacity: 0.55 }, label: makeLabel('MR-CL: ' + mrCl.toFixed(4), '#52c41a', 'insideEndTop') })
  assignLabelPositions(mrMarkLines)

  // MR 图图例 categories: 主系列 + 控制限/中心线
  const mrLegendCats = [{ name: '移动极差', color: '#722ed1', type: 'solid' }]
  if (mrUcl != null) mrLegendCats.push({ name: '控制限 MR-UCL', color: COLOR_CTRL, type: 'dashed' })
  if (mrCl != null) mrLegendCats.push({ name: '中心线 MR-CL', color: COLOR_CL, type: 'solid' })

  const mrOption = {
    title: { text: 'MR 图 - 移动极差', left: 'center', textStyle: { fontSize: 13, fontWeight: 600 }, top: 6 },
    tooltip: { trigger: 'axis', confine: true, formatter(params) {
      if (!Array.isArray(params)) params = [params]
      const idx = params[0]?.dataIndex ?? 0
      const originalIdx = idx + 1
      let html = `<strong>数据点 ${originalIdx + 1}</strong>`
      if (timeSeries && timeSeries[originalIdx]) html += `<br/><small style="color:#8c8c8c">${formatTimeLabel(timeSeries[originalIdx])}</small>`
      params.forEach(p => { html += `<br/>${p.marker}${p.seriesName}: ${p.data}` })
      return html
    } },
    legend: {
      data: mrLegendCats.map(c => c.name),
      top: 28, itemGap: 14, textStyle: { fontSize: 11, fontWeight: 'bold' }
    },
    grid: { left: 80, right: 120, top: 56, bottom: 40, containLabel: true },
    xAxis: { type: 'category', data: indexLabels(values.length).slice(1), name: '数据点序号', nameTextStyle: { fontSize: 10 }, nameGap: 24, axisLabel: { fontSize: 9, interval: Math.floor(values.length / 18) || 0, margin: 6 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', min: 0, scale: true, name: '极差 MR', nameTextStyle: { fontSize: 10 }, nameGap: 14, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [
      {
        name: '移动极差', type: 'line', data: mrValues.slice(1), symbol: 'circle', symbolSize: 4,
        lineStyle: { color: '#722ed1', width: 1.5 },
        itemStyle: { color: '#722ed1' },
        markLine: { silent: true, symbol: 'none', data: mrMarkLines }
      },
      ...buildLegendSeries(mrLegendCats.filter(c => c.name !== '移动极差'))
    ]
  }

  chartInstance.setOption(iOption, true)
  subChartInstance.setOption(mrOption, true)

  // I 图: legend 联动 markLine 显示/隐藏
  const iLegendMap = {}
  if (limits.usl != null || limits.lsl != null) iLegendMap['规格限 USL/LSL'] = ['USL', 'LSL']
  if (limits.target != null) iLegendMap['目标值 Target'] = ['Target']
  if (limits.ucl != null || limits.lcl != null) iLegendMap['控制限 UCL/LCL'] = ['UCL', 'LCL']
  if (limits.cl != null) iLegendMap['中心线 CL'] = ['CL']
  setupMarkLineLegendToggle(chartInstance, iLegendMap, '测量值', allMarkLines)

  // MR 图: legend 联动 markLine 显示/隐藏
  const mrLegendMap = {}
  if (mrUcl != null) mrLegendMap['控制限 MR-UCL'] = ['UCL']
  if (mrCl != null) mrLegendMap['中心线 MR-CL'] = ['CL']
  setupMarkLineLegendToggle(subChartInstance, mrLegendMap, '移动极差', mrMarkLines)
}

function renderXbarR(data) {
  if (!chartInstance || !subChartInstance || !data) return

  const { timeSeries, values, zones, oocFlags, limits } = data
  if (!values || !values.length) return

  // 从后端返回数据读取子组大小，默认 5
  const subgroupSize = (data.subgroupSize && data.subgroupSize >= 2) ? data.subgroupSize : 5
  const numSubgroups = Math.floor(values.length / subgroupSize)

  if (numSubgroups < 2) {
    renderIMR(data)
    return
  }

  const xbarData = []
  const rData = []
  const xbarTimeLabels = []

  for (let i = 0; i < numSubgroups; i++) {
    const start = i * subgroupSize
    const end = start + subgroupSize
    const subgroup = values.slice(start, end).filter(v => v != null && !isNaN(v))
    
    if (subgroup.length > 0) {
      const sum = subgroup.reduce((a, b) => a + b, 0)
      const mean = sum / subgroup.length
      const max = Math.max(...subgroup)
      const min = Math.min(...subgroup)
      const range = max - min

      xbarData.push(mean)
      rData.push(range)
      xbarTimeLabels.push(timeSeries[start] || `子组${i + 1}`)
    }
  }

  if (xbarData.length < 2) {
    renderIMR(data)
    return
  }

  const grandMean = xbarData.reduce((a, b) => a + b, 0) / xbarData.length
  const avgRange = rData.reduce((a, b) => a + b, 0) / rData.length

  // 根据子组大小动态获取控制图系数
  const { A2, D3, D4 } = getCoefficients(subgroupSize)

  const xbarUcl = grandMean + A2 * avgRange
  const xbarLcl = grandMean - A2 * avgRange
  const rUcl = D4 * avgRange
  const rLcl = D3 * avgRange

  // X̄ 图控制限: 优先使用 version 手动限, 否则用 A2 系数计算值(避免同时画两套造成重复)
  const xbarEffUcl = limits.ucl != null ? limits.ucl : xbarUcl
  const xbarEffLcl = limits.lcl != null ? limits.lcl : xbarLcl
  const xbarEffCl = limits.cl != null ? limits.cl : grandMean

  // 统一颜色方案: 控制限红、中心线绿、规格限橙、目标值蓝
  const COLOR_CTRL = '#f5222d'
  const COLOR_CL = '#52c41a'
  const COLOR_SPEC = '#fa8c16'
  const COLOR_TARGET = '#1677ff'

  // X̄ 图标线: 规格限 + 目标值 + 控制限(去重后), label 位置由 assignLabelPositions 统一分配
  const xbarMarkLines = [
    ...buildSpecLines(limits),
    ...buildTargetLine(limits),
    { yAxis: Number(xbarEffUcl), name: 'UCL', lineStyle: { color: COLOR_CTRL, type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('UCL: ' + Number(xbarEffUcl).toFixed(4), COLOR_CTRL) },
    { yAxis: Number(xbarEffLcl), name: 'LCL', lineStyle: { color: COLOR_CTRL, type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('LCL: ' + Number(xbarEffLcl).toFixed(4), COLOR_CTRL) },
    { yAxis: Number(xbarEffCl), name: 'CL', lineStyle: { color: COLOR_CL, type: 'solid', width: 1.5, opacity: 0.55 }, label: makeLabel('CL: ' + Number(xbarEffCl).toFixed(4), COLOR_CL) }
  ]
  assignLabelPositions(xbarMarkLines)

  // X̄ 图图例: 按实际存在的标线类型构建 categories(用于生成不可见 series 让 legend 显示颜色)
  const xbarLegendCats = [{ name: '子组均值', color: '#1890ff', type: 'solid' }]
  if (limits.usl != null || limits.lsl != null) xbarLegendCats.push({ name: '规格限 USL/LSL', color: COLOR_SPEC, type: 'dotted' })
  if (limits.target != null) xbarLegendCats.push({ name: '目标值 Target', color: COLOR_TARGET, type: 'dashdot' })
  xbarLegendCats.push({ name: '控制限 UCL/LCL', color: COLOR_CTRL, type: 'dashed' })
  xbarLegendCats.push({ name: '中心线 CL', color: COLOR_CL, type: 'solid' })

  const xbarOosFlags = computeOosFlags(xbarData, limits)
  const xbarClientOocFlags = computeOocFlags(xbarData, limits)
  const xbarEffectiveOoc = mergeOocFlags(null, xbarClientOocFlags, xbarData.length)
  const pointColors = getPointColors(xbarData, xbarEffectiveOoc, xbarOosFlags, null)
  const pointSymbols = getPointSymbols(xbarData, xbarEffectiveOoc, xbarOosFlags, null)
  const pointSizes = getPointSizes(xbarData, xbarEffectiveOoc, xbarOosFlags, null)

  const xbarOption = {
    title: {
      text: `X̄ 图 - 均值控制图`,
      left: 'center',
      textStyle: { fontSize: 14, fontWeight: 600 },
      top: 8
    },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, confine: true, extraCssText: 'z-index:999', formatter(params) {
      if (!Array.isArray(params)) params = [params]
      const idx = params[0]?.dataIndex ?? 0
      let html = `<strong>子组 ${idx + 1}</strong>`
      if (xbarTimeLabels[idx]) html += `<br/><small style="color:#8c8c8c">${formatTimeLabel(xbarTimeLabels[idx])}</small>`
      html += `<br/>${params[0].marker}${params[0].seriesName}: <strong>${xbarData[idx]}</strong>`

      const isOOS = xbarOosFlags && xbarOosFlags[idx] === 1
      const isOOC = xbarEffectiveOoc && xbarEffectiveOoc[idx] === 1

      if (isOOS) {
        html += `<br/><span style="color:#cf1322;font-weight:600">⚠ 超规格限 (OOS)</span>`
      } else if (isOOC) {
        html += `<br/><span style="color:#f5222d;font-weight:600">⚡ 超控制限 (OOC)</span>`
      } else if (limits) {
        const inSpec = (limits.usl == null || xbarData[idx] <= limits.usl) && (limits.lsl == null || xbarData[idx] >= limits.lsl)
        const inCtrl = (xbarEffUcl == null || xbarData[idx] <= xbarEffUcl) && (xbarEffLcl == null || xbarData[idx] >= xbarEffLcl)
        if (inSpec && inCtrl) {
          html += `<br/><span style="color:#52c41a">✓ 正常</span>`
        } else if (inSpec && !inCtrl) {
          html += `<br/><span style="color:#faad14;font-weight:500">⚠ 接近控制限</span>`
        } else {
          html += `<br/><span style="color:#8c8c8c">待评估</span>`
        }
      }
      return html
    } },
    legend: {
      data: xbarLegendCats.map(c => c.name),
      top: 34, itemGap: 14, textStyle: { fontSize: 11, fontWeight: 'bold' }
    },
    grid: { left: 80, right: 120, top: 64, bottom: 55, containLabel: true },
    xAxis: { type: 'category', data: indexLabels(xbarData.length), name: '子组序号', nameTextStyle: { fontSize: 11 }, nameGap: 28, axisLabel: { fontSize: 10, interval: Math.floor(xbarData.length / 12) || 0, margin: 8 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', scale: true, name: '均值 X̄', nameTextStyle: { fontSize: 11 }, nameGap: 16, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [
      {
        name: '子组均值', type: 'line', data: xbarData,
        symbol: (val, params) => pointSymbols[params.dataIndex] || 'circle',
        symbolSize: (val, params) => pointSizes[params.dataIndex] || 8,
        lineStyle: { color: '#1890ff', width: 2 },
        itemStyle: { color(params) { return pointColors[params.dataIndex] || '#1890ff' } },
        markLine: {
          silent: true,
          symbol: 'none',
          data: xbarMarkLines
        }
      },
      ...buildLegendSeries(xbarLegendCats.filter(c => c.name !== '子组均值'))
    ]
  }

  // R 图标线: 仅控制限(规格限不适用于极差图), label 位置由 assignLabelPositions 统一分配
  const rMarkLines = [
    { yAxis: Number(rUcl), name: 'R-UCL', lineStyle: { color: COLOR_CTRL, type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('R-UCL: ' + rUcl.toFixed(4), COLOR_CTRL) },
    ...(rLcl > 0 ? [{ yAxis: Number(rLcl), name: 'R-LCL', lineStyle: { color: COLOR_CTRL, type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('R-LCL: ' + rLcl.toFixed(4), COLOR_CTRL) }] : []),
    { yAxis: Number(avgRange), name: 'R-CL', lineStyle: { color: COLOR_CL, type: 'solid', width: 1.5, opacity: 0.55 }, label: makeLabel('R-CL: ' + avgRange.toFixed(4), COLOR_CL) }
  ]
  assignLabelPositions(rMarkLines)

  // R 图图例: 仅控制限(规格限不适用于极差图) - 用 categories 生成不可见 series
  const rLegendCats = [
    { name: '极差', color: '#722ed1', type: 'solid' },
    { name: '控制限 R-UCL/R-LCL', color: COLOR_CTRL, type: 'dashed' },
    { name: '中心线 R-CL', color: COLOR_CL, type: 'solid' }
  ]

  const rOption = {
    title: { text: 'R 图 - 极差控制图', left: 'center', textStyle: { fontSize: 13, fontWeight: 600 }, top: 6 },
    tooltip: { trigger: 'axis', confine: true, formatter(params) {
      if (!Array.isArray(params)) params = [params]
      const idx = params[0]?.dataIndex ?? 0
      let html = `<strong>子组 ${idx + 1}</strong>`
      if (xbarTimeLabels[idx]) html += `<br/><small style="color:#8c8c8c">${formatTimeLabel(xbarTimeLabels[idx])}</small>`
      params.forEach(p => { if (p.value != null) html += `<br/>${p.marker}${p.seriesName}: ${p.data}` })
      return html
    } },
    legend: {
      data: rLegendCats.map(c => c.name),
      top: 30, itemGap: 14, textStyle: { fontSize: 11, fontWeight: 'bold' }
    },
    grid: { left: 80, right: 120, top: 56, bottom: 40, containLabel: true },
    xAxis: { type: 'category', data: indexLabels(rData.length), name: '子组序号', nameTextStyle: { fontSize: 10 }, nameGap: 24, axisLabel: { fontSize: 9, interval: Math.floor(rData.length / 15) || 0, margin: 6 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', min: 0, scale: true, name: '极差 R', nameTextStyle: { fontSize: 10 }, nameGap: 14, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [
      {
        name: '极差', type: 'line', data: rData, symbol: 'circle', symbolSize: 6,
        lineStyle: { color: '#722ed1', width: 2 },
        itemStyle: { color: '#722ed1' },
        markLine: {
          silent: true,
          symbol: 'none',
          data: rMarkLines
        }
      },
      ...buildLegendSeries(rLegendCats.filter(c => c.name !== '极差'))
    ]
  }

  chartInstance.setOption(xbarOption, true)
  subChartInstance.setOption(rOption, true)

  // X̄ 图: legend 联动 markLine 显示/隐藏
  const xbarLegendMap = {}
  if (limits.usl != null || limits.lsl != null) xbarLegendMap['规格限 USL/LSL'] = ['USL', 'LSL']
  if (limits.target != null) xbarLegendMap['目标值 Target'] = ['Target']
  xbarLegendMap['控制限 UCL/LCL'] = ['UCL', 'LCL']
  xbarLegendMap['中心线 CL'] = ['CL']
  setupMarkLineLegendToggle(chartInstance, xbarLegendMap, '子组均值', xbarMarkLines)

  // R 图: legend 联动 markLine 显示/隐藏
  const rLegendMap = {
    '控制限 R-UCL/R-LCL': ['R-UCL', 'R-LCL'],
    '中心线 R-CL': ['R-CL']
  }
  setupMarkLineLegendToggle(subChartInstance, rLegendMap, '极差', rMarkLines)
}

function renderHistogram(data) {
  if (!chartInstance || !data) return

  const { values, limits, capability } = data
  if (!values || !values.length) return

  const validValues = values.filter(v => v != null && !isNaN(v)).map(v => Number(v))

  if (validValues.length < 3) {
    chartInstance.setOption({
      title: { text: '数据不足，无法生成直方图', left: 'center', subtext: '至少需要3个有效数据点' }
    }, true)
    return
  }

  const minVal = Math.min(...validValues)
  const maxVal = Math.max(...validValues)
  const range = maxVal - minVal || 1
  const binCount = Math.min(Math.ceil(Math.sqrt(validValues.length)), 50)
  const binWidth = range / binCount

  const histogramData = new Array(binCount).fill(0)
  for (const v of validValues) {
    let idx = Math.floor((v - minVal) / binWidth)
    if (idx >= binCount) idx = binCount - 1
    if (idx < 0) idx = 0
    histogramData[idx]++
  }

  // bar/line 数据用 [binCenter, freq/normalY] 二维数组, 配合 value 类型 X 轴
  // 这样 markLine 的 xAxis 数值能正确定位到 USL/LSL/UCL/LCL/CL/Target
  const barData = []
  const normalCurve = []
  const mean = validValues.reduce((a, b) => a + b, 0) / validValues.length
  const stdDev = Math.sqrt(validValues.reduce((sum, v) => sum + (v - mean) ** 2, 0) / (validValues.length - 1))
  const maxFreq = Math.max(...histogramData)

  for (let i = 0; i < binCount; i++) {
    const binStart = minVal + i * binWidth
    const binEnd = binStart + binWidth
    const binCenter = (binStart + binEnd) / 2
    barData.push([binCenter, histogramData[i]])
    const normalY = (1 / (stdDev * Math.sqrt(2 * Math.PI))) *
                    Math.exp(-0.5 * ((binCenter - mean) / stdDev) ** 2)
    normalCurve.push([binCenter, normalY * maxFreq * binWidth * stdDev * Math.sqrt(2 * Math.PI)])
  }

  // 标线: 垂直线(xAxis), label 位置由 assignLabelPositions 统一分配
  // 控制限红/中心线绿/规格限橙/目标值蓝/均值蓝
  const markLines = []
  if (limits?.usl != null) markLines.push({ xAxis: Number(limits.usl), name: 'USL', lineStyle: { color: '#fa8c16', type: 'dotted', width: 1.5, opacity: 0.75 }, label: makeLabel('USL: ' + limits.usl, '#fa8c16') })
  if (limits?.lsl != null) markLines.push({ xAxis: Number(limits.lsl), name: 'LSL', lineStyle: { color: '#fa8c16', type: 'dotted', width: 1.5, opacity: 0.75 }, label: makeLabel('LSL: ' + limits.lsl, '#fa8c16') })
  if (limits?.ucl != null) markLines.push({ xAxis: Number(limits.ucl), name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('UCL: ' + limits.ucl, '#f5222d') })
  if (limits?.lcl != null) markLines.push({ xAxis: Number(limits.lcl), name: 'LCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2, opacity: 0.75 }, label: makeLabel('LCL: ' + limits.lcl, '#f5222d') })
  if (limits?.target != null) markLines.push({ xAxis: Number(limits.target), name: 'Target', lineStyle: { color: '#1677ff', type: 'dashdot', width: 1.5, opacity: 0.75 }, label: makeLabel('Target: ' + limits.target, '#1677ff') })
  if (limits?.cl != null) markLines.push({ xAxis: Number(limits.cl), name: 'CL', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5, opacity: 0.55 }, label: makeLabel('CL: ' + limits.cl, '#52c41a') })
  markLines.push({ xAxis: mean, name: '均值', lineStyle: { color: '#1890ff', type: 'dashdot', width: 2, opacity: 0.75 }, label: makeLabel('均值: ' + mean.toFixed(4), '#1890ff') })
  assignLabelPositions(markLines)

  // 直方图标线图例 categories: 按实际存在的标线类型构建, 颜色与图表内标线一致
  const histLegendCats = [{ name: '频次分布', color: '#1890ff', type: 'solid' }, { name: '正态拟合', color: '#f5222d', type: 'solid' }]
  if (limits?.usl != null || limits?.lsl != null) histLegendCats.push({ name: '规格限 USL/LSL', color: '#fa8c16', type: 'dotted' })
  if (limits?.ucl != null || limits?.lcl != null) histLegendCats.push({ name: '控制限 UCL/LCL', color: '#f5222d', type: 'dashed' })
  if (limits?.target != null) histLegendCats.push({ name: '目标值 Target', color: '#1677ff', type: 'dashdot' })
  if (limits?.cl != null) histLegendCats.push({ name: '中心线 CL', color: '#52c41a', type: 'solid' })
  histLegendCats.push({ name: '均值', color: '#1890ff', type: 'dashdot' })

  const option = {
    title: { text: '直方图 - 数据分布', left: 'center', textStyle: { fontSize: 14, fontWeight: 600 }, top: 8 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, confine: true, formatter(params) {
      if (!Array.isArray(params)) params = [params]
      let html = `<strong>分布区间</strong>`
      params.forEach(p => {
        if (p.value && Array.isArray(p.value) && p.value.length === 2) {
          html += `<br/>${p.marker}${p.seriesName}: <strong>${p.value[1]}</strong> <small style="color:#8c8c8c">(中心: ${p.value[0].toFixed(4)})</small>`
        }
      })
      return html
    } },
    legend: { data: histLegendCats.map(c => c.name), top: 30, itemGap: 14, textStyle: { fontSize: 11, fontWeight: 'bold' } },
    grid: { left: 85, right: 120, top: 62, bottom: 70, containLabel: true },
    xAxis: {
      type: 'value',
      name: '测量值',
      min: minVal,
      max: maxVal,
      nameTextStyle: { fontSize: 11 },
      nameGap: 28,
      axisLabel: { fontSize: 10, formatter: (val) => Number(val).toFixed(2) },
      axisTick: { alignWithLabel: true }
    },
    yAxis: [
      { type: 'value', name: '频次', position: 'left' },
      { type: 'value', name: '概率密度', position: 'right', show: false }
    ],
    series: [
      {
        name: '频次分布',
        type: 'bar',
        data: barData,
        barWidth: '99%',
        itemStyle: { color: '#1890ff', opacity: 0.7, borderRadius: [2, 2, 0, 0] },
        markLine: { silent: true, symbol: 'none', data: markLines }
      },
      {
        name: '正态拟合',
        type: 'line',
        data: normalCurve,
        smooth: true,
        symbol: 'none',
        lineStyle: { color: '#f5222d', width: 2 },
        yAxisIndex: 1
      },
      ...buildLegendSeries(histLegendCats.filter(c => c.name !== '频次分布' && c.name !== '正态拟合'))
    ]
  }

  chartInstance.setOption(option, true)

  // 直方图: legend 联动 markLine 显示/隐藏
  const histLegendMap = {}
  if (limits?.usl != null || limits?.lsl != null) histLegendMap['规格限 USL/LSL'] = ['USL', 'LSL']
  if (limits?.ucl != null || limits?.lcl != null) histLegendMap['控制限 UCL/LCL'] = ['UCL', 'LCL']
  if (limits?.target != null) histLegendMap['目标值 Target'] = ['Target']
  if (limits?.cl != null) histLegendMap['中心线 CL'] = ['CL']
  histLegendMap['均值'] = ['均值']
  setupMarkLineLegendToggle(chartInstance, histLegendMap, '频次分布', markLines)
}

function renderBoxplot(data) {
  if (!chartInstance || !data) return

  const { values, timeSeries, limits } = data
  if (!values || !values.length) return

  const validData = values.map((v, i) => ({
    value: v,
    time: timeSeries[i] || ''
  })).filter(d => d.value != null && !isNaN(d.value))

  if (validData.length < 4) {
    chartInstance.setOption({
      title: { text: '数据不足，无法生成箱线图', left: 'center', subtext: '至少需要4个有效数据点' }
    }, true)
    return
  }

  const sortedValues = validData.map(d => d.value).sort((a, b) => a - b)
  const n = sortedValues.length
  const q1Idx = Math.floor(n * 0.25)
  const q3Idx = Math.floor(n * 0.75)
  const q1 = sortedValues[q1Idx]
  const q3 = sortedValues[q3Idx]
  const median = sortedValues[Math.floor(n * 0.5)]
  const iqr = q3 - q1
  const lowerFence = q1 - 1.5 * iqr
  const upperFence = q3 + 1.5 * iqr

  const outliers = sortedValues.filter(v => v < lowerFence || v > upperFence)
  const whiskerMin = sortedValues.find(v => v >= lowerFence) || sortedValues[0]
  const whiskerMax = sortedValues.findLast(v => v <= upperFence) || sortedValues[n - 1]

  const boxData = [[whiskerMin, q1, median, q3, whiskerMax]]
  const outlierData = outliers.length > 0 ? [outliers] : [[]]

  const option = {
    title: { text: '箱线图 - 数据分布', left: 'center', textStyle: { fontSize: 14, fontWeight: 600 }, top: 8 },
    tooltip: {
      trigger: 'item',
      confine: true,
      formatter: function(params) {
        if (params.seriesType === 'boxplot') {
          return `最小值: ${params.data[0]}<br/>Q1: ${params.data[1]}<br/>中位数: ${params.data[2]}<br/>Q3: ${params.data[3]}<br/>最大值: ${params.data[4]}`
        }
        return `异常值: ${params.data}`
      }
    },
    grid: { left: 85, right: 75, top: 45, bottom: 50, containLabel: true },
    yAxis: { type: 'value', name: '测量值', scale: true },
    xAxis: { type: 'category', data: ['全部数据'] },
    series: [
      {
        name: 'boxplot',
        type: 'boxplot',
        data: boxData,
        itemStyle: {
          borderColor: '#1890ff',
          borderWidth: 1.5,
          color: '#e6f7ff'
        }
      },
      {
        name: 'outlier',
        type: 'scatter',
        data: outlierData,
        symbolSize: 8,
        itemStyle: { color: '#f5222d' }
      }
    ]
  }

  boxplotStats.value = { n, median, q1, q3, iqr, outliers }

  chartInstance.setOption(option, true)
}

function renderScatter(data) {
  if (!chartInstance || !data) return

  const { timeSeries, values, zones, oocFlags, limits } = data
  if (!values || !values.length) return

  const oosFlags = computeOosFlags(values, limits)
  const pointColors = getPointColors(values, oocFlags, oosFlags, zones)
  const scatterData = values.map((v, i) => [i, v])

  const allMarkLines = [...buildMarkLines(limits), ...buildSpecLines(limits)]
  assignLabelPositions(allMarkLines)

  let trendLine = null
  if (values.length >= 2) {
    const n = values.length
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0
    for (let i = 0; i < n; i++) {
      sumX += i
      sumY += values[i]
      sumXY += i * values[i]
      sumX2 += i * i
    }
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX)
    const intercept = (sumY - slope * sumX) / n
    trendLine = [
      [0, intercept],
      [n - 1, slope * (n - 1) + intercept]
    ]
  }

  // 散点图标线图例 categories: 按实际存在的标线类型构建, 颜色与图表内标线一致
  const scatterLegendCats = [{ name: '测量值', color: '#1890ff', type: 'solid' }]
  if (trendLine) scatterLegendCats.push({ name: '趋势线', color: '#faad14', type: 'dashed' })
  if (limits?.usl != null || limits?.lsl != null) scatterLegendCats.push({ name: '规格限 USL/LSL', color: '#fa8c16', type: 'dotted' })
  if (limits?.ucl != null || limits?.lcl != null) scatterLegendCats.push({ name: '控制限 UCL/LCL', color: '#f5222d', type: 'dashed' })
  if (limits?.cl != null) scatterLegendCats.push({ name: '中心线 CL', color: '#52c41a', type: 'solid' })

  const option = {
    title: { text: '散点图 - 趋势分析', left: 'center', textStyle: { fontSize: 14, fontWeight: 600 }, top: 8 },
    tooltip: { trigger: 'item', confine: true, formatter: function(p) {
      const idx = p.data[0]
      let html = `<strong>数据点 ${idx + 1}</strong>`
      if (timeSeries && timeSeries[idx]) html += `<br/><small style="color:#8c8c8c">${formatTimeLabel(timeSeries[idx])}</small>`
      html += `<br/>值: ${p.data[1]}`
      return html
    } },
    legend: { data: scatterLegendCats.map(c => c.name), top: 30, itemGap: 14, textStyle: { fontSize: 11, fontWeight: 'bold' } },
    grid: { left: 75, right: 120, top: 60, bottom: 55, containLabel: true },
    xAxis: { type: 'value', name: '数据点序号', minInterval: 1 },
    yAxis: { type: 'value', scale: true, name: '测量值', nameTextStyle: { fontSize: 11 } },
    series: [
      {
        name: '测量值',
        type: 'scatter',
        data: scatterData,
        symbolSize: 8,
        itemStyle: { color(params) { return pointColors[params.dataIndex] || '#1890ff' } },
        markLine: { silent: true, symbol: 'none', data: allMarkLines }
      },
      ...(trendLine ? [{
        name: '趋势线',
        type: 'line',
        data: trendLine,
        symbol: 'none',
        lineStyle: { color: '#faad14', width: 2, type: 'dashed' }
      }] : []),
      ...buildLegendSeries(scatterLegendCats.filter(c => c.name !== '测量值' && c.name !== '趋势线'))
    ]
  }

  chartInstance.setOption(option, true)

  // 散点图: legend 联动 markLine 显示/隐藏
  const scatterLegendMap = {}
  if (limits?.usl != null || limits?.lsl != null) scatterLegendMap['规格限 USL/LSL'] = ['USL', 'LSL']
  if (limits?.ucl != null || limits?.lcl != null) scatterLegendMap['控制限 UCL/LCL'] = ['UCL', 'LCL']
  if (limits?.cl != null) scatterLegendMap['中心线 CL'] = ['CL']
  setupMarkLineLegendToggle(chartInstance, scatterLegendMap, '测量值', allMarkLines)
}

function updateChart(data) {
  disposeCharts()
  boxplotStats.value = null

  if (!chartRef.value) return
  const container = chartRef.value
  container.innerHTML = ''

  const isDualChart = currentChartType.value === 'imr' || currentChartType.value === 'xbar_r'

  const mainDiv = document.createElement('div')
  mainDiv.style.cssText = isDualChart ? 'width:100%;height:65%' : 'width:100%;height:100%'
  container.appendChild(mainDiv)
  chartInstance = echarts.init(mainDiv)

  if (isDualChart) {
    const subDiv = document.createElement('div')
    subDiv.style.cssText = 'width:100%;height:35%'
    container.appendChild(subDiv)
    subChartInstance = echarts.init(subDiv)
  }

  switch (currentChartType.value) {
    case 'imr':
      renderIMR(data)
      break
    case 'xbar_r':
      renderXbarR(data)
      break
    case 'histogram':
      renderHistogram(data)
      break
    case 'boxplot':
      renderBoxplot(data)
      break
    case 'scatter':
      renderScatter(data)
      break
    case 'pchart':
      renderCountChart(data)
      break
    default:
      renderIMR(data)
  }
}

// 计数型图渲染(P/NP/C/U)：单面板，P/U 描点为比率，NP/C 描点为计数
function renderCountChart(data) {
  if (!chartInstance || !data) return
  const { timeSeries, values, sampleSizes, zones, oocFlags, limits } = data
  if (!values || !values.length) return

  // 统一颜色方案: 控制限红、中心线绿、规格限橙、目标值蓝
  const COLOR_CTRL = '#f5222d'
  const COLOR_CL = '#52c41a'
  const COLOR_SPEC = '#fa8c16'
  const COLOR_TARGET = '#1677ff'

  const rawType = (props.chartType || 'P').replace(/[-_]/g, '').toUpperCase()
  const isRateChart = rawType === 'P' || rawType === 'U'
  const chartTitleMap = { P: 'P 图 - 不合格率控制图', NP: 'NP 图 - 不合格数控制图', C: 'C 图 - 缺陷数控制图', U: 'U 图 - 单位缺陷数控制图' }
  const yAxisNameMap = { P: '不合格率 p', NP: '不合格数 np', C: '缺陷数 c', U: '单位缺陷数 u' }
  const seriesNameMap = { P: '不合格率', NP: '不合格数', C: '缺陷数', U: '单位缺陷数' }
  const chartTitle = chartTitleMap[rawType] || '计数型控制图'
  const yAxisName = yAxisNameMap[rawType] || '值'
  const seriesName = seriesNameMap[rawType] || '值'

  // P/U 图描点值为 比率 = 不合格数/样本量；NP/C 图描点值为原始计数
  const plotValues = values.map((v, i) => {
    if (isRateChart) {
      const sz = sampleSizes && sampleSizes[i] ? Number(sampleSizes[i]) : 1
      return sz > 0 ? Number(v) / sz : 0
    }
    return Number(v)
  })

  const allMarkLines = [...buildMarkLines(limits), ...buildSpecLines(limits), ...buildTargetLine(limits)]
  assignLabelPositions(allMarkLines)
  const oosFlags = computeOosFlags(plotValues, limits)
  const clientOocFlags = computeOocFlags(plotValues, limits)
  const effectiveOocFlags = mergeOocFlags(oocFlags, clientOocFlags, plotValues.length)
  const pointColors = getPointColors(plotValues, effectiveOocFlags, oosFlags, zones)
  const pointSymbols = getPointSymbols(plotValues, effectiveOocFlags, oosFlags, zones)
  const pointSizes = getPointSizes(plotValues, effectiveOocFlags, oosFlags, zones)

  // 计数图图例 categories: 主系列 + 按实际存在的标线类型
  const countLegendCats = [{ name: seriesName, color: '#fa8c16', type: 'solid' }]
  if (limits.usl != null || limits.lsl != null) countLegendCats.push({ name: '规格限 USL/LSL', color: COLOR_SPEC, type: 'dotted' })
  if (limits.target != null) countLegendCats.push({ name: '目标值 Target', color: COLOR_TARGET, type: 'dashdot' })
  if (limits.ucl != null || limits.lcl != null) countLegendCats.push({ name: '控制限 UCL/LCL', color: COLOR_CTRL, type: 'dashed' })
  if (limits.cl != null) countLegendCats.push({ name: '中心线 CL', color: COLOR_CL, type: 'solid' })

  const option = {
    title: { text: chartTitle, left: 'center', textStyle: { fontSize: 14, fontWeight: 600 }, top: 8 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, confine: true, extraCssText: 'z-index:999', formatter(params) {
      if (!Array.isArray(params)) params = [params]
      const idx = params[0]?.dataIndex ?? 0
      const val = plotValues[idx]
      const rawVal = values[idx]
      const sz = sampleSizes && sampleSizes[idx] ? sampleSizes[idx] : null
      let html = `<strong>样本 ${idx + 1}</strong>`
      if (timeSeries && timeSeries[idx]) html += `<br/><small style="color:#8c8c8c">${formatTimeLabel(timeSeries[idx])}</small>`
      html += `<br/>${params[0].marker}${seriesName}: <strong>${val.toFixed(4)}</strong>`
      if (isRateChart && sz != null) html += `<br/><small style="color:#8c8c8c">(${rawVal}/${sz})</small>`

      const isOOS = oosFlags && oosFlags[idx] === 1
      const isOOC = effectiveOocFlags && effectiveOocFlags[idx] === 1
      if (isOOS) {
        html += `<br/><span style="color:#cf1322;font-weight:600">⚠ 超规格限 (OOS)</span>`
        if (limits) html += `<br/><small style="color:#8c8c8c">USL=${limits.usl ?? '-'} LSL=${limits.lsl ?? '-'}</small>`
      } else if (isOOC) {
        html += `<br/><span style="color:#f5222d;font-weight:600">⚡ 超控制限 (OOC)</span>`
        if (limits) html += `<br/><small style="color:#8c8c8c">UCL=${limits.ucl ?? '-'} LCL=${limits.lcl ?? '-'}</small>`
      } else if (limits) {
        const inSpec = (limits.usl == null || val <= limits.usl) && (limits.lsl == null || val >= limits.lsl)
        const inCtrl = (limits.ucl == null || val <= limits.ucl) && (limits.lcl == null || val >= limits.lcl)
        if (inSpec && inCtrl) html += `<br/><span style="color:#52c41a">✓ 正常</span>`
        else if (!inCtrl) html += `<br/><span style="color:#faad14;font-weight:500">⚠ 接近控制限</span>`
        else html += `<br/><span style="color:#8c8c8c">待评估</span>`
      }
      return html
    } },
    legend: { data: countLegendCats.map(c => c.name), top: 32, itemGap: 14, textStyle: { fontSize: 11, fontWeight: 'bold' } },
    grid: { left: 80, right: 120, top: 60, bottom: 55, containLabel: true },
    xAxis: { type: 'category', data: indexLabels(plotValues.length), name: '样本序号', nameTextStyle: { fontSize: 11 }, nameGap: 28, axisLabel: { fontSize: 10, interval: Math.floor(plotValues.length / 15) || 0, margin: 8 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', scale: true, name: yAxisName, nameTextStyle: { fontSize: 11 }, nameGap: 16, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [
      {
        name: seriesName, type: 'line', data: plotValues,
        symbol: (val, params) => pointSymbols[params.dataIndex] || 'circle',
        symbolSize: (val, params) => pointSizes[params.dataIndex] || 6,
        lineStyle: { color: '#fa8c16', width: 1.5 },
        itemStyle: { color(params) { return pointColors[params.dataIndex] || '#fa8c16' } },
        markLine: { silent: true, symbol: 'none', data: allMarkLines }
      },
      ...buildLegendSeries(countLegendCats.filter(c => c.name !== seriesName))
    ]
  }
  chartInstance.setOption(option, true)

  // 计数图: legend 联动 markLine 显示/隐藏
  const countLegendMap = {}
  if (limits.usl != null || limits.lsl != null) countLegendMap['规格限 USL/LSL'] = ['USL', 'LSL']
  if (limits.target != null) countLegendMap['目标值 Target'] = ['Target']
  if (limits.ucl != null || limits.lcl != null) countLegendMap['控制限 UCL/LCL'] = ['UCL', 'LCL']
  if (limits.cl != null) countLegendMap['中心线 CL'] = ['CL']
  setupMarkLineLegendToggle(chartInstance, countLegendMap, seriesName, allMarkLines)
}

function capClass(val) {
  if (val == null) return ''
  if (val >= 1.67) return 'cpk-excellent'
  if (val >= 1.33) return 'cpk-good'
  if (val >= 1.0) return 'cpk-acceptable'
  return 'cpk-poor'
}

defineExpose({ refresh: handleResize, getCurrentType: () => currentChartType.value })
</script>

<style scoped>
.spc-chart-container {
  width: 100%;
}
.spc-control-chart {
  width: 100%;
  min-height: 560px;
}
.spc-capability-panel {
  margin-top: 12px;
  padding: 12px 16px;
  background: var(--accent-lighter);
  border-radius: 8px;
  border: 1px solid var(--accent-primary);
  opacity: 0.7;
}
.cap-row {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  align-items: center;
  line-height: 2;
}
.cap-label {
  font-size: 12px;
  color: var(--text-secondary);
  margin-right: 4px;
}
.cap-highlight {
  background: var(--bg-secondary);
  padding: 2px 8px;
  border-radius: 4px;
  border: 1px solid var(--accent-primary);
}
.cap-tag {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 10px;
  font-size: 11px;
  font-weight: 600;
}
.tag-normal {
  background: rgba(56,158,13,0.08);
  color: #389e0d;
  border: 1px solid rgba(56,158,13,0.3);
}
.tag-non-normal {
  background: rgba(207,19,34,0.06);
  color: #cf1322;
  border: 1px solid rgba(207,19,34,0.3);
}
.cpk-excellent { color: #389e0d; }
.cpk-good { color: #1890ff; }
.cpk-acceptable { color: #faad14; }
.cpk-poor { color: #f5222d; }
.pass { color: #389e0d; font-weight: 700; }
.fail { color: #cf1322; font-weight: 700; }

.chart-toolbar {
  margin-bottom: 12px;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  border-radius: 6px;
  border: 1px solid var(--border-color);
}
.chart-type-selector {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
.type-btn {
  padding: 6px 16px;
  border: 1px solid var(--border-input);
  border-radius: 4px;
  background: var(--bg-secondary);
  cursor: pointer;
  font-size: 13px;
  transition: all 0.2s;
  color: var(--text-secondary);
}
.type-btn:hover {
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}
.type-btn.active {
  background: var(--accent-primary);
  border-color: var(--accent-primary);
  color: var(--text-on-accent);
}

.spc-boxplot-stats {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px 0;
  margin-top: 10px;
  padding: 10px 16px;
  background: var(--bg-tertiary);
  border-radius: 8px;
  border: 1px solid var(--border-color);
  font-size: 12px;
}

.stat-item {
  color: var(--text-secondary);
  line-height: 1.8;
}

.stat-item strong {
  font-weight: 600;
  color: var(--text-primary);
}

.stat-divider {
  width: 1px;
  height: 16px;
  background: var(--border-input);
  flex-shrink: 0;
}

.stat-danger strong { color: #f5222d; }
.stat-safe strong { color: #389e0d; }
</style>