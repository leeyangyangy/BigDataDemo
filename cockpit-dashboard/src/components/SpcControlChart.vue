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
      </div>
      <div class="cap-row">
        <span class="cap-label">均值</span><strong>{{ capabilityInfo.mean ?? '-' }}</strong>
        <span class="cap-label">σ</span><strong>{{ capabilityInfo.stdDev ?? '-' }}</strong>
        <span class="cap-label">样本N</span><strong>{{ capabilityInfo.sampleCount ?? 0 }}</strong>
        <span class="cap-label cap-highlight" v-if="capabilityInfo.passRate != null">
          合格率 <strong :class="capabilityInfo.passRate >= 95 ? 'pass' : 'fail'">{{ capabilityInfo.passRate }}%</strong>
          <small>({{ capabilityInfo.passCount }}/{{ capabilityInfo.passCount + capabilityInfo.failCount }})</small>
        </span>
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
  height: { type: String, default: '520px' },
  chartType: { type: String, default: '' },
  showToolbar: { type: Boolean, default: true }
})

const emit = defineEmits(['chart-type-change'])

const chartRef = ref(null)
let chartInstance = null
let subChartInstance = null

const currentChartType = ref('imr')
const boxplotStats = ref(null)

const chartTypes = [
  { value: 'imr', label: 'I-MR图' },
  { value: 'xbar_r', label: 'Xbar-R图' },
  { value: 'histogram', label: '直方图' },
  { value: 'boxplot', label: '箱线图' },
  { value: 'scatter', label: '散点图' }
]

const capabilityInfo = computed(() => {
  if (!props.chartData?.capability) return null
  const c = props.chartData.capability
  return {
    cp: c.cp, cpk: c.cpk, pp: c.pp, ppk: c.ppk,
    mean: c.mean, stdDev: c.stdDev,
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

function buildMarkLines(limits) {
  const lines = []
  if (limits.ucl != null) lines.push({ yAxis: limits.ucl, name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { position: 'end', formatter: 'UCL: {c}', fontSize: 10 } })
  if (limits.lcl != null) lines.push({ yAxis: limits.lcl, name: 'LCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { position: 'end', formatter: 'LCL: {c}', fontSize: 10 } })
  if (limits.cl != null) lines.push({ yAxis: limits.cl, name: 'CL', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5 }, label: { position: 'end', formatter: 'CL: {c}', fontSize: 10 } })
  return lines
}

function buildSpecLines(limits) {
  const lines = []
  if (limits.usl != null) lines.push({ yAxis: limits.usl, name: 'USL', lineStyle: { color: '#ff4d4f', type: 'dotted', width: 1.2 }, label: { position: 'insideEndTop', formatter: 'USL: {c}', fontSize: 9, color: '#ff4d4f' } })
  if (limits.lsl != null) lines.push({ yAxis: limits.lsl, name: 'LSL', lineStyle: { color: '#ff4d4f', type: 'dotted', width: 1.2 }, label: { position: 'insideEndBottom', formatter: 'LSL: {c}', fontSize: 9, color: '#ff4d4f' } })
  return lines
}

function getPointColors(values, oocFlags, zones) {
  return values.map((v, i) => {
    if (oocFlags && oocFlags[i] === 1) return '#f5222d'
    if (zones && zones[i] === 2) return '#faad14'
    if (zones && zones[i] === 3) return '#90CAF9'
    return '#1890ff'
  })
}

function formatTimeLabel(val) {
  if (!val) return ''
  if (val.includes('T')) {
    return val.replace('T', ' ').substring(0, 19)
  }
  if (val.length > 19) return val.substring(0, 19)
  return val
}

function renderIMR(data) {
  if (!chartInstance || !subChartInstance || !data) return

  const { timeSeries, values, zones, oocFlags, limits } = data
  if (!values || !values.length) return

  const allMarkLines = [...buildMarkLines(limits), ...buildSpecLines(limits)]
  const pointColors = getPointColors(values, oocFlags, zones)

  const iOption = {
    title: {
      text: `I 图 - 单值控制图`,
      left: 'center',
      textStyle: { fontSize: 14, fontWeight: 600 },
      top: 8
    },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, confine: true, extraCssText: 'z-index:999', formatter(params) {
      if (!Array.isArray(params)) params = [params]
      let html = `<strong>${formatTimeLabel(params[0]?.axisValue || '')}</strong>`
      params.forEach(p => { html += `<br/>${p.marker}${p.seriesName}: ${p.data}` })
      return html
    } },
    legend: { data: ['测量值'], top: 30, itemGap: 16, textStyle: { fontSize: 12 } },
    grid: { left: 80, right: 45, top: 60, bottom: 55, containLabel: false },
    xAxis: { type: 'category', data: timeSeries, axisLabel: { rotate: 35, fontSize: 10, formatter: formatTimeLabel, interval: Math.floor(timeSeries.length / 15) || 0, margin: 10 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', scale: true, name: '测量值', nameTextStyle: { fontSize: 11 }, nameGap: 16, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [{
      name: '测量值', type: 'line', data: values, symbol: 'circle', symbolSize: 6,
      lineStyle: { color: '#1890ff', width: 1.5 },
      itemStyle: { color(params) { return pointColors[params.dataIndex] || '#1890ff' } },
      markLine: { silent: true, symbol: 'none', data: allMarkLines }
    }]
  }

  const mrValues = values.map((v, i) => i === 0 ? null : Math.abs(v - values[i - 1]))
  const validMr = mrValues.filter(v => v != null && !isNaN(v))

  let mrUcl = null, mrCl = null, mrLcl = null
  if (validMr.length > 0 && limits.cl != null) {
    const d2 = 1.128
    mrCl = validMr.reduce((a, b) => a + b, 0) / validMr.length
    const sigma = (limits.ucl != null ? limits.ucl.subtract(limits.cl).doubleValue() / 3 : mrCl / d2)
    mrUcl = mrCl * 3.267
    mrLcl = 0
  }

  const mrMarkLines = []
  if (mrUcl != null) mrMarkLines.push({ yAxis: mrUcl, name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { position: 'end', formatter: 'MR-UCL: {c}', fontSize: 10 } })
  if (mrCl != null) mrMarkLines.push({ yAxis: mrCl, name: 'CL', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5 }, label: { position: 'end', formatter: 'MR-CL: {c}', fontSize: 10 } })

  const mrOption = {
    title: { text: 'MR 图 - 移动极差', left: 'center', textStyle: { fontSize: 13, fontWeight: 600 }, top: 6 },
    tooltip: { trigger: 'axis', confine: true, formatter(params) {
      if (!Array.isArray(params)) params = [params]
      let html = `<strong>${formatTimeLabel(params[0]?.axisValue || '')}</strong>`
      params.forEach(p => { html += `<br/>${p.marker}${p.seriesName}: ${p.data}` })
      return html
    } },
    grid: { left: 80, right: 45, top: 38, bottom: 40 },
    xAxis: { type: 'category', data: timeSeries.slice(1), axisLabel: { rotate: 35, fontSize: 9, formatter: formatTimeLabel, interval: Math.floor(timeSeries.length / 18) || 0, margin: 8 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', min: 0, scale: true, name: '极差 MR', nameTextStyle: { fontSize: 10 }, nameGap: 14, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [{
      name: '移动极差', type: 'line', data: mrValues.slice(1), symbol: 'circle', symbolSize: 4,
      lineStyle: { color: '#722ed1', width: 1.5 },
      itemStyle: { color: '#722ed1' },
      markLine: { silent: true, symbol: 'none', data: mrMarkLines }
    }]
  }

  chartInstance.setOption(iOption, true)
  subChartInstance.setOption(mrOption, true)
}

function renderXbarR(data) {
  if (!chartInstance || !subChartInstance || !data) return

  const { timeSeries, values, zones, oocFlags, limits } = data
  if (!values || !values.length) return

  const subgroupSize = 5
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

  const A2 = 0.577
  const D3 = 0
  const D4 = 2.114

  const xbarUcl = grandMean + A2 * avgRange
  const xbarLcl = grandMean - A2 * avgRange
  const rUcl = D4 * avgRange
  const rLcl = D3 * avgRange

  const allMarkLines = [...buildMarkLines(limits), ...buildSpecLines(limits)]
  const pointColors = getPointColors(xbarData, null, zones)

  const xbarOption = {
    title: {
      text: `X̄ 图 - 均值控制图`,
      left: 'center',
      textStyle: { fontSize: 14, fontWeight: 600 },
      top: 8
    },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, confine: true, extraCssText: 'z-index:999', formatter(params) {
      if (!Array.isArray(params)) params = [params]
      let html = `<strong>${formatTimeLabel(params[0]?.axisValue || '')}</strong>`
      params.forEach(p => { html += `<br/>${p.marker}${p.seriesName}: ${p.data}` })
      return html
    } },
    legend: { data: ['子组均值'], top: 30, itemGap: 16, textStyle: { fontSize: 12 } },
    grid: { left: 80, right: 45, top: 60, bottom: 55, containLabel: false },
    xAxis: { type: 'category', data: xbarTimeLabels, axisLabel: { rotate: 35, fontSize: 10, formatter: formatTimeLabel, interval: Math.floor(xbarTimeLabels.length / 12) || 0, margin: 10 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', scale: true, name: '均值 X̄', nameTextStyle: { fontSize: 11 }, nameGap: 16, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [{
      name: '子组均值', type: 'line', data: xbarData, symbol: 'circle', symbolSize: 8,
      lineStyle: { color: '#1890ff', width: 2 },
      itemStyle: { color(params) { return pointColors[params.dataIndex] || '#1890ff' } },
      markLine: {
        silent: true,
        symbol: 'none',
        data: [
          ...allMarkLines,
          { yAxis: xbarUcl.toFixed(4), name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { position: 'end', formatter: 'UCL: {c}', fontSize: 10 } },
          { yAxis: xbarLcl.toFixed(4), name: 'LCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { position: 'end', formatter: 'LCL: {c}', fontSize: 10 } },
          { yAxis: grandMean.toFixed(4), name: 'CL', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5 }, label: { position: 'end', formatter: 'CL: {c}', fontSize: 10 } }
        ]
      }
    }]
  }

  const rOption = {
    title: { text: 'R 图 - 极差控制图', left: 'center', textStyle: { fontSize: 13, fontWeight: 600 }, top: 6 },
    tooltip: { trigger: 'axis', confine: true, formatter(params) {
      if (!Array.isArray(params)) params = [params]
      let html = `<strong>${formatTimeLabel(params[0]?.axisValue || '')}</strong>`
      params.forEach(p => { html += `<br/>${p.marker}${p.seriesName}: ${p.data}` })
      return html
    } },
    grid: { left: 80, right: 45, top: 38, bottom: 40 },
    xAxis: { type: 'category', data: xbarTimeLabels, axisLabel: { rotate: 35, fontSize: 9, formatter: formatTimeLabel, interval: Math.floor(xbarTimeLabels.length / 15) || 0, margin: 8 }, axisTick: { alignWithLabel: true } },
    yAxis: { type: 'value', min: 0, scale: true, name: '极差 R', nameTextStyle: { fontSize: 10 }, nameGap: 14, splitLine: { lineStyle: { type: 'dashed', opacity: 0.4 } } },
    series: [{
      name: '极差', type: 'line', data: rData, symbol: 'circle', symbolSize: 6,
      lineStyle: { color: '#722ed1', width: 2 },
      itemStyle: { color: '#722ed1' },
      markLine: {
        silent: true,
        symbol: 'none',
        data: [
          { yAxis: rUcl.toFixed(4), name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { position: 'end', formatter: 'R-UCL: {c}', fontSize: 10 } },
          ...(rLcl > 0 ? [{ yAxis: rLcl.toFixed(4), name: 'LCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { position: 'end', formatter: 'R-LCL: {c}', fontSize: 10 } }] : []),
          { yAxis: avgRange.toFixed(4), name: 'CL', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5 }, label: { position: 'end', formatter: 'R-CL: {c}', fontSize: 10 } }
        ]
      }
    }]
  }

  chartInstance.setOption(xbarOption, true)
  subChartInstance.setOption(rOption, true)
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

  const bins = []
  const normalCurve = []
  const mean = validValues.reduce((a, b) => a + b, 0) / validValues.length
  const stdDev = Math.sqrt(validValues.reduce((sum, v) => sum + (v - mean) ** 2, 0) / (validValues.length - 1))
  const maxFreq = Math.max(...histogramData)

  for (let i = 0; i < binCount; i++) {
    const binStart = minVal + i * binWidth
    const binEnd = binStart + binWidth
    bins.push(`${binStart.toFixed(1)}~${binEnd.toFixed(1)}`)

    const x = (binStart + binEnd) / 2
    const normalY = (1 / (stdDev * Math.sqrt(2 * Math.PI))) *
                    Math.exp(-0.5 * ((x - mean) / stdDev) ** 2)
    normalCurve.push(normalY * maxFreq * binWidth * stdDev * Math.sqrt(2 * Math.PI))
  }

  const markLines = []
  if (limits?.usl != null) markLines.push({ xAxis: limits.usl, name: 'USL', lineStyle: { color: '#ff4d4f', type: 'dotted', width: 2 }, label: { formatter: 'USL {c}', fontSize: 10, position: 'insideEndTop', rotate: 0 } })
  if (limits?.lsl != null) markLines.push({ xAxis: limits.lsl, name: 'LSL', lineStyle: { color: '#ff4d4f', type: 'dotted', width: 2 }, label: { formatter: 'LSL {c}', fontSize: 10, position: 'insideEndBottom', rotate: 0 } })
  if (limits?.ucl != null) markLines.push({ xAxis: limits.ucl, name: 'UCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { formatter: 'UCL {c}', fontSize: 10, position: 'insideEndTop', rotate: 0 } })
  if (limits?.lcl != null) markLines.push({ xAxis: limits.lcl, name: 'LCL', lineStyle: { color: '#f5222d', type: 'dashed', width: 2 }, label: { formatter: 'LCL {c}', fontSize: 10, position: 'insideEndBottom', rotate: 0 } })
  if (limits?.cl != null) markLines.push({ xAxis: limits.cl, name: '目标值', lineStyle: { color: '#52c41a', type: 'solid', width: 1.5 }, label: { formatter: 'CL {c}', fontSize: 10, position: 'insideStartTop', rotate: 0 } })
  markLines.push({ xAxis: mean, name: '均值', lineStyle: { color: '#1890ff', type: 'dashdot', width: 2 }, label: { formatter: '均值 {c}', fontSize: 10, position: 'insideStartBottom', rotate: 0 } })

  const option = {
    title: { text: '直方图 - 数据分布', left: 'center', textStyle: { fontSize: 14, fontWeight: 600 }, top: 8 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, confine: true },
    legend: { data: ['频次分布', '正态拟合'], top: 30, itemGap: 16, textStyle: { fontSize: 12 } },
    grid: { left: 85, right: 45, top: 62, bottom: 70 },
    xAxis: {
      type: 'category',
      data: bins,
      axisLabel: {
        rotate: 0,
        fontSize: 9,
        interval: Math.max(1, Math.floor(binCount / 6)),
        margin: 10,
        formatter: (val) => val.length > 8 ? val.substring(0, 7) + '..' : val
      },
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
        data: histogramData,
        barWidth: '90%',
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
      }
    ]
  }

  chartInstance.setOption(option, true)
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
    grid: { left: 85, right: 45, top: 45, bottom: 50 },
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

  const pointColors = getPointColors(values, oocFlags, zones)
  const scatterData = values.map((v, i) => [i, v])

  const allMarkLines = [...buildMarkLines(limits), ...buildSpecLines(limits)]

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

  const option = {
    title: { text: '散点图 - 趋势分析', left: 'center', textStyle: { fontSize: 14, fontWeight: 600 }, top: 8 },
    tooltip: { trigger: 'item', confine: true, formatter: function(p) { return `${formatTimeLabel(timeSeries[p.data[0]] || '')}<br/>值: ${p.data[1]}` } },
    legend: { data: ['测量值', ...trendLine ? ['趋势线'] : []], top: 30, itemGap: 16, textStyle: { fontSize: 12 } },
    grid: { left: 75, right: 45, top: 60, bottom: 55 },
    xAxis: { type: 'value', name: '序号', minInterval: 1 },
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
      }] : [])
    ]
  }

  chartInstance.setOption(option, true)
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
    default:
      renderIMR(data)
  }
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
  min-height: 500px;
}
.spc-capability-panel {
  margin-top: 12px;
  padding: 12px 16px;
  background: linear-gradient(135deg, #f0f5ff 0%, #e6f7ff 100%);
  border-radius: 8px;
  border: 1px solid #bae7ff;
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
  color: #666;
  margin-right: 4px;
}
.cap-highlight {
  background: #fff;
  padding: 2px 8px;
  border-radius: 4px;
  border: 1px solid #91d5ff;
}
.cap-tag {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 10px;
  font-size: 11px;
  font-weight: 600;
}
.tag-normal {
  background: #f6ffed;
  color: #389e0d;
  border: 1px solid #b7eb8f;
}
.tag-non-normal {
  background: #fff2f0;
  color: #cf1322;
  border: 1px solid #ffa39e;
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
  background: #fafafa;
  border-radius: 6px;
  border: 1px solid #f0f0f0;
}
.chart-type-selector {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
.type-btn {
  padding: 6px 16px;
  border: 1px solid #d9d9d9;
  border-radius: 4px;
  background: #fff;
  cursor: pointer;
  font-size: 13px;
  transition: all 0.2s;
  color: #666;
}
.type-btn:hover {
  border-color: #1890ff;
  color: #1890ff;
}
.type-btn.active {
  background: #1890ff;
  border-color: #1890ff;
  color: #fff;
}

.spc-boxplot-stats {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px 0;
  margin-top: 10px;
  padding: 10px 16px;
  background: #fafbfc;
  border-radius: 8px;
  border: 1px solid #e8e8e8;
  font-size: 12px;
}

.stat-item {
  color: #555;
  line-height: 1.8;
}

.stat-item strong {
  font-weight: 600;
  color: #333;
}

.stat-divider {
  width: 1px;
  height: 16px;
  background: #d9d9d9;
  flex-shrink: 0;
}

.stat-danger strong { color: #f5222d; }
.stat-safe strong { color: #389e0d; }
</style>