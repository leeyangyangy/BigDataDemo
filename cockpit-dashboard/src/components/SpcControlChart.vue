<template>
  <div ref="chartRef" class="spc-control-chart"></div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  chartData: { type: Object, default: null },
  height: { type: String, default: '500px' }
})

const chartRef = ref(null)
let chartInstance = null

onMounted(() => {
  initChart()
})

onUnmounted(() => {
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
})

watch(() => props.chartData, (newVal) => {
  if (newVal) {
    nextTick(() => updateChart(newVal))
  }
}, { deep: true })

function initChart() {
  if (!chartRef.value) return
  chartInstance = echarts.init(chartRef.value)
  chartRef.value.style.height = props.height

  const resizeHandler = () => chartInstance?.resize()
  window.addEventListener('resize', resizeHandler)

  if (props.chartData) {
    updateChart(props.chartData)
  }
}

function updateChart(data) {
  if (!chartInstance || !data) return

  const { timeSeries, values, zones, oocFlags, limits, capability } = data

  const markLines = []
  const markAreas = []

  if (limits.ucl != null) {
    markLines.push({
      yAxis: limits.ucl, name: 'UCL',
      lineStyle: { color: '#f5222d', type: 'dashed', width: 2 },
      label: { position: 'end', formatter: 'UCL: {c}', fontSize: 11 }
    })
  }
  if (limits.lcl != null) {
    markLines.push({
      yAxis: limits.lcl, name: 'LCL',
      lineStyle: { color: '#f5222d', type: 'dashed', width: 2 },
      label: { position: 'end', formatter: 'LCL: {c}', fontSize: 11 }
    })
  }
  if (limits.cl != null) {
    markLines.push({
      yAxis: limits.cl, name: 'CL',
      lineStyle: { color: '#52c41a', type: 'solid', width: 2 },
      label: { position: 'end', formatter: 'CL: {c}', fontSize: 11 }
    })
  }
  if (limits.usl != null) {
    markLines.push({
      yAxis: limits.usl, name: 'USL',
      lineStyle: { color: '#ff4d4f', type: 'dotted', width: 1.5 },
      label: { position: 'insideEndTop', formatter: 'USL: {c}', fontSize: 10, color: '#ff4d4f' }
    })
  }
  if (limits.lsl != null) {
    markLines.push({
      yAxis: limits.lsl, name: 'LSL',
      lineStyle: { color: '#ff4d4f', type: 'dotted', width: 1.5 },
      label: { position: 'insideEndBottom', formatter: 'LSL: {c}', fontSize: 10, color: '#ff4d4f' }
    })
  }

  if (limits.ucl != null && limits.cl != null) {
    markAreas.push([
      { yAxis: limits.cl, itemStyle: { color: 'rgba(82,196,26,0.06)' } },
      { yAxis: limits.ucl }
    ])
    markAreas.push([
      { yAxis: limits.lcl, itemStyle: { color: 'rgba(82,196,26,0.06)' } },
      { yAxis: limits.cl }
    ])
  }

  const pointColors = values.map((v, i) => {
    if (oocFlags[i] === 1) return '#f5222d'
    if (zones[i] === 2) return '#faad14'
    if (zones[i] === 3) return '#90CAF9'
    return '#1890ff'
  })

  const option = {
    title: {
      text: `SPC控制图 (版本V${data.versionNo || 1})`,
      left: 'center',
      textStyle: { fontSize: 16, fontWeight: 600 }
    },
    tooltip: {
      trigger: 'axis',
      formatter(params) {
        const p = params[0]
        const idx = p.dataIndex
        let html = `<b>${p.axisValue}</b><br/>`
        html += `测量值: ${p.value}<br/>`
        if (oocFlags[idx] === 1) html += `<span style="color:#f5222d">⚠ 超出控制限</span><br/>`
        if (zones[idx]) html += `区域: ${['', 'A区', 'B区', 'C区'][zones[idx]]}<br/>`
        return html
      }
    },
    grid: {
      left: 80, right: 40, top: 80, bottom: 60
    },
    xAxis: {
      type: 'category',
      data: timeSeries,
      axisLabel: {
        rotate: 45,
        fontSize: 10,
        formatter(val) {
          if (val && val.length > 16) return val.substring(5, 16)
          return val
        }
      }
    },
    yAxis: {
      type: 'value',
      scale: true
    },
    series: [{
      type: 'line',
      data: values,
      symbol: 'circle',
      symbolSize: 6,
      lineStyle: { color: '#1890ff', width: 1.5 },
      itemStyle: {
        color(params) {
          return pointColors[params.dataIndex] || '#1890ff'
        }
      },
      markLine: {
        silent: true,
        symbol: 'none',
        data: markLines
      },
      markArea: {
        silent: true,
        data: markAreas
      }
    }]
  }

  if (capability) {
    option.title.subtext = `Cp=${capability.cp ?? '-'}  Cpk=${capability.cpk ?? '-'}  均值=${capability.mean ?? '-'}  σ=${capability.stdDev ?? '-'}  N=${capability.sampleCount ?? 0}`
    option.title.subtextStyle = { fontSize: 12, color: '#666' }
  }

  chartInstance.setOption(option, true)
}

function refresh() {
  if (chartInstance) chartInstance.resize()
}

defineExpose({ refresh })
</script>

<style scoped>
.spc-control-chart {
  width: 100%;
  min-height: 500px;
}
</style>
