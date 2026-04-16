<template>
  <div ref="chartRef" class="heatmap"></div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import * as echarts from 'echarts'
import { useTheme } from '../composables/useTheme'

const { isDark } = useTheme()
const chartRef = ref(null)
let chart = null

const cities = ['江门', '惠州', '肇庆', '佛山', '中山', '重庆', '珠海', '香港', '东莞', '深圳', '广州']
const years = Array.from({ length: 25 }, (_, i) => 2000 + i)

function generateData() {
  const data = []
  for (let i = 0; i < cities.length; i++) {
    for (let j = 0; j < years.length; j++) {
      data.push([j, i, Math.round((18 + Math.random() * 12) * 100) / 100])
    }
  }
  return data
}

function getChartOption() {
  const textColor = isDark.value ? '#9aa0a6' : '#333'
  const bgColor = isDark.value ? '#1a2332' : '#fff'

  return {
    backgroundColor: 'transparent',
    tooltip: {
      position: 'top',
      backgroundColor: isDark.value ? '#2d3748' : '#fff',
      borderColor: isDark.value ? '#3d4a5c' : '#e8e8e8',
      textStyle: { color: textColor },
      formatter: (params) => `${params.data[2]}°C`
    },
    grid: {
      left: '80',
      right: '50',
      top: '10',
      bottom: '60'
    },
    xAxis: {
      type: 'category',
      data: years,
      splitArea: { show: true },
      axisLabel: { rotate: 45, fontSize: 10, color: textColor },
      axisLine: { lineStyle: { color: isDark.value ? '#3d4a5c' : '#ddd' } }
    },
    yAxis: {
      type: 'category',
      data: cities,
      splitArea: { show: true },
      axisLabel: { color: textColor },
      axisLine: { lineStyle: { color: isDark.value ? '#3d4a5c' : '#ddd' } }
    },
    visualMap: {
      min: 18,
      max: 30,
      calculable: true,
      orient: 'horizontal',
      left: 'center',
      bottom: 0,
      textStyle: { color: textColor },
      inRange: {
        color: ['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8',
                 '#ffffbf', '#fee090', '#fdae61', '#d73027', '#a50026']
      }
    },
    series: [{
      type: 'heatmap',
      data: generateData(),
      label: { show: false },
      emphasis: {
        itemStyle: {
          shadowBlur: 10,
          shadowColor: 'rgba(0, 0, 0, 0.5)'
        }
      }
    }]
  }
}

function initChart() {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  chart.setOption(getChartOption())
}

function refresh() {
  if (chart) {
    chart.setOption({
      series: [{ data: generateData() }],
      ...getChartOption()
    })
  }
}

function handleResize() {
  chart?.resize()
}

watch(isDark, () => {
  if (chart) {
    chart.setOption(getChartOption())
  }
})

onMounted(() => {
  initChart()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chart?.dispose()
})

defineExpose({ refresh })
</script>

<style scoped>
.heatmap {
  width: 100%;
  height: 100%;
}
</style>
