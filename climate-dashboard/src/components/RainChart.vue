<template>
  <div ref="chartRef" class="rain-chart"></div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import * as echarts from 'echarts'
import { useTheme } from '../composables/useTheme'

const { isDark } = useTheme()
const chartRef = ref(null)
let chart = null

const cityNames = ['广州', '深圳', '东莞', '香港', '珠海', '肇庆', '中山', '佛山', '澳门', '江门']

function generateSeries() {
  return cityNames.map(city => ({
    name: city,
    type: 'line',
    smooth: true,
    stack: 'Total',
    areaStyle: { opacity: 0.3 },
    data: Array.from({ length: 13 }, () => Math.round(8000 + Math.random() * 17000))
  }))
}

function getChartOption() {
  const textColor = isDark.value ? '#9aa0a6' : '#666'
  
  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      backgroundColor: isDark.value ? '#2d3748' : '#fff',
      borderColor: isDark.value ? '#3d4a5c' : '#e8e8e8',
      textStyle: { color: textColor }
    },
    legend: {
      top: 0,
      textStyle: { fontSize: 11, color: textColor },
      itemWidth: 15,
      itemHeight: 10
    },
    grid: {
      left: '50',
      right: '20',
      top: '50',
      bottom: '30'
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: Array.from({ length: 13 }, (_, i) => (2012 + i).toString()),
      axisLine: { lineStyle: { color: isDark.value ? '#3d4a5c' : '#ddd' } },
      axisLabel: { color: textColor }
    },
    yAxis: {
      type: 'value',
      axisLine: { lineStyle: { color: isDark.value ? '#3d4a5c' : '#ddd' } },
      axisLabel: {
        color: textColor,
        formatter: (value) => value.toLocaleString()
      },
      splitLine: {
        lineStyle: { color: isDark.value ? '#2d3748' : '#eee' }
      }
    },
    series: generateSeries()
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
      series: generateSeries(),
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
.rain-chart {
  width: 100%;
  height: 100%;
}
</style>
