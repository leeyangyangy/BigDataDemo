<template>
  <div class="card">
    <h2>📈 良率变化趋势</h2>
    <div class="selector">
      <label>选择产品-片号-分等:</label>
      <select v-model="selectedKey" @change="updateChart">
        <option value="">请选择</option>
        <option v-for="key in productKeys" :key="key" :value="key">
          {{ key }}
        </option>
      </select>
    </div>
    <div ref="chartRef" class="chart"></div>
    <div v-if="!selectedKey" class="empty-message">
      请选择产品查看趋势
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  yieldRates: {
    type: Array,
    default: () => []
  },
  historicalData: {
    type: Object,
    default: () => ({})
  }
})

const chartRef = ref(null)
const STORAGE_KEY = 'yield-trend-selected-key'
const savedKey = localStorage.getItem(STORAGE_KEY)
const selectedKey = ref(savedKey || '')
let chartInstance = null

const productKeys = computed(() => {
  const keys = new Set()
  props.yieldRates.forEach(item => {
    keys.add(`${item.productName}-${item.productCode}-${item.binRank}`)
  })
  return Array.from(keys)
})

watch(selectedKey, (newValue) => {
  if (newValue) {
    localStorage.setItem(STORAGE_KEY, newValue)
  }
})

const initChart = () => {
  if (!chartRef.value) return

  const existingChart = echarts.getInstanceByDom(chartRef.value)
  if (existingChart) {
    existingChart.dispose()
  }

  chartInstance = echarts.init(chartRef.value)

  window.addEventListener('resize', handleResize)
}

const updateChart = () => {
  if (!chartInstance) {
    return
  }

  if (!selectedKey.value || !props.yieldRates || props.yieldRates.length === 0) {
    chartInstance.clear()
    return
  }

  const data = props.historicalData[selectedKey.value] || []
  const timestamps = data.map(d => d.timestamp)
  const rates = data.map(d => d.yieldRate)

  const isMobile = window.innerWidth <= 768
  const option = {
    title: {
      text: selectedKey.value,
      left: 'center',
      textStyle: { color: '#00d4ff', fontSize: isMobile ? 13 : 18 }
    },
    tooltip: {
      trigger: 'axis',
      formatter: '{b}<br/>良率: {c}%'
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: isMobile ? '15%' : '3%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: timestamps,
      axisLine: { lineStyle: { color: '#666' } },
      axisLabel: { color: '#aaa', fontSize: isMobile ? 10 : 12, rotate: isMobile ? 30 : 0, hideOverlap: true }
    },
    yAxis: {
      type: 'value',
      min: 0,
      max: 100,
      axisLine: { lineStyle: { color: '#666' } },
      axisLabel: { color: '#aaa', fontSize: isMobile ? 10 : 12 },
      splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }
    },
    series: [{
      name: '良率',
      type: 'line',
      smooth: true,
      data: rates,
      lineStyle: { color: '#00d4ff', width: 3 },
      itemStyle: { color: '#00d4ff' },
      areaStyle: {
        color: {
          type: 'linear',
          x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(0, 212, 255, 0.5)' },
            { offset: 1, color: 'rgba(0, 212, 255, 0)' }
          ]
        }
      }
    }]
  }

  chartInstance.setOption(option)
}

const handleResize = () => {
  if (chartInstance) {
    chartInstance.resize()
  }
}

onMounted(() => {
  nextTick(() => {
    initChart()
    if (selectedKey.value) {
      updateChart()
    }
  })
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  if (chartInstance) {
    chartInstance.dispose()
  }
})

watch(selectedKey, () => {
  if (selectedKey.value && chartInstance) {
    updateChart()
  }
})

watch(() => props.historicalData, () => {
  if (chartInstance) {
    updateChart()
  }
}, { deep: true })

watch(() => props.yieldRates, () => {
  const keys = new Set()
  props.yieldRates.forEach(item => {
    keys.add(`${item.productName}-${item.productCode}-${item.binRank}`)
  })
  if (selectedKey.value && !keys.has(selectedKey.value)) {
    selectedKey.value = ''
  }
  if (chartInstance) {
    updateChart()
  }
}, { deep: true })
</script>

<style scoped>
.card {
  background: var(--bg-secondary);
  border-radius: 15px;
  padding: 20px;
  border: 1px solid var(--border-color);
}
.card h2 {
  font-size: 24px;
  margin-bottom: 20px;
  color: var(--accent-primary);
  border-bottom: 2px solid rgba(var(--accent-rgb), 0.3);
  padding-bottom: 10px;
}
.selector {
  margin-bottom: 20px;
}
.selector label {
  margin-right: 10px;
  color: var(--text-secondary);
}
.selector select {
  padding: 8px 15px;
  border-radius: 5px;
  border: 1px solid var(--border-input);
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 14px;
  cursor: pointer;
}
.selector select:focus {
  outline: none;
  border-color: var(--accent-primary);
}
.selector select option {
  background: var(--bg-secondary);
  color: var(--text-primary);
}
.chart {
  width: 100%;
  height: 400px;
}
.empty-message {
  text-align: center;
  padding: 40px;
  color: var(--text-tertiary);
  font-size: 18px;
}

/* ==================== 移动端优化 ==================== */
@media (max-width: 768px) {
  .card {
    padding: 12px;
    border-radius: 10px;
  }
  .card h2 {
    font-size: 17px;
    margin-bottom: 12px;
    padding-bottom: 8px;
  }
  .selector {
    margin-bottom: 12px;
  }
  .selector label {
    display: block;
    margin-right: 0;
    margin-bottom: 4px;
    font-size: 13px;
  }
  .selector select {
    width: 100%;
    padding: 9px 12px;
    font-size: 13px;
  }
  .chart {
    height: 280px;
  }
  .empty-message {
    padding: 30px 16px;
    font-size: 15px;
  }
}
</style>
