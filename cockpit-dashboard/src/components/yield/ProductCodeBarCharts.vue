<template>
  <div class="card">
    <h2>📊 产品-片号良率对比</h2>
    <div class="selector">
      <label>选择产品:</label>
      <select v-model="selectedProduct" @change="updateProduct">
        <option value="">请选择</option>
        <option v-for="product in productList" :key="product" :value="product">
          {{ product }}
        </option>
      </select>
    </div>

    <div v-if="selectedProduct" class="pagination">
      <button @click="prevPage" :disabled="currentPage === 1" class="page-btn">
        ◀ 上一页
      </button>
      <span class="page-info">第 {{ currentPage }} / {{ totalPages }} 页</span>
      <button @click="nextPage" :disabled="currentPage === totalPages" class="page-btn">
        下一页 ▶
      </button>
    </div>

    <div v-if="selectedProduct" class="charts-container">
      <div v-for="(chartData, index) in currentPageData" :key="index" class="chart-item">
        <div :ref="el => setChartRef(el, index)" class="chart"></div>
      </div>
    </div>

    <div v-if="!selectedProduct" class="empty-message">
      请选择产品查看片号对比
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
  }
})

const ITEMS_PER_PAGE = 2
const STORAGE_KEY = 'product-code-bars-selected-product'
const chartRefs = ref([])
const chartInstances = ref([])
const savedProduct = localStorage.getItem(STORAGE_KEY)
const selectedProduct = ref(savedProduct || '')
const currentPage = ref(1)

const productList = computed(() => {
  const products = new Set()
  props.yieldRates.forEach(item => {
    products.add(item.productName)
  })
  return Array.from(products).sort()
})

watch(selectedProduct, (newValue) => {
  if (newValue) {
    localStorage.setItem(STORAGE_KEY, newValue)
  }
})

const productCodeList = computed(() => {
  if (!selectedProduct.value) return []
  const productData = props.yieldRates.filter(item => item.productName === selectedProduct.value)
  const productCodes = [...new Set(productData.map(item => item.productCode))].sort()
  return productCodes
})

const totalPages = computed(() => {
  return Math.ceil(productCodeList.value.length / ITEMS_PER_PAGE)
})

const currentPageData = computed(() => {
  const start = (currentPage.value - 1) * ITEMS_PER_PAGE
  const end = start + ITEMS_PER_PAGE
  return productCodeList.value.slice(start, end)
})

const setChartRef = (el, index) => {
  if (el) {
    chartRefs.value[index] = el
  }
}

const initCharts = () => {
  chartInstances.value.forEach(instance => {
    if (instance) {
      instance.dispose()
    }
  })
  chartInstances.value = []
  chartRefs.value = []

  nextTick(() => {
    setTimeout(() => {
      currentPageData.value.forEach((productCode, index) => {
        if (chartRefs.value[index]) {
          const existingChart = echarts.getInstanceByDom(chartRefs.value[index])
          if (existingChart) {
            existingChart.dispose()
          }
          const chart = echarts.init(chartRefs.value[index])
          chartInstances.value[index] = chart
          updateSingleChart(chart, productCode)
        }
      })
    }, 100)
  })
}

const updateSingleChart = (chart, productCode) => {
  const productData = props.yieldRates.filter(
    item => item.productName === selectedProduct.value && item.productCode === productCode
  )

  const binRanks = productData.map(item => item.binRank)
  const yieldRates = productData.map(item => item.yieldRate)
  const isMobile = window.innerWidth <= 768

  const colors = yieldRates.map(rate => {
    if (rate >= 90) return '#00ff88'
    if (rate >= 70) return '#ffd700'
    return '#ff4444'
  })

  const option = {
    title: {
      text: productCode,
      left: 'center',
      textStyle: {
        color: '#00d4ff',
        fontSize: isMobile ? 13 : 16
      }
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'shadow'
      },
      formatter: '{b}<br/>良率: {c}%'
    },
    grid: {
      left: '10%',
      right: '10%',
      bottom: '15%',
      top: '20%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: binRanks,
      axisLine: { lineStyle: { color: '#666' } },
      axisLabel: { color: '#aaa', fontSize: isMobile ? 10 : 12, hideOverlap: true }
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
      type: 'bar',
      data: yieldRates.map((value, index) => ({
        value,
        itemStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: colors[index] },
              { offset: 1, color: colors[index] + '80' }
            ]
          },
          borderRadius: [4, 4, 0, 0]
        }
      })),
      barWidth: '50%',
      label: {
        show: true,
        position: 'top',
        color: '#fff',
        fontSize: isMobile ? 10 : 12,
        formatter: '{c}%'
      }
    }]
  }

  chart.setOption(option, true)
}

const updateAllCharts = () => {
  if (!selectedProduct.value || !props.yieldRates || props.yieldRates.length === 0) {
    chartInstances.value.forEach(chart => {
      if (chart) {
        chart.clear()
      }
    })
    return
  }

  const productData = props.yieldRates.filter(item => item.productName === selectedProduct.value)
  if (productData.length === 0) {
    chartInstances.value.forEach(chart => {
      if (chart) {
        chart.clear()
      }
    })
    return
  }

  chartInstances.value.forEach((chart, index) => {
    if (chart && currentPageData.value[index]) {
      updateSingleChart(chart, currentPageData.value[index])
    }
  })
}

const updateProduct = () => {
  currentPage.value = 1
  chartRefs.value = []
  initCharts()
}

const prevPage = () => {
  if (currentPage.value > 1) {
    currentPage.value--
    chartRefs.value = []
    initCharts()
  }
}

const nextPage = () => {
  if (currentPage.value < totalPages.value) {
    currentPage.value++
    chartRefs.value = []
    initCharts()
  }
}

const handleResize = () => {
  chartInstances.value.forEach(chart => {
    if (chart) {
      chart.resize()
    }
  })
}

onMounted(() => {
  nextTick(() => {
    window.addEventListener('resize', handleResize)
    if (selectedProduct.value) {
      initCharts()
    }
  })
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chartInstances.value.forEach(chart => {
    if (chart) {
      chart.dispose()
    }
  })
})

watch(selectedProduct, () => {
  currentPage.value = 1
  chartRefs.value = []
  if (selectedProduct.value) {
    initCharts()
  }
})

watch(() => props.yieldRates, () => {
  const products = new Set()
  props.yieldRates.forEach(item => {
    products.add(item.productName)
  })
  if (selectedProduct.value && !products.has(selectedProduct.value)) {
    selectedProduct.value = ''
    currentPage.value = 1
  } else if (selectedProduct.value) {
    updateAllCharts()
  }
}, { deep: true })

watch(currentPage, () => {
  chartRefs.value = []
  initCharts()
})
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
.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 20px;
  margin-bottom: 20px;
}
.page-btn {
  padding: 8px 20px;
  border-radius: 5px;
  border: 1px solid var(--border-input);
  background: var(--bg-input);
  color: var(--accent-primary);
  font-size: 14px;
  cursor: pointer;
  transition: all 0.3s;
}
.page-btn:hover:not(:disabled) {
  background: rgba(var(--accent-rgb), 0.2);
  border-color: var(--accent-primary);
}
.page-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.page-info {
  color: var(--text-secondary);
  font-size: 14px;
}
.charts-container {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
  gap: 20px;
}
.chart-item {
  background: var(--bg-tertiary);
  border-radius: 10px;
  padding: 10px;
  border: 1px solid rgba(var(--accent-rgb), 0.1);
}
.chart {
  width: 100%;
  height: 320px;
}
.empty-message {
  text-align: center;
  padding: 60px 40px;
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
  .pagination {
    gap: 12px;
    margin-bottom: 12px;
  }
  .page-btn {
    padding: 7px 14px;
    font-size: 13px;
  }
  .page-info {
    font-size: 12px;
  }
  .charts-container {
    grid-template-columns: 1fr;
    gap: 12px;
  }
  .chart-item {
    padding: 8px;
  }
  .chart {
    height: 260px;
  }
  .empty-message {
    padding: 40px 16px;
    font-size: 15px;
  }
}
</style>
