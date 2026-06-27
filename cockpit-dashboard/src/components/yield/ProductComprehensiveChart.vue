<template>
  <div class="card">
    <h2>📊 产品综合良率趋势</h2>
    <div class="selectors">
      <div class="selector">
        <label>选择产品:</label>
        <select v-model="selectedProduct" @change="resetAndUpdate">
          <option value="">请选择</option>
          <option v-for="product in productList" :key="product" :value="product">
            {{ product }}
          </option>
        </select>
      </div>
      <div class="toggle-container">
        <label class="toggle-label">
          <input
            type="checkbox"
            v-model="showValues"
            @change="updateChart"
            class="toggle-input"
          />
          <span class="toggle-slider"></span>
          <span class="toggle-text">显示数值</span>
        </label>
      </div>
      <div class="toggle-container">
        <label class="toggle-label">
          <input
            type="checkbox"
            v-model="autoYAxisRange"
            @change="updateChart"
            class="toggle-input"
          />
          <span class="toggle-slider"></span>
          <span class="toggle-text">自动纵轴</span>
        </label>
      </div>
      <div v-if="!autoYAxisRange" class="y-axis-range">
        <label>纵轴范围:</label>
        <select v-model="yAxisMin" @change="updateChart" class="range-select">
          <option :value="0">0</option>
          <option :value="30">30</option>
          <option :value="50">50</option>
          <option :value="60">60</option>
          <option :value="70">70</option>
          <option :value="80">80</option>
          <option :value="90">90</option>
        </select>
        <span class="range-separator">-</span>
        <select v-model="yAxisMax" @change="updateChart" class="range-select">
          <option :value="100">100</option>
          <option :value="95">95</option>
          <option :value="90">90</option>
          <option :value="85">85</option>
          <option :value="80">80</option>
          <option :value="70">70</option>
        </select>
      </div>
          <span class="toggle-text">日期筛选 -- 开发中……</span>

    </div>
    <div v-if="selectedProduct" class="pagination">
      <button
        @click="prevPage"
        :disabled="currentPage === 1"
        class="page-btn"
      >
        ◀ 上一页
      </button>
      <span class="page-info">
        第 {{ currentPage }} / {{ totalPages }} 页 (共 {{ productCodes.length }} 个片号)
      </span>
      <button
        @click="nextPage"
        :disabled="currentPage === totalPages"
        class="page-btn"
      >
        下一页 ▶
      </button>
      <div class="items-per-page">
        <label>每页显示:</label>
        <select v-model="itemsPerPage" @change="handleItemsPerPageChange" class="range-select">
          <option :value="3">3</option>
          <option :value="5">5</option>
          <option :value="8">8</option>
          <option :value="10">10</option>
          <option :value="15">15</option>
        </select>
      </div>
    </div>
    <div ref="chartRef" class="chart"></div>
    <div v-if="!selectedProduct" class="empty-message">
      请选择产品查看综合趋势
    </div>
    <div v-if="selectedProduct" class="legend-tip">
      💡 提示：点击图例可以显示/隐藏对应片号的曲线
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
const STORAGE_KEY = 'product-comprehensive-selected-product'
const STORAGE_KEY_SHOW_VALUES = 'product-comprehensive-show-values'
const STORAGE_KEY_ITEMS_PER_PAGE = 'product-comprehensive-items-per-page'
const STORAGE_KEY_AUTO_Y_AXIS = 'product-comprehensive-auto-y-axis'
const savedProduct = localStorage.getItem(STORAGE_KEY)
const savedShowValues = localStorage.getItem(STORAGE_KEY_SHOW_VALUES)
const savedItemsPerPage = localStorage.getItem(STORAGE_KEY_ITEMS_PER_PAGE)
const savedAutoYAxis = localStorage.getItem(STORAGE_KEY_AUTO_Y_AXIS)
const selectedProduct = ref(savedProduct || '')
const showValues = ref(savedShowValues === 'true')
const itemsPerPage = ref(savedItemsPerPage ? parseInt(savedItemsPerPage) : 5)
const autoYAxisRange = ref(savedAutoYAxis === 'true')
const currentPage = ref(1)
const yAxisMin = ref(0)
const yAxisMax = ref(100)
let chartInstance = null
const productCodes = ref([])
let hiddenProductCodes = new Set()

const RANK_PRIORITY = ['未分等', 'JXBS', 'BS', 'AA', 'A', 'BB', 'B', 'CC', 'C', 'DD', 'D']

const productList = computed(() => {
  const products = new Set()
  props.yieldRates.forEach(item => {
    products.add(item.productName)
  })
  return Array.from(products).sort()
})

const totalPages = computed(() => {
  return Math.ceil(productCodes.value.length / itemsPerPage.value)
})

const currentProductCodes = computed(() => {
  const start = (currentPage.value - 1) * itemsPerPage.value
  const end = start + itemsPerPage.value
  return productCodes.value.slice(start, end)
})

watch(selectedProduct, (newValue) => {
  if (newValue) {
    localStorage.setItem(STORAGE_KEY, newValue)
  }
})

watch(showValues, (newValue) => {
  localStorage.setItem(STORAGE_KEY_SHOW_VALUES, String(newValue))
})

watch(itemsPerPage, (newValue) => {
  localStorage.setItem(STORAGE_KEY_ITEMS_PER_PAGE, String(newValue))
})

watch(autoYAxisRange, (newValue) => {
  localStorage.setItem(STORAGE_KEY_AUTO_Y_AXIS, String(newValue))
})

const resetAndUpdate = () => {
  currentPage.value = 1
  hiddenProductCodes.clear()
  updateChart()
}

const handleItemsPerPageChange = () => {
  currentPage.value = 1
  hiddenProductCodes.clear()
  updateChart()
}

const prevPage = () => {
  if (currentPage.value > 1) {
    currentPage.value--
    updateChart()
  }
}

const nextPage = () => {
  if (currentPage.value < totalPages.value) {
    currentPage.value++
    updateChart()
  }
}

const calculateYAxisRange = (productData, currentProductCodesList, binRanksList) => {
  let minVal = 100
  let maxVal = 0

  currentProductCodesList.forEach(productCode => {
    binRanksList.forEach(binRank => {
      const item = productData.find(
        i => i.productCode === productCode && i.binRank === binRank
      )
      if (item && item.yieldRate !== null && item.yieldRate !== undefined) {
        minVal = Math.min(minVal, item.yieldRate)
        maxVal = Math.max(maxVal, item.yieldRate)
      }
    })
  })

  if (minVal === 100 && maxVal === 0) {
    return { min: 0, max: 100 }
  }

  const padding = 5
  return {
    min: Math.max(0, Math.floor((minVal - padding) / 5) * 5),
    max: Math.min(100, Math.ceil((maxVal + padding) / 5) * 5)
  }
}

const initChart = () => {
  if (!chartRef.value) return

  const existingChart = echarts.getInstanceByDom(chartRef.value)
  if (existingChart) {
    existingChart.dispose()
  }

  chartInstance = echarts.init(chartRef.value)

  chartInstance.on('legendselectchanged', (params) => {
    const selected = params.selected
    Object.keys(selected).forEach(code => {
      if (selected[code]) {
        hiddenProductCodes.delete(code)
      } else {
        hiddenProductCodes.add(code)
      }
    })
  })

  window.addEventListener('resize', handleResize)

  if (selectedProduct.value && chartInstance) {
    updateChart()
  }
}

const updateChart = () => {
  if (!chartInstance) {
    return
  }

  if (!selectedProduct.value || !props.yieldRates || props.yieldRates.length === 0) {
    chartInstance.clear()
    return
  }

  const productData = props.yieldRates.filter(item => item.productName === selectedProduct.value)

  if (productData.length === 0) {
    chartInstance.clear()
    return
  }

  productCodes.value = [...new Set(productData.map(item => item.productCode))].sort()

  const allBinRanks = [...new Set(productData.map(item => item.binRank))]
  const binRanks = allBinRanks.sort((a, b) => {
    const indexA = RANK_PRIORITY.indexOf(a)
    const indexB = RANK_PRIORITY.indexOf(b)

    if (indexA !== -1 && indexB !== -1) {
      return indexA - indexB
    } else if (indexA !== -1) {
      return -1
    } else if (indexB !== -1) {
      return 1
    } else {
      return a.localeCompare(b)
    }
  })

  let currentYAxisMin, currentYAxisMax
  if (autoYAxisRange.value) {
    const range = calculateYAxisRange(productData, currentProductCodes.value, binRanks)
    currentYAxisMin = range.min
    currentYAxisMax = range.max
  } else {
    currentYAxisMin = yAxisMin.value
    currentYAxisMax = yAxisMax.value
  }

  const colors = [
    '#00d4ff', '#00ff88', '#ffd700', '#ff4444', '#ff00ff',
    '#00ffcc', '#ff8800', '#88ff00', '#ff0088', '#8800ff'
  ]

  const symbols = ['circle', 'diamond', 'triangle', 'rect', 'pin']
  const lineTypes = ['solid', 'dashed', 'dotted', 'solid', 'dashed']

  const series = currentProductCodes.value.map((productCode, index) => {
    const binRankData = binRanks.map(binRank => {
      const key = `${selectedProduct.value}-${productCode}-${binRank}`
      const data = props.historicalData[key] || []
      if (data.length > 0) {
        return data[data.length - 1].yieldRate
      }
      const item = productData.find(
        i => i.productCode === productCode && i.binRank === binRank
      )
      return item ? item.yieldRate : '-'
    })

    const colorIndex = productCodes.value.indexOf(productCode)
    const seriesIndex = currentProductCodes.value.indexOf(productCode)

    const seriesConfig = {
      name: productCode,
      type: 'line',
      smooth: true,
      data: binRankData,
      lineStyle: {
        color: colors[colorIndex % colors.length],
        width: 2.5,
        type: lineTypes[seriesIndex % lineTypes.length]
      },
      itemStyle: {
        color: colors[colorIndex % colors.length],
        borderColor: '#fff',
        borderWidth: 1
      },
      symbol: symbols[seriesIndex % symbols.length],
      symbolSize: 8
    }

    if (showValues.value) {
      const positions = ['top', 'bottom', 'top', 'bottom', 'top']
      const distances = [12, 15, 12, 15, 12]
      seriesConfig.label = {
        show: true,
        position: positions[seriesIndex % positions.length],
        color: '#fff',
        fontSize: 10,
        formatter: (params) => {
          if (params.value === '-') {
            return ''
          }
          return params.value + '%'
        },
        distance: distances[seriesIndex % distances.length],
        padding: [3, 6, 3, 6],
        backgroundColor: 'rgba(0, 0, 0, 0.6)',
        borderRadius: 3
      }
    }

    return seriesConfig
  })

  const legendSelected = {}
  productCodes.value.forEach(code => {
    legendSelected[code] = !hiddenProductCodes.has(code)
  })

  const isMobile = window.innerWidth <= 768
  const option = {
    title: {
      text: selectedProduct.value,
      left: 'center',
      textStyle: { color: '#00d4ff', fontSize: isMobile ? 14 : 18 }
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross'
      },
      enterable: true,
      hideDelay: 999999,
      triggerOn: 'mousemove|click',
      formatter: (params) => {
        let result = params[0].axisValue + '<br/>'
        params.forEach(param => {
          if (param.value !== '-') {
            result += `${param.marker} ${param.seriesName}: ${param.value}%<br/>`
          }
        })
        return result
      }
    },
    legend: {
      data: currentProductCodes.value,
      selected: legendSelected,
      top: isMobile ? 24 : 30,
      textStyle: { color: '#aaa', fontSize: isMobile ? 10 : 12 },
      type: 'scroll',
      itemWidth: isMobile ? 12 : 25,
      itemHeight: isMobile ? 8 : 14
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: isMobile ? '12%' : '8%',
      top: isMobile ? 60 : 70,
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: binRanks,
      name: '分等',
      nameTextStyle: { color: '#aaa', fontSize: isMobile ? 10 : 12 },
      axisLine: { lineStyle: { color: '#666' } },
      axisLabel: { color: '#aaa', fontSize: isMobile ? 10 : 14 }
    },
    yAxis: {
      type: 'value',
      min: currentYAxisMin,
      max: currentYAxisMax,
      interval: Math.ceil((currentYAxisMax - currentYAxisMin) / 10),
      name: '良率 (%)',
      nameTextStyle: { color: '#aaa', fontSize: isMobile ? 10 : 12 },
      axisLine: { lineStyle: { color: '#666' } },
      axisLabel: { color: '#aaa', fontSize: isMobile ? 10 : 14, margin: isMobile ? 8 : 15 },
      splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }
    },
    series: series
  }

  chartInstance.setOption(option, true)
}

let resizeTimer = null
const handleResize = () => {
  if (chartInstance) {
    chartInstance.resize()
  }
  if (resizeTimer) clearTimeout(resizeTimer)
  resizeTimer = setTimeout(() => {
    if (chartInstance && selectedProduct.value) {
      updateChart()
    }
  }, 300)
}

onMounted(() => {
  nextTick(() => {
    initChart()
  })
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  if (chartInstance) {
    chartInstance.dispose()
  }
})

watch(selectedProduct, () => {
  if (selectedProduct.value && chartInstance) {
    resetAndUpdate()
  }
})

watch(() => [props.yieldRates, props.historicalData], () => {
  const products = new Set()
  props.yieldRates.forEach(item => {
    products.add(item.productName)
  })
  if (selectedProduct.value && !products.has(selectedProduct.value)) {
    selectedProduct.value = ''
    currentPage.value = 1
    hiddenProductCodes.clear()
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
.selectors {
  display: flex;
  gap: 20px;
  align-items: center;
  margin-bottom: 15px;
  flex-wrap: wrap;
}
.selector {
  display: flex;
  align-items: center;
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
.toggle-container {
  display: flex;
  align-items: center;
}
.toggle-label {
  display: flex;
  align-items: center;
  cursor: pointer;
  gap: 8px;
}
.toggle-input {
  display: none;
}
.toggle-slider {
  width: 44px;
  height: 24px;
  background: var(--bg-input);
  border-radius: 12px;
  position: relative;
  transition: all 0.3s;
  border: 1px solid var(--border-input);
}
.toggle-slider::before {
  content: '';
  position: absolute;
  width: 18px;
  height: 18px;
  background: #fff;
  border-radius: 50%;
  top: 2px;
  left: 2px;
  transition: all 0.3s;
}
.toggle-input:checked + .toggle-slider {
  background: rgba(var(--accent-rgb), 0.3);
  border-color: var(--accent-primary);
}
.toggle-input:checked + .toggle-slider::before {
  left: 22px;
  background: var(--accent-primary);
}
.toggle-text {
  color: var(--text-secondary);
  font-size: 14px;
  user-select: none;
}
.y-axis-range {
  display: flex;
  align-items: center;
  gap: 8px;
}
.y-axis-range label {
  color: var(--text-secondary);
  font-size: 14px;
}
.range-select {
  padding: 6px 10px;
  border-radius: 5px;
  border: 1px solid var(--border-input);
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 13px;
  cursor: pointer;
  min-width: 60px;
}
.range-select:focus {
  outline: none;
  border-color: var(--accent-primary);
}
.range-select option {
  background: var(--bg-secondary);
  color: var(--text-primary);
}
.range-separator {
  color: var(--text-secondary);
  font-size: 14px;
}
.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 15px;
  margin-bottom: 15px;
  padding: 10px;
  background: var(--bg-tertiary);
  border-radius: 8px;
  flex-wrap: wrap;
}
.page-btn {
  padding: 8px 16px;
  border: 1px solid var(--border-input);
  background: var(--bg-secondary);
  color: var(--accent-primary);
  border-radius: 5px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.3s;
}
.page-btn:hover:not(:disabled) {
  background: rgba(var(--accent-rgb), 0.2);
  border-color: var(--accent-primary);
}
.page-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
.page-info {
  color: var(--text-secondary);
  font-size: 14px;
}
.items-per-page {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-left: 20px;
  padding-left: 20px;
  border-left: 1px solid var(--border-color);
}
.items-per-page label {
  color: var(--text-secondary);
  font-size: 14px;
}
.chart {
  width: 100%;
  height: 750px;
}
.empty-message {
  text-align: center;
  padding: 60px 40px;
  color: var(--text-tertiary);
  font-size: 18px;
}
.legend-tip {
  text-align: center;
  margin-top: 10px;
  color: var(--text-tertiary);
  font-size: 12px;
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
  .selectors {
    gap: 10px;
    margin-bottom: 10px;
  }
  .selector select {
    width: 100%;
    padding: 9px 12px;
    font-size: 13px;
  }
  .selector {
    width: 100%;
  }
  .toggle-text {
    font-size: 12px;
  }
  .toggle-slider {
    width: 38px;
    height: 20px;
  }
  .toggle-slider::before {
    width: 14px;
    height: 14px;
  }
  .toggle-input:checked + .toggle-slider::before {
    left: 20px;
  }
  .y-axis-range {
    width: 100%;
  }
  .range-select {
    padding: 6px 8px;
    font-size: 12px;
    min-width: 50px;
  }
  .pagination {
    gap: 8px;
    padding: 8px;
  }
  .page-btn {
    padding: 7px 12px;
    font-size: 13px;
  }
  .page-info {
    width: 100%;
    text-align: center;
    font-size: 12px;
    order: -1;
  }
  .items-per-page {
    width: 100%;
    margin-left: 0;
    padding-left: 0;
    border-left: none;
    justify-content: center;
  }
  .items-per-page label {
    font-size: 12px;
  }
  .chart {
    height: 380px;
  }
  .empty-message {
    padding: 40px 16px;
    font-size: 15px;
  }
  .legend-tip {
    font-size: 11px;
  }
}
</style>
