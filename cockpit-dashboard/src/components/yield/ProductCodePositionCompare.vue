<template>
  <div class="card">
    <h2>📊 片号位置对比</h2>
    <div class="selectors">
      <div class="selector">
        <label>选择产品:</label>
        <select v-model="selectedProduct" @change="resetSelection">
          <option value="">请选择</option>
          <option v-for="product in productList" :key="product" :value="product">
            {{ product }}
          </option>
        </select>
      </div>
      <div class="selector">
        <label>选择片号:</label>
        <select v-model="selectedBaseProductCode" @change="updateChart" :disabled="!selectedProduct">
          <option value="">请选择</option>
          <option v-for="code in baseProductCodeList" :key="code" :value="code">
            {{ code }}
          </option>
        </select>
      </div>
    </div>
    <div ref="chartRef" class="chart"></div>
    <div v-if="!selectedProduct" class="empty-message">
      请选择产品查看片号位置对比
    </div>
    <div v-if="selectedProduct && baseProductCodeList.length === 0" class="empty-message">
      该产品暂无带位置后缀（A、B、C等）的片号数据
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
const STORAGE_KEY = 'product-code-position-selected'
const savedProduct = localStorage.getItem(`${STORAGE_KEY}-product`)
const savedBaseCode = localStorage.getItem(`${STORAGE_KEY}-basecode`)
const selectedProduct = ref(savedProduct || '')
const selectedBaseProductCode = ref(savedBaseCode || '')
let chartInstance = null

const RANK_PRIORITY = ['未分等', 'JXBS', 'BS', 'AA', 'A', 'BB', 'B', 'CC', 'C', 'DD', 'D']

const productList = computed(() => {
  const products = new Set()
  props.yieldRates.forEach(item => {
    products.add(item.productName)
  })
  return Array.from(products).sort()
})

const productCodeList = computed(() => {
  if (!selectedProduct.value) return []
  return [...new Set(props.yieldRates
    .filter(item => item.productName === selectedProduct.value)
    .map(item => item.productCode)
  )].sort()
})

const productCodeWithPositionList = computed(() => {
  if (!selectedProduct.value) return []
  return productCodeList.value.filter(code => hasPosition(code))
})

const baseProductCodeList = computed(() => {
  if (!selectedProduct.value) return []
  const baseCodeCount = {}

  productCodeWithPositionList.value.forEach(code => {
    const baseCode = extractBaseProductCode(code)
    if (baseCode) {
      baseCodeCount[baseCode] = (baseCodeCount[baseCode] || 0) + 1
    }
  })

  return Object.keys(baseCodeCount)
    .filter(baseCode => baseCodeCount[baseCode] >= 1)
    .sort()
})

const extractBaseProductCode = (productCode) => {
  const match = productCode.match(/^(\d+)/)
  return match ? match[1] : null
}

const extractPosition = (productCode) => {
  const match = productCode.match(/[A-Za-z]+$/)
  return match ? match[0] : ''
}

const hasPosition = (productCode) => {
  return /[A-Za-z]+$/.test(productCode)
}

const resetSelection = () => {
  selectedBaseProductCode.value = ''
  updateChart()
}

watch(selectedProduct, (newValue) => {
  if (newValue) {
    localStorage.setItem(`${STORAGE_KEY}-product`, newValue)
  } else {
    localStorage.removeItem(`${STORAGE_KEY}-product`)
  }
})

watch(selectedBaseProductCode, (newValue) => {
  if (newValue) {
    localStorage.setItem(`${STORAGE_KEY}-basecode`, newValue)
  } else {
    localStorage.removeItem(`${STORAGE_KEY}-basecode`)
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

  if (selectedProduct.value && selectedBaseProductCode.value) {
    updateChart()
  }
}

const updateChart = () => {
  if (!chartInstance) {
    return
  }

  if (!selectedProduct.value || !selectedBaseProductCode.value || !props.yieldRates || props.yieldRates.length === 0) {
    chartInstance.clear()
    return
  }

  const relatedProductCodes = productCodeWithPositionList.value.filter(code => {
    const baseCode = extractBaseProductCode(code)
    return baseCode === selectedBaseProductCode.value
  })

  if (relatedProductCodes.length === 0) {
    chartInstance.clear()
    return
  }

  const productData = props.yieldRates.filter(item =>
    item.productName === selectedProduct.value &&
    relatedProductCodes.includes(item.productCode)
  )

  if (productData.length === 0) {
    chartInstance.clear()
    return
  }

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

  const colors = [
    '#00d4ff', '#00ff88', '#ffd700', '#ff4444', '#ff00ff',
    '#00ffcc', '#ff8800', '#88ff00', '#ff0088', '#8800ff'
  ]

  const series = relatedProductCodes.map((productCode, index) => {
    const position = extractPosition(productCode) || productCode
    const data = binRanks.map(binRank => {
      const item = productData.find(
        i => i.productCode === productCode && i.binRank === binRank
      )
      return item ? item.yieldRate : null
    })

    return {
      name: position,
      type: 'bar',
      data: data,
      itemStyle: {
        color: colors[index % colors.length]
      }
    }
  })

  const isMobile = window.innerWidth <= 768
  const option = {
    title: {
      text: `${selectedProduct.value} - ${selectedBaseProductCode.value} 各位置对比`,
      left: 'center',
      textStyle: { color: '#00d4ff', fontSize: isMobile ? 13 : 18 }
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'shadow'
      },
      formatter: (params) => {
        let result = params[0].axisValue + '<br/>'
        params.forEach(param => {
          if (param.value !== null && param.value !== undefined) {
            result += `${param.marker} ${param.seriesName}: ${param.value}%<br/>`
          }
        })
        return result
      }
    },
    legend: {
      data: relatedProductCodes.map(code => extractPosition(code) || code),
      top: isMobile ? 32 : 40,
      textStyle: { color: '#aaa', fontSize: isMobile ? 10 : 12 },
      type: 'scroll',
      itemWidth: isMobile ? 12 : 25,
      itemHeight: isMobile ? 8 : 14
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      top: isMobile ? 70 : 90,
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: binRanks,
      axisLine: { lineStyle: { color: '#666' } },
      axisLabel: { color: '#aaa', fontSize: isMobile ? 10 : 14, hideOverlap: true }
    },
    yAxis: {
      type: 'value',
      min: 0,
      max: 100,
      name: '良率 (%)',
      nameTextStyle: { color: '#aaa', fontSize: isMobile ? 10 : 12 },
      axisLine: { lineStyle: { color: '#666' } },
      axisLabel: { color: '#aaa', fontSize: isMobile ? 10 : 14 },
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
    if (chartInstance && selectedProduct.value && selectedBaseProductCode.value) {
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

watch(() => [props.yieldRates, props.historicalData], () => {
  const products = new Set()
  props.yieldRates.forEach(item => {
    products.add(item.productName)
  })
  if (selectedProduct.value && !products.has(selectedProduct.value)) {
    selectedProduct.value = ''
    selectedBaseProductCode.value = ''
  } else if (selectedProduct.value && selectedBaseProductCode.value) {
    if (!baseProductCodeList.value.includes(selectedBaseProductCode.value)) {
      selectedBaseProductCode.value = ''
    }
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
.selector select:disabled {
  opacity: 0.5;
  cursor: not-allowed;
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
  height: 500px;
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
  .selectors {
    flex-direction: column;
    align-items: stretch;
    gap: 10px;
    margin-bottom: 10px;
  }
  .selector {
    width: 100%;
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
    height: 340px;
  }
  .empty-message {
    padding: 40px 16px;
    font-size: 15px;
  }
}
</style>
