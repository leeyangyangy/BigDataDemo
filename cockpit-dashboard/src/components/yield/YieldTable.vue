<template>
  <div class="card">
    <h2>📋 当前在制产品良率列表</h2>
    <div class="table-container">
      <div v-if="groupedData.length === 0" class="empty-message">
        暂无数据
      </div>
      <div v-else class="product-groups">
        <div v-for="(productGroup, productName) in groupedData" :key="productName" class="product-group">
          <div class="product-header" @click="toggleProduct(productName)">
            <span class="product-toggle">{{ expandedProducts.has(productName) ? '▼' : '▶' }}</span>
            <span class="product-name">{{ productName }}</span>
            <span class="product-count">({{ Object.keys(productGroup.productCodes).length }} 个片号)</span>
          </div>

          <div v-show="expandedProducts.has(productName)" class="product-content">
            <div v-for="(productCodeGroup, productCode) in productGroup.productCodes" :key="productCode" class="product-code-group">
              <div class="product-code-header" @click="toggleProductCode(productName, productCode)">
                <span class="product-code-toggle">{{ expandedProductCodes.has(`${productName}-${productCode}`) ? '▼' : '▶' }}</span>
                <span class="product-code-name">{{ productCode }}</span>
                <span v-if="productCodeGroup.primaryYield !== null" class="rank-yield" :class="getYieldClass(productCodeGroup.primaryYield)">
                  {{ productCodeGroup.primaryRank }}: {{ productCodeGroup.primaryYield }}%
                </span>
              </div>

              <div v-show="expandedProductCodes.has(`${productName}-${productCode}`)" class="product-code-content">
                <div class="table-scroll">
                <table class="inner-table">
                  <thead>
                    <tr>
                      <th>分等</th>
                      <th>老化前总数</th>
                      <th>老化后数量</th>
                      <th>良率 (%)</th>
                      <th>良率柱状图</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="item in productCodeGroup.items" :key="item.binRank">
                      <td>{{ item.binRank }}</td>
                      <td>{{ item.beforeTotal }}</td>
                      <td>{{ item.afterCount }}</td>
                      <td :class="getYieldClass(item.yieldRate)">
                        {{ item.yieldRate }}%
                      </td>
                      <td>
                        <div class="bar-container">
                          <div
                            class="bar"
                            :class="getYieldClass(item.yieldRate)"
                            :style="{ width: item.yieldRate + '%' }"
                          ></div>
                          <span class="bar-label">{{ item.yieldRate }}%</span>
                        </div>
                      </td>
                    </tr>
                  </tbody>
                </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'

const props = defineProps({
  yieldRates: {
    type: Array,
    default: () => []
  }
})

const getYieldClass = (rate) => {
  if (rate >= 90) return 'yield-high'
  if (rate >= 70) return 'yield-medium'
  return 'yield-low'
}

const expandedProducts = ref(new Set())
const expandedProductCodes = ref(new Set())

const RANK_PRIORITY = ['JXBS', 'BS', 'A', 'B', 'C', 'D']

const groupedData = computed(() => {
  const result = {}

  props.yieldRates.forEach(item => {
    const productName = item.productName
    const productCode = item.productCode

    if (!result[productName]) {
      result[productName] = {
        productCodes: {}
      }
    }

    if (!result[productName].productCodes[productCode]) {
      result[productName].productCodes[productCode] = {
        items: [],
        primaryRank: null,
        primaryYield: null
      }
    }

    result[productName].productCodes[productCode].items.push(item)
  })

  Object.keys(result).forEach(productName => {
    Object.keys(result[productName].productCodes).forEach(productCode => {
      const group = result[productName].productCodes[productCode]

      for (const rank of RANK_PRIORITY) {
        const foundItem = group.items.find(item => item.binRank === rank)
        if (foundItem) {
          group.primaryRank = rank
          group.primaryYield = foundItem.yieldRate
          break
        }
      }

      if (!group.primaryRank && group.items.length > 0) {
        group.primaryRank = group.items[0].binRank
        group.primaryYield = group.items[0].yieldRate
      }
    })
  })

  return result
})

const toggleProduct = (productName) => {
  if (expandedProducts.value.has(productName)) {
    expandedProducts.value.delete(productName)
  } else {
    expandedProducts.value.add(productName)
  }
}

const toggleProductCode = (productName, productCode) => {
  const key = `${productName}-${productCode}`
  if (expandedProductCodes.value.has(key)) {
    expandedProductCodes.value.delete(key)
  } else {
    expandedProductCodes.value.add(key)
  }
}
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
.table-container {
  max-height: 600px;
  overflow-y: auto;
}
.table-container::-webkit-scrollbar {
  width: 8px;
}
.table-container::-webkit-scrollbar-track {
  background: rgba(var(--accent-rgb), 0.1);
  border-radius: 4px;
}
.table-container::-webkit-scrollbar-thumb {
  background: rgba(var(--accent-rgb), 0.5);
  border-radius: 4px;
}
.product-groups {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.product-group {
  border: 1px solid rgba(var(--accent-rgb), 0.2);
  border-radius: 10px;
  overflow: hidden;
}
.product-header {
  background: linear-gradient(90deg, rgba(var(--accent-rgb), 0.2), rgba(0, 255, 136, 0.1));
  padding: 15px 20px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 10px;
  transition: all 0.3s;
}
.product-header:hover {
  background: linear-gradient(90deg, rgba(var(--accent-rgb), 0.3), rgba(0, 255, 136, 0.2));
}
.product-toggle {
  font-size: 14px;
  color: var(--accent-primary);
  width: 20px;
}
.product-name {
  font-size: 18px;
  font-weight: bold;
  color: var(--text-primary);
  flex: 1;
}
.product-count {
  font-size: 14px;
  color: var(--text-tertiary);
}
.product-content {
  background: var(--bg-tertiary);
}
.product-code-group {
  border-top: 1px solid var(--border-color);
}
.product-code-header {
  padding: 12px 20px 12px 40px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 10px;
  background: var(--bg-tertiary);
  transition: all 0.3s;
}
.product-code-header:hover {
  background: var(--bg-secondary);
}
.product-code-toggle {
  font-size: 12px;
  color: #00ff88;
  width: 18px;
}
.product-code-name {
  font-size: 16px;
  color: var(--text-secondary);
}
.rank-yield {
  margin-left: auto;
  font-size: 14px;
  font-weight: bold;
  padding: 4px 12px;
  border-radius: 4px;
  background: var(--bg-input);
}
.product-code-content {
  padding: 0 20px 20px 60px;
}
.inner-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 10px;
}
.inner-table th {
  background: rgba(var(--accent-rgb), 0.15);
  color: var(--accent-primary);
  font-weight: bold;
  padding: 10px;
  text-align: center;
  font-size: 14px;
}
.inner-table td {
  padding: 10px;
  text-align: center;
  border-bottom: 1px solid var(--border-color);
  font-size: 14px;
}
.bar-container {
  position: relative;
  width: 150px;
  height: 24px;
  background: var(--bg-input);
  border-radius: 12px;
  overflow: hidden;
  margin: 0 auto;
}
.bar {
  height: 100%;
  border-radius: 12px;
  transition: width 0.5s ease;
  min-width: 4px;
}
.bar.yield-high {
  background: linear-gradient(90deg, #00ff88, #00cc6a);
}
.bar.yield-medium {
  background: linear-gradient(90deg, #ffd700, #ccac00);
}
.bar.yield-low {
  background: linear-gradient(90deg, #ff4444, #cc3333);
}
.bar-label {
  position: absolute;
  right: 8px;
  top: 50%;
  transform: translateY(-50%);
  font-size: 12px;
  font-weight: bold;
  color: var(--text-primary);
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.5);
}
.yield-high {
  color: #00ff88;
  font-weight: bold;
}
.yield-medium {
  color: #ffd700;
  font-weight: bold;
}
.yield-low {
  color: #ff4444;
  font-weight: bold;
}
.empty-message {
  text-align: center;
  padding: 60px 40px;
  color: var(--text-tertiary);
  font-size: 18px;
}
.table-scroll {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
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
  .table-container {
    max-height: 70vh;
  }
  .product-header {
    padding: 10px 12px;
    gap: 6px;
  }
  .product-name {
    font-size: 15px;
  }
  .product-count {
    font-size: 12px;
  }
  .product-code-header {
    padding: 9px 12px 9px 22px;
    gap: 6px;
  }
  .product-code-name {
    font-size: 13px;
  }
  .rank-yield {
    font-size: 12px;
    padding: 3px 8px;
  }
  .product-code-content {
    padding: 0 8px 10px 22px;
  }
  .inner-table th,
  .inner-table td {
    padding: 7px 8px;
    font-size: 12px;
    white-space: nowrap;
  }
  .bar-container {
    width: 100px;
    height: 18px;
  }
  .bar-label {
    font-size: 10px;
  }
  .empty-message {
    padding: 40px 20px;
    font-size: 15px;
  }
}
</style>
