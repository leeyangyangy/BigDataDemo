<template>
  <div class="filter-bar">
    <div class="filter-group">
      <label>选择城市</label>
      <select v-model="selectedCity" class="filter-select">
        <option value="all">全部城市</option>
        <option v-for="city in cities" :key="city.value" :value="city.value">
          {{ city.label }}
        </option>
      </select>
    </div>
    <div class="filter-group">
      <label>选择年份</label>
      <select v-model="selectedYear" class="filter-select">
        <option value="all">全部年份</option>
        <option v-for="year in years" :key="year" :value="year">{{ year }}</option>
      </select>
    </div>
    <button class="btn-apply" @click="applyFilter">应用筛选</button>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const emit = defineEmits(['filter'])

const selectedCity = ref('all')
const selectedYear = ref('all')

const cities = [
  { value: 'guangzhou', label: '广州' },
  { value: 'shenzhen', label: '深圳' },
  { value: 'zhuhai', label: '珠海' },
  { value: 'foshan', label: '佛山' },
  { value: 'huizhou', label: '惠州' },
  { value: 'dongguan', label: '东莞' },
  { value: 'zhongshan', label: '中山' },
  { value: 'jiangmen', label: '江门' },
  { value: 'zhaoqing', label: '肇庆' },
  { value: 'macau', label: '澳门' },
  { value: 'hongkong', label: '香港' }
]

const years = Array.from({ length: 15 }, (_, i) => 2010 + i)

function applyFilter() {
  emit('filter', {
    city: selectedCity.value,
    year: selectedYear.value
  })
}
</script>

<style scoped>
.filter-bar {
  display: flex;
  gap: 15px;
  align-items: flex-end;
  margin-bottom: 25px;
  flex-wrap: wrap;
}

.filter-group {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.filter-group label {
  font-size: 14px;
  color: var(--text-secondary);
  font-weight: 500;
  transition: color var(--transition-speed);
}

.filter-select {
  padding: 10px 40px 10px 12px;
  border: 1px solid var(--border-input);
  border-radius: 8px;
  font-size: 14px;
  background-color: var(--bg-input);
  color: var(--text-primary);
  cursor: pointer;
  min-width: 180px;
  outline: none;
  transition: all var(--transition-speed);
}

.filter-select:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 2px rgba(var(--accent-rgb), 0.1);
}

.btn-apply {
  padding: 10px 50px;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 80%, black));
  color: white;
  border: none;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all var(--transition-speed);
}

.btn-apply:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(var(--accent-rgb), 0.4);
}

@media (max-width: 600px) {
  .filter-bar {
    flex-direction: column;
    align-items: stretch;
  }
  .btn-apply {
    width: 100%;
  }
}
</style>
