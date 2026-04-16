<template>
  <div class="dashboard">
    <FilterBar @filter="handleFilter" />

    <div class="stats-grid">
      <StatCard
        v-for="(stat, index) in stats"
        :key="index"
        :value="stat.value"
        :label="stat.label"
      />
    </div>

    <div class="charts-section">
      <ChartCard title="温度变化趋势">
        <TempHeatmap ref="tempRef" />
      </ChartCard>
      <ChartCard title="降水量分布">
        <RainChart ref="rainRef" />
      </ChartCard>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import FilterBar from './FilterBar.vue'
import StatCard from './StatCard.vue'
import ChartCard from './ChartCard.vue'
import TempHeatmap from './TempHeatmap.vue'
import RainChart from './RainChart.vue'

const tempRef = ref(null)
const rainRef = ref(null)

const stats = ref([
  { value: '23.23', label: '平均温度 (°C)' },
  { value: '528290', label: '总降水量 (mm)' },
  { value: '3078', label: '极端天气天数' },
  { value: '8.82', label: '平均风速 (m/s)' }
])

function handleFilter(filter) {
  if (filter.city !== 'all' || filter.year !== 'all') {
    stats.value[0].value = (Math.random() * 10 + 18).toFixed(2)
    stats.value[1].value = Math.floor(Math.random() * 500000 + 30000).toLocaleString()
    stats.value[2].value = Math.floor(Math.random() * 4000 + 1000).toString()
    stats.value[3].value = (Math.random() * 6 + 5).toFixed(2)
  } else {
    stats.value[0].value = '23.23'
    stats.value[1].value = '528290'
    stats.value[2].value = '3078'
    stats.value[3].value = '8.82'
  }

  tempRef.value?.refresh()
  rainRef.value?.refresh()
}
</script>

<style scoped>
.dashboard {
  max-width: 1400px;
  margin: 0 auto;
  padding: 0 20px 20px;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 20px;
  margin-bottom: 25px;
}

.charts-section {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
}

@media (max-width: 1024px) {
  .stats-grid {
    grid-template-columns: repeat(2, 1fr);
  }
  .charts-section {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 600px) {
  .stats-grid {
    grid-template-columns: 1fr;
  }
}
</style>
