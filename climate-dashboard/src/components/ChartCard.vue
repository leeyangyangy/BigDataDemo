<template>
  <div class="chart-container">
    <div class="chart-title">{{ title }}</div>
    <div class="chart-box">
      <slot></slot>
    </div>
  </div>
</template>

<script setup>
defineProps({
  title: {
    type: String,
    required: true
  }
})
</script>

<style scoped>
.chart-container {
  background: var(--bg-secondary);
  border-radius: 16px;
  padding: 20px;
  box-shadow: var(--shadow-sm);
  transition: background-color var(--transition-speed), box-shadow var(--transition-speed);
}

.chart-title {
  font-size: 16px;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 15px;
  transition: color var(--transition-speed);
}

.chart-box {
  width: 100%;
  height: 400px;
}
</style>
