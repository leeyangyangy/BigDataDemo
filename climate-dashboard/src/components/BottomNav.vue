<template>
  <div class="bottom-nav">
    <div
      v-for="(item, index) in navItems"
      :key="index"
      class="nav-item"
      :class="{ active: activeIndex === index }"
      @click="activeIndex = index"
    >
      <span class="nav-icon">{{ item.icon }}</span>
      <span>{{ item.label }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const activeIndex = ref(1)

const navItems = [
  { icon: '🏠', label: '首页' },
  { icon: '📊', label: '数据' },
  { icon: '💬', label: 'AIChat' },
  { icon: '🗺️', label: '地图' },
  { icon: '☰', label: '后台' }
]
</script>

<style scoped>
.bottom-nav {
  position: fixed;
  bottom: 20px;
  left: 50%;
  transform: translateX(-50%);
  background: var(--bg-nav);
  backdrop-filter: blur(10px);
  display: flex;
  justify-content: space-around;
  padding: 12px 30px;
  border-radius: 25px;
  box-shadow: var(--shadow-nav);
  z-index: 1000;
  gap: 25px;
  transition: background-color var(--transition-speed), box-shadow var(--transition-speed);
}

.nav-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
  cursor: pointer;
  color: var(--text-tertiary);
  transition: all 0.3s ease, color var(--transition-speed);
  font-size: 12px;
  padding: 8px 16px;
  border-radius: 10px;
}

.nav-item:hover {
  color: var(--accent-primary);
  background: var(--bg-nav-hover);
}

.nav-item.active {
  color: var(--accent-primary);
  background: var(--bg-nav-active);
  transform: translateY(-2px);
}

.nav-icon {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 20px;
}

@media (max-width: 600px) {
  .bottom-nav {
    padding: 10px 15px;
    gap: 10px;
    width: calc(100% - 40px);
    justify-content: space-between;
  }
  
  .nav-item {
    padding: 6px 10px;
    flex: 1;
  }
}
</style>
