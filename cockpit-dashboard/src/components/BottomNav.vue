<template>
  <div class="bottom-nav" :class="{ 'light-mode': isLightMode, 'dark-mode': !isLightMode }" ref="navRef">
    <div class="nav-glow-bg" v-if="isLightMode"></div>
    <div class="nav-indicator" :style="indicatorStyle" :class="{ 'light-indicator': isLightMode }"></div>
    <div
      v-for="(item, index) in navItems"
      :key="item.key"
      class="nav-item"
      :class="{ active: activeKey === item.key, 'light-item': isLightMode }"
      :ref="el => itemRefs[index] = el"
      @click="handleClick(item.key, $event)"
      @mouseenter="handleHover(index)"
      @mouseleave="handleLeave(index)"
    >
      <span class="ripple-container">
        <span
          class="ripple"
          :ref="el => rippleRefs[index] = el"
          :class="{ 'light-ripple': isLightMode }"
        ></span>
      </span>
      <span class="icon-wrapper" :class="{ 'bounce': clickedIndex === index, 'light-icon-wrapper': isLightMode && activeKey === item.key }">
        <span class="nav-icon">{{ item.icon }}</span>
        <span class="icon-glow" :class="{ 'light-glow': isLightMode }" v-if="activeKey === item.key"></span>
        <span class="icon-ring" v-if="activeKey === item.key && isLightMode"></span>
      </span>
      <span class="nav-label" :class="{ 'light-label': isLightMode && activeKey === item.key }">{{ item.label }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, nextTick, onMounted, watch } from 'vue'
import { useTheme } from '../composables/useTheme'

const props = defineProps({
  activeIndex: { type: Number, default: 1 },
  userRole: { type: String, default: '' },
  activeKey: { type: String, default: 'home' }
})

const emit = defineEmits(['navigate'])

const { isDark } = useTheme()

const activeIndex = ref(props.activeIndex)
const hoveredIndex = ref(-1)
const clickedIndex = ref(-1)
const indicatorStyle = ref({})
const navRef = ref(null)
const itemRefs = ref([])
const rippleRefs = ref([])

const allNavItems = [
  { key: 'home', icon: '🏠', label: '首页' },
  { key: 'data', icon: '📊', label: '数据' },
  // { key: 'chat', icon: '💬', label: 'AIChat' },
  // { key: 'map', icon: '🗺️', label: '地图' },
  { key: 'admin', icon: '☰', label: '后台' }
]

const adminSubItems = [
  { key: 'admin-product', icon: '📦', label: '产品' },
  { key: 'admin-process', icon: '⚙️', label: '工序' },
  { key: 'admin-standard', icon: '📏', label: '工艺' },
  { key: 'admin-equipment', icon: '🔧', label: '设备' },
  { key: 'admin-workshop', icon: '🏭', label: '车间' },
  { key: 'admin-user', icon: '👥', label: '用户' }
]

const navItems = computed(() => {
  const isAdminMode = props.activeKey.startsWith('admin')
  if (isAdminMode) {
    return [{ key: 'home', icon: '🏠', label: '首页' }, ...adminSubItems]
  }
  if (props.userRole === 'ADMIN') return allNavItems
  return allNavItems.filter(item => item.key !== 'admin')
})

const activeKey = ref(props.activeKey || 'home')

function findIndexByKey(key) {
  return navItems.value.findIndex(item => item.key === key)
}

watch(() => props.activeKey, (key) => {
  if (!key) return
  activeKey.value = key
  nextTick(() => {
    const idx = findIndexByKey(key)
    if (idx >= 0) {
      activeIndex.value = idx
      updateIndicator(idx)
    }
  })
}, { immediate: true })

const isLightMode = computed(() => !isDark.value)

const updateIndicator = (index) => {
  nextTick(() => {
    if (itemRefs.value[index]) {
      const item = itemRefs.value[index]
      const nav = navRef.value
      if (item && nav) {
        const itemRect = item.getBoundingClientRect()
        const navRect = nav.getBoundingClientRect()
        
        indicatorStyle.value = {
          width: `${itemRect.width}px`,
          left: `${itemRect.left - navRect.left}px`,
          transition: 'all 0.45s cubic-bezier(0.68, -0.55, 0.265, 1.55)'
        }
      }
    }
  })
}

const handleClick = (key, event) => {
  activeKey.value = key
  const idx = findIndexByKey(key)
  if (idx >= 0) clickedIndex.value = idx
  emit('navigate', key)

  createRipple(event, idx >= 0 ? idx : 0)

  setTimeout(() => {
    clickedIndex.value = -1
  }, 600)

  nextTick(() => updateIndicator(idx >= 0 ? idx : 0))
}

const handleHover = (index) => {
  hoveredIndex.value = index
}

const handleLeave = (index) => {
  hoveredIndex.value = -1
}

const createRipple = (event, index) => {
  nextTick(() => {
    const button = event.currentTarget
    const rect = button.getBoundingClientRect()
    const size = Math.max(rect.width, rect.height) * 2
    const x = event.clientX - rect.left - size / 2
    const y = event.clientY - rect.top - size / 2
    
    const ripple = rippleRefs.value[index]
    if (ripple) {
      ripple.style.width = ripple.style.height = `${size}px`
      ripple.style.left = `${x}px`
      ripple.style.top = `${y}px`
      ripple.classList.remove('show')
      
      void ripple.offsetWidth
      
      ripple.classList.add('show')
      
      setTimeout(() => {
        ripple.classList.remove('show')
      }, 700)
    }
  })
}

onMounted(() => {
  setTimeout(() => {
    const idx = findIndexByKey(activeKey.value)
    if (idx >= 0) updateIndicator(idx)
  }, 150)

  window.addEventListener('resize', () => {
    const idx = findIndexByKey(activeKey.value)
    if (idx >= 0) updateIndicator(idx)
  })
})
</script>

<style scoped>
.bottom-nav {
  position: fixed;
  bottom: 20px;
  left: 50%;
  transform: translateX(-50%);
  background: var(--bg-nav);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  display: flex;
  justify-content: space-around;
  padding: 8px 20px;
  border-radius: 30px;
  box-shadow: var(--shadow-nav);
  z-index: 1000;
  gap: 5px;
  transition: all 0.4s ease;
  overflow: hidden;
  border: 1px solid transparent;
}

.bottom-nav.light-mode {
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 
    0 8px 32px rgba(24, 144, 255, 0.12),
    0 2px 8px rgba(0, 0, 0, 0.08),
    0 0 0 1px rgba(24, 144, 255, 0.08),
    inset 0 1px 0 rgba(255, 255, 255, 0.9),
    inset 0 -1px 0 rgba(24, 144, 255, 0.05);
  border-color: rgba(24, 144, 255, 0.15);
}

.bottom-nav.dark-mode {
  box-shadow: var(--shadow-nav), inset 0 1px 0 rgba(255, 255, 255, 0.05);
}

.nav-glow-bg {
  position: absolute;
  top: -50%;
  left: -10%;
  right: -10%;
  bottom: -50%;
  background: radial-gradient(
    ellipse at center,
    rgba(24, 144, 255, 0.06) 0%,
    transparent 70%
  );
  pointer-events: none;
  animation: bg-breathe 4s ease-in-out infinite;
}

.nav-indicator {
  position: absolute;
  top: 6px;
  height: calc(100% - 12px);
  background: var(--bg-nav-active);
  border-radius: 24px;
  pointer-events: none;
  z-index: 0;
  will-change: transform, width, left;
  transition: all 0.45s cubic-bezier(0.68, -0.55, 0.265, 1.55);
}

.nav-indicator.light-indicator {
  background: linear-gradient(
    135deg,
    rgba(24, 144, 255, 0.18) 0%,
    rgba(24, 144, 255, 0.25) 50%,
    rgba(64, 169, 255, 0.18) 100%
  );
  box-shadow: 
    inset 0 1px 3px rgba(255, 255, 255, 0.6),
    inset 0 -1px 3px rgba(24, 144, 255, 0.15),
    0 2px 8px rgba(24, 144, 255, 0.15);
  border: 1px solid rgba(24, 144, 255, 0.2);
}

.nav-item {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 2px;
  cursor: pointer;
  color: var(--text-tertiary);
  font-size: 11px;
  font-weight: 500;
  padding: 10px 18px;
  border-radius: 24px;
  z-index: 1;
  user-select: none;
  -webkit-tap-highlight-color: transparent;
  transition: color 0.35s ease, transform 0.35s ease;
}

.nav-item.light-item {
  color: #a0aec0;
}

.nav-item:hover .icon-wrapper {
  transform: translateY(-5px) scale(1.15);
}

.nav-item.light-item:hover {
  color: #1890ff;
}

.nav-item:hover .nav-icon {
  animation: float 2s ease-in-out infinite;
}

.nav-item.active {
  color: var(--accent-primary);
}

.nav-item.active.light-item {
  color: #096dd9;
  text-shadow: 0 1px 2px rgba(24, 144, 255, 0.2);
}

.nav-item.active .icon-wrapper {
  animation: pulse 2s ease-in-out infinite;
}

.icon-wrapper {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  margin-bottom: 2px;
  transition: transform 0.45s cubic-bezier(0.68, -0.55, 0.265, 1.55);
  will-change: transform;
}

.icon-wrapper.bounce {
  animation: bounce 0.65s cubic-bezier(0.68, -0.55, 0.265, 1.55);
}

.icon-wrapper.light-icon-wrapper {
  filter: drop-shadow(0 2px 4px rgba(24, 144, 255, 0.3));
}

.nav-icon {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 22px;
  filter: grayscale(40%);
  opacity: 0.65;
  transition: all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
  will-change: transform, filter, opacity;
}

.nav-item.active .nav-icon,
.nav-item:hover .nav-icon {
  filter: grayscale(0%) brightness(1.1);
  opacity: 1;
  transform: scale(1.2);
}

.icon-glow {
  position: absolute;
  width: 36px;
  height: 36px;
  background: radial-gradient(circle, rgba(var(--accent-rgb), 0.4) 0%, transparent 70%);
  border-radius: 50%;
  animation: glow-pulse 2s ease-in-out infinite;
  pointer-events: none;
}

.icon-glow.light-glow {
  width: 44px;
  height: 44px;
  background: radial-gradient(
    circle,
    rgba(24, 144, 255, 0.35) 0%,
    rgba(24, 144, 255, 0.15) 40%,
    transparent 70%
  );
  animation: light-glow-pulse 2.5s ease-in-out infinite;
}

.icon-ring {
  position: absolute;
  width: 38px;
  height: 38px;
  border: 2px solid rgba(24, 144, 255, 0.25);
  border-radius: 50%;
  animation: ring-expand 2s ease-in-out infinite;
  pointer-events: none;
}

.nav-label {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.03em;
  opacity: 0.7;
  transition: all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
  white-space: nowrap;
}

.nav-label.light-label {
  color: #096dd9;
  font-weight: 700;
  opacity: 1;
  text-shadow: 0 1px 2px rgba(24, 144, 255, 0.15);
  transform: translateY(-1px) scale(1.02);
}

.nav-item.active .nav-label,
.nav-item:hover .nav-label {
  opacity: 1;
  transform: translateY(-1px);
}

.ripple-container {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  overflow: hidden;
  border-radius: 24px;
  pointer-events: none;
}

.ripple {
  position: absolute;
  border-radius: 50%;
  background: radial-gradient(circle, rgba(var(--accent-rgb), 0.3) 0%, transparent 70%);
  transform: scale(0);
  pointer-events: none;
  opacity: 0;
}

.ripple.light-ripple {
  background: radial-gradient(
    circle,
    rgba(24, 144, 255, 0.4) 0%,
    rgba(100, 180, 255, 0.2) 40%,
    transparent 70%
  );
  box-shadow: 0 0 20px rgba(24, 144, 255, 0.3);
}

.ripple.show {
  animation: ripple-effect 0.7s cubic-bezier(0.25, 0.46, 0.45, 0.94) forwards;
}

@keyframes bounce {
  0% { transform: scale(1); }
  15% { transform: scale(1.35); }
  30% { transform: scale(0.9); }
  50% { transform: scale(1.15); }
  70% { transform: scale(0.97); }
  85% { transform: scale(1.06); }
  100% { transform: scale(1); }
}

@keyframes pulse {
  0%, 100% { transform: scale(1); }
  50% { transform: scale(1.06); }
}

@keyframes glow-pulse {
  0%, 100% { 
    opacity: 0.5; 
    transform: scale(1); 
  }
  50% { 
    opacity: 1; 
    transform: scale(1.2); 
  }
}

@keyframes light-glow-pulse {
  0%, 100% { 
    opacity: 0.6; 
    transform: scale(0.95); 
  }
  33% { 
    opacity: 1; 
    transform: scale(1.15); 
  }
  66% { 
    opacity: 0.85; 
    transform: scale(1.05); 
  }
}

@keyframes ring-expand {
  0%, 100% { 
    transform: scale(0.95); 
    opacity: 0.4; 
    border-width: 2px;
  }
  50% { 
    transform: scale(1.25); 
    opacity: 0.15; 
    border-width: 1px;
  }
}

@keyframes float {
  0%, 100% { transform: translateY(-5px) rotate(0deg); }
  25% { transform: translateY(-7px) rotate(-4deg); }
  75% { transform: translateY(-6px) rotate(4deg); }
}

@keyframes ripple-effect {
  0% {
    transform: scale(0);
    opacity: 1;
  }
  60% {
    opacity: 0.6;
  }
  100% {
    transform: scale(2.8);
    opacity: 0;
  }
}

@keyframes bg-breathe {
  0%, 100% { 
    opacity: 0.6; 
    transform: scale(1);
  }
  50% { 
    opacity: 1; 
    transform: scale(1.02);
  }
}

@media (max-width: 600px) {
  .bottom-nav {
    padding: 6px 12px;
    gap: 2px;
    width: calc(100% - 40px);
    justify-content: space-between;
    bottom: 16px;
  }
  
  .bottom-nav.light-mode {
    box-shadow: 
      0 6px 24px rgba(24, 144, 255, 0.1),
      0 2px 6px rgba(0, 0, 0, 0.06),
      0 0 0 1px rgba(24, 144, 255, 0.1);
  }
  
  .nav-item {
    padding: 8px 10px;
    flex: 1;
  }

  .nav-label {
    font-size: 9px;
  }

  .nav-icon {
    font-size: 18px;
    width: 20px;
    height: 20px;
  }

  .icon-wrapper {
    width: 24px;
    height: 24px;
  }

  .icon-glow.light-glow {
    width: 36px;
    height: 36px;
  }

  .icon-ring {
    width: 32px;
    height: 32px;
  }
}
</style>