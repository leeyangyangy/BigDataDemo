<template>
  <div class="theme-switcher" ref="switcherRef">
    <button class="trigger-btn" @click="togglePanel" :title="'当前: ' + currentThemeLabel">
      <span class="theme-icon">{{ currentIcon }}</span>
      <svg class="arrow-icon" :class="{ open: isOpen }" width="12" height="12" viewBox="0 0 12 12">
        <path d="M6 8L2 4h8L6 8z" fill="currentColor"/>
      </svg>
    </button>

    <Transition name="dropdown">
      <div v-if="isOpen" class="panel">
        <div class="section">
          <div class="section-title">主题模式</div>
          <div class="theme-options">
            <button
              v-for="(theme, key) in themes"
              :key="key"
              class="theme-option"
              :class="{ active: currentTheme === key }"
              @click="selectTheme(key)"
            >
              <span class="option-icon">{{ theme.icon }}</span>
              <span class="option-label">{{ theme.label }}</span>
              <span v-if="currentTheme === key" class="check-icon">✓</span>
            </button>
          </div>
        </div>

        <div class="divider"></div>

        <div class="section">
          <div class="section-title">主题颜色</div>
          <div class="color-grid">
            <button
              v-for="color in accentColors"
              :key="color.value"
              class="color-option"
              :class="{ active: currentAccentColor === color.value }"
              :style="{ backgroundColor: color.value }"
              :title="color.label"
              @click="selectColor(color.value)"
            >
              <span v-if="currentAccentColor === color.value" class="color-check">✓</span>
            </button>
          </div>
          <div class="custom-color">
            <label>自定义:</label>
            <input
              type="color"
              :value="currentAccentColor"
              @input="selectColor($event.target.value)"
              class="color-picker"
            />
          </div>
        </div>

        <div class="divider"></div>

        <div class="preview-section">
          <div class="preview-card">
            <div class="preview-stat">23.23°C</div>
            <div class="preview-label">预览效果</div>
          </div>
        </div>
      </div>
    </Transition>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useTheme } from '../composables/useTheme'

const { currentTheme, currentAccentColor, themes, accentColors, setTheme, setAccentColor } = useTheme()

const switcherRef = ref(null)
const isOpen = ref(false)

const currentIcon = computed(() => {
  if (currentTheme.value === 'auto') {
    return '💻'
  }
  return currentTheme.value === 'dark' ? '🌙' : '☀️'
})

const currentThemeLabel = computed(() => {
  return themes[currentTheme.value]?.label || '未知'
})

function togglePanel() {
  isOpen.value = !isOpen.value
}

function selectTheme(theme) {
  setTheme(theme)
}

function selectColor(color) {
  setAccentColor(color)
}

function handleClickOutside(event) {
  if (switcherRef.value && !switcherRef.value.contains(event.target)) {
    isOpen.value = false
  }
}

onMounted(() => {
  document.addEventListener('click', handleClickOutside)
})

onUnmounted(() => {
  document.removeEventListener('click', handleClickOutside)
})
</script>

<style scoped>
.theme-switcher {
  position: relative;
  z-index: 1100;
}

.trigger-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 14px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 20px;
  cursor: pointer;
  font-size: 16px;
  transition: all 0.3s ease;
  box-shadow: var(--shadow-sm);
  color: var(--text-primary);
}

.trigger-btn:hover {
  transform: translateY(-1px);
  box-shadow: var(--shadow-md);
  border-color: var(--accent-primary);
}

.theme-icon {
  font-size: 18px;
}

.arrow-icon {
  transition: transform 0.3s ease;
  color: var(--text-tertiary);
}

.arrow-icon.open {
  transform: rotate(180deg);
}

.panel {
  position: absolute;
  top: calc(100% + 10px);
  right: 0;
  min-width: 260px;
  background: var(--bg-secondary);
  border-radius: 16px;
  padding: 16px;
  box-shadow: var(--shadow-lg);
  border: 1px solid var(--border-color);
}

.section {
  margin-bottom: 4px;
}

.section:last-child {
  margin-bottom: 0;
}

.section-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-secondary);
  margin-bottom: 10px;
}

.theme-options {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.theme-option {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  background: transparent;
  border: 1px solid transparent;
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s ease;
  font-size: 14px;
  color: var(--text-primary);
}

.theme-option:hover {
  background: var(--bg-nav-hover);
}

.theme-option.active {
  background: var(--bg-nav-active);
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

.option-icon {
  font-size: 18px;
  flex-shrink: 0;
}

.option-label {
  flex: 1;
  text-align: left;
}

.check-icon {
  color: var(--accent-primary);
  font-weight: bold;
}

.divider {
  height: 1px;
  background: var(--border-color);
  margin: 14px 0;
}

.color-grid {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 8px;
  margin-bottom: 12px;
}

.color-option {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  border: 2px solid transparent;
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
}

.color-option:hover {
  transform: scale(1.15);
}

.color-option.active {
  border-color: var(--text-primary);
  box-shadow: 0 0 0 2px var(--bg-secondary), 0 0 0 4px currentColor;
}

.color-check {
  color: white;
  font-size: 14px;
  font-weight: bold;
  text-shadow: 0 1px 2px rgba(0,0,0,0.3);
}

.custom-color {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 13px;
  color: var(--text-secondary);
}

.custom-color label {
  white-space: nowrap;
}

.color-picker {
  width: 40px;
  height: 28px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  background: transparent;
}

.color-picker::-webkit-color-swatch-wrapper {
  padding: 2px;
}

.color-picker::-webkit-color-swatch {
  border-radius: 4px;
  border: 1px solid var(--border-color);
}

.preview-section {
  margin-top: 4px;
}

.preview-card {
  background: var(--bg-card);
  border-radius: 12px;
  padding: 16px;
  text-align: center;
  color: var(--text-on-accent);
}

.preview-stat {
  font-size: 24px;
  font-weight: 700;
  margin-bottom: 4px;
}

.preview-label {
  font-size: 12px;
  opacity: 0.9;
}

.dropdown-enter-active,
.dropdown-leave-active {
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}

.dropdown-enter-from,
.dropdown-leave-to {
  opacity: 0;
  transform: translateY(-10px) scale(0.95);
}
</style>
