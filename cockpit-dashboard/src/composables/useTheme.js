import { ref, watch, onMounted } from 'vue'

const THEME_KEY = 'climate-dashboard-theme'
const ACCENT_COLOR_KEY = 'climate-dashboard-accent-color'

export const themes = {
  light: {
    name: 'light',
    label: '白天模式',
    icon: '☀️'
  },
  dark: {
    name: 'dark',
    label: '夜晚模式',
    icon: '🌙'
  },
  auto: {
    name: 'auto',
    label: '自动模式',
    icon: '💻'
  }
}

export const accentColors = [
  { value: '#1890ff', label: '默认蓝' },
  { value: '#722ed1', label: '科技紫' },
  { value: '#13c2c2', label: '青碧色' },
  { value: '#52c41a', label: '成功绿' },
  { value: '#faad14', label: '警告黄' },
  { value: '#f5222d', label: '错误红' },
  { value: '#eb2f96', label: '品红色' },
  { value: '#fa8c16', label: '活力橙' }
]

const currentTheme = ref(localStorage.getItem(THEME_KEY) || 'auto')
const currentAccentColor = ref(localStorage.getItem(ACCENT_COLOR_KEY) || '#1890ff')
const isDark = ref(false)

function getSystemDarkMode() {
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

function applyTheme(theme) {
  if (theme === 'auto') {
    isDark.value = getSystemDarkMode()
  } else {
    isDark.value = theme === 'dark'
  }

  document.documentElement.setAttribute('data-theme', isDark.value ? 'dark' : 'light')
}

function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)
  return result ? {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16)
  } : null
}

function rgbToHex(r, g, b) {
  return '#' + [r, g, b].map(x => {
    const hex = Math.round(x).toString(16)
    return hex.length === 1 ? '0' + hex : hex
  }).join('')
}

function lightenColor(hex, percent) {
  const rgb = hexToRgb(hex)
  if (!rgb) return hex
  return rgbToHex(
    Math.min(255, rgb.r + (255 - rgb.r) * percent),
    Math.min(255, rgb.g + (255 - rgb.g) * percent),
    Math.min(255, rgb.b + (255 - rgb.b) * percent)
  )
}

function darkenColor(hex, percent) {
  const rgb = hexToRgb(hex)
  if (!rgb) return hex
  return rgbToHex(
    rgb.r * (1 - percent),
    rgb.g * (1 - percent),
    rgb.b * (1 - percent)
  )
}

function applyAccentColor(color) {
  document.documentElement.style.setProperty('--accent-primary', color)
  document.documentElement.style.setProperty('--accent-light', color + '1a')
  document.documentElement.style.setProperty('--accent-lighter', color + '0d')

  const rgb = hexToRgb(color)
  if (rgb) {
    document.documentElement.style.setProperty('--accent-rgb', `${rgb.r}, ${rgb.g}, ${rgb.b}`)
  }

  const lightStart = lightenColor(color, 0.35)
  const lightEnd = darkenColor(color, 0.15)
  const darkStart = darkenColor(color, 0.25)
  const darkEnd = darkenColor(color, 0.5)

  document.documentElement.style.setProperty('--card-gradient-light-start', lightStart)
  document.documentElement.style.setProperty('--card-gradient-light-end', lightEnd)
  document.documentElement.style.setProperty('--card-gradient-dark-start', darkStart)
  document.documentElement.style.setProperty('--card-gradient-dark-end', darkEnd)

  if (isDark.value) {
    document.documentElement.style.setProperty(
      '--bg-card',
      `linear-gradient(135deg, ${darkStart}, ${darkEnd})`
    )
  } else {
    document.documentElement.style.setProperty(
      '--bg-card',
      `linear-gradient(135deg, ${lightStart}, ${lightEnd})`
    )
  }
}

let mediaQueryListener = null

export function useTheme() {
  function setTheme(theme) {
    currentTheme.value = theme
    localStorage.setItem(THEME_KEY, theme)
    applyTheme(theme)
    applyAccentColor(currentAccentColor.value)
  }

  function setAccentColor(color) {
    currentAccentColor.value = color
    localStorage.setItem(ACCENT_COLOR_KEY, color)
    applyAccentColor(color)
  }

  function toggleTheme() {
    const nextTheme = currentTheme.value === 'light' ? 'dark' : 
                      currentTheme.value === 'dark' ? 'auto' : 'light'
    setTheme(nextTheme)
  }

  onMounted(() => {
    applyTheme(currentTheme.value)
    applyAccentColor(currentAccentColor.value)

    mediaQueryListener = window.matchMedia('(prefers-color-scheme: dark)')
    mediaQueryListener.addEventListener('change', (e) => {
      if (currentTheme.value === 'auto') {
        isDark.value = e.matches
        document.documentElement.setAttribute('data-theme', e.matches ? 'dark' : 'light')
        applyAccentColor(currentAccentColor.value)
      }
    })
  })

  return {
    currentTheme,
    currentAccentColor,
    isDark,
    themes,
    accentColors,
    setTheme,
    setAccentColor,
    toggleTheme
  }
}
