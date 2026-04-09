<template>
  <div class="app-container">
    <div class="header-bar">
      <h1 class="header">粤港澳大湾区各城市气候数据综合分析</h1>
      <ThemeSwitcher />
    </div>
    <Dashboard />
    <BottomNav />
    <ChatFab />
  </div>
</template>

<script setup>
import Dashboard from './components/Dashboard.vue'
import BottomNav from './components/BottomNav.vue'
import ChatFab from './components/ChatFab.vue'
import ThemeSwitcher from './components/ThemeSwitcher.vue'
import './styles/theme.css'
</script>

<style>
.app-container {
  min-height: 100vh;
  padding-bottom: 100px;
}

.header-bar {
  max-width: 1400px;
  margin: 0 auto;
  padding: 20px 20px 0;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header {
  color: var(--text-primary);
  font-size: 24px;
  font-weight: 600;
}
</style>
