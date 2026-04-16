<template>
  <div class="chat-fab" @click="handleClick">
    <span>💬</span>
  </div>
</template>

<script setup>
function handleClick() {
  alert('AI 聊天功能开发中...')
}
</script>

<style scoped>
.chat-fab {
  position: fixed;
  right: 30px;
  bottom: 110px;
  width: 56px;
  height: 56px;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 70%, black));
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  font-size: 28px;
  cursor: pointer;
  box-shadow: var(--shadow-fab);
  z-index: 999;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.chat-fab:hover {
  transform: scale(1.1) rotate(5deg);
  box-shadow: 0 8px 25px rgba(var(--accent-rgb), 0.5);
}

.chat-fab:active {
  transform: scale(0.95);
}
</style>
