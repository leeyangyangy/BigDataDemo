<template>
  <div class="login-overlay" @click.self="$emit('close')">
    <div class="login-card">
      <button class="close-btn" @click="$emit('close')">&times;</button>
      <h2 class="login-title">飓芯科技工艺驾驶舱登录</h2>
      <p class="login-subtitle">请使用工号登录</p>

      <div class="form-group">
        <label>工号</label>
        <input
          v-model="empNo"
          type="text"
          class="form-input"
          placeholder="请输入工号"
          @keyup.enter="handleLogin"
        />
      </div>

      <div class="form-group">
        <label>密码</label>
        <input
          v-model="password"
          type="password"
          class="form-input"
          placeholder="请输入密码"
          @keyup.enter="handleLogin"
        />
      </div>

      <div class="error-msg" v-if="errorMsg">{{ errorMsg }}</div>

      <button class="login-btn" @click="handleLogin" :disabled="loading">
        {{ loading ? '登录中...' : '登 录' }}
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { authApi, setToken, setUser } from '../utils/api.js'

const emit = defineEmits(['close', 'success'])

const empNo = ref('')
const password = ref('')
const loading = ref(false)
const errorMsg = ref('')

async function handleLogin() {
  errorMsg.value = ''

  if (!empNo.value.trim()) {
    errorMsg.value = '请输入工号'
    return
  }
  if (!password.value.trim()) {
    errorMsg.value = '请输入密码'
    return
  }

  loading.value = true
  try {
    const res = await authApi.login(empNo.value.trim(), password.value)
    if (res.code === 200 && res.data) {
      setToken(res.data.token)
      setUser({
        userId: res.data.userId,
        empNo: res.data.empNo,
        username: res.data.username,
        role: res.data.role,
        email: res.data.email,
        phone: res.data.phone,
        workshopId: res.data.workshopId
      })
      emit('success', res.data)
    } else {
      errorMsg.value = res.msg || '登录失败'
    }
  } catch (e) {
    errorMsg.value = '网络错误，请稍后重试'
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.login-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0, 0, 0, 0.6);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 2000;
  backdrop-filter: blur(4px);
}

.login-card {
  background: var(--bg-secondary);
  border-radius: 20px;
  padding: 40px 36px;
  width: 380px;
  max-width: 90vw;
  box-shadow: var(--shadow-lg);
  position: relative;
}

.close-btn {
  position: absolute;
  top: 16px;
  right: 16px;
  background: none;
  border: none;
  font-size: 24px;
  color: var(--text-tertiary);
  cursor: pointer;
  line-height: 1;
  padding: 4px;
}

.close-btn:hover {
  color: var(--text-primary);
}

.login-title {
  text-align: center;
  color: var(--text-primary);
  font-size: 22px;
  font-weight: 700;
  margin-bottom: 4px;
}

.login-subtitle {
  text-align: center;
  color: var(--text-secondary);
  font-size: 13px;
  margin-bottom: 28px;
}

.form-group {
  margin-bottom: 18px;
}

.form-group label {
  display: block;
  font-size: 13px;
  font-weight: 500;
  color: var(--text-secondary);
  margin-bottom: 6px;
}

.form-input {
  width: 100%;
  padding: 10px 14px;
  border: 1px solid var(--border-input);
  border-radius: 10px;
  font-size: 14px;
  background: var(--bg-input);
  color: var(--text-primary);
  outline: none;
  transition: border-color 0.3s, box-shadow 0.3s;
}

.form-input:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(var(--accent-rgb), 0.12);
}

.error-msg {
  color: #f5222d;
  font-size: 13px;
  margin-bottom: 12px;
  text-align: center;
}

.login-btn {
  width: 100%;
  padding: 12px;
  border: none;
  border-radius: 10px;
  font-size: 15px;
  font-weight: 600;
  cursor: pointer;
  background: linear-gradient(135deg, var(--accent-primary), color-mix(in srgb, var(--accent-primary) 80%, black));
  color: white;
  transition: all 0.3s;
}

.login-btn:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 4px 16px rgba(var(--accent-rgb), 0.4);
}

.login-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
</style>
