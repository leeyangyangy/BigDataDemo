import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [vue()],

  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },

  server: {
    port: 5174,
    host: '0.0.0.0',
    open: true,
    cors: true,

    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        configure: (proxy, options) => {
          proxy.on('error', (err, req, res) => {
            console.warn('[vite] http proxy error:', req.url, err.message)
          })
          proxy.on('proxyReq', (proxyReq, req, res) => {
            const clientIp = req.socket.remoteAddress || req.connection?.remoteAddress
            if (clientIp && clientIp !== '127.0.0.1' && clientIp !== '::1' && clientIp !== '::ffff:127.0.0.1') {
              proxyReq.setHeader('X-Forwarded-For', clientIp)
              proxyReq.setHeader('X-Real-IP', clientIp)
            }
            console.log('Proxying:', req.method, req.url, '->', options.target + req.url, '| client:', clientIp)
          })
        }
      },
      '/actuator': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        configure: (proxy, options) => {
          proxy.on('proxyReq', (proxyReq, req, res) => {
            const clientIp = req.socket.remoteAddress || req.connection?.remoteAddress
            if (clientIp && clientIp !== '127.0.0.1' && clientIp !== '::1') {
              proxyReq.setHeader('X-Forwarded-For', clientIp)
              proxyReq.setHeader('X-Real-IP', clientIp)
            }
          })
        }
      }
    }
  },

  build: {
    target: 'esnext',
    outDir: 'dist',
    assetsDir: 'assets',
    sourcemap: false,
    minify: 'terser',
    terserOptions: {
      compress: {
        drop_console: true,
        drop_debugger: true
      }
    },
    rollupOptions: {
      output: {
        chunkFileNames: 'assets/js/[name]-[hash].js',
        entryFileNames: 'assets/js/[name]-[hash].js',
        assetFileNames: '[ext]/[name]-[hash].[ext]',
        manualChunks(id) {
          if (!id.includes('node_modules')) return
          if (id.includes('echarts') || id.includes('zrender')) return 'echarts'
          if (id.includes('@vue') || /[/\\]vue[/\\]/.test(id)) return 'vue'
        }
      }
    }
  },

  css: {
    preprocessorOptions: {}
  },

  optimizeDeps: {
    include: ['echarts', 'vue']
  }
})
