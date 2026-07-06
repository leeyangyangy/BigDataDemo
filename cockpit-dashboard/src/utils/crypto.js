import CryptoJS from 'crypto-js'
import JSEncrypt from 'jsencrypt'
import { StatusCode } from './statusCode.js'
import { StatusMsg } from './statusMsg.js'

// 会话级 AES key/iv, 登录时由前端随机生成, RSA 加密上送给后端。
// sessionStorage 确保标签页关闭即清除, 不在 localStorage 长期保存。
const KV_KEY = 'cockpit_enc_kv'

// 开发环境不加密, 与后端 AESUtil.isEncryptionEnabled() 保持一致。
const ENCRYPTION_ENABLED = import.meta.env.MODE !== 'development'

// 缓存的 RSA 公钥, 避免每次请求都拉取
let cachedPublicKey = null
let cachedFingerprint = null

export function isEncryptionEnabled() {
  return ENCRYPTION_ENABLED
}

/**
 * 拉取后端 RSA 公钥。后端启动时随机生成, 重启后指纹变化即视为新密钥。
 * 此函数幂等, 多次调用只会发一次 HTTP 请求。
 *
 * @param {Function} fetchPublicKey  fetch 公钥的 API 函数, 由调用方注入 (避免循环依赖)
 */
export async function ensurePublicKey(fetchPublicKey) {
  if (!ENCRYPTION_ENABLED) return null
  if (cachedPublicKey) return cachedPublicKey

  const res = await fetchPublicKey()
  if (!res || res.code !== StatusCode.SUCCESS || !res.data || !res.data.publicKey) {
    throw new Error('拉取 RSA 公钥失败')
  }
  cachedPublicKey = res.data.publicKey
  cachedFingerprint = res.data.fingerprint
  console.log(`[Crypto] RSA 公钥已就绪, 指纹=${cachedFingerprint}, encryptionEnabled=${res.data.encryptionEnabled}`)
  if (res.data.encryptionEnabled === false) {
    // 后端 dev profile 不启用, 前端也不加密, 防止 dev/prod 联调时格式错位
    console.warn('[Crypto] 后端未启用加密, 前端将走明文通道')
  }
  return cachedPublicKey
}

/**
 * 生成 16 字节随机 AES key + 16 字节 IV, 用 RSA 公钥加密后上送给后端。
 * 同时把明文 KV 缓存到 sessionStorage 供后续 AES 加解密使用。
 *
 * @param {Function} exchangeFn  后端 key-exchange 接口, 形如 (encKey, encIv) => Promise<R>
 */
export async function generateAndExchangeKey(exchangeFn) {
  if (!ENCRYPTION_ENABLED) return

  if (!cachedPublicKey) {
    throw new Error('RSA 公钥尚未拉取, 请先调用 ensurePublicKey()')
  }

  // 随机生成 16 字节 key + 16 字节 IV
  const key = generateRandomBytes(16)
  const iv = generateRandomBytes(16)

  // RSA 加密 (PKCS1Padding) 后输出 Base64
  const encKey = rsaEncrypt(cachedPublicKey, key)
  const encIv = rsaEncrypt(cachedPublicKey, iv)
  if (!encKey || !encIv) {
    throw new Error('RSA 加密 KV 失败')
  }

  // 先把明文 KV 存到 sessionStorage, 这样后端 key-exchange 响应被加密时,
  // 前端 api.js 的 decryptResponse 才能正确解密。
  const kv = {
    key: CryptoJS.enc.Base64.stringify(CryptoJS.lib.WordArray.create(key)),
    iv: CryptoJS.enc.Base64.stringify(CryptoJS.lib.WordArray.create(iv))
  }
  sessionStorage.setItem(KV_KEY, JSON.stringify(kv))

  try {
    // 上送后端
    const res = await exchangeFn(encKey, encIv)
    if (!res || res.code !== StatusCode.SUCCESS) {
      // 失败时回滚 KV, 避免后续请求用错误的 KV 加密
      sessionStorage.removeItem(KV_KEY)
      throw new Error(StatusMsg.KEY_EXCHANGE_FAILED + ': ' + (res?.msg || '未知错误'))
    }
    console.log('[Crypto] AES KV 已生成并上送, 后续请求将走加密通道')
  } catch (e) {
    sessionStorage.removeItem(KV_KEY)
    throw e
  }
}

/**
 * 清除会话级 KV (登出 / 切换用户时调用)。
 */
export function clearKey() {
  sessionStorage.removeItem(KV_KEY)
  // 不清除 cachedPublicKey, 因为切换账号时公钥通常不变,
  // 而且公钥本身是公开的, 缓存它不影响安全。
}

function loadKv() {
  const raw = sessionStorage.getItem(KV_KEY)
  if (!raw) return null
  try {
    return JSON.parse(raw)
  } catch {
    return null
  }
}

function generateRandomBytes(n) {
  const arr = new Uint8Array(n)
  crypto.getRandomValues(arr)
  return arr
}

function rsaEncrypt(publicKeyBase64, bytes) {
  const encryptor = new JSEncrypt()
  // JSEncrypt.setPublicKey 接收 PEM 格式, 这里把 Base64 包成 SPKI PEM
  const pem = `-----BEGIN PUBLIC KEY-----\n${publicKeyBase64}\n-----END PUBLIC KEY-----`
  encryptor.setPublicKey(pem)
  // JSEncrypt 只能加密字符串, 不能直接处理字节数组;
  // 先转 Base64 字符串, 后端用 RSA 解密后再 Base64 解码出原始字节
  const b64 = CryptoJS.enc.Base64.stringify(CryptoJS.lib.WordArray.create(bytes))
  const encrypted = encryptor.encrypt(b64)
  // encrypted 已经是 Base64
  return encrypted
}

export function encrypt(data) {
  if (!ENCRYPTION_ENABLED || !data) return data
  const kv = loadKv()
  if (!kv) {
    // KV 未就绪 (未登录或 key-exchange 未完成), 跳过加密让请求走明文
    return data
  }
  try {
    const text = typeof data === 'string' ? data : JSON.stringify(data)
    const encrypted = CryptoJS.AES.encrypt(
      text,
      CryptoJS.enc.Base64.parse(kv.key),
      {
        iv: CryptoJS.enc.Base64.parse(kv.iv),
        mode: CryptoJS.mode.CBC,
        padding: CryptoJS.pad.Pkcs7
      }
    )
    return encrypted.toString()
  } catch (error) {
    console.error('[Crypto] 加密失败:', error)
    return data
  }
}

export function decrypt(encryptedData) {
  if (!ENCRYPTION_ENABLED || !encryptedData) return encryptedData
  if (typeof encryptedData !== 'string') return encryptedData
  const kv = loadKv()
  if (!kv) {
    console.warn('[Crypto] decrypt 跳过: sessionStorage 无 KV (cockpit_enc_kv). 可能未登录或 key-exchange 未完成')
    return encryptedData
  }
  try {
    const decrypted = CryptoJS.AES.decrypt(
      encryptedData,
      CryptoJS.enc.Base64.parse(kv.key),
      {
        iv: CryptoJS.enc.Base64.parse(kv.iv),
        mode: CryptoJS.mode.CBC,
        padding: CryptoJS.pad.Pkcs7
      }
    )
    const decryptedStr = decrypted.toString(CryptoJS.enc.Utf8)
    if (!decryptedStr) {
      console.warn('[Crypto] 解密结果为空 (KV 可能不匹配), key=', kv.key?.substring(0, 8) + '...', 'iv=', kv.iv?.substring(0, 8) + '...')
      return encryptedData
    }
    try {
      return JSON.parse(decryptedStr)
    } catch {
      return decryptedStr
    }
  } catch (error) {
    console.error('[Crypto] 解密失败:', error.message, 'key=', kv.key?.substring(0, 8) + '...', 'iv=', kv.iv?.substring(0, 8) + '...')
    return encryptedData
  }
}

export function getEncryptedBody(data) {
  if (!ENCRYPTION_ENABLED || !data) return data
  if (!loadKv()) return data
  return { encrypted: true, data: encrypt(data) }
}

export function decryptResponse(response) {
  if (!ENCRYPTION_ENABLED || !response) return response
  if (response.encrypted && response.data) {
    return decrypt(response.data)
  }
  return response
}
