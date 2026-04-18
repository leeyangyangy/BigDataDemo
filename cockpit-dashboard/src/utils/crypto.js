import CryptoJS from 'crypto-js'

const AES_KEY = 'SPC@2026#Secure!Key'
const AES_IV = 'SPC2026SecureIV16'

const ENCRYPTION_ENABLED = import.meta.env.MODE !== 'development'

export function isEncryptionEnabled() {
  return ENCRYPTION_ENABLED
}

export function encrypt(data) {
  if (!ENCRYPTION_ENABLED || !data) return data
  try {
    const text = typeof data === 'string' ? data : JSON.stringify(data)
    const encrypted = CryptoJS.AES.encrypt(text, CryptoJS.enc.Utf8.parse(AES_KEY), {
      iv: CryptoJS.enc.Utf8.parse(AES_IV),
      mode: CryptoJS.mode.CBC,
      padding: CryptoJS.pad.Pkcs7
    })
    return encrypted.toString()
  } catch (error) {
    console.error('[Crypto] 加密失败:', error)
    return data
  }
}

export function decrypt(encryptedData) {
  if (!ENCRYPTION_ENABLED || !encryptedData) return encryptedData
  if (typeof encryptedData !== 'string') return encryptedData
  try {
    const decrypted = CryptoJS.AES.decrypt(encryptedData, CryptoJS.enc.Utf8.parse(AES_KEY), {
      iv: CryptoJS.enc.Utf8.parse(AES_IV),
      mode: CryptoJS.mode.CBC,
      padding: CryptoJS.pad.Pkcs7
    })
    const decryptedStr = decrypted.toString(CryptoJS.enc.Utf8)
    if (!decryptedStr) {
      console.warn('[Crypto] 解密结果为空，返回原始数据')
      return encryptedData
    }
    try {
      return JSON.parse(decryptedStr)
    } catch {
      return decryptedStr
    }
  } catch (error) {
    console.error('[Crypto] 解密失败:', error)
    return encryptedData
  }
}

export function getEncryptedBody(data) {
  if (!ENCRYPTION_ENABLED || !data) return data
  return { encrypted: true, data: encrypt(data) }
}

export function decryptResponse(response) {
  if (!ENCRYPTION_ENABLED || !response) return response
  if (response.encrypted && response.data) {
    return decrypt(response.data)
  }
  return response
}
