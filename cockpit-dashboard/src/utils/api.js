const BASE_URL = '/api'

class ApiClient {
  constructor(baseURL) {
    this.baseURL = baseURL
  }

  async request(url, options = {}) {
    const config = {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers
      },
      ...options
    }

    try {
      const response = await fetch(`${this.baseURL}${url}`, config)

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status} ${response.statusText}`)
      }

      if (response.status === 204) {
        return null
      }

      return await response.json()
    } catch (error) {
      console.error('API Request Failed:', error)
      throw error
    }
  }

  get(url, params = {}) {
    const queryString = new URLSearchParams(params).toString()
    const fullUrl = queryString ? `${url}?${queryString}` : url
    return this.request(fullUrl, { method: 'GET' })
  }

  post(url, data = {}) {
    return this.request(url, {
      method: 'POST',
      body: JSON.stringify(data)
    })
  }

  put(url, data = {}) {
    return this.request(url, {
      method: 'PUT',
      body: JSON.stringify(data)
    })
  }

  delete(url, params = {}) {
    const queryString = new URLSearchParams(params).toString()
    const fullUrl = queryString ? `${url}?${queryString}` : url
    return this.request(fullUrl, { method: 'DELETE' })
  }
}

const api = new ApiClient(BASE_URL)

export default api

export const climateApi = {
  getStats: (params) => api.get('/climate/stats', params),
  getTemperatureData: (params) => api.get('/climate/temperature', params),
  getRainfallData: (params) => api.get('/climate/rainfall', params),
  getCities: () => api.get('/climate/cities'),
  getYears: () => api.get('/climate/years')
}
