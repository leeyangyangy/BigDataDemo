// 数据中心组件注册表
//
// 维护说明:
//   - 新增数据中心专用组件时, 在此注册一个 key + 异步加载函数
//   - 后端 sys_workshop_component 表只存 key, 实际组件在此解析
//   - 后台 DataCenterManagement.vue 调用 /api/admin/data-center/available-components
//     获取可用组件清单用于勾选, 须与此处 key 对齐
//   - SPC tab 保持独立, 不在此注册

import { markRaw } from 'vue'
import YieldDashboard from '../yield/YieldDashboard.vue'

// 同步加载现有组件 (轻量级, 无需异步分包)
const components = {
  yield_dashboard: {
    key: 'yield_dashboard',
    name: '良率监控',
    description: '综合良率趋势、产品码分布、对比、明细表、趋势图',
    component: markRaw(YieldDashboard)
  }
  // 后续新增数据中心专用组件在此追加, 例如:
  //   oee_dashboard: { key: 'oee_dashboard', name: '设备OEE', description: '...', component: markRaw(OeeDashboard) }
}

/**
 * 根据 component_key 获取组件定义
 * @param {string} key
 * @returns {{key:string,name:string,description:string,component:object}|null}
 */
export function getComponent(key) {
  return components[key] || null
}

/**
 * 列出所有可用组件 (供后台勾选用, 须与后端 available-components 接口对齐)
 */
export function listAvailableComponents() {
  return Object.values(components).map(c => ({
    key: c.key,
    name: c.name,
    description: c.description
  }))
}

export default components
