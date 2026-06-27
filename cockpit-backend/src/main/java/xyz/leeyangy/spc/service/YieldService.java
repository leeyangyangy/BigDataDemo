package xyz.leeyangy.spc.service;

import xyz.leeyangy.spc.vo.YieldDataVO;

import java.util.List;
import java.util.Map;

/**
 * 良率监控服务
 */
public interface YieldService {

    /**
     * 处理来自 RabbitMQ 的 summaries 消息，更新内存中的良率数据
     */
    void updateYieldData(Map<String, Map<String, Object>> summaries);

    /**
     * 获取良率数据（可按日期范围、车间筛选）
     */
    YieldDataVO getYieldData(String workshop, String startDate, String endDate);

    /**
     * 搜索良率数据（关键字 + 日期范围 + 车间）
     */
    YieldDataVO searchYieldData(String workshop, String keyword, String startDate, String endDate);

    /**
     * 获取可用的车间列表
     */
    List<String> getWorkshops();
}
