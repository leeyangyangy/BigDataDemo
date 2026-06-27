package xyz.leeyangy.spc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import xyz.leeyangy.spc.service.YieldService;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 良率大屏 RabbitMQ 消费者
 * 接收测试站推送的 summaries 消息并更新良率数据
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class YieldRabbitConsumer {

    private final YieldService yieldService;
    private final ObjectMapper objectMapper;

    @RabbitListener(queues = YieldRabbitConfig.QUEUE_NAME)
    public void consume(Message message) {
        try {
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            log.info("[Yield] 收到 RabbitMQ 消息，长度: {} 字节", body.length());

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = objectMapper.readValue(body, Map.class);
            Object summariesObj = payload.get("summaries");
            if (!(summariesObj instanceof Map)) {
                log.warn("[Yield] 消息缺少 summaries 字段，跳过");
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> summaries = (Map<String, Map<String, Object>>) summariesObj;
            log.info("[Yield] summaries 数量: {}", summaries.size());
            yieldService.updateYieldData(summaries);
            log.info("[Yield] 数据更新成功");
        } catch (Exception e) {
            log.error("[Yield] 处理 RabbitMQ 消息失败: {}", e.getMessage(), e);
        }
    }
}
