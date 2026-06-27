package xyz.leeyangy.spc.config;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 良率大屏 RabbitMQ 配置
 * 监听 yield_dashboard fanout 交换机（与原 Python 服务一致）
 */
@Configuration
public class YieldRabbitConfig {

    public static final String EXCHANGE_NAME = "yield_dashboard";
    public static final String QUEUE_NAME = "spc-yield-dashboard-queue";

    @Bean
    public FanoutExchange yieldDashboardExchange() {
        return new FanoutExchange(EXCHANGE_NAME, true, false);
    }

    @Bean
    public Queue yieldDashboardQueue() {
        return new Queue(QUEUE_NAME, false, false, true);
    }

    @Bean
    public Binding yieldBinding(Queue yieldDashboardQueue, FanoutExchange yieldDashboardExchange) {
        return BindingBuilder.bind(yieldDashboardQueue).to(yieldDashboardExchange);
    }
}
