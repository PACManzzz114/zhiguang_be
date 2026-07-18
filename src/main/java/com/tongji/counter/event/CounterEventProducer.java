package com.tongji.counter.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 计数事件生产者。
 *
 * <p>职责：将业务产生的计数增量事件异步发送到 Kafka 主题，供聚合消费者处理。</p>
 */
@Service
public class CounterEventProducer {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;

    public CounterEventProducer(KafkaTemplate<String, String> kafka, ObjectMapper objectMapper) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
    }

    /**
     * 发布计数事件到 Kafka。
     * @param event 计数事件（实体类型、ID、指标、delta 等）
     * @return Kafka 异步发送结果；序列化或同步发送失败时返回异常 Future
     */
    public CompletableFuture<SendResult<String, String>> publish(CounterEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            String messageKey = event.getEntityType() + ":" + event.getEntityId();
            // 同一实体使用稳定 key，在多分区下保证点赞/取消等事件的顺序
            return kafka.send(CounterTopics.EVENTS, messageKey, payload);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
