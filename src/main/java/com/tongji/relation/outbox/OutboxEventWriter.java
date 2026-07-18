package com.tongji.relation.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Outbox 事件统一写入器。
 *
 * <p>业务表和 Outbox 表必须在同一个事务中成功写入。任何序列化异常、数据库异常
 * 或非预期的影响行数都会向上抛出运行时异常，让外层 {@code @Transactional} 方法回滚。</p>
 */
@Component
public class OutboxEventWriter {
    private final OutboxMapper outboxMapper;
    private final ObjectMapper objectMapper;

    public OutboxEventWriter(OutboxMapper outboxMapper, ObjectMapper objectMapper) {
        this.outboxMapper = outboxMapper;
        this.objectMapper = objectMapper;
    }

    /**
     * 写入一条 Outbox 事件。
     *
     * @param eventId        Outbox 事件 ID
     * @param aggregateType  聚合类型
     * @param aggregateId    聚合 ID
     * @param type           事件类型
     * @param payload        待序列化的事件载荷
     */
    public void write(long eventId,
                      String aggregateType,
                      Long aggregateId,
                      String type,
                      Object payload) {
        final String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize Outbox event: " + type, e);
        }

        int inserted = outboxMapper.insert(eventId, aggregateType, aggregateId, type, payloadJson);
        if (inserted != 1) {
            throw new IllegalStateException(
                    "Expected to insert one Outbox event, but inserted " + inserted + ": " + type
            );
        }
    }
}
