package com.tongji.counter.service.impl;

import com.tongji.counter.event.CounterEvent;
import com.tongji.counter.event.CounterEventProducer;
import com.tongji.counter.schema.CounterKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RedissonClient;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CounterServiceImplTest {

    @Mock
    private StringRedisTemplate redis;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private CounterEventProducer eventProducer;
    @Mock
    private ApplicationEventPublisher eventPublisher;
    @Mock
    private RedissonClient redisson;

    private CounterServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new CounterServiceImpl(redis, eventProducer, eventPublisher, redisson);
    }

    @Test
    void marksMetricDirtyAndDeletesSnapshotWhenKafkaDeliveryFails() {
        CounterEvent event = CounterEvent.of("knowpost", "42", "like", 1, 7L, 1);
        when(eventProducer.publish(event))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("kafka unavailable")));
        when(redis.opsForValue()).thenReturn(valueOperations);

        service.publishCounterEvent(event);

        verify(valueOperations).set(
                CounterKeys.dirtyKey("like", "knowpost", "42"),
                "1",
                Duration.ofDays(1)
        );
        verify(redis).delete(CounterKeys.sdsKey("knowpost", "42"));
    }
}
