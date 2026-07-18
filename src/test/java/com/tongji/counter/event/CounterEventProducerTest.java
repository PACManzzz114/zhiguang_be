package com.tongji.counter.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CounterEventProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafka;

    @Test
    void sendsEventsWithStableEntityKeyAndReturnsKafkaFuture() {
        @SuppressWarnings("unchecked")
        SendResult<String, String> sendResult = mock(SendResult.class);
        when(kafka.send(eq(CounterTopics.EVENTS), eq("knowpost:42"), anyString()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));
        CounterEventProducer producer = new CounterEventProducer(kafka, new ObjectMapper());

        SendResult<String, String> actual = producer.publish(
                CounterEvent.of("knowpost", "42", "like", 1, 7L, 1)
        ).join();

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(CounterTopics.EVENTS), eq("knowpost:42"), payload.capture());
        assertThat(actual).isSameAs(sendResult);
        assertThat(payload.getValue()).contains("\"metric\":\"like\"");
    }
}
