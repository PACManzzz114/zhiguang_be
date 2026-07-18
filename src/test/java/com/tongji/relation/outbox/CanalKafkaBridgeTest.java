package com.tongji.relation.outbox;

import com.alibaba.otter.canal.client.CanalConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CanalKafkaBridgeTest {

    @Mock
    private KafkaTemplate<String, String> kafka;
    @Mock
    private TaskExecutor taskExecutor;
    @Mock
    private CanalConnector connector;

    private CanalKafkaBridge bridge;

    @BeforeEach
    void setUp() {
        bridge = new CanalKafkaBridge(
                kafka,
                new ObjectMapper(),
                taskExecutor,
                false,
                "localhost",
                11111,
                "example",
                "",
                "",
                "zhiguang\\.outbox",
                100,
                1000L
        );
    }

    @Test
    void buildsStableKeysForRelationAndKnowPostEvents() {
        String relationKey = bridge.buildMessageKey(
                "following",
                null,
                "100",
                "{\"fromUserId\":1,\"toUserId\":2}"
        );
        String knowPostKey = bridge.buildMessageKey(
                "knowpost",
                "200",
                "101",
                "{\"id\":200}"
        );

        assertThat(relationKey).isEqualTo("following:1:2");
        assertThat(knowPostKey).isEqualTo("knowpost:200");
    }

    @Test
    void acknowledgesCanalBatchOnlyAfterKafkaDeliverySucceeds() {
        @SuppressWarnings("unchecked")
        SendResult<String, String> sendResult = mock(SendResult.class);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);

        bridge.awaitKafkaDeliveryAndAck(connector, 7L, List.of(future));

        verify(connector).ack(7L);
    }

    @Test
    void doesNotAcknowledgeCanalBatchWhenKafkaDeliveryFails() {
        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException("kafka unavailable"));

        assertThatThrownBy(() -> bridge.awaitKafkaDeliveryAndAck(connector, 8L, List.of(future)))
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("kafka unavailable");

        verify(connector, never()).ack(anyLong());
    }
}
