package com.tongji.relation.outbox;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.core.task.TaskExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

/**
 * Canal→Kafka 桥接器。
 * 职责：订阅 outbox 表的行级变更（ROWDATA），仅转发 INSERT/UPDATE 的 payload 字段到 Kafka 主题；批次确认位点确保至少一次语义。
 * 可靠性：解析失败或非关心类型不提交位点；停止时断开 Canal 连接并清理资源。
 */
@Service
public class CanalKafkaBridge implements SmartLifecycle {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final boolean enabled;
    private final String host;
    private final int port;
    private final String destination;
    private final String username;
    private final String password;
    private final String filter;
    private final int batchSize;
    private final long intervalMs;
    private volatile boolean running;
    private final TaskExecutor taskExecutor;
    private CanalConnector connector;
    private static final Logger log = LoggerFactory.getLogger(CanalKafkaBridge.class);

    /**
     * Canal 到 Kafka 的桥接器。
     * @param kafka Kafka 模板
     * @param objectMapper JSON 序列化器
     * @param enabled 是否启用
     * @param host Canal 主机
     * @param port Canal 端口
     * @param destination 实例名
     * @param username 用户名
     * @param password 密码
     * @param filter 订阅过滤表达式
     * @param batchSize 拉取批次大小
     * @param intervalMs 空轮询间隔毫秒
     */
    public CanalKafkaBridge(KafkaTemplate<String, String> kafka,
                            ObjectMapper objectMapper,
                            @Qualifier("taskExecutor") TaskExecutor taskExecutor,
                            @Value("${canal.enabled}") boolean enabled,
                            @Value("${canal.host}") String host,
                            @Value("${canal.port}") int port,
                            @Value("${canal.destination}") String destination,
                            @Value("${canal.username}") String username,
                            @Value("${canal.password}") String password,
                            @Value("${canal.filter}") String filter,
                            @Value("${canal.batchSize}") int batchSize,
                            @Value("${canal.intervalMs}") long intervalMs) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.taskExecutor = taskExecutor;
        this.enabled = enabled;
        this.host = host;
        this.port = port;
        this.destination = destination;
        this.username = username;
        this.password = password;
        this.filter = filter;
        this.batchSize = batchSize;
        this.intervalMs = intervalMs;
    }

    /**
     * 启动桥接器：消费 Canal 并投递到 Kafka。
     */
    @Override
    public void start() {
        if (running) {
            log.info("Canal bridge start skipped: running={} enabled={} host={} port={} dest={} filter={}", running, enabled, host, port, destination, filter);
            return;
        }
        // 标记运行并使用全局线程池异步执行主循环
        running = true;
        taskExecutor.execute(() -> {
            try {
                // 创建 Canal 单实例连接器并建立连接
                connector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), destination, username, password);
                log.info("Canal connecting to {}:{} dest={} user={} filter={}", host, port, destination, username, filter);
                connector.connect();
                // 订阅过滤表达式，仅拉取关心的表（如 outbox）
                connector.subscribe(filter);
                // 回滚到上次确认位点，保证一致性处理
                connector.rollback();
                log.info("Canal connected and subscribed: host={} port={} dest={} filter={} batchSize={} intervalMs={}ms", host, port, destination, filter, batchSize, intervalMs);
                while (running) {
                    // 拉取一批未确认消息（不自动 ack）
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    // 空批次或心跳时，按间隔休眠并继续轮询
                    if (batchId == -1 || message.getEntries() == null || message.getEntries().isEmpty()) {
                        try {
                            Thread.sleep(intervalMs);
                        } catch (InterruptedException ignored) {}
                        continue;
                    }
                    try {
                        List<CompletableFuture<SendResult<String, String>>> sendFutures = new ArrayList<>();

                        for (CanalEntry.Entry entry : message.getEntries()) {
                            // 仅处理行级数据变更事件
                            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                                continue;
                            }

                            CanalEntry.RowChange rowChange;
                            try {
                                // 解析失败时保留当前 Canal 位点，避免静默跳过 Outbox 事件
                                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            } catch (Exception e) {
                                throw new IllegalStateException("Failed to parse Canal row change", e);
                            }

                            CanalEntry.EventType eventType = rowChange.getEventType();
                            // Outbox 只依赖 INSERT/UPDATE；其他事件不需要转发
                            if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE) {
                                continue;
                            }

                            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                                ObjectNode rowNode = objectMapper.createObjectNode();
                                String outboxId = null;
                                String aggregateType = null;
                                String aggregateId = null;
                                String payload = null;

                                for (CanalEntry.Column col : rowData.getAfterColumnsList()) {
                                    String name = col.getName().toLowerCase(Locale.ROOT);
                                    String value = col.getIsNull() ? null : col.getValue();

                                    switch (name) {
                                        case "id" -> outboxId = value;
                                        case "aggregate_type" -> aggregateType = value;
                                        case "aggregate_id" -> aggregateId = value;
                                        case "type" -> rowNode.put("eventType", value);
                                        case "payload" -> payload = value;
                                        default -> {
                                        }
                                    }
                                }

                                if (payload == null || payload.isBlank()) {
                                    throw new IllegalStateException("Outbox row is missing payload: " + outboxId);
                                }

                                rowNode.put("id", outboxId);
                                rowNode.put("aggregateType", aggregateType);
                                if (aggregateId == null) {
                                    rowNode.putNull("aggregateId");
                                } else {
                                    rowNode.put("aggregateId", aggregateId);
                                }
                                rowNode.put("payload", payload);

                                ArrayNode dataArray = objectMapper.createArrayNode();
                                dataArray.add(rowNode);

                                ObjectNode msgNode = objectMapper.createObjectNode();
                                msgNode.put("table", entry.getHeader().getTableName());
                                msgNode.put("type", eventType == CanalEntry.EventType.INSERT ? "INSERT" : "UPDATE");
                                msgNode.set("data", dataArray);

                                String messageKey = buildMessageKey(aggregateType, aggregateId, outboxId, payload);
                                String json = objectMapper.writeValueAsString(msgNode);
                                sendFutures.add(kafka.send(OutboxTopics.CANAL_OUTBOX, messageKey, json));
                            }
                        }

                        // 只有当前批次的 Kafka 消息全部发送成功，才推进 Canal 位点
                        awaitKafkaDeliveryAndAck(connector, batchId, sendFutures);
                    } catch (Exception batchError) {
                        try {
                            connector.rollback(batchId);
                        } catch (Exception rollbackError) {
                            log.error("Canal batch rollback failed: batchId={}", batchId, rollbackError);
                        }
                        log.warn("Canal batch delivery failed and will be retried: batchId={}", batchId, batchError);
                        try {
                            Thread.sleep(intervalMs);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Canal bridge error", e);
            } finally {
                if (connector != null) {
                    // 断开 Canal 连接（资源清理）
                    try {
                        connector.disconnect();
                        log.info("Canal disconnected: dest={}", destination);
                    } catch (Exception ex) {
                        log.warn("Canal disconnect failed: dest={} err={}", destination, ex.getMessage());
                    }
                }
            }
        });
    }

    /**
     * 为同一业务实体生成稳定的 Kafka key，保证多分区下的事件顺序。
     * 关注事件按双方用户维度分区，其他事件优先按聚合类型和聚合 ID 分区。
     */
    String buildMessageKey(String aggregateType, String aggregateId, String outboxId, String payload) {
        if ("following".equalsIgnoreCase(aggregateType)) {
            try {
                JsonNode relationEvent = objectMapper.readTree(payload);
                JsonNode fromUserId = relationEvent.get("fromUserId");
                JsonNode toUserId = relationEvent.get("toUserId");
                if (fromUserId != null && toUserId != null) {
                    return "following:" + fromUserId.asText() + ":" + toUserId.asText();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to build Kafka key from relation event", e);
            }
        }

        if (aggregateType != null && !aggregateType.isBlank()
                && aggregateId != null && !aggregateId.isBlank()) {
            return aggregateType + ":" + aggregateId;
        }

        if (outboxId != null && !outboxId.isBlank()) {
            return "outbox:" + outboxId;
        }

        throw new IllegalStateException("Cannot build Kafka key for Outbox event");
    }

    /**
     * 等待当前 Canal 批次对应的 Kafka Future 全部完成，再确认 Canal 位点。
     * 任一 Future 失败都会在 ack 前抛出异常，由调用方回滚当前批次。
     */
    void awaitKafkaDeliveryAndAck(CanalConnector currentConnector,
                                  long batchId,
                                  List<CompletableFuture<SendResult<String, String>>> sendFutures) {
        CompletableFuture<?>[] futures = sendFutures.toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
        currentConnector.ack(batchId);
    }

    /**
     * 停止桥接器。
     */
    @Override
    public void stop() {
        running = false;
    }

    /**
     * 是否处于运行状态。
     * @return 运行状态
     */
    @Override
    public boolean isRunning() {
        return running;
    }
}
