package com.tongji.counter.schema;

/**
 * Redis Key 生成工具。
 */
public final class CounterKeys {
    private CounterKeys() {}

    public static String sdsKey(String entityType, String entityId) {
        return String.format("cnt:%s:%s:%s", CounterSchema.SCHEMA_ID, entityType, entityId); // 固定结构计数（SDS）键
    }

    // 分片键：bm:{metric}:{etype}:{eid}:{chunk}
    public static String bitmapKey(String metric, String entityType, String entityId, long chunk) {
        return String.format("bm:%s:%s:%s:%d", metric, entityType, entityId, chunk); // 位图事实层（分片）
    }

    // 聚合增量持久化桶（Hash）：agg:{schema}:{etype}:{eid}
    public static String aggKey(String entityType, String entityId) {
        return String.format("agg:%s:%s:%s", CounterSchema.SCHEMA_ID, entityType, entityId); // 刷写前的增量存储桶
    }

    // Kafka 事件最终投递失败标记，读取时强制按 Bitmap 事实重建该指标
    public static String dirtyKey(String metric, String entityType, String entityId) {
        return String.format("dirty:cnt:%s:%s:%s:%s", CounterSchema.SCHEMA_ID, metric, entityType, entityId);
    }
}
