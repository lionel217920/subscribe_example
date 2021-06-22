package recordgenerator;

import org.apache.kafka.common.TopicPartition;

/**
 * 偏移量提交回调
 */
public interface OffsetCommitCallBack {

    /**
     * 提交回调方法
     *
     * @param tp 消息分区
     * @param timestamp
     * @param offset
     * @param metadata
     */
    void commit(TopicPartition tp, long timestamp, long offset, String metadata);
}
