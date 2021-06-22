package common;

import com.alibaba.dts.formats.avro.Record;
import org.apache.kafka.common.TopicPartition;

/**
 * 数据变更对象，最终我们使用这个对象解析数据
 */
public class UserRecord {
    private final TopicPartition topicPartition;
    private final long offset;
    private final Record record;
    private final UserCommitCallBack userCommitCallBack;

    /**
     * 构造方法创建变更对象
     *
     * @param tp 消息分区
     * @param offset 偏移量
     * @param record avro反序列化对象
     * @param userCommitCallBack 回调接口
     */
    public UserRecord(TopicPartition tp, long offset, Record record, UserCommitCallBack userCommitCallBack) {
        this.topicPartition = tp;
        this.offset = offset;
        this.record = record;
        this.userCommitCallBack = userCommitCallBack;
    }

    public long getOffset() {
        return offset;
    }

    public Record getRecord() {
        return record;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    /**
     * 执行回调方法
     *
     * @param metadata
     */
    public void commit(String metadata) {
        // 传参给回调方法
        userCommitCallBack.commit(topicPartition, record, offset, metadata);
    }
}
