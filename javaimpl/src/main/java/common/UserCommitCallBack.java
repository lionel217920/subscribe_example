package common;

import com.alibaba.dts.formats.avro.Record;
import org.apache.kafka.common.TopicPartition;

/**
 * 数据消费成功后回调
 */
public interface UserCommitCallBack {

    void commit(TopicPartition tp, Record record, long offset, String metadata);
}
