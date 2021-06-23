package metastore;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.Future;

/**
 * kafka消费者偏移量处理
 *
 * @param <V>
 */
public interface MetaStore<V> {

    /**
     * 序列化，也就是消费的位点提交到kafka中或者本地
     *
     * @param topicPartition 消息分区
     * @param group 消费组名称，DTS配置文件中的
     * @param value checkPoint
     * @return
     */
    Future<V> serializeTo(TopicPartition topicPartition, String group, V value);

    /**
     * 反序列化，也就是从本地或者从kafka中读取最新的消费位点
     *
     * @param topicPartition 消息分区
     * @param group 消费组名称，DTS配置文件中的
     * @return
     */
    V deserializeFrom(TopicPartition topicPartition, String group);
}
