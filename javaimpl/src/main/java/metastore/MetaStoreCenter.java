package metastore;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import common.Checkpoint;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka消费位点处理工厂
 */
public class MetaStoreCenter {

    private static final Logger log = LoggerFactory.getLogger(MetaStoreCenter.class);

    /**
     * 不通处理方式的映射
     */
    private final Map<String, MetaStore<Checkpoint>> registeredStore = new HashMap<>();

    public MetaStoreCenter() {

    }

    /**
     * 注册
     *
     * @param name metaStore name
     * @param metaStore metaStore
     */
    public void registerStore(String name, MetaStore metaStore) {
        log.info("MetaStoreCenter: register metaStore {}", name);
        registeredStore.put(name, metaStore);
    }

    public void store(TopicPartition topicPartition, String group, Checkpoint value) {
        registeredStore.values().forEach(v -> {
            v.serializeTo(topicPartition, group, value);
        });
    }

    public Checkpoint seek(String storeName, TopicPartition tp, String group) {
        MetaStore<Checkpoint> metaStore = registeredStore.get(storeName);
        if (null != metaStore) {
            return metaStore.deserializeFrom(tp, group);
        } else {
            return null;
        }
    }
}
