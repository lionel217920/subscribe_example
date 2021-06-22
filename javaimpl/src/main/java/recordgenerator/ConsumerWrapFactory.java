package recordgenerator;

import java.util.Properties;

/**
 * 消费者包装器工厂，kafka里面的消费者
 */
public interface ConsumerWrapFactory {

    public ConsumerWrap getConsumerWrap(Properties properties);

    public static class KafkaConsumerWrapFactory implements ConsumerWrapFactory {
        @Override
        public ConsumerWrap getConsumerWrap(Properties properties) {
            return new ConsumerWrap.DefaultConsumerWrap(properties);
        }
    }
}
