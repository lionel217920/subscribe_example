package recordgenerator;

import common.Checkpoint;
import common.Context;
import metastore.LocalFileMetaStore;
import metastore.MetaStoreCenter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordprocessor.EtlRecordProcessor;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static common.Util.sleepMS;
import static common.Util.swallowErrorClose;
import static recordgenerator.Names.GROUP_NAME;
import static recordgenerator.Names.KAFKA_TOPIC;
import static recordgenerator.Names.SUBSCRIBE_MODE_NAME;
import static recordgenerator.Names.TRY_BACK_TIME_MS;
import static recordgenerator.Names.TRY_TIME;
import static recordgenerator.Names.USE_CONFIG_CHECKPOINT_NAME;

/**
 * 记录生产者，也就是kafka里的consumer
 */
public class RecordGenerator implements Runnable, Closeable {

    private static final Logger log = LoggerFactory.getLogger(RecordGenerator.class);

    /**
     * 本地消息消费位点，键值
     */
    private static final String LOCAL_FILE_STORE_NAME = "localCheckpointStore";

    /**
     * kafka消费位点，键值
     */
    private static final String KAFKA_STORE_NAME = "kafkaCheckpointStore";

    /**
     * DTS配置
     */
    private final Properties properties;

    /**
     * 获取kafka失败，重置次数
     */
    private final int tryTime;

    /**
     * 上下文
     */
    private final Context context;

    /**
     * kafka消息分区
     */
    private final TopicPartition topicPartition;

    /**
     * DTS配置
     */
    private final String groupID;

    /**
     * kafka消费者包装器工厂
     */
    private final ConsumerWrapFactory consumerWrapFactory;

    /**
     * 初始化消费位点
     */
    private final Checkpoint initialCheckpoint;

    /**
     * 提交的消费位点
     */
    private volatile Checkpoint toCommitCheckpoint = null;

    /**
     * 消息偏移量处理
     */
    private final MetaStoreCenter metaStoreCenter = new MetaStoreCenter();

    /**
     * 是否使用指定的消费点位
     */
    private final AtomicBoolean useCheckpointConfig;

    /**
     * kafka消费组分区模式
     */
    private final ConsumerSubscribeMode subscribeMode;

    private final long tryBackTimeMS;
    private volatile boolean existed;

    /**
     * 构造方法
     *
     * @param properties 配置文件
     * @param context 上下文
     * @param initialCheckpoint 初始消费点位
     * @param consumerWrapFactory 消费者包装器，kafka里面的消费者
     */
    public RecordGenerator(Properties properties, Context context, Checkpoint initialCheckpoint, ConsumerWrapFactory consumerWrapFactory) {
        this.properties = properties;
        this.tryTime = Integer.valueOf(properties.getProperty(TRY_TIME, "150"));
        this.tryBackTimeMS = Long.valueOf(properties.getProperty(TRY_BACK_TIME_MS, "10000"));
        this.context = context;
        this.consumerWrapFactory = consumerWrapFactory;
        this.initialCheckpoint = initialCheckpoint;
        this.topicPartition = new TopicPartition(properties.getProperty(KAFKA_TOPIC), 0);
        this.groupID = properties.getProperty(GROUP_NAME);
        this.subscribeMode = parseConsumerSubscribeMode(properties.getProperty(SUBSCRIBE_MODE_NAME, "assign"));
        this.useCheckpointConfig = new AtomicBoolean(StringUtils.equalsIgnoreCase(properties.getProperty(USE_CONFIG_CHECKPOINT_NAME), "true"));
        existed = false;
        metaStoreCenter.registerStore(LOCAL_FILE_STORE_NAME, new LocalFileMetaStore(LOCAL_FILE_STORE_NAME));
        log.info("RecordGenerator: try time [" + tryTime + "], try backTimeMS [" + tryBackTimeMS + "]");
    }


    private ConsumerWrap getConsumerWrap() {
        return consumerWrapFactory.getConsumerWrap(properties);
    }


    public void run() {

        int haveTryTime = 0;
        String message = "first start";
        ConsumerWrap kafkaConsumerWrap = null;
        while (!existed) {
            // 从上下文中获取数据消费者，也就是数据处理者processor
            EtlRecordProcessor recordProcessor = context.getRecordProcessor();
            try {
                kafkaConsumerWrap = getConsumerWrap(message);
                while (!existed) {
                    // kafka consumer is not threadsafe, so if you want commit checkpoint to kafka, commit it in same thread
                    mayCommitCheckpoint();
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumerWrap.poll();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        log.info("【Record Generator】receive kafka record is {}", record);
                        int offerTryCount = 0;
                        if (record.value() == null || record.value().length <= 48) {
                            // dStore may generate special mock record to push up consumer offset for next fetchRequest if all data is filtered
                            continue;
                        } else {
                            log.info("RecordGenerator: receive record, offset [" + record.offset() + "], value size [" + (record.value() == null ? 0 : record.value().length) + "]" );
                        }
                        // 数据消费者入队
                        while (!recordProcessor.offer(1000, TimeUnit.MILLISECONDS, record) && !existed) {
                            if (++offerTryCount % 10 == 0) {
                                log.info("RecordGenerator: offer record has failed for a period (10s) [ " + record + "]");
                            }
                        }
                    }

                }
            } catch (Throwable e) {
                if (isErrorRecoverable(e) && haveTryTime++ < tryTime) {
                    log.warn("RecordGenerator: error meet cause " + e.getMessage() + ", recover time [" + haveTryTime + "]", e);
                    sleepMS(tryBackTimeMS);
                    message = "reconnect";
                } else {
                    log.error("RecordGenerator: unrecoverable error  " + e.getMessage() + ", have try time [" + haveTryTime + "]", e);
                    this.existed = true;
                }
            } finally {
                swallowErrorClose(kafkaConsumerWrap);
            }
        }

    }

    private void mayCommitCheckpoint() {
        if (null != toCommitCheckpoint) {
            commitCheckpoint(toCommitCheckpoint.getTopicPartition(), toCommitCheckpoint);
            toCommitCheckpoint = null;
        }
    }

    public void setToCommitCheckpoint(Checkpoint committedCheckpoint) {
        this.toCommitCheckpoint = committedCheckpoint;
    }

    private ConsumerWrap getConsumerWrap(String message) {
        ConsumerWrap kafkaConsumerWrap = getConsumerWrap();
        Checkpoint checkpoint = null;
        // we encourage  user impl their own checkpoint store, but plan b is also  supported
//        metaStoreCenter.registerStore(KAFKA_STORE_NAME, new KafkaMetaStore(kafkaConsumerWrap.getRawConsumer()));
        if (useCheckpointConfig.compareAndSet(true, false)) {
            log.info("RecordGenerator: force use initial checkpoint [{}] to start", checkpoint);
            checkpoint = initialCheckpoint;
        } else {
            checkpoint = getCheckpoint();
            if (null == checkpoint || Checkpoint.INVALID_STREAM_CHECKPOINT == checkpoint) {
                checkpoint = initialCheckpoint;
                log.info("RecordGenerator: use initial checkpoint [{}] to start", checkpoint);
            } else {
                log.info("RecordGenerator: load checkpoint from checkpoint store success, current checkpoint [{}]", checkpoint);
            }
        }
        switch (subscribeMode) {
            case SUBSCRIBE: {
                kafkaConsumerWrap.subscribeTopic(topicPartition, () -> {
                    Checkpoint ret = metaStoreCenter.seek(KAFKA_STORE_NAME, topicPartition, groupID);
                    if (null == ret) {
                        ret = initialCheckpoint;
                    }
                    return ret;
                });
                break;
            }
            case ASSIGN:{
                kafkaConsumerWrap.assignTopic(topicPartition, checkpoint);
                break;
            }
            default: {
                throw new RuntimeException("RecordGenerator: unknown mode not support");
            }
        }

        log.info("RecordGenerator:" + message + ", checkpoint " + checkpoint);
        return kafkaConsumerWrap;
    }

    private Checkpoint getCheckpoint() {
        // use local checkpoint priority
        Checkpoint checkpoint = metaStoreCenter.seek(LOCAL_FILE_STORE_NAME, topicPartition, groupID);
        if (null == checkpoint) {
            checkpoint = metaStoreCenter.seek(KAFKA_STORE_NAME, topicPartition, groupID);
        }
        return checkpoint;

    }

    public void commitCheckpoint(TopicPartition topicPartition, Checkpoint checkpoint) {
        if (null != topicPartition && null != checkpoint) {
            metaStoreCenter.store(topicPartition, groupID, checkpoint);
        }
    }

    private boolean isErrorRecoverable(Throwable e) {
        return true;
    }

    public Checkpoint getInitialCheckpoint() {
        return initialCheckpoint;
    }

    public void close() {
        existed = true;
    }

    /**
     * kafka消费者分区
     */
    private static enum ConsumerSubscribeMode {

        /**
         * 手动
         */
        ASSIGN,

        /**
         * 自动
         */
        SUBSCRIBE,

        UNKNOWN;
    }

    /**
     * 构建消费订阅模式
     *
     * @param value 配置文件中的值
     * @return
     */
    private ConsumerSubscribeMode parseConsumerSubscribeMode(String value) {
        if (StringUtils.equalsIgnoreCase("assign", value)) {
            return ConsumerSubscribeMode.ASSIGN;
        } else if (StringUtils.equalsIgnoreCase("subscribe", value)) {
            return ConsumerSubscribeMode.SUBSCRIBE;
        } else {
            throw new RuntimeException("RecordGenerator: unknown subscribe mode [" + value + "]");
        }
    }
}

