package boot;

import common.RecordListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordgenerator.OffsetCommitCallBack;
import recordprocessor.EtlRecordProcessor;
import recordgenerator.RecordGenerator;
import recordgenerator.ConsumerWrapFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;
import common.Checkpoint;
import common.Context;
import common.WorkThread;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static recordgenerator.Names.*;
import static common.Util.*;

public class Boot {
    private static final Logger log = LoggerFactory.getLogger(Boot.class);

    /**
     * 程序是否停止
     */
    private static final AtomicBoolean existed = new AtomicBoolean(false);

    /**
     * 启动方法
     *
     * @param configFile 配置文件路径
     * @param recordListeners 数据监听
     */
    public static void boot(String configFile, Map<String, RecordListener> recordListeners) {
        boot(loadConfig(configFile), recordListeners);
    }

    /**
     * 启动方法
     *
     * @param properties 配置文件
     * @param recordListeners 数据监听
     */
    public static void boot(Properties properties, Map<String, RecordListener> recordListeners) {
        // first init log4j ，初始化log4j
        initLog4j();
        require(null != recordListeners && !recordListeners.isEmpty(), "record listener required");
        // 获取上下文
        Context context = getStreamContext(properties);
        // check config，校验配置文件
        checkConfig(properties);

        // 获取记录生产者【线程】
        RecordGenerator recordGenerator = getRecordGenerator(context, properties);
        // 获取记录消费者【线程】
        EtlRecordProcessor etlRecordProcessor = getEtlRecordProcessor(context, properties, recordGenerator);
        // 消费者注册消费监听
        recordListeners.forEach((k, v) -> {
            log.info("Boot: register record listener " + k);
            etlRecordProcessor.registerRecordListener(k, v);
        });
        // 注册信号量
        registerSignalHandler(context);

        // 开启工作线程
        List<WorkThread> startStream = startWorker(etlRecordProcessor, recordGenerator);

        while (!existed.get() ) {
            sleepMS(1000);
        }
        log.info("StreamBoot: shutting down...");
        // 线程停止，程序结束
        for (WorkThread workThread : startStream) {
            workThread.stop();
        }

    }

    /**
     * 开启工作线程，数据生产者和数据消费者线程执行
     *
     * @param etlRecordProcessor 数据生产者
     * @param recordGenerator 数据消费者
     * @return
     */
    private static List<WorkThread> startWorker(EtlRecordProcessor etlRecordProcessor, RecordGenerator recordGenerator) {
        List<WorkThread> ret = new LinkedList<>();
        ret.add(new WorkThread(etlRecordProcessor, "Record Processor"));
        ret.add(new WorkThread(recordGenerator, "Record Generator"));
        for (WorkThread workThread : ret) {
            workThread.start();
        }
        return ret;
    }

    /**
     * 注册信号量
     *
     * @param context
     */
    private static void registerSignalHandler(Context context) {
        SignalHandler signalHandler = new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                // SIG_INT
                if (signal.getNumber() == 2) {
                    existed.compareAndSet(false, true);
                }
            }
        };
        Signal.handle(new Signal("INT"), signalHandler);
    }

    /**
     * 创建上下文
     *
     * @param properties
     * @return
     */
    private static Context getStreamContext(Properties properties) {
        return new Context();
    }

    /**
     * offset@timestamp or timestamp 偏移量@时间戳 或者 时间戳
     * 根据时间戳获取消费点位
     *
     * @param checkpoint 消费点位
     * @return
     */
    private static Checkpoint parseCheckpoint(String checkpoint) {
        require(null != checkpoint, "checkpoint should not be null");
        String[] offsetAndTS = checkpoint.split("@");
        Checkpoint streamCheckpoint = null;
        if (offsetAndTS.length == 1) {
            streamCheckpoint =  new Checkpoint(null, Long.valueOf(offsetAndTS[0]), -1, "");
        } else if (offsetAndTS.length >= 2) {
            streamCheckpoint =  new Checkpoint(null, Long.valueOf(offsetAndTS[0]), Long.valueOf(offsetAndTS[1]), "");
        }
        return streamCheckpoint;
    }

    /**
     * 获取记录生产者 == kafka里的consumer
     *
     * @param context 上下文
     * @param properties 配置文件
     * @return
     */
    private static RecordGenerator getRecordGenerator(Context context, Properties properties) {

        RecordGenerator recordGenerator = new RecordGenerator(properties, context,
                parseCheckpoint(properties.getProperty(INITIAL_CHECKPOINT_NAME)),
                new ConsumerWrapFactory.KafkaConsumerWrapFactory());
        context.setStreamSource(recordGenerator);
        return recordGenerator;
    }

    /**
     * 获取记录处理者【线程】
     *
     * @param context 上下人
     * @param properties 配置文件
     * @param recordGenerator 数据生产者
     * @return
     */
    private static EtlRecordProcessor getEtlRecordProcessor(Context context, Properties properties, RecordGenerator recordGenerator) {

        EtlRecordProcessor etlRecordProcessor = new EtlRecordProcessor(new OffsetCommitCallBack() {
            @Override
            public void commit(TopicPartition tp, long timestamp, long offset, String metadata) {
                // 回调之后设置信息消费点位
                recordGenerator.setToCommitCheckpoint(new Checkpoint(tp, timestamp, offset, metadata));
            }
        }, context);
        context.setRecordProcessor(etlRecordProcessor);
        return etlRecordProcessor;
    }

    /**
     * 校验DTS必要的配置
     *
     * @param properties 配置信息
     */
    private static void checkConfig(Properties properties) {
        require(null != properties.getProperty(USER_NAME), "use should supplied");
        require(null != properties.getProperty(PASSWORD_NAME), "password should supplied");
        require(null != properties.getProperty(SID_NAME), "sid should supplied");
        require(null != properties.getProperty(KAFKA_TOPIC), "kafka topic should supplied");
        require(null != properties.getProperty(KAFKA_BROKER_URL_NAME), "broker url should supplied");
    }

    /**
     * 初始化log4j
     *
     * @return log4j配置文件，返回结果用不到
     */
    private static Properties initLog4j() {
        Properties properties = new Properties();
        InputStream log4jInput = null;
        try {
            log4jInput = Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties");
            PropertyConfigurator.configure(log4jInput);
        } catch (Exception e) {
        } finally {
            swallowErrorClose(log4jInput);
        }
        return properties;
    }

    /**
     * 使用配置文件加载配置
     *
     * @param filePath 文件路径
     * @return
     */
    private static Properties loadConfig(String filePath) {
        Properties ret = new Properties();
        InputStream toLoad = null;
        try {
            toLoad = new BufferedInputStream(new FileInputStream(filePath));
            ret.load(toLoad);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }  finally {
            swallowErrorClose(toLoad);
        }
        return ret;
    }
}
