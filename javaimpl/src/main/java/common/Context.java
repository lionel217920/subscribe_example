package common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordgenerator.RecordGenerator;
import recordprocessor.EtlRecordProcessor;

/**
 * 上下文信息，目的是在生产者中获取到消费者，将从kafka获取到的数据放入到消费者中的队列中去
 */
public class Context {

    private static final Logger log = LoggerFactory.getLogger(Context.class);

    /**
     * 记录生产者
     */
    private RecordGenerator streamSource;

    /**
     * 记录消费者
     */
    private EtlRecordProcessor recordProcessor;

    public void setStreamSource(RecordGenerator streamSource) {
        this.streamSource = streamSource;
    }

    public EtlRecordProcessor getRecordProcessor() {
        return recordProcessor;
    }

    public  void setRecordProcessor(EtlRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

}
