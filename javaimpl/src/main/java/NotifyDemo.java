import common.RecordListener;
import boot.MysqlRecordPrinter;
import common.UserRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static boot.Boot.boot;
import static recordgenerator.Names.*;

public class NotifyDemo {

    private static final Logger log = LoggerFactory.getLogger(NotifyDemo.class);

    public static Map<String, RecordListener> buildRecordListener() {
        // user can impl their own listener
        RecordListener mysqlRecordPrintListener = new RecordListener() {
            @Override
            public void consume(UserRecord record) {
                // consume record
                // MysqlRecordPrinter show how to go through record fields and get general attributes
                String ret = MysqlRecordPrinter.recordToString(record.getRecord());
        //        log.info(ret);
                record.commit(String.valueOf(record.getRecord().getSourceTimestamp()));
            }
        };
        return Collections.singletonMap("mysqlRecordPrinter", mysqlRecordPrintListener);
    }

    /**
     * This demo use  hard coded config. User can modify variable value for test
     * The detailed describe for var in resources/demoConfig
     *
     * 获取DTS配置【示例Demo使用硬编码，测试时可以修改变量】
     */
    public static Properties getConfigs() {
        Properties properties = new Properties();
        // user password and sid for auth，使用password和sid进行认证
        properties.setProperty(USER_NAME, "your user name");
        properties.setProperty(PASSWORD_NAME, "your password");
        properties.setProperty(SID_NAME, "your sid");
        // kafka consumer group general same with sid，消费组名称和sid(消费组id)保持一致
        properties.setProperty(GROUP_NAME, "your sid");
        // topic to consume, partition is 0，消费的topic，分区是0 ？？
        properties.setProperty(KAFKA_TOPIC, "your topic");
        // kafka broker url，broker地址
        properties.setProperty(KAFKA_BROKER_URL_NAME, "your broker url");
        // initial checkpoint for first seek(a timestamp to set, eg 1566180200 if you want (Mon Aug 19 10:03:21 CST 2019))
        // 首次消费点位，设置时间戳
        properties.setProperty(INITIAL_CHECKPOINT_NAME, "start timestamp");
        // if force use config checkpoint when start. for checkpoint reset
        // 如果强制使用配置的消费点位，消费点位会重置
        properties.setProperty(USE_CONFIG_CHECKPOINT_NAME, "true");
        // use consumer assign or subscribe interface
        // when use subscribe mode, group config is required. kafka consumer group is enabled
        properties.setProperty(SUBSCRIBE_MODE_NAME, "assign");
        return properties;
    }

    public static void main(String[] args) throws InterruptedException {

        try{
            boot(getConfigs(), buildRecordListener());
        }catch(Throwable e){
            log.error("NotifyDemo: failed cause " + e.getMessage(), e);
            throw e;
        } finally {
            System.exit(0);
        }
    }
}
