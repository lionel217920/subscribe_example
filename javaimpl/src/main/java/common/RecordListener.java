package common;

/**
 * 数据消费监听接口
 */
public interface RecordListener {

    /**
     * 数据消费监听接口
     *
     * @param record
     */
    void consume(UserRecord record);

}
