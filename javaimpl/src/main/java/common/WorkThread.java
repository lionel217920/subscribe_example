package common;

import java.io.Closeable;

import static common.Util.swallowErrorClose;

/**
 * 工作线程（数据生产者，数据消费者，数据消费者Commit）
 *
 * @param <T>
 */
public class WorkThread<T extends Runnable & Closeable> {
    private final T r;
    private final Thread worker;

    public WorkThread(T r, String threadName) {
        this.r = r;
        worker = new Thread(r);
        this.worker.setName(threadName);
    }

    public WorkThread(T r) {
        this.r = r;
        worker = new Thread(r);
    }

    public void start() {
        worker.start();
    }

    public void stop() {
        swallowErrorClose(r);
        try {
            worker.join(10000, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}