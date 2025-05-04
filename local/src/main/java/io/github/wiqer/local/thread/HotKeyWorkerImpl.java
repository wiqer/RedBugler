package io.github.wiqer.local.thread;


import org.jctools.queues.MpscArrayQueue;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * 注意，多任务单消费
 */
public class HotKeyWorkerImpl extends FastThread implements HotKeyWorker {
    /**
     * 这个搞太多似乎也没啥意义
     */
    static final int MAXIMUM_CAPACITY = Integer.MAX_VALUE >>> 4;

    private static final int IDLE = -1;
    private static final int WORK = 1;
    private static int halfQueueCapacity;
    private final Consumer<Throwable> jvmExistHandler;
    private final Queue<Runnable> queue;
    private volatile int state = IDLE;
    private volatile boolean shutdown = false;

    public HotKeyWorkerImpl(String threadName, Consumer<Throwable> jvmExistHandler) {
        this(threadName, jvmExistHandler, Integer.MAX_VALUE - 2);
    }

    public HotKeyWorkerImpl(String threadName, Consumer<Throwable> jvmExistHandler, int queueCapacity) {
        super(threadName);
        this.jvmExistHandler = jvmExistHandler;
        queueCapacity = tableSizeFor(queueCapacity);
        this.queue = new MpscArrayQueue<>(queueCapacity);
        HotKeyWorkerImpl.halfQueueCapacity = queueCapacity >>> 2;
    }

    /**
     * copy from HashMap
     *
     * @param cap
     * @return
     */
    static final int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    @Override
    public void run() {
        try {
            do {
                Runnable avail = queue.poll();
                if (avail != null) {
                    avail.run();
                } else {
                    state = IDLE;
                    if (!queue.isEmpty()) {
                        state = WORK;
                    } else {
                        LockSupport.park(this);
                        if (Thread.currentThread().isInterrupted() && shutdown) {
                            break;
                        }
                    }
                }
            } while (true);
        } catch (Throwable e) {
            jvmExistHandler.accept(e);
            //代码不应该走到这里
            System.exit(129);
        }
    }

    @Override
    public void submit(Runnable completableFuture) {
//        if(state == WORK){
//            int queueSize = queue.size();
//            int flowSize = queueSize - halfQueueCapacity;
//            if (flowSize > 0) {
//                int randomValue = ThreadLocalRandom.current().nextInt();
//                if((randomValue & (halfQueueCapacity - 1)) < flowSize){
//                    //直接丢弃任务
//                    return;
//                }
//            }
//        }

        queue.offer(completableFuture);
        int t_state = this.state;
        if (t_state == IDLE) {
            this.state = WORK;
            LockSupport.unpark(this);
        }
    }

    @Override
    public void shuwdown() {
        shutdown = true;
        interrupt();
    }

    @Override
    public Thread thread() {
        return this;
    }

    @Override
    public void execute(Runnable command) {
        submit(command);
    }
}
