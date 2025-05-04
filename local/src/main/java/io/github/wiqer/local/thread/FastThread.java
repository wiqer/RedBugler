package io.github.wiqer.local.thread;

import java.util.function.Consumer;

public class FastThread extends Thread {

    public FastThread(String threadName) {
        super(threadName);
    }

    public FastThread(Runnable runnable) {
        super(runnable);
    }

    public FastThread(Runnable runnable, String name) {
        super(runnable, name);
    }

    public FastThread(String threadName, Consumer<Throwable> jvmExistHandler, int maxValue) {
    }
}
