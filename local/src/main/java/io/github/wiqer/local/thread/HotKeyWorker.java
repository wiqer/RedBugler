package io.github.wiqer.local.thread;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface HotKeyWorker extends Executor {
    void submit(Runnable runnable);

    void shuwdown();

    Thread thread();
}
