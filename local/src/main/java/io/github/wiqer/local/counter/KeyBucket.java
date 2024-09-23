package io.github.wiqer.local.counter;

import java.util.concurrent.ConcurrentSkipListMap;

public class KeyBucket {

    private ConcurrentSkipListMap<Long,Long> map = new ConcurrentSkipListMap<>();
}
