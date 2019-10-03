/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.pravega.common.ExponentialMovingAverage;
import io.pravega.common.MathHelpers;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * See {@link AppendBatchSizeTracker}.
 * 
 * This implementation tracks three things:
 * 1. The time between appends
 * 2. The size of each append
 * 3. The number of unackedAppends there are outstanding
 * 
 * If the number of unacked appends is <= 1 batching is disabled. This improves latency for low volume and 
 * synchronous writers. Otherwise the batch size is set to the amount of data that will be written in the next
 * {@link #MAX_BATCH_TIME_MILLIS} or half the server round trip time (whichever is less)
 */
@Slf4j
class AppendBatchSizeTrackerImpl implements AppendBatchSizeTracker {
    private static final int MAX_BATCH_TIME_MILLIS = 100;
    private static final int MAX_BATCH_SIZE = 32 * 1024;

    private final Supplier<Long> clock;
    private final AtomicLong lastAppendNumber;
    private final AtomicLong lastAppendTime;
    private final AtomicLong lastAckNumber;
    private final ExponentialMovingAverage eventSize = new ExponentialMovingAverage(1024, 0.1, true);
    private final ExponentialMovingAverage millisBetweenAppends = new ExponentialMovingAverage(10, 0.1, false);
    private final ExponentialMovingAverage appendsOutstanding = new ExponentialMovingAverage(2, 0.05, false);

    AppendBatchSizeTrackerImpl() {
        clock = System::currentTimeMillis;
        lastAppendTime = new AtomicLong(clock.get());
        lastAckNumber = new AtomicLong(0);
        lastAppendNumber = new AtomicLong(0);
    }

    @Override
    public void recordAppend(long eventNumber, int size, String segment) {
        long now = Math.max(lastAppendTime.get(), clock.get());
        long last = lastAppendTime.getAndSet(now);
        lastAppendNumber.set(eventNumber);
        millisBetweenAppends.addNewSample(now - last);
        appendsOutstanding.addNewSample(eventNumber - lastAckNumber.get());
        eventSize.addNewSample(size);
        if (segment.contains("test/")) {
            log.info("=> Record Append : appendsOutstanding, {}, millisBetweenAppend, {}, eventSize, {}, eventNumber, {}, size, {}",
                     appendsOutstanding.getCurrentValue(), millisBetweenAppends.getCurrentValue(), eventSize.getCurrentValue(), eventNumber,
                     size);
        }
    }

    @Override
    public void recordAck(long eventNumber, String segment) {
        lastAckNumber.getAndSet(eventNumber);
        appendsOutstanding.addNewSample(lastAppendNumber.get() - eventNumber);
        if (segment.contains("test/")) {
            log.info("=> Record Ack : appendsOutstanding, {}, millisBetweenAppends, {}, eventSize, {}, eventNumber, {}, ",
                     appendsOutstanding.getCurrentValue(), millisBetweenAppends.getCurrentValue(), eventSize.getCurrentValue(), eventNumber);
        }
    }

    /**
     * Returns a block size that is an estimate of how much data will be written in the next
     * {@link #MAX_BATCH_TIME_MILLIS} or half the server round trip time (whichever is less).
     */
    @Override
    public int getAppendBlockSize(String segment) {
        final int blocksize;
        long numInflight = lastAppendNumber.get() - lastAckNumber.get();
        if (numInflight <= 1) {
            blocksize = 0;
        } else {
            double appendsInMaxBatch = Math.max(1.0, MAX_BATCH_TIME_MILLIS / millisBetweenAppends.getCurrentValue());
            double targetAppendsOutstanding = MathHelpers.minMax(appendsOutstanding.getCurrentValue() * 0.5, 1.0,
                                                                 appendsInMaxBatch);
            blocksize = (int) MathHelpers.minMax((long) (targetAppendsOutstanding * eventSize.getCurrentValue()), 0,
                                                 MAX_BATCH_SIZE);
        }
        if (segment.contains("test/")) {
            log.info("=> get appendBlockSize : appendsOutstanding, {}, millisBetweenAppends, {}, eventSize, {}, blocksize, {}, " +
                             "numInFlight, {}",
                     appendsOutstanding.getCurrentValue(), millisBetweenAppends.getCurrentValue(), eventSize.getCurrentValue(), blocksize, numInflight);
        }
        return blocksize;
    }

    @Override
    public int getBatchTimeout() {
        return MAX_BATCH_TIME_MILLIS;
    }
}
