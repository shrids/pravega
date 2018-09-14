/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Barrier {
    /**
     * Wait until all the parties have invoked await() on this barrier.
     *
     * @return the arrival index of the current thread, where index {@code getParties() - 1} indicates the first
     * to arrive and zero indicates the last to arrive.
     * @throws BrokenBarrierException if another thread was interrupted or timed out while the current thread was waiting, or the barrier was reset, or the barrier was broken when await was called, or the barrier action (if present) failed due to an exception
     * @throws InterruptedException   if the current thread was interrupted while waiting
     */
    int await() throws BrokenBarrierException, InterruptedException;

    /**
     * Wait until all the parties have invoked await() on this barrier or the specified timeout has elapsed.
     *
     * @param timeout timeout
     * @param unit    {@link TimeUnit}
     * @return
     * @throws BrokenBarrierException x.
     * @throws InterruptedException   y.
     * @throws TimeoutException       z.
     */
    int await(long timeout, TimeUnit unit) throws BrokenBarrierException, InterruptedException, TimeoutException;

    /**
     * Reset the barrier to un-released state.
     * - if a thread is waiting then it should get       a BrokenBarrierException.
     */
    void reset();

    /**
     * Returns whether or not all the parties have invoked await and the Barrier is in released state.
     *
     * @return True if the barrier is in released state.
     */
    boolean isReleased();

    /**
     * The number of parties which are a part of this barrier.
     *
     * @return
     */
    int getParties();
}

