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

public class DistributedBarrier implements Barrier{
    @Override
    public int await() throws BrokenBarrierException, InterruptedException {
        return 0;
    }

    @Override
    public int await(long timeout, TimeUnit unit) throws BrokenBarrierException, InterruptedException, TimeoutException {
        return 0;
    }

    @Override
    public void reset() {

    }

    @Override
    public boolean isReleased() {
        return false;
    }

    @Override
    public int getParties() {
        return 0;
    }
}
