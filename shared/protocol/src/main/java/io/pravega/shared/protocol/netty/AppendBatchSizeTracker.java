/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

public interface AppendBatchSizeTracker {

    /**
     * Records that an append has been sent.
     * 
     * @param eventNumber the number of the event
     * @param size the size of the event
     * @param segment Segment name.
     */
    void recordAppend(long eventNumber, int size, String segment);

    /**
     * Records that one or more events have been acked.
     * 
     * @param eventNumber the number of the last event.
     * @param segment Segment name.
     */
    long recordAck(long eventNumber, String segment);

    /**
     * Returns the size that should be used for the next append block.
     *
     * @param segment Segment name.
     * @return Integer indicating block size that should be used for the next append.
     */
    int getAppendBlockSize(String segment);
    
    /**
     * Returns the timeout that should be used for append blocks.
     *
     * @return Integer indicating the batch timeout.
     */
    int getBatchTimeout();

}