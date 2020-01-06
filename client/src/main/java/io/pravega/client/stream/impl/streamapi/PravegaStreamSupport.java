/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl.streamapi;

import io.pravega.client.stream.Serializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class PravegaStreamSupport {

    // Suppresses default constructor, ensuring non-instantiability.
    private PravegaStreamSupport() {
    }

    public <T> java.util.stream.Stream<T> getUnorderedEventStream(PravegaStream stream, Serializer<T> serializer) {
        return stream.getUnorderedEventStream(serializer);
    }

    public java.util.stream.Stream<byte[]> getUnorderedByteStream(PravegaStream stream) {
        return stream.getUnorderedByteStream();
    }

    public <T> java.util.stream.Stream<T> getOrderedEventStream(PravegaStream stream, Serializer<T> serializer) {
        return stream.getUnorderedEventStream(serializer);
    }

    public java.util.stream.Stream<byte[]> getOrderedByteStream(PravegaStream stream) {
        return stream.getUnorderedByteStream();
    }
}
