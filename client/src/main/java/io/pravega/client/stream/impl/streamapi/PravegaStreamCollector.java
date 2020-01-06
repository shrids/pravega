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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

@RequiredArgsConstructor
@Slf4j
public class PravegaStreamCollector<T> implements Collector<T, PravegaStream, PravegaStream> {
    private final PravegaStream stream;
    private final Function<T, String> getRoutingKey;
    private final Serializer<T> serializer;
    private final StreamConfiguration streamConfig;

    private final AtomicReference<EventStreamClientFactory> factoryRef = new AtomicReference<>();
    private final AtomicReference<EventStreamWriter<T>> writerRef = new AtomicReference<>();
    private final AtomicBoolean isStreamCreated = new AtomicBoolean(false);

    @Override
    public Supplier<PravegaStream> supplier() {
        log.debug("*=> get supplier invoked");
        return () -> {
            log.debug("*=> Create a Pravega Stream writer");
            if (!isStreamCreated.getAndSet(true)) {
                createScopeAndStream();
                createEventWriter();
            }
            return stream;
        };
    }

    @Synchronized
    private void createScopeAndStream() {
        @Cleanup
        StreamManager streamManager = StreamManager.create(stream.getClientConfig());
        streamManager.createScope(stream.getScope());
        streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig); // default stream config with 1 segment.
    }

    @Synchronized
    private void createEventWriter() {
        if (factoryRef.getAndSet(EventStreamClientFactory.withScope(stream.getScope(), stream.getClientConfig())) != null) {
            throw new IllegalStateException("Illegal usage of method");
        }
        EventStreamClientFactory factory = factoryRef.get();
        if (writerRef.getAndSet(factory.createEventWriter(stream.getStreamName(), serializer, EventWriterConfig.builder().build())) != null) {
            throw new IllegalStateException("Illegal usage of method");
        }
    }

    @Override
    public BiConsumer<PravegaStream, T> accumulator() {
        log.debug("*=> get accumulator");
        return (pravegaStream, t) -> {
            EventStreamWriter<T> writer = writerRef.get();
            writer.writeEvent(getRoutingKey.apply(t), t);
            log.debug(" *=> wrote event");
        };
    }

    @Override
    public BinaryOperator<PravegaStream> combiner() {
        return (str1, st2) -> {
            log.debug("No need to combine streams as events can be written to the same stream.");
            return stream;
        };
    }

    @Override
    public Function<PravegaStream, PravegaStream> finisher() {
        return pravegaStream -> {
            log.debug("*=> Finisher called");
            writerRef.get().close(); // flush and close all events.
            factoryRef.get().close();
            return pravegaStream;
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.CONCURRENT,
                Collector.Characteristics.UNORDERED));
    }
}
