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

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.StreamImpl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

@Slf4j
public class PravegaStream extends StreamImpl implements AutoCloseable {

    private final static int MAX_NULL_COUNT = 2;
    private final static long READ_TIMEOUT = 5_000L;

    @Getter
    private final ClientConfig clientConfig;
    private final BatchClientFactory bf;
    private final EventStreamClientFactory cf;

    /**
     * Creates a new instance of the Stream class.
     *
     * @param scope      The scope of the stream.
     * @param streamName The name of the stream.
     * @param clientConfig The client configuration.
     */
    public PravegaStream(ClientConfig clientConfig, String scope, String streamName) {
        super(scope, streamName);
        this.clientConfig = clientConfig;
        this.bf = BatchClientFactory.withScope(scope, clientConfig);
        this.cf = EventStreamClientFactory.withScope(scope, clientConfig);
    }

    /**
     * Get unordered Byte Stream.
     * @return A Java stream corresponding to a Pravega Stream.
     */
    public java.util.stream.Stream<byte[]> getUnorderedByteStream() {
        return getUnorderedEventStream(new ByteArraySerializer());
    }

    /**
     * Get EventStream.
     * @param serializer Serializer that can be used to deserialize data
     * @param <T> Event type.
     * @return A Java stream corresponding to a Pravega Stream.
     */
    public <T> java.util.stream.Stream<T> getUnorderedEventStream(Serializer<T> serializer) {
        final StreamSegmentsIterator segIterator = bf.getSegments(Stream.of(getScope(), getStreamName()), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
        SegmentRange[] segmentRanges = Lists.newArrayList(segIterator.getIterator()).toArray(new SegmentRange[0]);
        Spliterator<T> itr = new PravegaSplitIterator<T>(segmentRanges, 0, segmentRanges.length-1, serializer);
        return StreamSupport.<T>stream(itr, true);
    }

    /**
     * Get unordered Byte Stream.
     * @return A Java stream corresponding to a Pravega Stream.
     */
    public java.util.stream.Stream<byte[]> getOrderedByteStream() {
        return getOrderedEventStream(new ByteArraySerializer());
    }

    public <T> java.util.stream.Stream<T> getOrderedEventStream(Serializer<T> serializer) {
        ReaderGroupManager r = ReaderGroupManager.withScope(getScope(), clientConfig);
        String readerGroupName = "rg-" + RandomStringUtils.randomAlphanumeric(5);
        log.debug("Creating a reader group {}", readerGroupName);
        r.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(getScopedName()).build());
        Spliterator<T> itr = new PravegaOrderedSplitIterator<T>(readerGroupName, new AtomicInteger(0), serializer);
        return StreamSupport.<T>stream(itr, true);
    }

    @Override
    public void close() throws Exception {
        bf.close();
    }

    private class PravegaOrderedSplitIterator<T> implements Spliterator<T> {

        private final Serializer<T> serializer;
        private final String readerGroupName;
        private final int processorCount = Runtime.getRuntime().availableProcessors();
        private final AtomicInteger readerIndex;
        private final EventStreamReader<T> reader;

        PravegaOrderedSplitIterator( String readerGroupName, AtomicInteger readerIndex, Serializer<T> serializer) {
            this.readerGroupName = readerGroupName;
            this.serializer = serializer;
            this.readerIndex = readerIndex;
            this.reader = cf.createReader("reader-" + readerIndex.get(), readerGroupName, serializer, ReaderConfig.builder().build());
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            EventRead<T> eventRead = null;
            T data = null;
            int retryCount = 0;
            do {
                eventRead = reader.readNextEvent(READ_TIMEOUT);
                if (eventRead.isCheckpoint()) {
                    return true;
                }

                data = eventRead.getEvent();
                if (data != null) {
                    action.accept(data);
                    return true;
                } else {
                    retryCount++;

                }
            } while ((data == null) && (retryCount < MAX_NULL_COUNT));
            return false;
        }


        @Override
        public Spliterator<T> trySplit() {
            int updatedReaderIndex = readerIndex.incrementAndGet();
            log.debug("Reader Index: {}  processorCount: {}", updatedReaderIndex, processorCount);
            if (updatedReaderIndex >= processorCount) {
                return null;
            } else {
                log.debug("Creating a reader-{} to increase parallelism.", updatedReaderIndex);
                return new PravegaOrderedSplitIterator<T>(readerGroupName, readerIndex, serializer);
            }
        }

        @Override
        public long estimateSize() {
            // computing the number elements in the all the segments is time consuming.
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return IMMUTABLE;
        }
    }

    private class PravegaSplitIterator<T> implements Spliterator<T> {
        private final Serializer<T> serializer;
        private final SegmentRange[] segmentRanges;
        private int lo;
        private int high;
        private AtomicReference<SegmentIterator<T>> ref = new AtomicReference<>();


        PravegaSplitIterator(SegmentRange[] array, int lo, int high, Serializer<T> serializer) {
            this.segmentRanges = array;
            this.lo = lo;
            this.high = high;
            this.serializer = serializer;
            log.debug("=> lo {}, hi {}", lo, high);
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {

            // return true if there are elements to be read.
            final UnaryOperator<SegmentIterator<T>> createSegmentItr = itr -> {
                if (itr == null) {
                    return bf.readSegment(segmentRanges[lo], serializer);
                }
                return itr;
            };

            if ((lo <= high) && ref.updateAndGet(createSegmentItr).hasNext()) {
                T data = ref.get().next();
                log.debug("Reading event {}", data);
                action.accept(data);
                return true;
            } else if (lo <= high) { // there are other segments to be read from update the index.
                lo++;
                closeSegmentReaderIfPresent();
                return true;
            } else {
                closeSegmentReaderIfPresent();
                return false;
            }
        }

        private void closeSegmentReaderIfPresent() {
            SegmentIterator<T> itr = ref.getAndSet(null);
            if (itr != null) {
                itr.close();
            }
        }

        @Override
        public Spliterator<T> trySplit() {
            log.debug("=> trying split fence {} index {}", high, lo);
            if (high -lo <= 1) {
                return null;
            }
            int mid = lo + (high-lo) / 2;
            int oldHigh = high;
            high = mid;
            log.debug("=> split decided  mid {}", mid);
            return new PravegaSplitIterator<T>(segmentRanges, mid + 1, oldHigh, serializer); // ordering is not maintained.
        }

        @Override
        public long estimateSize() {
            // computing the number elements in the all the segments is time consuming.
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            // TODO: experiment with other options.
            return IMMUTABLE;
        }
    }

    //    public static PravegaStream of(Stream stream, ClientConfig clientConfig) {
    //        return new PravegaStream(clientConfig, stream.getScope(), stream.getStreamName());
    //    }
}
