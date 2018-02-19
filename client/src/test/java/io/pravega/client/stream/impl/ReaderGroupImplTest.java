/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.curator.shaded.com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReaderGroupImplTest {

    private static final String SCOPE = "scope";
    private static final String GROUP_NAME = "readerGroup";
    private ReaderGroupImpl readerGroup;
    @Mock
    private SynchronizerConfig synchronizerConfig;
    @Mock
    private ClientFactory clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @Mock
    private ReaderGroupState state;

    private Serializer<ReaderGroupState.ReaderGroupStateInit> initSerializer = new JavaSerializer<>();
    private Serializer<ReaderGroupState.ReaderGroupStateUpdate> updateSerializer = new JavaSerializer<>();

    @Before
    public void setUp() throws Exception {
        readerGroup = new ReaderGroupImpl(SCOPE, GROUP_NAME, synchronizerConfig, initSerializer,
                updateSerializer, clientFactory, controller, connectionFactory);
        when(synchronizer.getState()).thenReturn(state);
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamCutDuplicateStreamCut() {
        readerGroup.resetReadersToStreamCut(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s1"), createStreamCut("s1", 3)).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamMissingStreamCut() {
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(state.getStreamNames()).thenReturn(ImmutableSet.of("s1", "s2"));

        readerGroup.resetReadersToStreamCut(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2)).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamExtraStreamCut() {
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(state.getStreamNames()).thenReturn(ImmutableSet.of("s1"));

        readerGroup.resetReadersToStreamCut(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 2)).build());
    }

    @Test
    public void resetReadersToStreamCut() {
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(state.getStreamNames()).thenReturn(ImmutableSet.of("s1", "s2"));

        readerGroup.resetReadersToStreamCut(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build());
        verify(synchronizer, times(1)).updateState(any(Function.class));
    }

    @Test
    public void resetReadersToCheckpoint() {
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(state.getStreamNames()).thenReturn(ImmutableSet.of("s1"));

        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(2).forEach(segNum -> positions.put(new Segment(SCOPE, "s1", segNum), 10L));
        Checkpoint checkpoint = new CheckpointImpl("testChkPoint", positions);
        readerGroup.resetReadersToCheckpoint(checkpoint);
        verify(synchronizer, times(1)).updateState(any(Function.class));
    }

    private StreamCut createStreamCut(String streamName, int numberOfSegments) {
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(numberOfSegments).forEach(segNum -> positions.put(new Segment(SCOPE, streamName, segNum), 10L));
        return new StreamCut(createStream(streamName), positions);
    }

    private Stream createStream(String streamName) {
        return new StreamImpl(SCOPE, streamName);
    }
}