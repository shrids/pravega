/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.CheckpointImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class ReaderGroupConfigTest {
    private static final String SCOPE = "scope";

    @Test
    public void testValidConfig() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("s1", getStartStreamCut("s1"))
                .stream("s2", getStartStreamCut("s2"))
                .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStartStreamCut("s1"), cfg.getStartingStreamCuts().get("s1"));
        assertEquals(getStartStreamCut("s2"), cfg.getStartingStreamCuts().get("s2"));
    }

    @Test
    public void testValidConfig2() {
        Checkpoint checkpoint = Mockito.mock(Checkpoint.class);
        CheckpointImpl checkpointImpl = Mockito.mock(CheckpointImpl.class);
        when(checkpoint.asImpl()).thenReturn(checkpointImpl);
        when(checkpointImpl.getPositions()).thenReturn(ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStartStreamCut("s1"))
                .put(Stream.of(SCOPE, "s2"), getStartStreamCut("s2")).build());

        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .startFromCheckpoint(checkpoint)
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStartStreamCut("s1"), cfg.getStartingStreamCuts().get("s1"));
        assertEquals(getStartStreamCut("s2"), cfg.getStartingStreamCuts().get("s2"));
    }

    @Test
    public void testValidConfig3() {
        Map<Stream, StreamCut> streamCuts = ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStartStreamCut("s1"))
                .put(Stream.of(SCOPE, "s2"), getStartStreamCut("s2")).build();

        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .startFromStreamCut(streamCuts)
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStartStreamCut("s1"), cfg.getStartingStreamCuts().get("s1"));
        assertEquals(getStartStreamCut("s2"), cfg.getStartingStreamCuts().get("s2"));
    }

    @Test
    public void testValidConfig4() {
        Map<Stream, StreamCut> streamCuts = ImmutableMap.<Stream, StreamCut>builder()
                .put(Stream.of(SCOPE, "s1"), getStartStreamCut("s1"))
                .put(Stream.of(SCOPE, "s2"), getStartStreamCut("s2")).build();

        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .startFromStreamCut(streamCuts)
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(getStartStreamCut("s1"), cfg.getStartingStreamCuts().get("s1"));
        assertEquals(getStartStreamCut("s2"), cfg.getStartingStreamCuts().get("s2"));
    }

    @Test
    public void testValidConfig5() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .stream("s1")
                                                 .stream("s2", getStartStreamCut("s2"))
                                                 .build();

        assertEquals(-1, cfg.getAutomaticCheckpointIntervalMillis());
        assertEquals(3000L, cfg.getGroupRefreshTimeMillis());
        assertEquals(StreamCut.UNBOUNDED, cfg.getStartingStreamCuts().get("s1"));
        assertEquals(getStartStreamCut("s2"), cfg.getStartingStreamCuts().get("s2"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingStreamNames() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInValidStartStreamCut() {
        ReaderGroupConfig cfg = ReaderGroupConfig.builder()
                                                 .disableAutomaticCheckpoints()
                                                 .stream("s1", getStartStreamCut("s2"))
                                                 .stream("s2", getStartStreamCut("s1"))
                                                 .build();
    }

    private StreamCut getStartStreamCut(String streamName) {
        ImmutableMap<Segment, Long> positions = ImmutableMap.<Segment, Long>builder().put(new Segment(SCOPE,
                streamName, 0), 10L).build();
        return new StreamCutImpl(Stream.of(SCOPE, streamName), positions);

    }

}