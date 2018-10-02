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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataInput.ElementDeserializer;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.RevisionDataOutput.ElementSerializer;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

@NotThreadSafe
@EqualsAndHashCode
@Slf4j
public class StreamCutBarrierState {

    private static final StreamCutBarrierStateSerializer SERIALIZER = new StreamCutBarrierStateSerializer();

    private final List<String> ids;
    /**
     * Maps StreamCutBarrierId to parties yet to join the barrier.
     */
    private final Map<String, List<String>> remainingParties;
    /**
     *  Maps StreamCutBarrier id to positions in segments.
     */
    private final Map<String, Map<Stream, Map<Segment, Long>>> streamCutBarrierPositions;

    public StreamCutBarrierState() {
        this(new ArrayList<>(), new HashMap<>(), new HashMap<>());
    }

    @Builder
    private StreamCutBarrierState(List<String> checkpoints, Map<String, List<String>> remainingParties,
                                  Map<String, Map<Stream, Map<Segment, Long>>> streamCutBarrierPositions) {
        Preconditions.checkNotNull(checkpoints);
        Preconditions.checkNotNull(remainingParties);
        Preconditions.checkNotNull(streamCutBarrierPositions);
        this.ids = checkpoints;
        this.remainingParties = remainingParties;
        this.streamCutBarrierPositions = streamCutBarrierPositions;
    }
    
    void beginNewStreamCutBarrier(String barrierId, Set<String> parties, Map<Segment, Long> knownPositions) {
        if (!streamCutBarrierPositions.containsKey(barrierId)) {
            if (!parties.isEmpty()) {
                remainingParties.put(barrierId, new ArrayList<>(parties));
            }
            streamCutBarrierPositions.put(barrierId, convertToStreamMap(knownPositions));
            ids.add(barrierId);
        }
    }

    private Map<Stream, Map<Segment, Long>> convertToStreamMap(final Map<Segment, Long> positions) {
        return positions.entrySet().stream()
                        .collect(groupingBy(o -> o.getKey().getStream(), toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
    
    String getBarrierIdForParty(final String party) {
        OptionalInt min = getBarrierIds(party).stream().mapToInt(ids::indexOf).min();
        if (min.isPresent()) {
            return ids.get(min.getAsInt());
        } else {
            return null;
        }
    }
    
    private List<String> getBarrierIds(String party) {
        return remainingParties.entrySet()
                               .stream()
                               .filter(entry -> entry.getValue().contains(party))
                               .map(Map.Entry::getKey)
                               .collect(Collectors.toList());
    }

    void removeParty(String party, Map<Segment, Long> position) {
        for (String barrierId : getBarrierIds(party)) {
            joinStreamCutBarrier(barrierId, party, position);
        }
    }
    
    void joinStreamCutBarrier(String barrierId, String party, Map<Segment, Long> position) {
        log.debug("==> joinStreamCutBarrier for BarrierId: {} with party: {} and postion:{}", barrierId, party, position);
        List<String> parties = remainingParties.get(barrierId);
        if (parties != null) {
            boolean removed = parties.remove(party);
            Preconditions.checkState(removed, party + " has already joined the StreamCutBarrier.");
            Map<Stream, Map<Segment, Long>> positions = streamCutBarrierPositions.get(barrierId);
            positions.putAll(convertToStreamMap(position));
            if (parties.isEmpty()) {
                remainingParties.remove(barrierId);
                log.info("==> All parties have joined the BarrierId: {}", barrierId);
            }
        }
    }
    
    boolean isStreamCutBarrierComplete(String checkpointId) {
        return !remainingParties.containsKey(checkpointId);
    }
    
    Map<Stream, Map<Segment, Long>> getPositionsForCompletedStreamCutBarrier(String barrierId) {
        if (remainingParties.containsKey(barrierId)) {
            return null;
        }
        return streamCutBarrierPositions.get(barrierId);
    }

    boolean hasOngoingStreamCutBarrier() {
        return !remainingParties.isEmpty();
    }
    
    void clearStreamCutBarrierBefore(String barrierId) {
        if (streamCutBarrierPositions.containsKey(barrierId)) {
            for (Iterator<String> iterator = ids.iterator(); iterator.hasNext();) {
                String cp = iterator.next();
                if (cp.equals(barrierId)) {
                    break;
                }
                remainingParties.remove(cp);
                streamCutBarrierPositions.remove(cp);
                iterator.remove();
            }
        }
    }

    /**
     * @return A copy of this object
     */
    StreamCutBarrierState copy() {
        List<String> cps = new ArrayList<>(ids);
        Map<String, List<String>> ucph = new HashMap<>(remainingParties.size());
        remainingParties.forEach((cp, hosts) -> ucph.put(cp, new ArrayList<>(hosts)));
        Map<String, Map<Stream, Map<Segment, Long>>> cpps = new HashMap<>();
        streamCutBarrierPositions.forEach((cp, pos) -> cpps.put(cp, new HashMap<>(pos))); //TODO: fix bug?
        return new StreamCutBarrierState(cps, ucph, cpps);
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("StreamCutBarrierState { ongoingBarriers: ");
        sb.append(ids.toString());
        sb.append(",  partiesBlockingEachStreamCutBarrier: ");
        sb.append(remainingParties.toString());
        sb.append(" }");
        return sb.toString();
    }

    @VisibleForTesting
    static class StreamCutBarrierStateBuilder implements ObjectBuilder<StreamCutBarrierState> {
    }

    private static class StreamCutBarrierStateSerializer
            extends VersionedSerializer.WithBuilder<StreamCutBarrierState, StreamCutBarrierStateBuilder> {
        @Override
        protected StreamCutBarrierStateBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }
        
        private void read00(RevisionDataInput input, StreamCutBarrierStateBuilder builder) throws IOException {
            ElementDeserializer<String> stringDeserializer = RevisionDataInput::readUTF;
            ElementDeserializer<Long> longDeserializer = RevisionDataInput::readLong;
            ElementDeserializer<Segment> segmentDeserializer = in -> Segment.fromScopedName(in.readUTF());
            ElementDeserializer<Stream> streamElementDeserializer = in -> Stream.of(in.readUTF());
            builder.checkpoints(input.readCollection(stringDeserializer, ArrayList::new));
            builder.remainingParties(input.readMap(stringDeserializer, in -> in.readCollection(stringDeserializer, ArrayList::new)));
            builder.streamCutBarrierPositions(input.readMap(stringDeserializer, in -> in.readMap(streamElementDeserializer, in1 -> in1.readMap(segmentDeserializer, longDeserializer))));
        }

        private void write00(StreamCutBarrierState object, RevisionDataOutput output) throws IOException {
            ElementSerializer<String> stringSerializer = RevisionDataOutput::writeUTF;
            ElementSerializer<Long> longSerializer = RevisionDataOutput::writeLong;
            ElementSerializer<Segment> segmentSerializer = (out, segment) -> out.writeUTF(segment.getScopedName());
            ElementSerializer<Stream> streamSerializer = (out, stream) -> out.writeUTF(stream.getScopedName());
            output.writeCollection(object.ids, stringSerializer);
            output.writeMap(object.remainingParties, stringSerializer, (out, hosts) -> out.writeCollection(hosts, stringSerializer));
            output.writeMap(object.streamCutBarrierPositions, stringSerializer, (out, map) -> out.writeMap(map, streamSerializer, (out1, map1) -> out1.writeMap(map1, segmentSerializer, longSerializer)));
        }
    }

    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @SneakyThrows(IOException.class)
    public static StreamCutBarrierState fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }
}
