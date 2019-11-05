package io.pravega.client.stream.impl;

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

public class PravegaStream extends StreamImpl implements AutoCloseable {

    private final SegmentRange[] segmentRanges; // immutable after construction.
    private final BatchClientFactory bf;

    /**
     * Creates a new instance of the Stream class.
     *
     * @param scope      The scope of the stream.
     * @param streamName The name of the stream.
     */
    public PravegaStream(String scope, String streamName, ClientConfig clientConfig) {
        super(scope, streamName);
        this.bf = BatchClientFactory.withScope(scope, clientConfig);
        final StreamSegmentsIterator segIterator = bf.getSegments(Stream.of(scope, streamName), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
        this.segmentRanges = Lists.newArrayList(segIterator.getIterator()).toArray(new SegmentRange[0]);
    }

    private Spliterator<byte[]> spliterator() {
        return new PravegaSplitIterator(segmentRanges, 0, segmentRanges.length);
    }

    /**
     * Get Byte Stream.
     * @return
     */
    public java.util.stream.Stream<byte[]> getByteStream() {
        // TODO: experiment with parallel flag set as true.
        Spliterator<byte[]> itr = spliterator();
        return StreamSupport.<byte[]>stream(itr, false);
    }

    @Override
    public void close() throws Exception {
        bf.close();
    }

    // This split iterator does not ensure ordering guarantees. (A newer splitIterator which ensures ordering guarantees can also be
    // created).
    private class PravegaSplitIterator implements Spliterator<byte[]> {
        private final SegmentRange[] segmentRanges;
        private int index;
        private int fence;
        private AtomicReference<SegmentIterator<byte[]>> ref = new AtomicReference<>();

        PravegaSplitIterator(SegmentRange[] array, int origin, int fence) {
            this.segmentRanges = array;
            this.index = origin;
            this.fence = fence;
        }

        @Override
        public boolean tryAdvance(Consumer<? super byte[]> action) {
            // return true if there are elements to be read.
            final UnaryOperator<SegmentIterator<byte[]>> createSegmentItr = itr -> {
                if (itr == null) {
                    return bf.readSegment(segmentRanges[index], new ByteArraySerializer());
                }
                return itr;
            };

            if ((index < fence) && ref.updateAndGet(createSegmentItr).hasNext()) {
                byte[] data = ref.get().next();
                action.accept(data);
                return true;
            } else if (index < fence) { // there are other segments to be read from update the index.
                index++;
                closeSegmentReaderIfPresent();
                return true;
            } else {
                closeSegmentReaderIfPresent();
                return false;
            }
        }

        private void closeSegmentReaderIfPresent() {
            SegmentIterator<byte[]> itr = ref.getAndSet(null);
            if (itr != null)
                itr.close();
        }

        @Override
        public Spliterator<byte[]> trySplit() {
            int lo = index, mid = (lo + fence) >>> 1;
            return (lo >= mid) ? null :
                    new PravegaSplitIterator(segmentRanges, mid, fence); // ordering is not maintained.
        }

        @Override
        public long estimateSize() {
            return fence - index;
        }

        @Override
        public int characteristics() {
            // TODO: experiment with other options.
            return IMMUTABLE | SUBSIZED ;
        }
    }
}
