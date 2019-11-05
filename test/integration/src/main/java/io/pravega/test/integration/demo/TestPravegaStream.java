package io.pravega.test.integration.demo;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.PravegaStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestPravegaStream {
    public static void main(String[] args) {
        URI controllerURI = URI.create("tcp://localhost:9090");
        //Map with has a mapping of routing key to its corresponding key.
        final Map<String, String> keyReverseMap = ImmutableMap.<String, String>builder().put("0.1", "14")
                                                                                        .put("0.2", "11")
                                                                                        .put("0.3", "2")
                                                                                        .put("0.4", "1")
                                                                                        .put("0.5", "10")
                                                                                        .put("0.6", "3")
                                                                                        .put("0.7", "5")
                                                                                        .put("0.8", "7")
                                                                                        .put("0.9", "6")
                                                                                        .put("1.0", "4")
                                                                                        .build();

        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        final String scope = "test";
        final String stream = "test2";
        EventStreamClientFactory cf = EventStreamClientFactory.withScope(scope, clientConfig);

        // create scope and stream
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(4)).build());

        EventStreamWriter<byte[]> writer = cf.createEventWriter(stream, new ByteArraySerializer(), EventWriterConfig.builder().build());
        // write to segment 0
        writer.writeEvent(keyReverseMap.get("0.1"), "event-0".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.1"), "event-0".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.1"), "event-0".getBytes(StandardCharsets.UTF_8));
        // write to segment 1
        writer.writeEvent(keyReverseMap.get("0.4"), "event-1".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.4"), "event-1".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.4"), "event-1".getBytes(StandardCharsets.UTF_8));
        // write to segment 2
        writer.writeEvent(keyReverseMap.get("0.6"), "event-2".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.6"), "event-2".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.6"), "event-2".getBytes(StandardCharsets.UTF_8));
        // write to segment 3
        writer.writeEvent(keyReverseMap.get("0.9"), "event-3".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.9"), "event-3".getBytes(StandardCharsets.UTF_8));
        writer.writeEvent(keyReverseMap.get("0.9"), "event-3".getBytes(StandardCharsets.UTF_8));

        writer.flush();

        PravegaStream str = new PravegaStream(scope, stream, clientConfig);
        // obtain a java.util.stream.Stream of bytes[]
        Stream<byte[]> byteStream = str.getByteStream();
        //All aggregate options (map, flatMap, filter ...) supported by normal Java stream can be supported.
        List<String> result = byteStream.map(bytes -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)).toString()) // decode it.
                                    .peek(s -> System.out.println(s)) // peek
                                    .collect(Collectors.toList());
        System.out.println(result);

    }
}
