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
import io.pravega.client.stream.impl.UTF8StringSerializer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class TestPravegaStream {

    @Test
    public void testByteStream() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        final String scope = "test";
        final String stream = "test00";

        // Step 1: Write data
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        // Step 2: Create a Pravega Stream
        PravegaStream str = new PravegaStream(scope, stream, clientConfig);
        // read a ByteStream.
        Stream<byte[]> byteStream = str.getByteStream();
        //All aggregate options (map, flatMap, filter ...) supported by normal Java stream can be supported.
        Map<String, Long> result = byteStream.map(bytes -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)).toString()) // decode it.
                                             .peek(s -> System.out.println(s)) // peek
                                             .collect(Collectors.groupingBy(
                                                     Function.identity(), Collectors.counting()
                                             ));
        System.out.println("===============================================");
        System.out.println(result);
    }

    @Test
    public void testEventStream() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        final String scope = "test";
        final String stream = "test10";

        // Step 1
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        PravegaStream str = new PravegaStream(scope, stream, clientConfig);
        // obtain a java.util.stream.Stream of bytes[]
        Stream<String> eventStream = str.getEventStream(new UTF8StringSerializer());
        //All aggregate options (map, flatMap, filter ...) supported by normal Java stream can be supported.
        Map<String, Long> result = eventStream
                .peek(s -> System.out.println(s)) // peek
                .collect(Collectors.groupingBy(
                        Function.identity(), Collectors.counting()
                ));
        System.out.println("===============================================");
        System.out.println(result);
    }

    private static void createAndWriteToStream(URI controllerURI, ClientConfig clientConfig, String scope, String stream) {
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
    }
}
