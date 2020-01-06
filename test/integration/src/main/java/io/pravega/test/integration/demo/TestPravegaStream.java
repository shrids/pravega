/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
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
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.pravega.client.stream.impl.streamapi.PravegaStream;
import io.pravega.client.stream.impl.streamapi.PravegaStreamCollector;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestPravegaStream {

    @Test
    public void testByteStream() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(controllerURI)
                                                      .build();
        final String scope = "test";
        final String stream = "test-" + RandomStringUtils.randomAlphabetic(5);

        // Step 1: Write data
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        // Step 2: Create a Pravega Stream
        PravegaStream str = new PravegaStream(clientConfig, scope, stream);
        // read a ByteStream.
        Stream<byte[]> byteStream = str.getUnorderedByteStream();
        //All aggregate options (map, flatMap, filter ...) supported by normal Java stream can be supported.
        Map<String, Long> result = byteStream.map(bytes -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes))
                                                                                 .toString()) // decode it.
                                             .peek(s -> System.out.println(s)) // peek
                                             .collect(Collectors.groupingBy(
                                                     Function.identity(), Collectors.counting()
                                             ));
        assertEquals("", 100L, result.get("event-1").longValue());
        assertEquals("", 100L, result.get("event-2").longValue());
        assertEquals("", 100L, result.get("event-3").longValue());
        assertEquals("", 100L, result.get("event-4").longValue());
    }

    @Test
    public void testEventStream() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(controllerURI)
                                                      .build();
        final String scope = "test";
        final String stream = "test-" + RandomStringUtils.randomAlphabetic(5);

        // Step 1
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        PravegaStream str = new PravegaStream(clientConfig, scope, stream);
        // obtain a java.util.stream.Stream of bytes[]
        Stream<String> eventStream = str.getUnorderedEventStream(new UTF8StringSerializer());
        //All aggregate options (map, flatMap, filter ...) supported by normal Java stream can be supported.
        Map<String, Long> result = eventStream
                .peek(s -> System.out.println(s)) // peek
                .collect(Collectors.groupingBy(
                        Function.identity(), Collectors.counting()
                ));

        assertEquals("", 100L, result.get("event-1").longValue());
        assertEquals("", 100L, result.get("event-2").longValue());
        assertEquals("", 100L, result.get("event-3").longValue());
        assertEquals("", 100L, result.get("event-4").longValue());
    }

    @Test
    public void testEventStreamCollector() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(controllerURI)
                                                      .build();
        final String scope = "test";
        final String stream = "test-" + RandomStringUtils.randomAlphanumeric(5);

        // Step 1
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        PravegaStream outputStream = new PravegaStream(clientConfig, scope, stream + "result");
        UTF8StringSerializer serializer = new UTF8StringSerializer();

        PravegaStream str = new PravegaStream(clientConfig, scope, stream);
        PravegaStream filteredStream = str.getUnorderedEventStream(serializer)
                                          .peek(s -> System.out.println(s))
                                          .filter(s -> s.contains("event-0") || s.contains("event-1"))
                                          .collect(new PravegaStreamCollector<>(outputStream, s -> s.split("-")[1], // find routing key
                                                  serializer, StreamConfiguration.builder().build()));

        // Verification
        assertEquals("Expected to read 100 events of type event-0 and event-1", 200L,
                filteredStream.getUnorderedEventStream(serializer).count());

        Map<String, Long> result = filteredStream.getUnorderedEventStream(serializer)
                                                 .collect(Collectors.groupingBy(
                                                         Function.identity(), Collectors.counting()
                                                 ));
        assertNull(result.get("event-2"));
        assertNull(result.get("event-3"));
        assertNull(result.get("event-3"));
        assertEquals(100L, result.get("event-0")
                                 .longValue());
        assertEquals(100L, result.get("event-1")
                                 .longValue());
    }

    @Test
    public void testOrderedEventStream() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(controllerURI)
                                                      .build();
        final String scope = "test";
        final String stream = "test-" + RandomStringUtils.randomAlphabetic(5);

        // Step 1
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        PravegaStream str = new PravegaStream(clientConfig, scope, stream);
        //All aggregate options (map, flatMap, filter ...) supported by normal Java stream can be supported.
        Map<String, Long> result = str.getOrderedEventStream(new UTF8StringSerializer())
                                      .peek(s -> System.out.println(s)) // peek
                                      .collect(Collectors.groupingBy(
                                              Function.identity(), Collectors.counting()
                                      ));

        assertEquals("", 100L, result.get("event-1").longValue());
        assertEquals("", 100L, result.get("event-2").longValue());
        assertEquals("", 100L, result.get("event-3").longValue());
        assertEquals("", 100L, result.get("event-4").longValue());
    }

    @Test
    public void testOrderedByteStream() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(controllerURI)
                                                      .build();
        final String scope = "test";
        final String stream = "test-" + RandomStringUtils.randomAlphabetic(5);

        // Step 1: Write data
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        // Step 2: Create a Pravega Stream
        PravegaStream str = new PravegaStream(clientConfig, scope, stream);
        // read a ByteStream.
        //All aggregate options (map, flatMap, filter ...) supported by normal Java stream can be supported.
        Map<String, Long> result = str.getOrderedByteStream()
                                      .map(bytes -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes))
                                                                          .toString()) // decode it.
                                      .peek(s -> System.out.println(s)) // peek
                                      .collect(Collectors.groupingBy(
                                              Function.identity(), Collectors.counting()
                                      ));
        assertEquals("", 100L, result.get("event-1").longValue());
        assertEquals("", 100L, result.get("event-2").longValue());
        assertEquals("", 100L, result.get("event-3").longValue());
        assertEquals("", 100L, result.get("event-4").longValue());
    }

    @Test
    public void testOrderedEventStreamCollector() {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(controllerURI)
                                                      .build();
        final String scope = "test";
        final String stream = "test-" + RandomStringUtils.randomAlphanumeric(5);

        // Step 1
        createAndWriteToStream(controllerURI, clientConfig, scope, stream);

        PravegaStream str = new PravegaStream(clientConfig, scope, stream);
        PravegaStream outputStream = new PravegaStream(clientConfig, scope, stream + "result");
        UTF8StringSerializer serializer = new UTF8StringSerializer();

        PravegaStream filteredStream = str.getOrderedEventStream(serializer)
                                          .peek(s -> System.out.println(s))
                                          .filter(s -> s.contains("event-0") || s.contains("event-1"))
                                          .collect(new PravegaStreamCollector<>(outputStream, s -> s.split("-")[1],
                                                  serializer, StreamConfiguration.builder().build()));

        // Verification
        assertEquals("Expected to read 100 events of type event-0 and event-1", 200L, filteredStream.getUnorderedEventStream(serializer)
                                                                                                    .count());

        Map<String, Long> result = filteredStream.getUnorderedEventStream(serializer)
                                                 .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()
                                                 ));
        assertNull(result.get("event-2"));
        assertNull(result.get("event-3"));
        assertNull(result.get("event-3"));
        assertEquals(100L, result.get("event-0").longValue());
        assertEquals(100L, result.get("event-1").longValue());
    }

    @Test
    public void testOrderedEventStreamCollectorWithStreamScaling() throws Exception {
        final URI controllerURI = URI.create("tcp://localhost:9090");
        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(controllerURI)
                                                      .build();
        final String scope = "test";
        final String stream = "test-" + RandomStringUtils.randomAlphanumeric(5);

        // Step 1
        createAndWriteToStreamWithScale(controllerURI, clientConfig, io.pravega.client.stream.Stream.of(scope, stream));

        PravegaStream outputStream = new PravegaStream(clientConfig, scope, stream + "result");
        UTF8StringSerializer serializer = new UTF8StringSerializer();

        PravegaStream str = new PravegaStream(clientConfig, scope, stream);
        // verify if event 0 is the first to be read.
        Map<String, Long> result = str.getOrderedEventStream(serializer)
                                      .limit(100)
                                      .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        // Since ordering guarantees are maintained verify if event-0 is the only event read.
        assertEquals(100L, result.get("event-0").longValue());

        // Verify filtering capability.
        PravegaStream filteredStream = str.getOrderedEventStream(serializer)
                                          .peek(s -> System.out.println(s))
                                          .filter(s -> s.contains("event-0") || s.contains("event-1"))
                                          .collect(new PravegaStreamCollector<>(outputStream, s -> s.split("-")[1],
                                                  serializer, StreamConfiguration.builder().build()));
        assertEquals("Expected to read 100 events of type event-0 and event-1", 200L,
                filteredStream.getOrderedEventStream(serializer).count());
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
        streamManager.createStream(scope, stream, StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(5))
                                                                     .build());

        EventStreamWriter<byte[]> writer = cf.createEventWriter(stream, new ByteArraySerializer(), EventWriterConfig.builder()
                                                                                                                    .build());
        // write to segment 0
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.1"), "event-0".getBytes(StandardCharsets.UTF_8));
        }
        // write to segment 1
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.3"), "event-1".getBytes(StandardCharsets.UTF_8));
        }
        // write to segment 2
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.5"), "event-2".getBytes(StandardCharsets.UTF_8));
        }
        // write to segment 3
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.7"), "event-3".getBytes(StandardCharsets.UTF_8));
        }
        // write to segment 4
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.8"), "event-4".getBytes(StandardCharsets.UTF_8));
        }
        writer.flush();
    }

    private static void createAndWriteToStreamWithScale(URI controllerURI, ClientConfig clientConfig, io.pravega.client.stream.Stream stream) throws ExecutionException, InterruptedException {
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


        EventStreamClientFactory cf = EventStreamClientFactory.withScope(stream.getScope(), clientConfig);
        ScheduledExecutorService controlerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(2, "controller-client");
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                                                                           .clientConfig(clientConfig)
                                                                           .maxBackoffMillis(5000)
                                                                           .build(), controlerExecutor);
        // create scope and stream
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(stream.getScope());
        streamManager.createStream(stream.getScope(), stream.getStreamName(), StreamConfiguration.builder()
                                                                                                 .scalingPolicy(ScalingPolicy.fixed(1))
                                                                                                 .build());

        EventStreamWriter<byte[]> writer = cf.createEventWriter(stream.getStreamName(), new ByteArraySerializer(), EventWriterConfig.builder()
                                                                                                                                    .build());
        // write to segment 0
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.1"), "event-0".getBytes(StandardCharsets.UTF_8));
        }
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);

        Boolean scaleResult = controller.scaleStream(stream, Collections.singletonList(0L), map, controlerExecutor)
                                        .getFuture()
                                        .get();
        assertTrue("Manual scale of stream did not complete", scaleResult);

        // write to segment 1.#epoch.1
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.1"), "event-1".getBytes(StandardCharsets.UTF_8));
        }
        // write to segment 2.#epoch.2
        for (int i = 0; i < 100; i++) {
            writer.writeEvent(keyReverseMap.get("0.7"), "event-2".getBytes(StandardCharsets.UTF_8));
        }
        writer.flush();
    }
}
