/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Slf4j
public class EndToEndChannelLeakTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 40000)
    public void testDetectChannelLeakSegmentSealed() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope("test")
                                                        .streamName("test")
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        //Create a writer.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                connectionFactory);
        ReaderGroup readerGroup = groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints().
                build(), Collections.singleton("test"));
        //Create two readers.
        @Cleanup
        EventStreamReader<String> reader_1 = clientFactory.createReader("readerId1", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        System.out.println("===> 1 Active Channel Count: " + connectionFactory.getActiveChannelCount());
        //Write an event.
        writer.writeEvent("0", "zero").get();

        //Read an event.
        EventRead<String> event = reader_1.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("zero", event.getEvent());

        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0), map, executor).getFuture().get();
        assertTrue(result);

        //Write more events.
        writer.writeEvent("0", "one").get();
        writer.writeEvent("0", "two").get();
        writer.writeEvent("1", "three").get();

        int channelCount = connectionFactory.getActiveChannelCount(); //store the open channel count before reading.

        event = reader_1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        //Number of sockets will increase by 2 ( +3 for the new segments -1 since the older segment is sealed).
        assertEquals(channelCount + 2, connectionFactory.getActiveChannelCount());

        event = reader_1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        //no changes to socket count.
        assertEquals(channelCount + 2, connectionFactory.getActiveChannelCount());

        event = reader_1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        //no changes to socket count.
        assertEquals(channelCount + 2, connectionFactory.getActiveChannelCount());

    }

    @Test//(timeout = 30000)
    public void testDetectChannelLeakMultiReader() throws Exception {
        //TODO: in progress...
        StreamConfiguration config = StreamConfiguration.builder()
                .scope("test")
                .streamName("test1")
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        //Create a writer.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test1", new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                connectionFactory);
        ReaderGroup readerGroup = groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints().
                build(), Collections.singleton("test1"));

        //create a reader.
        @Cleanup
        EventStreamReader<String> reader_1 = clientFactory.createReader("readerId1", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        //Write an event.
        writer.writeEvent("0", "zero").get();
        System.out.println("===> 2 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        //Read an event.
        EventRead<String> event = reader_1.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("zero", event.getEvent());
        //this is +1 the socket count.
        System.out.println("===> 3 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        // scale
        Stream stream = new StreamImpl("test", "test1");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0), map, executor).getFuture().get();
        assertTrue(result);
        //same as 3
        System.out.println("===> 4 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        //Write more events.
        writer.writeEvent("0", "one").get();
        writer.writeEvent("0", "two").get();
        writer.writeEvent("1", "three").get();

        System.out.println("===> 5 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        //Add a new reader
        @Cleanup
        EventStreamReader<String> reader_2 = clientFactory.createReader("readerId2", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        System.out.println("===> 6 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        event = reader_1.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_1 is " + event.getEvent());
        System.out.println("===> 7 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint("chk1", executor); //create a check point to ensure rebalance of readers
        assertTrue(reader_2.readNextEvent(10000).isCheckpoint());
        TimeUnit.SECONDS.sleep(30);

        assertTrue(reader_1.readNextEvent(10000).isCheckpoint());
        System.out.println("===> 7a Active Channel Count: " + connectionFactory.getActiveChannelCount());
        event = reader_1.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_1 is " + event.getEvent());
        System.out.println("===> 8 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        event = reader_1.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_1 is " + event.getEvent());
        System.out.println("===> 9 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        event = reader_1.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_1 is " + event.getEvent());
        System.out.println("===> 10 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        event = reader_2.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_2 is " + event.getEvent());
        System.out.println("===> 11 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        event = reader_2.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_2 is " + event.getEvent());
        System.out.println("===> 12 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        event = reader_2.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_2 is " + event.getEvent());
        System.out.println("===> 13 Active Channel Count: " + connectionFactory.getActiveChannelCount());

        event = reader_2.readNextEvent(10000);
        assertNotNull(event);
        System.out.println("===> Data read by reader_2 is " + event.getEvent());
        System.out.println("===> 14 Active Channel Count: " + connectionFactory.getActiveChannelCount());
    }
}
