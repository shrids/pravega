/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services;

import io.pravega.test.system.framework.TestFrameworkException;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.utils.MarathonException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.pravega.test.system.framework.TestFrameworkException.Type.InternalError;

@Slf4j
public class HDFSService extends MarathonBasedService {

    private static final String HDFS_IMAGE = "dsrw/hdfs:2.7.3-1";
    //    private static final int HDFS_PORT = 8020;
    private int instances = 1;
    private double cpu = 1.0;
    private double mem = 1024.0;

    public HDFSService() {
        super("hdfs");
    }

    public HDFSService(final String id, int instances, double cpu, double mem) {
        super(id);
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    @Override
    public void start(final boolean wait) {
        deleteApp("hdfs");
        log.info("Starting HDFS Service: {}", getID());
        try {
            marathonClient.createApp(createHDFSApp());
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new TestFrameworkException(InternalError, "Exception while " +
                    "starting HDFS Service", ex);
        }
    }

    //This is a placeholder to perform clean up actions
    @Override
    public void clean() {
    }

    @Override
    public void stop() {
        log.info("Stopping HDFS Service : {}", getID());
        deleteApp(getID());
    }

    private App createHDFSApp() {
        App app = new App();
        app.setId(this.id);
        app.setCpus(cpu);
        app.setMem(mem);
        app.setInstances(instances);
        app.setContainer(new Container());
        app.getContainer().setType(CONTAINER_TYPE);
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage(HDFS_IMAGE);
        app.getContainer().getDocker().setNetwork(NETWORK_TYPE);

        return app;
    }
}
