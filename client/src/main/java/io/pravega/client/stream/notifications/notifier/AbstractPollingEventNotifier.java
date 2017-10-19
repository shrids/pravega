/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications.notifier;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.events.Event;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractPollingEventNotifier<T extends Event> extends AbstractEventNotifier<T> {

    @GuardedBy("$lock")
    StateSynchronizer<ReaderGroupState> synchronizer;
    @GuardedBy("$lock")
    private ScheduledFuture<?> pollingTaskFuture;
    private final Supplier<StateSynchronizer<ReaderGroupState>> synchronizerSupplier;
    private final AtomicBoolean pollingStarted = new AtomicBoolean();

    AbstractPollingEventNotifier(final NotificationSystem notifySystem, final ScheduledExecutorService executor,
                                 final Supplier<StateSynchronizer<ReaderGroupState>> synchronizerSupplier) {
        super(notifySystem, executor);
        this.synchronizerSupplier = synchronizerSupplier;
    }

    @Override
    @Synchronized
    public void unregisterListener(final Listener<T> listener) {
        super.unregisterListener(listener);
        if (!notifySystem.isListenerPresent(getType())) {
            cancelScheduledTask();
            synchronizer.close();
        }
    }

    @Override
    @Synchronized
    public void unregisterAllListeners() {
        super.unregisterAllListeners();
        cancelScheduledTask();
        synchronizer.close();
    }

    void cancelScheduledTask() {
        log.debug("Cancel the scheduled task to check");
        if (pollingTaskFuture != null) {
            pollingTaskFuture.cancel(true);
        }
        pollingStarted.set(false);
    }

    void startPolling(final Runnable pollingTask, int pollingIntervalSeconds) {
        if (!pollingStarted.getAndSet(true)) { //schedule the  only once
            synchronizer = synchronizerSupplier.get();
            pollingTaskFuture = executor.scheduleAtFixedRate(pollingTask, 0, pollingIntervalSeconds, TimeUnit.SECONDS);
        }
    }
}
