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
import java.util.function.Supplier;

import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.NotificationSystem;
import io.pravega.client.stream.notifications.events.EndOfDataEvent;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EndOfDataEventNotifier extends AbstractPollingEventNotifier<EndOfDataEvent> {
    private static final int UPDATE_INTERVAL_SECONDS = Integer.parseInt(
            System.getProperty("pravega.client.endOfDataEvent.poll.interval.seconds", String.valueOf(120)));

    public EndOfDataEventNotifier(final NotificationSystem notifySystem,
                                  final Supplier<StateSynchronizer<ReaderGroupState>> synchronizerSupplier,
                                  final ScheduledExecutorService executor) {
        super(notifySystem, executor, synchronizerSupplier);
    }

    @Override
    @Synchronized
    public void registerListener(final Listener<EndOfDataEvent> listener) {
        notifySystem.addListeners(getType(), listener, this.executor);
        //periodically check the for end of stream.
        startPolling(this::checkAndTriggerEndOfStreamNotification, UPDATE_INTERVAL_SECONDS);
    }

    @Override
    public String getType() {
        return EndOfDataEvent.class.getSimpleName();
    }

    private void checkAndTriggerEndOfStreamNotification() {
        this.synchronizer.fetchUpdates();
        ReaderGroupState state = this.synchronizer.getState();
        if (state.isEndOfData()) {
            notifySystem.notify(new EndOfDataEvent());
        }
    }
}
