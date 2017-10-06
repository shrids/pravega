/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

import io.pravega.client.stream.notifications.events.CustomEvent;
import io.pravega.client.stream.notifications.events.ScaleEvent;
import io.pravega.client.stream.notifications.notifier.CustomEventNotifier;
import io.pravega.client.stream.notifications.notifier.ScaleEventNotifier;

public class TestClass {

    public static void main(String[] args) {
        NotificationSystem system = NotificationSystem.INSTANCE;
        ScaleEventNotifier notification = new ScaleEventNotifier();

        notification.addListener(event -> {
            int numReader = event.getNumOfReaders();
            int segments = event.getNumOfSegments();
            if (numReader < segments) {
                System.out.println("Scale up number of readers based on my capacity");
            } else {
                System.out.println("More readers available time to shut down some");
            }
        });

        CustomEventNotifier customNotify = new CustomEventNotifier();
        customNotify.addListener(event -> {
            System.out.println("Custom Event notified");
        });

        //
        system.notify(ScaleEvent.builder().numOfReaders(3).numOfSegments(5).build());
        system.notify(new CustomEvent());
    }
}
