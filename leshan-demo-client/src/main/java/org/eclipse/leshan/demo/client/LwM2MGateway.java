/*******************************************************************************
 * Copyright (c) 2022    S.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.html.
 *
 * Contributors:
 *     S - initial API and implementation
 *******************************************************************************/
package org.eclipse.leshan.demo.client;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.eclipse.leshan.client.resource.SimpleInstanceEnabler;
import org.eclipse.leshan.core.Destroyable;
import org.eclipse.leshan.core.link.Link;
import org.eclipse.leshan.core.util.NamedThreadFactory;

public class LwM2MGateway extends SimpleInstanceEnabler implements Destroyable {
    private final ScheduledExecutorService scheduler;
    private String deviceID;
    private String prefix;
    private Link[] ioTDeviceObjects;

    public LwM2MGateway() {
        super();
        initialValues = new HashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("LwM2MGateway"));
    }

    public LwM2MGateway(String deviceID, String prefix, Link[] ioTDeviceObjects) {
        super();
        initialValues = new HashMap<>();
        this.deviceID = deviceID;
        this.prefix = prefix;
        this.ioTDeviceObjects = ioTDeviceObjects;

        initialValues.put(0, this.deviceID);
        initialValues.put(1, this.prefix);
        initialValues.put(3, this.ioTDeviceObjects);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("LwM2MGateway"));
    }

    @Override
    public void destroy() {
        scheduler.shutdown();
    }
}
