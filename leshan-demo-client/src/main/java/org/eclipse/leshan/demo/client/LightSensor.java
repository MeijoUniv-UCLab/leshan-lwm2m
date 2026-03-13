/*******************************************************************************
 * Copyright (c) 2022    Sierra Wireless and others.
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
 *     Sierra Wireless - initial API and implementation
 *******************************************************************************/
package org.eclipse.leshan.demo.client;

import java.util.Arrays;
import java.util.List;

import org.eclipse.leshan.client.resource.BaseInstanceEnabler;
import org.eclipse.leshan.client.servers.LwM2mServer;
import org.eclipse.leshan.core.Destroyable;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.response.ReadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LightSensor extends BaseInstanceEnabler implements Destroyable {

    private static final Logger LOG = LoggerFactory.getLogger(LightSensor.class);

    private static final int SENSOR_VALUE = 5700;
    private static final List<Integer> supportedResources = Arrays.asList(SENSOR_VALUE);
    // private final ScheduledExecutorService scheduler;
    private double light;

    public LightSensor() {
        // this.scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("LightSensor"));
    }

    public LightSensor(String value) {
        light = Double.parseDouble(value);
    }

    @Override
    public synchronized ReadResponse read(LwM2mServer server, int resourceId) {
        LOG.info("Read on LightSensor resource /{}/{}/{}", getModel().id, getId(), resourceId);
        switch (resourceId) {
        case SENSOR_VALUE:
            return ReadResponse.success(resourceId, getLight());
        default:
            return super.read(server, resourceId);
        }
    }

    // @Override
    // public synchronized ExecuteResponse execute(ServerIdentity identity, int resourceId, Arguments arguments) {
    // LOG.info("Execute on Temperature resource /{}/{}/{}", getModel().id, getId(), resourceId);
    // switch (resourceId) {
    // case RESET_MIN_MAX_MEASURED_VALUES:
    // resetMinMaxMeasuredValues();
    // return ExecuteResponse.success();
    // default:
    // return super.execute(identity, resourceId, arguments);
    // }
    // }

    private double getLight() {
        return light;
    }

    @Override
    public List<Integer> getAvailableResourceIds(ObjectModel model) {
        return supportedResources;
    }

    @Override
    public void destroy() {
        // scheduler.shutdown();
    }

    @Override
    public boolean setValue(String value) {
        if (Double.parseDouble(value) != light) {
            this.light = Double.parseDouble(value);
            return true;
        }
        return false;
    }
}
