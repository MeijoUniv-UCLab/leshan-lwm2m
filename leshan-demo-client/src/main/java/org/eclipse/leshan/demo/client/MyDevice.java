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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.eclipse.leshan.client.resource.BaseInstanceEnabler;
import org.eclipse.leshan.client.servers.LwM2mServer;
import org.eclipse.leshan.core.Destroyable;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.model.ResourceModel.Type;
import org.eclipse.leshan.core.node.LwM2mResource;
import org.eclipse.leshan.core.request.BindingMode;
import org.eclipse.leshan.core.request.argument.Arguments;
import org.eclipse.leshan.core.response.ExecuteResponse;
import org.eclipse.leshan.core.response.ReadResponse;
import org.eclipse.leshan.core.response.WriteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyDevice extends BaseInstanceEnabler implements Destroyable {

    private static final Logger LOG = LoggerFactory.getLogger(MyDevice.class);

    private static final Random RANDOM = new Random();
    private static final List<Integer> supportedResources = Arrays.asList(0, 1, 2, 13, 16, 17, 23, 24, 25, 26, 27);

    // private final Timer timer;
    private String manufacturer;
    private String modelNumber;
    private String serialNumber;
    private String deviceType;
    private String location1;
    private String location2;
    private String location3;
    private String uri;
    private String managerID;

    public MyDevice() {
        // notify new date each 5 second
        // this.timer = new Timer("Device-Current Time");
        // timer.schedule(new TimerTask() {
        // @Override
        // public void run() {
        // fireResourceChange(13);
        // }
        // }, 5000, 5000);
    }

    public MyDevice(String manufacturer, String modelNumber, String serialNumber, String deviceType, String location1,
            String location2, String location3, String uri, String managerID) {
        this.manufacturer = manufacturer;
        this.modelNumber = modelNumber;
        this.serialNumber = serialNumber;
        this.deviceType = deviceType;
        this.location1 = location1;
        this.location2 = location2;
        this.location3 = location3;
        this.uri = uri;
        this.managerID = managerID;
    }

    @Override
    public ReadResponse read(LwM2mServer server, int resourceid) {
        if (!server.isSystem())
            LOG.info("Read on Device resource /{}/{}/{}", getModel().id, getId(), resourceid);
        switch (resourceid) {
        case 0:
            return ReadResponse.success(resourceid, getManufacturer());
        case 1:
            return ReadResponse.success(resourceid, getModelNumber());
        case 2:
            return ReadResponse.success(resourceid, getSerialNumber());
        case 3:
            return ReadResponse.success(resourceid, getFirmwareVersion());
        case 9:
            return ReadResponse.success(resourceid, getBatteryLevel());
        case 10:
            return ReadResponse.success(resourceid, getMemoryFree());
        case 11:
            Map<Integer, Long> errorCodes = new HashMap<>();
            errorCodes.put(0, getErrorCode());
            return ReadResponse.success(resourceid, errorCodes, Type.INTEGER);
        case 13:
            return ReadResponse.success(resourceid, getCurrentTime());
        case 14:
            return ReadResponse.success(resourceid, getUtcOffset());
        case 15:
            return ReadResponse.success(resourceid, getTimezone());
        case 16:
            return ReadResponse.success(resourceid, getSupportedBinding());
        case 17:
            return ReadResponse.success(resourceid, getDeviceType());
        case 18:
            return ReadResponse.success(resourceid, getHardwareVersion());
        case 19:
            return ReadResponse.success(resourceid, getSoftwareVersion());
        case 20:
            return ReadResponse.success(resourceid, getBatteryStatus());
        case 21:
            return ReadResponse.success(resourceid, getMemoryTotal());
        case 23:
            return ReadResponse.success(resourceid, getLocation1());
        case 24:
            return ReadResponse.success(resourceid, getLocation2());
        case 25:
            return ReadResponse.success(resourceid, getLocation3());
        case 26:
            return ReadResponse.success(resourceid, getUri());
        case 27:
            return ReadResponse.success(resourceid, getManagerID());
        default:
            return super.read(server, resourceid);
        }
    }

    @Override
    public ExecuteResponse execute(LwM2mServer server, int resourceid, Arguments arguments) {
        String withArguments = "";
        if (!arguments.isEmpty())
            withArguments = " with arguments " + arguments;
        LOG.info("Execute on Device resource /{}/{}/{} {}", getModel().id, getId(), resourceid, withArguments);

        if (resourceid == 4) {
            new Timer("Reboot Lwm2mClient").schedule(new TimerTask() {
                @Override
                public void run() {
                    getLwM2mClient().stop(true);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    getLwM2mClient().start();
                }
            }, 500);
        }
        return ExecuteResponse.success();
    }

    @Override
    public WriteResponse write(LwM2mServer server, boolean replace, int resourceid, LwM2mResource value) {
        LOG.info("Write on Device resource /{}/{}/{}", getModel().id, getId(), resourceid);

        switch (resourceid) {
        case 13:
            return WriteResponse.notFound();
        case 14:
            setUtcOffset((String) value.getValue());
            fireResourceChange(resourceid);
            return WriteResponse.success();
        case 15:
            setTimezone((String) value.getValue());
            fireResourceChange(resourceid);
            return WriteResponse.success();
        case 17:
            deviceType = (String) value.getValue();
            fireResourceChange(resourceid);
            return WriteResponse.success();
        case 23:
            location1 = (String) value.getValue();
            fireResourceChange(resourceid);
            return WriteResponse.success();
        case 24:
            location2 = (String) value.getValue();
            fireResourceChange(resourceid);
            return WriteResponse.success();
        case 25:
            location3 = (String) value.getValue();
            fireResourceChange(resourceid);
            return WriteResponse.success();
        case 26:
            WriteResponse writeResponse = setUri((String) value.getValue());
            fireResourceChange(resourceid);
            return writeResponse;
        default:
            return super.write(server, replace, resourceid, value);
        }
    }

    protected String getManufacturer() {
        return manufacturer;
    }

    protected String getModelNumber() {
        return modelNumber;
    }

    protected String getSerialNumber() {
        return serialNumber;
    }

    protected String getFirmwareVersion() {
        return "1.0.0";
    }

    protected long getErrorCode() {
        return 0;
    }

    protected int getBatteryLevel() {
        return RANDOM.nextInt(101);
    }

    protected long getMemoryFree() {
        return Runtime.getRuntime().freeMemory() / 1024;
    }

    private Date getCurrentTime() {
        return new Date();
    }

    private String utcOffset = new SimpleDateFormat("X").format(Calendar.getInstance().getTime());

    protected String getUtcOffset() {
        return utcOffset;
    }

    protected void setUtcOffset(String t) {
        utcOffset = t;
    }

    private String timeZone = TimeZone.getDefault().getID();

    protected String getTimezone() {
        return timeZone;
    }

    protected void setTimezone(String t) {
        timeZone = t;
    }

    protected String getSupportedBinding() {
        return BindingMode.toString(EnumSet.of(BindingMode.U, BindingMode.T));
    }

    protected String getDeviceType() {
        return deviceType;
    }

    protected String getHardwareVersion() {
        return "1.0.1";
    }

    protected String getSoftwareVersion() {
        return "1.0.2";
    }

    protected int getBatteryStatus() {
        return RANDOM.nextInt(7);
    }

    protected long getMemoryTotal() {
        return Runtime.getRuntime().totalMemory() / 1024;
    }

    protected String getLocation1() {
        return location1;
    }

    protected String getLocation2() {
        return location2;
    }

    protected String getLocation3() {
        return location3;
    }

    protected String getUri() {
        return uri;
    }

    protected String getManagerID() {
        return managerID;
    }

    // DDNS時のNotify用のURIの書き換え
    @Override
    public boolean setValue(String value) {
        if (!value.equals(uri)) {
            this.uri = value;
            return true;
        }
        return false;
    }

    @Override
    public ObjectModel getModel() {
        return this.model;
    }

    // Notify用のURIの書き換え
    protected WriteResponse setUri(String value) {
        if (!value.equals(uri)) {
            uri = value;
            return WriteResponse.success();
        }
        return WriteResponse.badRequest("URI same, not updated.");
    }

    @Override
    public List<Integer> getAvailableResourceIds(ObjectModel model) {
        return supportedResources;
    }

    @Override
    public void destroy() {
        // timer.cancel();
    }
}
