/*******************************************************************************
 * Copyright (c) 2023    S.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.leshan.client.LeshanClient;
import org.eclipse.leshan.client.resource.LwM2mInstanceEnabler;
import org.eclipse.leshan.client.resource.LwM2mObjectEnabler;
import org.eclipse.leshan.client.resource.LwM2mObjectTree;
import org.eclipse.leshan.client.resource.ObjectsInitializer;
import org.eclipse.leshan.client.resource.SimpleInstanceEnabler;
import org.eclipse.leshan.client.servers.LwM2mServer;
import org.eclipse.leshan.core.link.Link;
import org.eclipse.leshan.core.link.lwm2m.MixedLwM2mLink;
import org.eclipse.leshan.core.model.LwM2mModelRepository;
import org.eclipse.leshan.core.model.ObjectModel;
import org.eclipse.leshan.core.model.StaticModel;
import org.eclipse.leshan.core.node.LwM2mObjectInstance;
import org.eclipse.leshan.core.node.LwM2mPath;
import org.eclipse.leshan.core.node.LwM2mSingleResource;
import org.eclipse.leshan.core.request.CreateRequest;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LegacyDeviceHandler extends SimpleInstanceEnabler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LegacyDeviceHandler.class);

    private LeshanClient client;
    private LwM2mModelRepository repository;
    private LwM2mServer lwM2mServer;
    private String prefix;

    // オブジェクト番号
    private final int device = 3;
    private final int gateway = 25;
    private final int light = 3301;
    private final int temperature = 3303;
    private final int humidity = 3304;
    private final int barometricPressure = 3315;
    private final int soundNoise = 3316;

    // オブジェクトが生成されているのかを判断するフラグ
    private AtomicBoolean gatewayFlag = new AtomicBoolean(true);

    private Map<Integer, Integer> resourceMappings = new HashMap<>();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicInteger legacyDeviceNum = new AtomicInteger(0);

    public LegacyDeviceHandler(LeshanClient client, LwM2mModelRepository repository) {
        super();
        try {
            // サーバへの登録待ち
            TimeUnit.SECONDS.sleep(2);
            this.lwM2mServer = client.getRegisteredServers().values().iterator().next();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.client = client;
        this.repository = repository;
        // Notify用のリソースマッピング
        resourceMappings.put(device, 26);
        resourceMappings.put(temperature, 5700);
        resourceMappings.put(humidity, 5700);
        resourceMappings.put(light, 5700);
        resourceMappings.put(barometricPressure, 5700);
        resourceMappings.put(soundNoise, 5700);
    }

    @Override
    public void run() {
        try {
            setupSocket();
        } catch (Exception e) {
            LOG.error("Error setting up socket: ", e);
            e.printStackTrace();
        }
    }

    private void setupSocket() throws IOException {
        String socketFilePath = "/tmp/unixSocket.sock";
        File socketFile = new File(socketFilePath);
        if (socketFile.exists()) {
            // 古いソケットファイルを削除
            if (!socketFile.delete()) {
                LOG.error("Failed to Delete the File: " + socketFilePath);
            }
        }
        AFUNIXSocketAddress socketAddress;
        try {
            socketAddress = AFUNIXSocketAddress.of(socketFile);
        } catch (SocketException e) {
            throw new IOException("Failed to create socket address", e);
        }

        try (AFUNIXServerSocket serverSocket = AFUNIXServerSocket.bindOn(socketAddress)) {
            LOG.info("Waiting for connection on " + socketFilePath + "...");

            while (!Thread.currentThread().isInterrupted()) {
                try (AFUNIXSocket sock = serverSocket.accept();
                        InputStream is = sock.getInputStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                    LOG.info("Connected on " + socketFilePath + ": " + sock);
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.contains("}")) {
                            try {
                                JsonNode jsonNode = objectMapper.readTree(line);
                                if (jsonNode instanceof com.fasterxml.jackson.databind.node.ObjectNode) {
                                    line = objectMapper.writeValueAsString(jsonNode);
                                }
                            } catch (Exception e) {
                                LOG.error("Failed to add receivedAt timestamp: ", e);
                            }
                            processRequest(line);
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Connection handling error: ", e);
                }
            }
        } catch (IOException e) {
            LOG.error("UnixDomainSocketServer error on " + socketFilePath + ": ", e);
            throw e;
        }
    }

    // 受信したデータを処理
    private void processRequest(String receiveData) {
        try {
            JsonNode jsonNode = objectMapper.readTree(receiveData);

            // 配列かどうかをチェック
            if (jsonNode.isArray()) {
                // 配列の場合、各要素を個別に処理
                for (JsonNode deviceNode : jsonNode) {
                    processDeviceInfo(deviceNode);
                }
            } else {
                // 単一オブジェクトの場合
                processDeviceInfo(jsonNode);
            }
        } catch (Exception e) {
            LOG.error("JSON processing error: ", e);
        }
    }

    // 個別のデバイス情報を処理
    private void processDeviceInfo(JsonNode deviceNode) {
        try {
            // デバイス情報を取り出す
            DeviceInfo deviceInfo = new DeviceInfo(deviceNode);

            if (gatewayFlag.compareAndSet(true, false)) {
                // GatewayObjectがなければ生成する
                createGatewayObject(deviceInfo);
            } else {
                // GatewayObjectがあればGatewayObjectInstanceを生成
                handleExistingGateway(deviceInfo);
            }
        } catch (Exception e) {
            LOG.error("Device info processing error: ", e);
        }
    }

    private void createGatewayObject(DeviceInfo info) {
        // GatewayObjectのリソース
        String deviceId = "Urn:dev:example:" + info.manufacturer + "-" + info.serialNumber;
        String prefix = info.serialNumber;
        Link[] ioTDeviceObject = new Link[] { new MixedLwM2mLink(null, new LwM2mPath(device)) };

        // レガシーデバイスのオブジェクト作成
        ObjectsInitializer objectsInitializer = new ObjectsInitializer(repository.getLwM2mModel());

        // デバイス情報を登録
        objectsInitializer.setInstancesForObject(device,
                new MyDevice(info.manufacturer, info.modelNumber, info.serialNumber, info.deviceType, info.location1,
                        info.location2, info.location3, info.uri, info.managerID));
        // 各センサデータを追加
        Map<Integer, LwM2mInstanceEnabler> sensorDataMap = new LinkedHashMap<>();
        if (info.light != null)
            sensorDataMap.put(light, new LightSensor(info.light));
        if (info.temperature != null)
            sensorDataMap.put(temperature, new TemperatureSensor(info.temperature));
        if (info.humidity != null)
            sensorDataMap.put(humidity, new HumiditySensor(info.humidity));
        if (info.barometricPressure != null)
            sensorDataMap.put(barometricPressure, new BarometricPressureSensor(info.barometricPressure));
        if (info.soundNoise != null)
            sensorDataMap.put(soundNoise, new SoundNoiseSensor(info.soundNoise));

        for (Map.Entry<Integer, LwM2mInstanceEnabler> entry : sensorDataMap.entrySet()) {
            objectsInitializer.setInstancesForObject(entry.getKey(), entry.getValue());

            // ioTDeviceObjectListに各センサオブジェクト番号を追加
            ioTDeviceObject = Arrays.copyOf(ioTDeviceObject, ioTDeviceObject.length + 1);
            ioTDeviceObject[ioTDeviceObject.length - 1] = new MixedLwM2mLink(null, new LwM2mPath(entry.getKey()));
        }

        List<LwM2mObjectEnabler> enablers = objectsInitializer.createAll();

        // GatwayObjectの作成
        ObjectModel objectModel = repository.getObjectModel(gateway);
        objectsInitializer = new ObjectsInitializer(new StaticModel(objectModel));
        objectsInitializer.setInstancesForObject(gateway, new LwM2MGateway(deviceId, prefix, ioTDeviceObject));

        LwM2mObjectEnabler object = objectsInitializer.create(gateway);

        client.getObjectTree().addObjectEnabler(object);
        client.createObjectTreeforGatewayObject(enablers, prefix);
        legacyDeviceNum.incrementAndGet();
    }

    // GatewayObjectInstanceの生成
    private void handleExistingGateway(DeviceInfo info) {
        prefix = info.serialNumber;
        LwM2mObjectTree objectTree = client.searchByPrefix(prefix);
        if (objectTree == null) {
            createGatewayObjInstance(info);
        } else if (objectTree != null) {
            updateGatewayObjInstance(objectTree, info);
        }
    }

    // GatewayObjectInstanceの生成
    private void createGatewayObjInstance(DeviceInfo info) {
        if (lwM2mServer != null) {
            // GatewayObjectのリソース
            String deviceId = "Urn:dev:example:" + info.manufacturer + "-" + info.serialNumber;
            String prefix = info.serialNumber;
            Link[] ioTDeviceObject = new Link[] { new MixedLwM2mLink(null, new LwM2mPath(device)) };

            // レガシーデバイスのオブジェクト作成
            ObjectsInitializer objectsInitializer = new ObjectsInitializer(repository.getLwM2mModel());
            // デバイス情報を登録
            objectsInitializer.setInstancesForObject(device,
                    new MyDevice(info.manufacturer, info.modelNumber, info.serialNumber, info.deviceType,
                            info.location1, info.location2, info.location3, info.uri, info.managerID));
            // 各センサデータを追加
            Map<Integer, LwM2mInstanceEnabler> sensorDataMap = new LinkedHashMap<>();
            if (info.light != null)
                sensorDataMap.put(light, new LightSensor(info.light));
            if (info.temperature != null)
                sensorDataMap.put(temperature, new TemperatureSensor(info.temperature));
            if (info.humidity != null)
                sensorDataMap.put(humidity, new HumiditySensor(info.humidity));
            if (info.barometricPressure != null)
                sensorDataMap.put(barometricPressure, new BarometricPressureSensor(info.barometricPressure));
            if (info.soundNoise != null)
                sensorDataMap.put(soundNoise, new SoundNoiseSensor(info.soundNoise));

            for (Map.Entry<Integer, LwM2mInstanceEnabler> entry : sensorDataMap.entrySet()) {
                objectsInitializer.setInstancesForObject(entry.getKey(), entry.getValue());

                // ioTDeviceObjectListに各センサオブジェクト番号を追加
                ioTDeviceObject = Arrays.copyOf(ioTDeviceObject, ioTDeviceObject.length + 1);
                ioTDeviceObject[ioTDeviceObject.length - 1] = new MixedLwM2mLink(null, new LwM2mPath(entry.getKey()));
            }

            List<LwM2mObjectEnabler> enablers = objectsInitializer.createAll();
            client.createObjectTreeforGatewayObject(enablers, prefix);

            // GatwayObjectInstanceの作成
            LwM2mObjectInstance LwM2MGatewayObjectInstance = new LwM2mObjectInstance(legacyDeviceNum.get(),
                    LwM2mSingleResource.newStringResource(0, deviceId),
                    LwM2mSingleResource.newStringResource(1, prefix),
                    LwM2mSingleResource.newCoreLinkResource(3, ioTDeviceObject));
            CreateRequest createRequestGateway = new CreateRequest(gateway, LwM2MGatewayObjectInstance);

            client.getObjectTree().getObjectEnabler(gateway).create(lwM2mServer, createRequestGateway);
            fireResourcesChange(new LwM2mPath(gateway, legacyDeviceNum.get()));
            legacyDeviceNum.incrementAndGet();
        }
    }

    // GatewayObjectInstance内のリソース情報を変更
    private void updateGatewayObjInstance(LwM2mObjectTree objectTree, DeviceInfo info) {
        if (lwM2mServer != null) {
            Map<Integer, String> valueMappings = new HashMap<>();
            valueMappings.put(device, info.uri);
            valueMappings.put(light, info.light);
            valueMappings.put(temperature, info.temperature);
            valueMappings.put(humidity, info.humidity);
            valueMappings.put(barometricPressure, info.barometricPressure);
            valueMappings.put(soundNoise, info.soundNoise);

            for (Map.Entry<Integer, Integer> entry : resourceMappings.entrySet()) {
                int objectId = entry.getKey();
                String value = valueMappings.get(objectId);

                if (value != null) {
                    if (objectTree.getObjectEnabler(objectId).getInstance(0).setValue(value)) {
                        // リソースの変更を通知
                        client.getEndpointsProvider().notify(info.serialNumber, String.valueOf(objectId));
                    }
                }
            }
        }
    }

    // デバイス情報を保持するための内部クラス
    private static class DeviceInfo {
        final String managerID, deviceType, manufacturer, modelNumber, serialNumber, location1, location2, location3,
                uri;
        // final boolean isPublic;

        String light, temperature, humidity, barometricPressure, soundNoise;

        DeviceInfo(JsonNode node) {
            try {
                this.managerID = getFieldSafely(node, "ManagerID");
                this.deviceType = getFieldSafely(node, "DeviceType");
                this.manufacturer = getFieldSafely(node, "Manufacturer");
                this.modelNumber = getFieldSafely(node, "ModelNumber");
                this.serialNumber = getFieldSafely(node, "SerialNumber");
                this.location1 = node.has("Location1") && node.get("Location1") != null ? node.get("Location1").asText()
                        : " ";
                this.location2 = node.has("Location2") && node.get("Location2") != null ? node.get("Location2").asText()
                        : " ";
                this.location3 = node.has("Location3") && node.get("Location3") != null ? node.get("Location3").asText()
                        : " ";
                this.uri = getFieldSafely(node, "URI");
            } catch (Exception e) {
                LOG.error("Error in DeviceInfo constructor with JSON: {}", node.toString(), e);
                throw e;
            }

            if (node.has("Light"))
                light = node.get("Light").asText();
            if (node.has("Temperature"))
                temperature = node.get("Temperature").asText();
            if (node.has("Humidity"))
                humidity = node.get("Humidity").asText();
            if (node.has("BarometricPressure"))
                barometricPressure = node.get("BarometricPressure").asText();
            if (node.has("SoundNoise"))
                soundNoise = node.get("SoundNoise").asText();
        }
    }

    private static String getFieldSafely(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null) {
            LOG.error("Missing required field: {}", fieldName);
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        if (fieldNode.isNull()) {
            LOG.error("Required field is null: {}", fieldName);
            throw new IllegalArgumentException("Required field is null: " + fieldName);
        }
        return fieldNode.asText();
    }
}
