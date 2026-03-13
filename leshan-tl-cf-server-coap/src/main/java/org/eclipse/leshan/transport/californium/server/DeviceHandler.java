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
package org.eclipse.leshan.transport.californium.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.eclipse.leshan.core.link.Link;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.request.ObserveRequest;
import org.eclipse.leshan.core.request.ReadRequest;
import org.eclipse.leshan.core.response.AbstractLwM2mResponse;
import org.eclipse.leshan.core.response.ReadResponse;
import org.eclipse.leshan.server.LeshanServer;
import org.eclipse.leshan.server.registration.Registration;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * LwM2Mデバイスの登録・更新イベントを処理し，外部サーバ（DNS，データベース）との連携を行うハンドラークラス EventServletから呼び出され，以下の主要機能を提供する： 1. 標準LwM2Mデバイスの登録処理 2.
 * レガシーデバイス（ゲートウェイ経由）の登録処理 3. デバイス情報の外部サーバへの登録（DNS + データベース） 4. デバイス状態変化の監視（Observe）
 */
public class DeviceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DeviceHandler.class);

    // 設定定数
    /** LwM2Mリクエストのタイムアウト時間（ミリ秒） */
    private static final long REQUEST_TIMEOUT = 5000L;
    /** 外部プロセス通信用UNIXドメインソケットのパス */
    private static final String SOCKET_PATH = "/tmp/leshanServer-ExternalProcess.sock";
    /** 標準LwM2Mデバイスオブジェクトのパス */
    private static final String DEVICE_OBJECT_PATH = "/3/0";
    /** ゲートウェイオブジェクトを示すリンク文字列 */
    // private static final String GATEWAY_OBJECT_LINK = "</25/0>";

    // 外部プロセス通信のレスポンスステータス定数
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_ERROR = "error";
    private static final String STATUS_SKIPPED = "skipped";

    // LwM2Mリソース識別子定数
    /** デバイスのURIリソース */
    private static final int URI_RESOURCE = 26;
    /** センサー値リソース（温度，湿度等で共通） */
    private static final int SENSOR_VALUE_RESOURCE = 5700;

    // 正規表現パターン（パフォーマンス向上のためクラスレベルでプリコンパイル）
    /** ゲートウェイオブジェクトのインスタンス番号を抽出するパターン */
    private static final Pattern GATEWAY_OBJECT_PATTERN = Pattern.compile("</25/([^>]+)>");
    /** Observeのパスを抽出するパターン */
    private static final Pattern OBSERVATION_PATH_PATTERN = Pattern.compile("observation=.*\\[path=(/[^,]+),.*\\]");
    /** レスポンスからURI値を抽出するパターン */
    private static final Pattern URI_VALUE_PATTERN = Pattern.compile("value=([^,]+)");
    /** URI更新パスを判定するパターン */
    private static final Pattern URI_UPDATE_PATH_PATTERN = Pattern.compile("/3/.*/26");

    /** LwM2MデバイスオブジェクトのリソースIDから意味のあるフィールド名へのマッピング */
    private static final Map<Integer, String> RESOURCE_ID_TO_FIELD_MAP = createResourceIdMapping();

    // コア機能コンポーネント
    /** JSONデータの変換処理を行うマッパー */
    private final ObjectMapper mapper;
    /** LwM2Mサーバインスタンス */
    private final LeshanServer server;

    // データストレージ（スレッドセーフなコレクション）
    /** デバイスのシリアル番号とURI情報のマッピング（DDNSの変更検出用） */
    private final ConcurrentHashMap<String, String> deviceUris = new ConcurrentHashMap<>(100000);
    /** 処理済みレガシーデバイスの重複チェック用マップ */
    private final ConcurrentHashMap<String, Boolean> processedLegacyDevices = new ConcurrentHashMap<>(100000);
    /** エンドポイント毎の登録情報を保存するマップ（更新検知用） */
    private final static Map<String, EndpointData> endpointsData = new ConcurrentHashMap<>();

    // 外部プロセス通信コンポーネント
    /** UNIXドメインソケット接続 */
    private AFUNIXSocket clientSocketUnix;
    /** 外部プロセスへの出力ストリーム */
    private PrintWriter out;
    /** 外部プロセスからの入力ストリーム */
    private BufferedReader in;

    /**
     * LwM2MデバイスオブジェクトのリソースIDから対応するフィールド名へのマッピングを作成 LwM2M仕様のDevice Object（Object ID: 3）の標準リソースIDを使用
     *
     * @return リソースID → フィールド名のマッピング
     */
    private static Map<Integer, String> createResourceIdMapping() {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "Manufacturer"); // 製造業者名
        map.put(1, "ModelNumber"); // モデル番号
        map.put(2, "SerialNumber"); // シリアル番号
        map.put(17, "DeviceType"); // デバイスタイプ
        map.put(23, "Location1"); // 位置情報1
        map.put(24, "Location2"); // 位置情報2
        map.put(25, "Location3"); // 位置情報3
        map.put(26, "URI"); // デバイスURI
        map.put(27, "ManagerID"); // 管理者ID
        return map;
    }

    /**
     * DeviceHandlerのコンストラクタ 初期化時に外部プロセスとの通信接続を確立
     */
    public DeviceHandler(ObjectMapper mapper, LeshanServer server) {
        this.mapper = mapper;
        this.server = server;
        // 外部プロセス（DNS，データベース処理）との通信を初期化
        connectToExternalProcesswithUnix();
    }

    /**
     * 外部プロセス（DNS・データベース処理）とUNIXドメインソケットで通信接続を確立 外部プロセスはDNS登録とOpensearchへのデータ保存を担当
     */
    public void connectToExternalProcesswithUnix() {
        try {
            File socketFile = new File(SOCKET_PATH);
            clientSocketUnix = AFUNIXSocket.newInstance();
            clientSocketUnix.connect(AFUNIXSocketAddress.of(socketFile));
            setupIOStreams(clientSocketUnix.getInputStream(), clientSocketUnix.getOutputStream());
            LOG.info("Successfully connected to external server via UNIX socket");
        } catch (IOException e) {
            LOG.error("Failed to connect via UNIXDomainSocket", e);
        }
    }

    /**
     * UNIXソケットの入出力ストリームを設定
     */
    private void setupIOStreams(InputStream inputStream, OutputStream outputStream) throws IOException {
        out = new PrintWriter(outputStream, true); // 自動フラッシュ有効
        in = new BufferedReader(new InputStreamReader(inputStream));
    }

    /**
     * デバイスの状態変化（登録・更新）に応じて適切な処理を実行
     *
     * @param registration LwM2Mデバイスの登録情報
     * @param state デバイスの状態（"registered" or "updated"）
     * @param changeObjectLinks オブジェクトリンクの変更があるかどうか，つまり，新しいレガシーデバイスが追加されたかどうか（updatedイベントの場合）
     */
    public void process(Registration registration, String state, Boolean changeObjectLinks) {
        switch (state) {
        case "registered":
            // 新規デバイス登録処理
            handleDeviceRegistration(registration);
            break;
        case "updated":
            // デバイス情報更新処理（主にレガシーデバイス追加）
            handleDeviceUpdate(registration, changeObjectLinks);
            break;
        default:
            LOG.warn("Unknown device state received: " + state);
            break;
        }
    }

    /**
     * 新規デバイス登録の処理 処理順序：エンドポイント保存 → デバイス情報読み取り → DNS登録 → DB保存
     *
     * @param registration 登録されたLwM2Mデバイス情報
     */
    private void handleDeviceRegistration(Registration registration) {
        LOG.info("Starting device registration process for endpoint: " + registration.getEndpoint());

        // Step 1: エンドポイント情報をローカルマップに保存（後の更新検知で使用）
        storeEndpointData(registration);

        // Step 2: LwM2Mデバイスオブジェクト（/3/0）から基本情報を読み取り
        JsonNode deviceInfo = readDeviceInformation(registration, null, false);
        if (deviceInfo == null) {
            LOG.error("Failed to read device information for endpoint: " + registration.getEndpoint());
            return;
        }

        // Step 3: 外部サーバ（DNS + データベース）への登録処理を実行
        registerDeviceInExternalServer(deviceInfo);
    }

    /**
     * エンドポイントの登録情報をローカルマップに保存 この情報は後のupdatedイベントで新しいオブジェクトリンクの検出に使用
     *
     * @param registration 保存する登録情報
     */
    private void storeEndpointData(Registration registration) {
        endpointsData.put(registration.getEndpoint(),
                new EndpointData(registration.getId(), registration.getObjectLinks()));
    }

    /**
     * デバイスを外部システム（DNS + データベース）に登録する処理 処理順序：登録可能性チェック → DNS登録 → URI保存 → DB保存
     *
     * @param deviceInfo 登録するデバイスの情報
     */
    private void registerDeviceInExternalServer(JsonNode deviceInfo) {
        String serialNumber = deviceInfo.get("SerialNumber").asText();
        String uri = deviceInfo.get("URI").asText();

        // 登録条件チェック（URI有効性 + 未登録チェック）
        if (!shouldRegisterDevice(uri, serialNumber)) {
            LOG.info("Device registration skipped - Serial: " + serialNumber + ", URI: " + uri);
            return;
        }

        // Step 1: DNS登録処理（外部サーバに依頼）
        JsonNode dnsRegisteredDeviceInfo = registerDeviceInDNS(deviceInfo);
        if (dnsRegisteredDeviceInfo == null) {
            LOG.error("DNS registration failed, aborting registration for device: " + serialNumber);
            return;
        }

        // Step 2: DNS登録結果のURI情報をローカルマップに保存
        saveDeviceURI(dnsRegisteredDeviceInfo);

        // Step 3: デバイス情報をデータベース（Opensearch）に保存
        saveDeviceInDatabase(dnsRegisteredDeviceInfo);
    }

    /**
     * デバイスを外部システムに登録すべきかどうかを判定 条件：URIが有効 かつ 未登録のデバイス（NoneはGatewayのURIに設定されており，登録しない）
     *
     * @param uri デバイスのURI
     * @param serialNumber デバイスのシリアル番号
     * @return 登録すべき場合true
     */
    private boolean shouldRegisterDevice(String uri, String serialNumber) {
        return !"None".equals(uri) && !deviceUris.containsKey(serialNumber);
    }

    /**
     * デバイスのDNS登録を外部プロセスに依頼
     *
     * @param deviceInfo 登録するデバイス情報
     * @return DNS登録後のデバイス情報（URIが更新される場合がある）
     */
    private JsonNode registerDeviceInDNS(JsonNode deviceInfo) {
        LOG.info("Requesting DNS registration for device: " + deviceInfo.get("SerialNumber").asText());
        return writeToExternalProcess("dns", deviceInfo);
    }

    /**
     * DNS登録後のデバイスURI情報をローカルマップに保存 このURIは後のNotify更新で変更検知のために使用
     *
     * @param deviceInfo DNS登録後のデバイス情報
     */
    private void saveDeviceURI(JsonNode deviceInfo) {
        String serialNumber = deviceInfo.get("SerialNumber").asText();
        String uri = deviceInfo.get("URI").asText();
        deviceUris.put(serialNumber, uri);
    }

    /**
     * デバイス情報をデータベース（Opensearch）に保存
     *
     * @param deviceInfo 保存するデバイス情報
     */
    private void saveDeviceInDatabase(JsonNode deviceInfo) {
        LOG.info("Requesting database save for device: " + deviceInfo.get("SerialNumber").asText());
        writeToExternalProcess("opensearch", deviceInfo);
    }

    /**
     * デバイス更新イベントの処理 主にゲートウェイデバイスに新しいレガシーデバイスが追加された場合の処理
     *
     * @param registration 更新された登録情報
     * @param changeObjectLinks オブジェクトリンクが変更されたかどうか
     */
    private void handleDeviceUpdate(Registration registration, Boolean changeObjectLinks) {
        // オブジェクトリンクの変更がない場合は処理不要，定期的なUpdateイベントである可能性が高く，新しいレガシーデバイスの追加がないためスキップ
        if (!Boolean.TRUE.equals(changeObjectLinks)) {
            LOG.debug("No object links changed, skipping update process");
            return;
        }

        // Step 1: 前回の登録と比較して新しいレガシーデバイスを検出
        List<String> newLegacyDevices = findNewLegacyDevices(registration);
        if (newLegacyDevices.isEmpty()) {
            LOG.info("No new legacy devices detected for endpoint: " + registration.getEndpoint());
            return;
        }

        // Step 2: 各新しいレガシーデバイスを順次登録処理
        for (String deviceInstance : newLegacyDevices) {
            handleLegacyDeviceRegistration(deviceInstance, registration);
        }
    }

    /**
     * デバイス情報の読み取り（標準・レガシー両対応） Device Object（/3/0）またはゲートウェイ経由でデバイス情報を取得し，外部システム登録用にフォーマット
     *
     * @param registration デバイスの登録情報
     * @param deviceInstance レガシーデバイスのインスタンス番号（標準デバイスの場合はnull）
     * @param isLegacy レガシーデバイスフラグ（true: レガシー，false: 標準）
     * @return フォーマット済みデバイス情報，失敗時はnull
     */
    private JsonNode readDeviceInformation(Registration registration, String deviceInstance, boolean isLegacy) {
        try {
            if (isLegacy) {
                // レガシーデバイス読み取り（deviceInstanceが必須）
                if (deviceInstance == null) {
                    LOG.error("Legacy device read requires device instance parameter");
                    return null;
                }
                return readLegacyDeviceInfo(deviceInstance, registration);
            } else {
                // 標準デバイス読み取り
                return readStandardDeviceInfo(registration);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * レガシーデバイス情報の読み取り処理 ゲートウェイオブジェクトを通じてレガシーデバイスの情報を取得し，監視を設定
     *
     * @param deviceInstance ゲートウェイ内のデバイスインスタンス番号
     * @param registration ゲートウェイデバイスの登録情報
     * @return 変換済みデバイス情報，失敗時はnull
     * @throws Exception
     */
    private JsonNode readLegacyDeviceInfo(String deviceInstance, Registration registration) throws Exception {
        // Step 1: ゲートウェイオブジェクト（/25/X）からメタ情報を取得
        String gatewayTarget = "/25/" + deviceInstance;
        JsonNode gatewayInfo = readObjectInformation(registration, gatewayTarget, server);
        if (gatewayInfo == null) {
            LOG.error("Failed to read gateway object for instance: " + deviceInstance);
            return null;
        }

        // Step 2: ゲートウェイ情報からレガシーデバイスのプレフィックスとオブジェクトリストを抽出
        String devicePrefix = extractPrefix(gatewayInfo);
        List<Integer> iotDeviceObjects = extractIotDeviceObjects(gatewayInfo);
        LOG.debug("Extracted device prefix: " + devicePrefix + ", objects: " + iotDeviceObjects.size());

        // Step 3: レガシーデバイスのリソース監視を設定（値変更検知用）
        startDeviceObservation(devicePrefix, iotDeviceObjects, registration);

        // Step 4: レガシーデバイスの実際のデバイス情報を読み取り
        String deviceTarget = devicePrefix + DEVICE_OBJECT_PATH; // 例："device1/3/0"
        JsonNode rawDeviceInfo = readObjectInformation(registration, deviceTarget, server);
        if (rawDeviceInfo == null) {
            LOG.error("Failed to read device object for prefix: " + devicePrefix);
            return null;
        }

        // Step 5: 生データを外部システム用フォーマットに変換
        return convertToDeviceInfo(rawDeviceInfo);
    }

    /**
     * 標準LwM2Mデバイスの情報読み取り Device Object（/3/0）から直接デバイス情報を取得
     *
     * @param registration デバイスの登録情報
     * @return 変換済みデバイス情報，失敗時はnull
     * @throws Exception
     */
    private JsonNode readStandardDeviceInfo(Registration registration) throws Exception {
        // LwM2M Device Object（/3/0）から情報を取得
        JsonNode rawDeviceInfo = readObjectInformation(registration, DEVICE_OBJECT_PATH, server);
        if (rawDeviceInfo == null) {
            LOG.error("Failed to read standard device object for endpoint: " + registration.getEndpoint());
            return null;
        }

        // 生データを外部システム用フォーマットに変換
        return convertToDeviceInfo(rawDeviceInfo);
    }

    /**
     * ゲートウェイ情報からレガシーデバイスのプレフィックスを抽出 プレフィックスはレガシーデバイスへのアクセスパスで使用
     *
     * @param gatewayInfo ゲートウェイオブジェクトから読み取った情報
     * @return レガシーデバイスのプレフィックス
     */
    private String extractPrefix(JsonNode gatewayInfo) {
        // ゲートウェイオブジェクトのリソースID 1（Prefix）から値を取得
        return gatewayInfo.get("content").get("resources").get(1).get("value").asText();
    }

    /**
     * ゲートウェイ情報からレガシーデバイスがサポートするIoTデバイスオブジェクトのリストを抽出
     *
     * @param gatewayInfo ゲートウェイオブジェクトから読み取った情報
     * @return IoTデバイスオブジェクトIDのリスト
     */
    private List<Integer> extractIotDeviceObjects(JsonNode gatewayInfo) {
        // ゲートウェイオブジェクトのリソースID 2（IoTDeviceObjectList）から値を取得
        String ioTDeviceObjectList = gatewayInfo.get("content").get("resources").get(2).get("value").asText();
        // フォーマット例："<3301>,<3303>,<3315>" → [3301, 3303, 3315]
        return Arrays.stream(ioTDeviceObjectList.replaceAll("[</>]", "").split(",")).map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    /**
     * ゲートウェイデバイスに新しく追加されたレガシーデバイスを検出 前回の登録時と現在のオブジェクトリンクを比較して新規追加を検出
     *
     * @param registration 現在の登録情報
     * @return 新しく追加されたレガシーデバイスのインスタンス番号リスト
     */
    private static List<String> findNewLegacyDevices(Registration registration) {
        List<String> newDevices = new ArrayList<>();

        // Step 1: 前回の登録時のオブジェクトリンクを取得
        Link[] previousLinks = getPreviousRegistrationLinks(registration.getEndpoint());
        if (previousLinks == null) {
            LOG.debug("No previous registration data found, treating as initial registration");
            return newDevices; // 初回登録の場合は新しいデバイスなし
        }

        // Step 2: 前回のリンクを文字列セットに変換
        Set<String> previousLinkStrings = createLinkStringSet(previousLinks);

        // Step 3: 現在のリンクと比較して新しいゲートウェイ配下のレガシーデバイスを検出
        newDevices = findNewLegacyDevice(registration.getObjectLinks(), previousLinkStrings);

        // Step 4: 現在の登録情報でローカルデータを更新（次回の比較用）
        updateEndpointData(registration);

        LOG.info("Detected " + newDevices.size() + " new legacy devices for endpoint: " + registration.getEndpoint());
        return newDevices;
    }

    /**
     * 指定されたエンドポイントの前回登録時のオブジェクトリンクを取得
     *
     * @param endpoint エンドポイント名
     * @return 前回のオブジェクトリンク配列，ない場合null
     */
    private static Link[] getPreviousRegistrationLinks(String endpoint) {
        EndpointData endpointData = endpointsData.get(endpoint);
        return endpointData != null ? endpointData.getLinks() : null;
    }

    /**
     * オブジェクトリンク配列を文字列セットに変換 高速な重複チェックのためにSetを使用
     *
     * @param links オブジェクトリンク配列
     * @return リンク文字列のセット
     */
    private static Set<String> createLinkStringSet(Link[] links) {
        return Arrays.stream(links).map(Link::toString).collect(Collectors.toSet());
    }

    /**
     * 現在のオブジェクトリンクから新しいゲートウェイデバイスを検出 ゲートウェイオブジェクト（/25/X）のパターンにマッチする新しいリンクを検索
     *
     * @param currentLinks 現在のオブジェクトリンク配列
     * @param previousLinkStrings 前回のリンク文字列セット
     * @return 新しいレガシーデバイスのインスタンス番号リスト
     */
    private static List<String> findNewLegacyDevice(Link[] currentLinks, Set<String> previousLinkStrings) {
        List<String> newDevices = new ArrayList<>();

        // 現在の全リンクをループでチェック
        for (Link currentLink : currentLinks) {
            String linkString = currentLink.toString();
            // 前回にはなかったリンクのみを処理（新規追加のみ）
            if (!previousLinkStrings.contains(linkString)) {
                // ゲートウェイオブジェクトのパターンでインスタンス番号を抽出
                String deviceInstance = extractGatewayDeviceInstance(linkString);
                if (deviceInstance != null) {
                    newDevices.add(deviceInstance);
                }
            }
        }
        return newDevices;
    }

    /**
     * リンク文字列からゲートウェイデバイスのインスタンス番号を抽出 パターン例："</25/1>" → "1"
     *
     * @param linkString オブジェクトリンクの文字列表現
     * @return ゲートウェイデバイスの場合はインスタンス番号，そうでない場合null
     */
    private static String extractGatewayDeviceInstance(String linkString) {
        Matcher matcher = GATEWAY_OBJECT_PATTERN.matcher(linkString);
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * エンドポイントデータを現在の登録情報で更新 次回の更新イベントで新しいデバイス検出に使用
     *
     * @param registration 更新する登録情報
     */
    private static void updateEndpointData(Registration registration) {
        endpointsData.put(registration.getEndpoint(),
                new EndpointData(registration.getId(), registration.getObjectLinks()));
    }

    /**
     * 単一レガシーデバイスの登録処理 レガシーデバイスはゲートウェイ経由で接続される非LwM2Mデバイス 処理順序：重複チェック → デバイス情報読み取り → 外部システム登録
     *
     * @param deviceInstance ゲートウェイ内のデバイスインスタンス番号
     * @param registration ゲートウェイデバイスの登録情報
     */
    private void handleLegacyDeviceRegistration(String deviceInstance, Registration registration) {
        String regId = registration.getId();

        // 重複処理チェック
        if (isDeviceAlreadyProcessed(regId, deviceInstance)) {
            return;
        }

        try {
            // 1. レガシーデバイス情報を読み取り
            JsonNode deviceInfo = readDeviceInformation(registration, deviceInstance, true);
            if (deviceInfo == null) {
                LOG.error("Failed to read legacy device information for instance: " + deviceInstance);
                return;
            }

            // 2. デバイスをDNSに登録し，DBに保存
            registerDeviceInExternalServer(deviceInfo);
        } catch (Exception e) {
            LOG.error("Error processing legacy device " + deviceInstance + " for registration "
                    + registration.getEndpoint(), e);
        }
    }

    /**
     * レガシーデバイスが既に処理済みかどうかをチェック 登録ID + デバイスインスタンスの組み合わせで一意性を保証
     *
     * @param regId 登録ID
     * @param deviceInstance デバイスインスタンス番号
     * @return 既に処理済みの場合true
     */
    private boolean isDeviceAlreadyProcessed(String regId, String deviceInstance) {
        String uniqueDeviceId = regId + "-" + deviceInstance;
        // ConcurrentHashMap.putIfAbsentでアトミックな重複チェックと登録を実行
        if (processedLegacyDevices.putIfAbsent(uniqueDeviceId, Boolean.TRUE) != null) {
            LOG.info("Device already processed, skipping: " + uniqueDeviceId);
            return true;
        }
        return false;
    }

    /**
     * 指定されたターゲットからLwM2Mオブジェクト情報を読み取り（標準・レガシー共通） TLV形式でReadRequestを送信し，JSON形式でレスポンスを取得
     *
     * @param registration デバイス登録情報
     * @param target 読み取りターゲットパス
     * @param server LwM2Mサーバーインスタンス
     * @return 読み取ったJSONデータ
     * @throws Exception 読み取りに失敗した場合
     */
    private JsonNode readObjectInformation(Registration registration, String target, LeshanServer server)
            throws Exception {
        // TLV形式でReadRequestを作成
        ReadRequest request = new ReadRequest(ContentFormat.TLV, target);
        // 指定されたタイムアウト時間でリクエストを送信
        ReadResponse response = server.send(registration, request, REQUEST_TIMEOUT);
        // レスポンスをJSONに変換して返す
        return mapper.readTree(mapper.writeValueAsString(response));
    }

    /**
     * LwM2Mオブジェクトの生データを外部システム登録用のフォーマットに変換
     *
     * @param rawInfo LwM2Mオブジェクトから読み取った生データ
     * @return 外部システム用にフォーマットされたデバイス情報
     */
    private JsonNode convertToDeviceInfo(JsonNode rawInfo) {
        ObjectNode resultNode = mapper.createObjectNode();

        // リソースIDを意味のあるフィールド名に変換
        extractResourceFields(rawInfo, resultNode);
        // UUIDと公開設定のデフォルト値を追加
        addDefaultFields(resultNode);

        return resultNode;
    }

    /**
     * LwM2Mオブジェクトのリソース情報から必要なフィールドを抽出
     *
     * @param info LwM2Mオブジェクトの生データ
     * @param resultNode 結果を格納するJSONオブジェクト
     */
    private void extractResourceFields(JsonNode info, ObjectNode resultNode) {
        // 各リソースをループ処理してフィールドを抽出
        for (JsonNode resource : info.path("content").path("resources")) {
            int resourceId = resource.path("id").asInt();
            String fieldName = RESOURCE_ID_TO_FIELD_MAP.get(resourceId);
            // マッピングテーブルに存在するリソースIDのみを処理
            if (fieldName != null) {
                resultNode.put(fieldName, resource.path("value").asText());
            }
        }
    }

    /**
     * デバイス情報にデフォルトフィールドを追加 外部システム登録で必要な標準フィールドを設定
     *
     * @param resultNode デフォルト値を追加するJSONオブジェクト
     */
    private void addDefaultFields(ObjectNode resultNode) {
        // 一意識別子を生成
        resultNode.put("UUID", UUID.randomUUID().toString());
        // 公開設定をデフォルト値に設定
        resultNode.put("Public", "unset");
    }

    /**
     * 外部プロセス（DNS・データベース処理）との通信を実行 UNIXドメインソケット経由で依頼を送信し，結果を受信する
     *
     * @param order 処理指示（"dns" または "opensearch"）
     * @param deviceInfo 処理対象のデバイス情報
     * @return 外部プロセスからの応答データ，失敗時null
     */
    private JsonNode writeToExternalProcess(String order, JsonNode deviceInfo) {
        try {
            // リクエストデータにorder（処理指示）を追加
            JsonNode requestData = createExternalProcessRequest(order, deviceInfo);
            // UNIXソケット経由でリクエストを送信
            sendToExternalProcess(requestData);

            // 外部プロセスからの応答を受信
            String response = in.readLine();
            // レスポンスを解析してエラーハンドリングを実行
            return processExternalProcessResponse(response, order);
        } catch (IOException e) {
            LOG.error("External Server communication error for order: " + order, e);
            return null;
        }
    }

    /**
     * 外部プロセス用のリクエストデータを作成 デバイス情報のコピーに処理指示（order）を追加
     *
     * @param order 処理指示（"dns"または"opensearch"）
     * @param deviceInfo 元のデバイス情報
     * @return 外部プロセス用リクエストデータ
     */
    private JsonNode createExternalProcessRequest(String order, JsonNode deviceInfo) {
        // デバイス情報の深いコピーを作成（元データの変更を防ぐ）
        ObjectNode requestData = deviceInfo.deepCopy();
        // 処理指示フィールドを追加
        requestData.put("order", order);
        return requestData;
    }

    /**
     * リクエストデータを外部プロセスに送信 JSONデータを文字列にシリアライズしてUNIXソケットに送信
     *
     * @param requestData 送信するリクエストデータ
     * @throws IOException 通信エラーが発生した場合
     */
    private void sendToExternalProcess(JsonNode requestData) throws IOException {
        // JSONをシリアライズして外部プロセスに送信（改行で区切り）
        out.println(mapper.writeValueAsString(requestData));
    }

    /**
     * 外部プロセスからの応答を解析し，ステータスに応じた処理を実行
     *
     * @param response 外部プロセスからの応答文字列（JSON形式）
     * @param order 実行した処理指示
     * @return 処理済み応答データ，エラー時null
     * @throws IOException JSON解析エラーが発生した場合
     */
    private JsonNode processExternalProcessResponse(String response, String order) throws IOException {
        // 応答文字列をJSONオブジェクトに変換
        JsonNode responseJson = mapper.readTree(response);
        // ステータスフィールドを取得して結果を判定
        String status = responseJson.get("status").asText();

        switch (status) {
        case STATUS_ERROR:
            // 外部プロセスでエラーが発生した場合（DNS登録失敗等）
            LOG.error("External Server failed for order: " + order);
            return null;
        case STATUS_SUCCESS:
            // 正常に処理が完了した場合（DNS登録成功，DB保存成功）
            LOG.info("External Server succeeded for order: " + order);
            return removeStatusField(responseJson); // statusフィールドを除去してデータ部分のみ返す
        case STATUS_SKIPPED:
            // 処理がスキップされた場合（既存データ等）
            LOG.info("External Server skipped order: " + order);
            return removeStatusField(responseJson);
        default:
            // 予期しないステータスの場合
            LOG.warn("Unknown status from External Server: " + status);
            return null;
        }
    }

    /**
     * 外部プロセスの応答からstatusフィールドを除去 純粋なデバイスデータのみを返す
     *
     * @param responseJson 元の応答JSON
     * @return statusフィールドを除去した応答データ
     */
    private JsonNode removeStatusField(JsonNode responseJson) {
        // JSONをObjectNodeにキャストしてフィールド操作を可能にする
        ObjectNode objectNode = (ObjectNode) responseJson;
        // statusフィールドを削除（データ部分のみ残す）
        objectNode.remove("status");
        return objectNode;
    }

    /**
     * レガシーデバイスの各リソースに対してObserve（監視）を設定 センサー値の変化やURI更新を自動検知するため
     *
     * @param prefix レガシーデバイスのプレフィックス
     * @param iotDeviceObjects 監視対象のオブジェクトIDリスト
     * @param registration ゲートウェイデバイスの登録情報
     */
    private void startDeviceObservation(String prefix, List<Integer> iotDeviceObjects, Registration registration) {
        // 各IoTデバイスオブジェクトに対してObserveを設定
        for (Integer objectId : iotDeviceObjects) {
            // オブジェクトIDに対応する監視すべきリソースIDを取得
            int resourceId = getResourceIdForObject(objectId);
            if (resourceId == -1) {
                // サポートされていないオブジェクトはスキップ
                LOG.debug("Unsupported object ID for observation: " + objectId);
                continue;
            }

            // 監視ターゲットパスを構築（例：/d1/3301/0/5700）
            String observationTarget = buildLegacyObservationTarget(prefix, objectId, resourceId);
            // 実際のObserveリクエストを送信
            sendObserveRequest(registration, observationTarget);
        }
    }

    /**
     * オブジェクトIDに対応する監視すべきリソースIDを取得 LwM2M仕様に基づいてオブジェクト毎の重要リソースを特定
     *
     * @param objectId LwM2MオブジェクトID
     * @return 監視すべきリソースID，サポート外の場合-1
     */
    private int getResourceIdForObject(int objectId) {
        switch (objectId) {
        case 3:
            // Device Object（デバイス情報）→ URIリソース（26）を監視
            return URI_RESOURCE;
        case 3301: // Illuminance Sensor（照度センサー）
        case 3303: // Temperature Sensor（温度センサー）
        case 3304: // Humidity Sensor（湿度センサー）
        case 3315: // Barometer Sensor（気圧センサー）
        case 3316: // Sound Sensor（音センサー）
            // 各種センサーオブジェクト → Sensor Valueリソース（5700）を監視
            return SENSOR_VALUE_RESOURCE;
        default:
            // 未対応のオブジェクトID
            return -1;
        }
    }

    /**
     * レガシーデバイスのObserve監視用ターゲットパスを構築 形式：/{prefix}/{objectId}/0/{resourceId}
     *
     * @param prefix レガシーデバイスのプレフィックス
     * @param objectId LwM2MオブジェクトID
     * @param resourceId 監視するリソースID
     * @return 構築されたObserveターゲットパス
     */
    private String buildLegacyObservationTarget(String prefix, int objectId, int resourceId) {
        // レガシーデバイスパスの構築（例：/d1/3301/0/5700）
        // prefix: デバイス識別子，objectId: センサータイプ，0: インスタンス，resourceId: 監視値
        return "/" + prefix + "/" + objectId + "/0/" + resourceId;
    }

    /**
     * 指定されたターゲットに対してObserveリクエスト（監視開始）を送信 リソース値の変更を自動検知するためのLwM2M機能
     *
     * @param registration ゲートウェイデバイスの登録情報
     * @param target 監視するターゲットパス
     */
    private void sendObserveRequest(Registration registration, String target) {
        // TLV形式でObserveリクエストを作成
        ObserveRequest request = new ObserveRequest(ContentFormat.TLV, target);
        try {
            // 指定されたタイムアウト時間内でObserveリクエストを送信
            server.send(registration, request, REQUEST_TIMEOUT);
        } catch (InterruptedException e) {
            // スレッド割り込み時の適切な処理
            Thread.currentThread().interrupt();
            LOG.warn("Observation request interrupted for target: " + target);
        } catch (Exception e) {
            // その他の通信エラー
            LOG.error("Failed to observe target: " + target + " for registration " + registration.getEndpoint(), e);
        }
    }

    /**
     * Observe通知による値更新を処理 URIリソースの変更時にDNS更新情報を準備
     *
     * @param response LwM2Mレスポンス（Observe通知）
     * @param prefix レガシーデバイスのプレフィックス
     */
    public void update(AbstractLwM2mResponse response, String prefix) {
        // 応答文字列から観測パスを抽出
        String observationPath = extractObservationPath(response.toString());
        // URIリソース（26）の更新かどうかを判定し，該当する場合のみURI更新処理
        if (observationPath != null && isUriUpdate(observationPath)) {
            // デバイスURI情報を更新（DNS変更対応）
            updateDeviceURI(response.toString(), prefix);
        }
    }

    /**
     * LwM2M応答文字列からObservation（監視）パスを抽出 応答に含まれる観測パス情報を正規表現で取得
     *
     * @param responseString 応答の文字列表現
     * @return 抽出されたパス，見つからない場合null
     */
    private String extractObservationPath(String responseString) {
        // パターン例："observation=...[path=/device1/3/0/26,...]" → "/device1/3/0/26"
        Matcher matcher = OBSERVATION_PATH_PATTERN.matcher(responseString);
        return matcher.find() ? matcher.group(1) : null;
    }

    /**
     * 指定されたパスがURIリソースの更新かどうかを判定 URI（リソースID：26）の変更はDNS更新のトリガーとなる
     *
     * @param path 監視パス
     * @return URIの更新の場合true
     */
    private boolean isUriUpdate(String path) {
        // パターン例："/3/.*/26" にマッチする場合はURI更新
        return URI_UPDATE_PATH_PATTERN.matcher(path).matches();
    }

    /**
     * デバイスのURI情報を更新し，DNS変更情報を作成 URIリソース（26）の値が変更された場合に新しいURIでDNSを更新
     *
     * @param response Observe通知の応答文字列
     * @param prefix レガシーデバイスのプレフィックス
     */
    private void updateDeviceURI(String response, String prefix) {
        // 応答から新しいURI値を抽出
        String newUri = extractUriFromResponse(response);
        if (newUri != null) {
            // DNS更新用の情報を作成（旧URI + 新URI）
            createDnsUpdateInfo(prefix, newUri);
        } else {
            // URI情報が応答に含まれていない場合
            LOG.info("URI not found in legacy device response");
        }
    }

    /**
     * LwM2M応答文字列から新しいURI値を抽出 URIリソース（26）の新しい値を正規表現で取得
     *
     * @param response 応答の文字列表現
     * @return 抽出されたURI値，見つからない場合null
     */
    private String extractUriFromResponse(String response) {
        // パターン例："value=http://device1.example.com" → "http://device1.example.com"
        Matcher matcher = URI_VALUE_PATTERN.matcher(response);
        return matcher.find() ? matcher.group(1) : null;
    }

    /**
     * DNS更新用の情報を作成 旧URIから新URIへの変更をDNSシステムに通知するための準備
     *
     * @param prefix レガシーデバイスのプレフィックス
     * @param newUri 新しいURI値
     */
    private void createDnsUpdateInfo(String prefix, String newUri) {
        // DNS更新リクエスト用のJSONオブジェクトを作成
        ObjectNode ddnsInfo = mapper.createObjectNode();
        // 以前のURIを取得（ローカルマップから）
        ddnsInfo.put("oldUri", deviceUris.get(prefix));
        // 新しいURIを設定
        ddnsInfo.put("uriWithIp", newUri);
        // 実際のDNS更新処理
        writeToExternalProcess("ddns", ddnsInfo);
    }

    /**
     * エンドポイント毎の登録データを保持する内部クラス デバイス更新時の新しいオブジェクトリンク検出で前回のデータとの比較に使用
     */
    private static class EndpointData {
        /** 前回登録時のオブジェクトリンク配列 */
        private Link[] links;

        /**
         * エンドポイントデータのコンストラクタ
         *
         * @param value 登録ID（現在未使用だが，将来の拡張のため保持）
         * @param links 保存するオブジェクトリンク配列
         */
        public EndpointData(String value, Link[] links) {
            this.links = links;
        }

        /**
         * 保存されたオブジェクトリンク配列を取得
         *
         * @return オブジェクトリンク配列
         */
        public Link[] getLinks() {
            return links;
        }
    }
}
