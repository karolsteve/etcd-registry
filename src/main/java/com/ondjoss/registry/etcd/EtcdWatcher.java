package com.ondjoss.registry.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.EquivalentAddressGroup;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Steve Tchatchouang at 11/04/2023
 */

@Slf4j
public class EtcdWatcher {
    private static final String SERVICE_ROOT = "/test";

    private final Client client;
    private final String serviceName;
    private Watch.Watcher watcher;
    private final AtomicReference<List<NodeData>> currentServers = new AtomicReference<>(Collections.emptyList());
    private Delegate delegate;

    public static EtcdWatcher make(Client client, String serviceName, Delegate delegate) {
        EtcdWatcher result = new EtcdWatcher(client, serviceName);
        result.delegate = delegate;
        return result;
    }

    private EtcdWatcher(Client client, String serviceName) {
        //no instance
        this.client = client;
        this.serviceName = serviceName;
    }

    public void refresh() {
        resolve();
    }

    public interface Delegate {

        void onError(Throwable throwable);

        void onAddresses(List<NodeData> servers);
    }

    public void start() {
        watchKeys();
        resolve();
    }

    public void stop() {
        client.close();
        if (watcher != null)
            watcher.close();
    }

    private void resolve() {
        String serviceDir = serviceDir();
        log.info("resolve key called {}", serviceDir);
        ByteSequence prefix = ByteSequence.from(serviceDir, StandardCharsets.UTF_8);
        CompletableFuture<GetResponse> getFuture = client.getKVClient().get(prefix, GetOption.builder().isPrefix(true).build());

        getFuture.whenComplete((response, throwable) -> {
            if (throwable != null) {
                log.error("Error resolving service {} from etcd: {}", serviceName, throwable.getMessage());
                delegate.onError(throwable);
                return;
            }

            List<NodeData> servers = new ArrayList<>();
            List<KeyValue> keyValues = response.getKvs();
            for (KeyValue kv : keyValues) {
                addNodeToServers(kv, servers);
            }

            updateCurrentServers(servers);

            log.warn("Current list size 2i {} -> key is " + SERVICE_ROOT + "/{}", currentServers, serviceName);
        });
    }

    private String serviceDir() {
        return String.format("%s/%s/", SERVICE_ROOT, serviceName);
    }

    private void updateCurrentServers(List<NodeData> newServers) {
        if (!newServers.equals(currentServers.get())) {
            currentServers.set(newServers);
            delegate.onAddresses(newServers);
        }
    }

    private static void addNodeToServers(KeyValue kv, List<NodeData> servers) {
        String key = kv.getKey().toString(StandardCharsets.UTF_8);
        int nodeID = Integer.parseInt(key.substring(key.lastIndexOf('/') + 5));
        String addr = kv.getValue().toString(StandardCharsets.UTF_8);
        NodeData nodeData = new NodeData(addr, nodeID);
        if (!servers.contains(nodeData)) {
            servers.add(nodeData);
            log.debug("added node {} to list", nodeData);
        } else {
            log.debug("Node {} is already in the list", nodeData);
        }
    }

    private void watchKeys() {
        String serviceDir = serviceDir();
        log.info("Watch key called {}", serviceDir);
        ByteSequence key = ByteSequence.from(serviceDir, StandardCharsets.UTF_8);
        watcher = client.getWatchClient().watch(key, WatchOption.builder().isPrefix(true).build(), new Watch.Listener() {
            @Override
            public void onNext(WatchResponse response) {
                var copy = new ArrayList<>(currentServers.get());
                for (WatchEvent event : response.getEvents()) {
                    KeyValue kv = event.getKeyValue();
                    switch (event.getEventType()) {
                        case PUT -> addNodeToServers(kv, copy);
                        case DELETE -> removeNodeInServers(kv, copy);
                        default -> log.warn("Unknown event type {}", event.getEventType());
                    }
                }

                updateCurrentServers(copy);
                log.warn("Current list size {}", currentServers);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Error watching keys", throwable);
            }

            @Override
            public void onCompleted() {
                log.info("Keys watched -> completed");
            }
        });
    }

    private void removeNodeInServers(KeyValue kv, ArrayList<NodeData> servers) {
        String itemKey = kv.getKey().toString(StandardCharsets.UTF_8);
        int nodeID = Integer.parseInt(itemKey.substring(itemKey.lastIndexOf('/') + 5));
        boolean removed = false;
        for (NodeData nodeData : servers) {
            if (nodeID == nodeData.nodeID) {
                servers.remove(nodeData);
                removed = true;
                break;
            }
        }

        if (removed) {
            log.debug("Key {} removed from list", itemKey);
        } else {
            log.warn("Fail to remove {}", itemKey);
        }
    }

    public record NodeData(String addr, int nodeID) {
        public EquivalentAddressGroup toEquivalentAddressGroup() {
            String[] split = addr.split(":");
            return new EquivalentAddressGroup(new InetSocketAddress(split[0], Integer.parseInt(split[1])));
        }
    }
}
