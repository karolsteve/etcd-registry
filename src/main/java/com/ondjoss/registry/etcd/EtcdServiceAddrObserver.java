package com.ondjoss.registry.etcd;

import io.etcd.jetcd.Client;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Steve Tchatchouang at 11/04/2023
 */

@Slf4j
public class EtcdServiceAddrObserver implements EtcdWatcher.Delegate {
    private final EtcdWatcher watcher;
    private final AtomicReference<List<EtcdWatcher.NodeData>> serviceAddrs = new AtomicReference<>(Collections.emptyList());
    @Getter
    private final String serviceName;

    @Setter
    private AddressesUpdatedListener addressesUpdatedListener;

    public static EtcdServiceAddrObserver create(String[] hosts, String serviceName) {
        Client client = Client.builder().endpoints(hosts).build();
        return new EtcdServiceAddrObserver(client, serviceName);
    }

    private EtcdServiceAddrObserver(Client client, String serviceName) {
        this.serviceName = serviceName;
        this.watcher = EtcdWatcher.make(client, serviceName, this);
    }

    public void start(String serviceRoot) {
        this.watcher.start(serviceRoot);
    }

    public void stop() {
        this.watcher.stop();
    }

    @Override
    public void onError(Throwable throwable) {
        log.error(serviceName, throwable);
    }

    @Override
    public void onAddresses(List<EtcdWatcher.NodeData> servers) {
        serviceAddrs.set(servers);
        if (addressesUpdatedListener != null) {
            addressesUpdatedListener.onAddressesUpdated(this);
        }
    }

    public List<EtcdWatcher.NodeData> getAddresses() {
        return serviceAddrs.get();
    }

    public interface AddressesUpdatedListener {
        void onAddressesUpdated(EtcdServiceAddrObserver observer);
    }
}
