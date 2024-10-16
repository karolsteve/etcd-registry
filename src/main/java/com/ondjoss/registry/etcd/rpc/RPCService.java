package com.ondjoss.registry.etcd.rpc;

import com.ondjoss.registry.etcd.EtcdServiceAddrObserver;
import com.ondjoss.registry.etcd.EtcdWatcher;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created By Steve Tchatchouang
 * Date : 18/08/2024 11:00
 */

@Slf4j
public abstract class RPCService<T extends RPCClient> implements EtcdServiceAddrObserver.AddressesUpdatedListener {
    private final EtcdServiceAddrObserver addrObserver;
    private final List<T> activeClients;
    private final Map<Integer, T> activeClientsByNodeID;
    private final RPCClientFactory<T> rpcClientFactory;
    private final ReadWriteLock rwLock;

    protected RPCService(String[] etcdHosts, String serviceName, RPCClientFactory<T> rpcClientFactory) {
        this.addrObserver = EtcdServiceAddrObserver.create(etcdHosts, serviceName);
        this.activeClients = new ArrayList<>();
        this.activeClientsByNodeID = new HashMap<>();
        this.rwLock = new ReentrantReadWriteLock();
        this.rpcClientFactory = rpcClientFactory;
    }

    public void start() {
        addrObserver.setAddressesUpdatedListener(this);
        addrObserver.start();
    }

    public void dispose() {
        log.info("Closing client");

        addrObserver.setAddressesUpdatedListener(null);
        addrObserver.stop();

        rwLock.writeLock().lock();
        try {
            var iterator = activeClients.listIterator();
            while (iterator.hasNext()) {
                T client = iterator.next();
                iterator.remove();
                activeClientsByNodeID.remove(client.nodeID());
                client.close();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void onAddressesUpdated(EtcdServiceAddrObserver observer) {
        List<EtcdWatcher.NodeData> newAddresses = observer.getAddresses();

        rwLock.writeLock().lock();
        try {
            boolean hasChanges = false;
            var iterator = activeClients.listIterator();
            while (iterator.hasNext()) {
                var client = iterator.next();
                if (newAddresses.stream().noneMatch(nodeData -> nodeData.nodeID() == client.nodeID())) {
                    hasChanges = true;
                    activeClientsByNodeID.remove(client.nodeID());
                    iterator.remove();
                    client.close();
                }
            }

            for (var nodeData : newAddresses) {
                Integer key = nodeData.nodeID();
                if (!activeClientsByNodeID.containsKey(key)) {
                    hasChanges = true;
                    T client = rpcClientFactory.createClient(nodeData);
                    activeClientsByNodeID.put(key, client);
                    activeClients.add(client);
                }
            }

            if (hasChanges) {
                onActiveClientsChanged(activeClients);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    protected <R> R exec(Function<T, R> func) {
        return client().map(func).orElseThrow();
    }
    protected void run(Consumer<T> func) {
        func.accept(client().orElseThrow());
    }

    protected final Optional<T> client() {
        T result;
        int maxDur = 5000;
        int sleepDur = 100;
        while ((result = client0()) == null && maxDur > 0) {
            log.info("Waiting for {} client", addrObserver.getServiceName());
            try {
                maxDur -= sleepDur;
                Thread.sleep(sleepDur);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for client to be created", e);
                Thread.currentThread().interrupt();
            }
        }

        if (result == null) {
            log.error("Could not get client after 5000 millis");
        }

        return Optional.ofNullable(result);
    }

    private T client0() {
        rwLock.readLock().lock();
        try {
            return getNextClient();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected T getNextClient() {
        return activeClients.isEmpty() ? null : activeClients.getFirst();
    }

    protected Optional<T> nodeClient(int nodeID) {
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(activeClientsByNodeID.get(nodeID));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected void onActiveClientsChanged(List<T> activeClients){}
}
