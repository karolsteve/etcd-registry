package me.karolsteve.registry.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.PutOption;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Steve Tchatchouang at 03/04/2023
 */

@Slf4j
public class EtcdRegistry {
    private static final ScheduledExecutorService registryExecutor = Executors.newSingleThreadScheduledExecutor();

    private final Client client;
    private final String key;
    private final String value;
    private final int ttl;
    private Future<?> registryFuture;

    public static EtcdRegistry create(Config config) {
        Client etcdClient = Client.builder().endpoints(config.endPoints).build();
        String key = config.serviceDir + "/" + config.serviceName + "/" + config.nodeID;
        return new EtcdRegistry(etcdClient, key, config.value, config.ttl);
    }

    private EtcdRegistry(Client client, String key, String value, int ttl) {
        this.client = client;
        this.key = key;
        this.value = value;
        this.ttl = ttl;
    }

    public void register() throws Exception {
        log.info("REGISTERING {} SERVICE REGISTRY...", this.key);

        Lease leaseClient = client.getLeaseClient();

        final AtomicLong leaseID = new AtomicLong();
        leaseID.set(leaseClient.grant(ttl).get().getID());

        Runnable registerRunnable = () -> {
            try (KV kvClient = client.getKVClient()) {
                kvClient.put(
                        ByteSequence.from(this.key, StandardCharsets.UTF_8),
                        ByteSequence.from(this.value, StandardCharsets.UTF_8),
                        PutOption.builder().withLeaseId(leaseID.get()).build()
                ).get();
            } catch (ExecutionException | InterruptedException e) {
                if (e.getMessage().contains("lease not found")) {
                    reKeepAlive(e, leaseClient, leaseID);
                }
                log.error("Error registering service.", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        keepAlive(leaseClient, leaseID);

        registryFuture = registryExecutor.scheduleAtFixedRate(registerRunnable, 0, 5, TimeUnit.SECONDS);
    }

    private void reKeepAlive(Exception e, Lease leaseClient, AtomicLong leaseID) {
        try {
            leaseClient.revoke(leaseID.get());
            leaseID.set(leaseClient.grant(ttl).get().getID());
            keepAlive(leaseClient, leaseID);
        } catch (InterruptedException | ExecutionException ex) {
            log.error(ex.getMessage(), ex);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void keepAlive(Lease leaseClient, AtomicLong leaseID) {
        leaseClient.keepAlive(leaseID.get(), new StreamObserver<>() {
            @Override
            public void onNext(LeaseKeepAliveResponse value) {
                log.debug("KEEPALIVE RECEIVED {}", leaseID);
            }

            @Override
            public void onError(Throwable t) {
                log.error(t.getMessage(), t);
            }

            @Override
            public void onCompleted() {
                log.info("Lease has been killed.");
            }
        });
    }

    private volatile boolean terminated;

    public void unregister() {
        if (terminated) {
            return;
        }

        terminated = true;

        if (registryFuture != null) {
            registryFuture.cancel(true);
        }

        log.info("UNREGISTERING {} SERVICE REGISTRY...", this.key);
        if (client == null) return;
        try {
            client.getKVClient().delete(ByteSequence.from(this.key, StandardCharsets.UTF_8)).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Builder
    @Getter
    public static class Config {
        private List<URI> endPoints;
        private String serviceDir;
        private String serviceName;
        private String nodeID;
        private String value;
        private int ttl;
    }
}
