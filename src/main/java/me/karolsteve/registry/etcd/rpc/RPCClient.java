package me.karolsteve.registry.etcd.rpc;

/**
 * Created By Steve Tchatchouang
 * Date : 18/08/2024 11:05
 */
public interface RPCClient {
    Integer nodeID();
    void close();
}
