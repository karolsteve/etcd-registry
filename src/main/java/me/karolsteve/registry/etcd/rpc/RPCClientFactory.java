package me.karolsteve.registry.etcd.rpc;

import me.karolsteve.registry.etcd.EtcdWatcher;

/**
 * Created By Steve Tchatchouang
 * Date : 18/08/2024 11:10
 */
public interface RPCClientFactory<T> {
    T createClient(EtcdWatcher.NodeData nodeData);
}
