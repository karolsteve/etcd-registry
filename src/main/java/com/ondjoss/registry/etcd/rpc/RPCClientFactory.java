package com.ondjoss.registry.etcd.rpc;

import com.ondjoss.registry.etcd.EtcdWatcher;

/**
 * Created By Steve Tchatchouang
 * Date : 18/08/2024 11:10
 */
public interface RPCClientFactory<T> {
    T createClient(EtcdWatcher.NodeData nodeData);
}
