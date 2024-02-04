# IDGEND

A simple and brute-force service for generating unique and pure digital IDs.

一个简单粗暴的生成唯一纯数字ID生成服务。

## Dependencies 依赖

1. Rust
2. Etcd server

## Feature 特点

1. unique ID 唯一ID
2. monotonically increasing 单调递增
3. High availability 高可用
4. suitable for scenarios with low QPS 适合QPS不高的场景

## Usage 使用

Assume that the hosts where you set up idgend are `192.128.0.1` and `192.128.0.2`, and the hosts of etcd server are `192.168.1.1`, `192.168.1.2`, `192.168.1.3`.

假设你搭建idgend的主机是`192.128.0.1`和`192.128.0.2`，而etcd server的主机是`192.168.1.1`, `192.168.1.2`, `192.168.1.3`。

```shell
idgend --web-addr 0.0.0.0:3000 \
       --advertise-client-url http://192.128.0.1:3000 \
       --etcd-server 192.168.1.1:2379 \
       --etcd-server 192.168.1.2:2379 \
       --etcd-server 192.168.1.3:2379
```

```shell
idgend --web-addr 0.0.0.0:3000 \
       --advertise-client-url http://192.128.0.2:3000 \
       --etcd-server 192.168.1.1:2379 \
       --etcd-server 192.168.1.2:2379 \
       --etcd-server 192.168.1.3:2379
```

Then you can use nginx as a proxy.

然后可以通过nignx做代理。

```nginx
upstream idgend { 
    server 192.128.0.1:3000;
    server 192.128.0.2:3000;
}
```

idgend is also very suitable for building as a Deployment on kubernetes.

idgend还十分适合在kubernetes上作为Deployment搭建。

## Implementation 实现

TODO

## LICENSE 开源协议

Apache-2.0
