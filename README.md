# IDGEND

🐮🍺简单粗暴的预生成唯一纯数字ID生成服务。

## 适用场景

1. 唯一ID（用户ID，订单号等）
1. 乱序ID
1. 纯数字简短ID
1. 分布式部署（性能+容灾）

## 依赖

1. Go
2. Etcd server

## 使用方法

```shell
go build
./idgend -etcd-endpoints etcd-0:2379,etcd-1:2379,etcd-2:2379
curl http://localhost:8080/id
```

## 特点

1. 足够简单，没有依赖数据库的自增或者依赖时钟的算法，不用担心会出现ID重复的可能。

## License

[MIT](/LICENSE)
