package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"go.etcd.io/etcd/client/v3/concurrency"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	MinID uint32 = 1000000000

	MaxID uint32 = 1<<32 - 1
)

const (
	initializeTimeout = 60 * 60

	requestTimeout = 30
)

const (
	InitializeKey = "/idgend/initialized"

	LockKeyPrefix = "/idgend/lock"

	IdKeyPrefix = "/idgend/id"
)

var (
	webAddr       = flag.String("web-addr", ":8080", "listening web address")
	etcdEndpoints = flag.String("etcd-endpoints", "", "etcd endpoints")
)

func main() {
	flag.Parse()

	client, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(*etcdEndpoints, ","),
	})
	if err != nil {
		slog.Error("create etcd client failed", "error", err)
		return
	}

	if err := initIDs(client); err != nil {
		slog.Error("init IDs failed", "error", err)
		return
	}

	slog.Info("start to run web server", "addr", *webAddr)
	if err := runWebServer(client); err != nil {
		slog.Error("run web server failed", "error", err)
	}
}

func initIDs(client *clientv3.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), initializeTimeout*time.Second)
	defer cancel()

	initialized, err := checkInitialized(ctx, client)
	if err != nil {
		return err
	}
	if initialized {
		slog.Info("IDs have initialized, skip")
		return nil
	}

	unlock, err := lock(ctx, client, initializeTimeout)
	if err != nil {
		return err
	}
	defer unlock()

	initialized, err = checkInitialized(ctx, client)
	if err != nil {
		return err
	}
	if initialized {
		slog.Info("IDs finish initialization")
		return nil
	}

	_, err = client.Delete(ctx, IdKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for id := MinID; id < MaxID; id += 1 {
		randKey := strconv.FormatInt(int64(rand.Uint32()), 36)
		_, err := client.Put(ctx, IdKeyPrefix+"/"+randKey, strconv.FormatUint(uint64(id), 10))
		if err != nil {
			return err
		}
	}

	_, err = client.Put(ctx, InitializeKey, "")
	if err != nil {
		return err
	}

	return nil
}

func lock(ctx context.Context, client *clientv3.Client, ttl int) (func(), error) {
	session, err := concurrency.NewSession(client, concurrency.WithTTL(ttl))
	if err != nil {
		return func() {}, nil
	}
	mutex := concurrency.NewMutex(session, LockKeyPrefix)
	if err := mutex.Lock(ctx); err != nil {
		return func() {}, nil
	}
	return func() {
		if err := mutex.Unlock(ctx); err != nil {
			slog.Error("unlock failed", "error", err)
		}
		if err := session.Close(); err != nil {
			slog.Error("close session failed", "error", err)
		}
	}, nil
}

func checkInitialized(ctx context.Context, client *clientv3.Client) (bool, error) {
	resp, err := client.Get(ctx, InitializeKey)
	if err != nil {
		return false, err
	}
	return resp.Count > 0, nil
}

func popId(client *clientv3.Client) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout*time.Second)
	defer cancel()

	unlock, err := lock(ctx, client, requestTimeout)
	if err != nil {
		return "", err
	}
	defer unlock()

	resp, err := client.Get(ctx, IdKeyPrefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", errors.New("IDs exhausted")
	}

	kv := resp.Kvs[0]

	id := string(kv.Value)

	if _, err := client.Delete(ctx, string(kv.Key)); err != nil {
		return "", err
	}

	return id, nil
}

func runWebServer(client *clientv3.Client) error {
	http.HandleFunc("/id", func(writer http.ResponseWriter, request *http.Request) {
		id, err := popId(client)
		if err != nil {
			slog.Error("pop id failed", "error", err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := fmt.Fprint(writer, id); err != nil {
			slog.Error("output id failed", "error", err)
		}
	})
	return http.ListenAndServe(*webAddr, nil)
}
