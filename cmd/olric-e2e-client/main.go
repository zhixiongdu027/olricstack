package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("addr", "", "Olric address host:port")
	op := flag.String("op", "put", "operation: put, get, expire, delete, or del")
	dmap := flag.String("dmap", "", "ignored compatibility flag")
	key := flag.String("key", "", "key")
	value := flag.String("value", "", "value")
	ttl := flag.Duration("ttl", 0, "ttl for put/expire")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if *addr == "" || *key == "" {
		log.Fatal("addr and key are required")
	}
	_ = *dmap

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: *addr})
	defer client.Close()

	switch *op {
	case "put":
		if err := client.Set(ctx, *key, *value, *ttl).Err(); err != nil {
			log.Fatalf("set: %v", err)
		}
	case "get":
		got, err := client.Get(ctx, *key).Result()
		if err != nil {
			log.Fatalf("get: %v", err)
		}
		fmt.Fprintln(os.Stdout, got)
	case "delete", "del":
		deleted, err := client.Del(ctx, *key).Result()
		if err != nil {
			log.Fatalf("del: %v", err)
		}
		fmt.Fprintln(os.Stdout, deleted)
	case "expire":
		if *ttl <= 0 {
			log.Fatal("ttl must be positive for expire")
		}
		ok, err := client.Expire(ctx, *key, *ttl).Result()
		if err != nil {
			log.Fatalf("expire: %v", err)
		}
		if ok {
			fmt.Fprintln(os.Stdout, 1)
		} else {
			fmt.Fprintln(os.Stdout, 0)
		}
	default:
		log.Fatalf("unsupported op %q", *op)
	}
}
