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
	op := flag.String("op", "put", "operation: put or get")
	dmap := flag.String("dmap", "", "DMap name")
	key := flag.String("key", "", "key")
	value := flag.String("value", "", "value")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if *addr == "" || *dmap == "" || *key == "" {
		log.Fatal("addr, dmap and key are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: *addr})
	defer client.Close()

	switch *op {
	case "put":
		if err := client.Do(ctx, "dm.put", *dmap, *key, *value).Err(); err != nil {
			log.Fatalf("dm.put: %v", err)
		}
	case "get":
		got, err := client.Do(ctx, "dm.get", *dmap, *key).Text()
		if err != nil {
			log.Fatalf("dm.get: %v", err)
		}
		fmt.Fprintln(os.Stdout, got)
	default:
		log.Fatalf("unsupported op %q", *op)
	}
}
