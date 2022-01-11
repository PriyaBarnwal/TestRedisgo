package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

const componentName string = "PUBSUB"

const SUBSCRIBE_TIMEOUT_SEC time.Duration = 10
const UNSUBSCRIBE_TIMEOUT_SEC time.Duration = 200
const REDIS_URL string = "REDIS_URL"

var Redis RedisRequestReply = RedisRequestReply{}

var startTime []time.Time

var diff []int

type RedisRequestReply struct {
	client *redis.Client
	//	subscriptions map[string]func(msg ...interface{})
}

//Init initialize redis
func (r *RedisRequestReply) Init(opts ...interface{}) {
	opt, err := redis.ParseURL("redis://@redis-single-master.flow.svc.cluster.local:6379")
	if err != nil {
		fmt.Println("Failed to parse redis URL ")
	}

	r.client = redis.NewClient(&redis.Options{
		Addr:         opt.Addr,
		Password:     opt.Password,
		DB:           opt.DB,
		MaxRetries:   2,
		MinIdleConns: 10,
		DialTimeout:  8 * time.Second,
		ReadTimeout:  9 * time.Second,
		PoolSize:     25,
		PoolTimeout:  60 * time.Second,
	})
	/*
		for elasticache
		r.client = redis.NewClient(&redis.Options{
			Addr:         "flowai-dev.hxzxkr.ng.0001.usw2.cache.amazonaws.com:6379",
			DB:           0,
			MaxRetries:   2,
			MinIdleConns: 10,
			DialTimeout:  8 * time.Second,
			ReadTimeout:  9 * time.Second,
			PoolSize:     25,
			PoolTimeout:  60 * time.Second,
		})
	*/
	_, pingErr := r.client.Ping().Result()
	if pingErr == nil {
		fmt.Println("Redis connected")
	} else {
		fmt.Printf("Redis ping message failed %v", pingErr)
	}
}

func main() {
	pubsub := &Redis

	pubsub.Init()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		if i == 0 {
			go subscription(pubsub, &wg)
		} else {
			go heartBeat(pubsub, &wg)
		}

	}

	go heartBeat(pubsub, &wg)

	wg.Wait()

	//fmt.Println(startTime)

	fmt.Println("difference array")
	fmt.Println(diff)
	sum := 0

	for j := 0; j < len(diff); j++ {
		sum += (diff[j])
	}

	avg := (float64(sum)) / (float64(len(diff)))
	fmt.Println("Sum = ", sum, "\nAverage = ", avg)
	//pubsub.Unsubscribe("topic/test")
}

func subscription(pubsub *RedisRequestReply, wg *sync.WaitGroup) {
	sub := pubsub.client.Subscribe("topic/test")
	// received, receivErr := sub.ReceiveMessage()

	// if receivErr != nil {
	// 	fmt.Printf("error receiving %v", receivErr)
	// } else {
	// 	fmt.Println()
	// 	fmt.Printf("received message %v", received)
	// }
	// Get the Channel to use
	channel := sub.Channel()
	// Itterate any messages sent on the channel
	for msg := range channel {

		fmt.Println()
		index, _ := strconv.Atoi(msg.Payload)
		fmt.Printf("message Received:%v", index)
		diff = append(diff, int(time.Now().Sub(startTime[index])))

		if index == 999 {
			sub.Close()
		}
	}

	defer wg.Done()
}

func heartBeat(pubsub *RedisRequestReply, wg *sync.WaitGroup) {
	i := 0
	for range time.Tick(time.Second * 2) {
		if i == 1000 {
			break
		}
		startTime = append(startTime, time.Now())
		pubsub.client.Publish("topic/test", i)
		fmt.Println()
		fmt.Printf("data sent %v", i)
		i++
	}

	defer wg.Done()
}
