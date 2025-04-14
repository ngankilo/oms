package main

import (
	"github.com/redis/go-redis/v9"
)

type Config struct {
	MulticastAddr   string
	GRPAddr         string
	RedisAddr       string
	MessageChannel  string // Redis Pub/Sub channel cho message
	GapEventChannel string // Redis Pub/Sub channel cho gap
}

var config = Config{
	MulticastAddr:   "233.218.133.80:30501",
	GRPAddr:         "localhost:12345",
	RedisAddr:       "localhost:6379",
	MessageChannel:  "pitch_messages",
	GapEventChannel: "gap_events",
}

var redisClient = redis.NewClient(&redis.Options{
	Addr: config.RedisAddr,
})

const unitID = 1
