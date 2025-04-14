package main

import (
	"context"
	"log"
	"strconv"
	"time"
)

func StartProcessor(seqChan <-chan uint32, gapRequestChan chan<- uint32) {
	ctx := context.Background()
	lastProcessedKey := "last_processed:" + strconv.Itoa(unitID)

	// Khôi phục last_processed
	lastProcessed, _ := redisClient.Get(ctx, lastProcessedKey).Int()
	if lastProcessed == 0 {
		lastProcessed = 0 // Bắt đầu từ 0 nếu chưa có
	}

	for range seqChan {
		for {
			nextSeq := lastProcessed + 1
			exists, _ := redisClient.ZScore(ctx, "received_sequences:"+strconv.Itoa(unitID), strconv.Itoa(nextSeq)).Result()
			if exists != 0 {
				// Sequence tiếp theo có sẵn, xử lý
				data, _ := redisClient.HGet(ctx, "packets:"+strconv.Itoa(unitID), strconv.Itoa(nextSeq)).Result()
				// Gửi vào channel cho logic giao dịch (thay Kafka bằng channel để low latency)
				// orderChan <- data // Uncomment khi tích hợp logic giao dịch

				// Xóa khỏi Redis
				redisClient.HDel(ctx, "packets:"+strconv.Itoa(unitID), strconv.Itoa(nextSeq))
				redisClient.ZRem(ctx, "received_sequences:"+strconv.Itoa(unitID), strconv.Itoa(nextSeq))
				lastProcessed = nextSeq
				redisClient.Set(ctx, lastProcessedKey, lastProcessed, 0)
			} else {
				// Phát hiện gap, yêu cầu retransmission
				log.Printf("Gap detected at sequence %d", nextSeq)
				gapRequestChan <- uint32(nextSeq)
				time.Sleep(10 * time.Millisecond) // Chờ ngắn để tránh CPU spike
				break
			}
		}
	}
}
