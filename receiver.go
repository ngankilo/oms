package main

import (
	"context"
	"encoding/binary"
	"log"
	"net"
)

type SequencedUnitHeader struct {
	HdrLength  uint16
	HdrCount   uint8
	HdrUnit    uint8
	HdrSequence uint32
}

func StartReceiver(seqChan chan<- uint32) {
	addr, err := net.ResolveUDPAddr("udp", config.MulticastAddr)
	if err != nil {
		log.Fatal("ResolveUDPAddr failed:", err)
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		log.Fatal("ListenMulticastUDP failed:", err)
	}
	defer conn.Close()

	log.Println("Listening for PITCH messages on", config.MulticastAddr)

	buffer := make([]byte, 1500) // MTU 1500 theo tài liệu
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("ReadFromUDP failed:", err)
			continue
		}

		// Giải mã Sequenced Unit Header
		if n < 8 {
			log.Println("Packet too short")
			continue
		}

		var header SequencedUnitHeader
		header.HdrLength = binary.LittleEndian.Uint16(buffer[0:2])
		header.HdrCount = buffer[2]
		header.HdrUnit = buffer[3]
		header.HdrSequence = binary.LittleEndian.Uint32(buffer[4:8])

		if header.HdrUnit != unitID {
			continue // Bỏ qua nếu không phải unit mong muốn
		}

		if header.HdrCount == 0 {
			// Heartbeat, bỏ qua hoặc log
			continue
		}

		// Lưu từng message trong frame
		ctx := context.Background()
		offset := 8
		for i := uint8(0); i < header.HdrCount; i++ {
			if offset >= n {
				log.Println("Invalid message length")
				break
			}

			msgLen := int(buffer[offset])
			if offset+msgLen > n {
				log.Println("Message truncated")
				break
			}

			seq := header.HdrSequence + uint32(i)
			// Lưu vào Redis
			redisClient.ZAdd(ctx, "received_sequences:"+string(unitID), redis.Z{
				Score:  float64(seq),
				Member: seq,
			})
			redisClient.HSet(ctx, "packets:"+string(unitID), seq, buffer[offset:offset+msgLen])

			// Thông báo sequence mới
			seqChan <- seq

			offset += msgLen
		}
	}
}
