package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"net"
	"time"
)

type GapRequest struct {
	Unit      uint8
	Sequence  uint32
	Count     uint16
}

func StartGRPClient(gapRequestChan <-chan uint32) {
	conn, err := net.Dial("tcp", config.GRPAddr)
	if err != nil {
		log.Fatal("GRP Dial failed:", err)
	}
	defer conn.Close()

	// Gửi Login message
	loginMsg := make([]byte, 30) // Sequenced Unit Header + Login
	binary.LittleEndian.PutUint16(loginMsg[0:2], 30) // HdrLength
	loginMsg[2] = 0                                  // HdrCount
	loginMsg[3] = 0                                  // HdrUnit
	binary.LittleEndian.PutUint32(loginMsg[4:8], 0)  // HdrSequence
	loginMsg[8] = 22                                 // Length
	loginMsg[9] = 0x01                               // Type: Login
	copy(loginMsg[10:14], []byte("0001"))            // SessionSubId
	copy(loginMsg[14:18], []byte("FIRM"))            // Username
	copy(loginMsg[18:20], []byte("  "))              // Filler
	copy(loginMsg[20:30], []byte("ABCD00    "))     // Password

	_, err = conn.Write(loginMsg)
	if err != nil {
		log.Fatal("GRP Login failed:", err)
	}

	// Đọc Login Response
	reader := bufio.NewReader(conn)
	resp := make([]byte, 11) // Header + Login Response
	_, err = reader.Read(resp)
	if err != nil {
		log.Fatal("Read Login Response failed:", err)
	}
	if resp[9] != 0x02 || resp[10] != 'A' {
		log.Fatal("Login rejected")
	}

	// Heartbeat goroutine
	go func() {
		heartbeat := make([]byte, 8)
		binary.LittleEndian.PutUint16(heartbeat[0:2], 8)
		heartbeat[2] = 0
		heartbeat[3] = 0
		binary.LittleEndian.PutUint32(heartbeat[4:8], 0)
		for {
			conn.Write(heartbeat)
			time.Sleep(1 * time.Second)
		}
	}()

	// Xử lý gap request
	for seq := range gapRequestChan {
		gapReq := GapRequest{
			Unit:     unitID,
			Sequence: seq,
			Count:    1, // Yêu cầu 1 message, điều chỉnh nếu cần
		}

		msg := make([]byte, 17) // Header + Gap Request
		binary.LittleEndian.PutUint16(msg[0:2], 17)
		msg[2] = 0
		msg[3] = 0
		binary.LittleEndian.PutUint32(msg[4:8], 0)
		msg[8] = 9
		msg[9] = 0x03
		msg[10] = gapReq.Unit
		binary.LittleEndian.PutUint32(msg[11:15], gapReq.Sequence)
		binary.LittleEndian.PutUint16(msg[15:17], gapReq.Count)

		_, err = conn.Write(msg)
		if err != nil {
			log.Println("Gap Request failed:", err)
			continue
		}

		// Đọc Gap Response (giả sử không block luồng chính)
		go func() {
			resp := make([]byte, 18)
			_, err := reader.Read(resp)
			if err != nil {
				log.Println("Read Gap Response failed:", err)
				return
			}
			if resp[9] != 0x04 || resp[17] != 'A' {
				log.Printf("Gap Request for seq %d rejected", seq)
			}
		}()
	}
}
