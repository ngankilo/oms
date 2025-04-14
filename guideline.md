Để đáp ứng yêu cầu mới, tôi sẽ điều chỉnh cơ chế **Processor** trong hệ thống PITCH trading để:
1. **Không lưu trữ gói tin vào Redis**: Thay vì lưu các gói tin vào Redis hash hoặc sorted set, Processor sẽ chỉ theo dõi sequence number để phát hiện gap.
2. **Sử dụng Redis Pub/Sub**: 
   - Chuyển tiếp các message nhận được qua Redis Pub/Sub đến một channel (ví dụ: `pitch_messages`) để các thành phần khác trong hệ thống (như logic giao dịch) tiêu thụ.
   - Bắn sự kiện mất gói tin (gap) qua một channel Redis Pub/Sub riêng (ví dụ: `gap_events`) để thông báo cho **GRP Client** gửi yêu cầu retransmission.
3. **Bảo đảm low latency**: Tối ưu để giảm độ trễ, tận dụng Redis Pub/Sub cho tốc độ truyền tải nhanh.

Tôi sẽ cập nhật code liên quan, tập trung vào **Receiver** và **Processor**, đồng thời thêm cơ chế Pub/Sub cho **GRP Client** để nhận sự kiện gap. Các phần khác (như GRP Client gửi TCP request) giữ nguyên nhưng sẽ được kích hoạt bởi Pub/Sub.

---

### Thiết Kế Cập Nhật

#### Kiến Trúc
1. **Receiver Layer**:
   - Nhận multicast UDP, giải mã `Sequenced Unit Header`.
   - Gửi message qua Redis Pub/Sub channel `pitch_messages`.
   - Thông báo sequence number qua channel Go để Processor kiểm tra.
2. **Processor**:
   - Theo dõi `last_processed` sequence (lưu trong Redis để khôi phục khi khởi động lại).
   - Kiểm tra gap bằng cách so sánh sequence nhận được với `last_processed + 1`.
   - Nếu không có gap, cập nhật `last_processed`.
   - Nếu có gap, publish sự kiện gap tới Redis Pub/Sub channel `gap_events`.
3. **GRP Client**:
   - Subscribe vào channel `gap_events` để nhận thông báo gap.
   - Gửi Gap Request qua TCP tới GRP khi nhận được sự kiện.
4. **Logic Giao Dịch (ngoài phạm vi code này)**:
   - Subscribe vào `pitch_messages` để xử lý message tuần tự.

#### Luồng Xử Lý
1. Receiver nhận UDP → Publish message vào `pitch_messages` → Gửi sequence qua channel Go.
2. Processor kiểm tra sequence → Nếu tuần tự, cập nhật `last_processed`; nếu có gap, publish `{unit, sequence, count}` vào `gap_events`.
3. GRP Client nhận sự kiện từ `gap_events` → Gửi Gap Request tới GRP.
4. Message retransmission được Receiver xử lý như message thông thường.

---

### Triển Khai Bằng Golang

#### Cấu Hình Dự Án
- **Thư viện**: Tiếp tục dùng `github.com/redis/go-redis/v9` cho Pub/Sub.
- **Cấu trúc thư mục**: Giữ nguyên như trước, chỉ cập nhật các file liên quan.

#### Code Cập Nhật

##### **config.go** - Cấu Hình
Không thay đổi nhiều, chỉ thêm các channel Pub/Sub:

```go
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
```

##### **receiver.go** - Nhận Multicast UDP và Publish
```go
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

	ctx := context.Background()
	buffer := make([]byte, 1500)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("ReadFromUDP failed:", err)
			continue
		}

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
			continue
		}

		if header.HdrCount == 0 {
			continue // Heartbeat
		}

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

			// Publish message qua Redis Pub/Sub
			err = redisClient.Publish(ctx, config.MessageChannel, buffer[offset:offset+msgLen]).Err()
			if err != nil {
				log.Println("Publish failed:", err)
			}

			// Gửi sequence qua channel
			seq := header.HdrSequence + uint32(i)
			seqChan <- seq

			offset += msgLen
		}
	}
}
```

- **Thay đổi**:
  - Không lưu message vào Redis hash hay sorted set.
  - Publish trực tiếp message vào channel `pitch_messages`.
  - Vẫn gửi sequence qua channel Go để Processor kiểm tra gap.

##### **processor.go** - Kiểm Tra Gap và Publish Sự Kiện
```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
)

type GapEvent struct {
	Unit     uint8  `json:"unit"`
	Sequence uint32 `json:"sequence"`
	Count    uint16 `json:"count"`
}

func StartProcessor(seqChan <-chan uint32) {
	ctx := context.Background()
	lastProcessedKey := "last_processed:" + strconv.Itoa(unitID)

	// Khôi phục last_processed
	lastProcessed, err := redisClient.Get(ctx, lastProcessedKey).Int()
	if err != nil {
		lastProcessed = 0
	}

	for seq := range seqChan {
		for {
			expectedSeq := lastProcessed + 1
			if seq == uint32(expectedSeq) {
				// Sequence tuần tự, cập nhật last_processed
				lastProcessed = expectedSeq
				redisClient.Set(ctx, lastProcessedKey, lastProcessed, 0)
				break
			} else if seq > uint32(expectedSeq) {
				// Phát hiện gap
				gapCount := seq - uint32(expectedSeq)
				if gapCount > 100 {
					gapCount = 100 // Giới hạn theo tài liệu
				}

				log.Printf("Gap detected: expected %d, got %d", expectedSeq, seq)
				gapEvent := GapEvent{
					Unit:     unitID,
					Sequence: uint32(expectedSeq),
					Count:    uint16(gapCount),
				}

				// Publish sự kiện gap
				eventData, _ := json.Marshal(gapEvent)
				err := redisClient.Publish(ctx, config.GapEventChannel, eventData).Err()
				if err != nil {
					log.Println("Publish gap event failed:", err)
				}

				// Không cập nhật last_processed, chờ gap được điền
				break
			}
			// Nếu seq < expectedSeq, bỏ qua (trùng lặp)
		}
	}
}
```

- **Thay đổi**:
  - Không lưu sequence hay message vào Redis.
  - Chỉ lưu `last_processed` để khôi phục trạng thái.
  - Khi phát hiện gap (`seq > last_processed + 1`), publish sự kiện gap với `{unit, sequence, count}` vào `gap_events`.
  - Giới hạn `count` tối đa 100 theo cấu hình PITCH (Section 8.1.1).

##### **grp.go** - Nhận Sự Kiện Gap Qua Pub/Sub
```go
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"net"
	"time"
)

func StartGRPClient() {
	// Kết nối TCP tới GRP
	conn, err := net.Dial("tcp", config.GRPAddr)
	if err != nil {
		log.Fatal("GRP Dial failed:", err)
	}
	defer conn.Close()

	// Gửi Login message
	loginMsg := make([]byte, 30)
	binary.LittleEndian.PutUint16(loginMsg[0:2], 30)
	loginMsg[2] = 0
	loginMsg[3] = 0
	binary.LittleEndian.PutUint32(loginMsg[4:8], 0)
	loginMsg[8] = 22
	loginMsg[9] = 0x01
	copy(loginMsg[10:14], []byte("0001"))
	copy(loginMsg[14:18], []byte("FIRM"))
	copy(loginMsg[18:20], []byte("  "))
	copy(loginMsg[20:30], []byte("ABCD00    "))

	_, err = conn.Write(loginMsg)
	if err != nil {
		log.Fatal("GRP Login failed:", err)
	}

	// Đọc Login Response
	reader := bufio.NewReader(conn)
	resp := make([]byte, 11)
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

	// Subscribe vào gap_events
	ctx := context.Background()
	pubsub := redisClient.Subscribe(ctx, config.GapEventChannel)
	defer pubsub.Close()

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Println("PubSub receive failed:", err)
			continue
		}

		var gapEvent GapEvent
		if err := json.Unmarshal([]byte(msg.Payload), &gapEvent); err != nil {
			log.Println("Unmarshal gap event failed:", err)
			continue
		}

		// Gửi Gap Request
		gapReq := make([]byte, 17)
		binary.LittleEndian.PutUint16(gapReq[0:2], 17)
		gapReq[2] = 0
		gapReq[3] = 0
		binary.LittleEndian.PutUint32(gapReq[4:8], 0)
		gapReq[8] = 9
		gapReq[9] = 0x03
		gapReq[10] = gapEvent.Unit
		binary.LittleEndian.PutUint32(gapReq[11:15], gapEvent.Sequence)
		binary.LittleEndian.PutUint16(gapReq[15:17], gapEvent.Count)

		_, err = conn.Write(gapReq)
		if err != nil {
			log.Println("Gap Request failed:", err)
			continue
		}

		// Đọc Gap Response bất đồng bộ
		go func() {
			resp := make([]byte, 18)
			_, err := reader.Read(resp)
			if err != nil {
				log.Println("Read Gap Response failed:", err)
				return
			}
			if resp[9] != 0x04 || resp[17] != 'A' {
				log.Printf("Gap Request for seq %d rejected", gapEvent.Sequence)
			}
		}()
	}
}
```

- **Thay đổi**:
  - Không nhận gap request từ channel Go, thay vào đó subscribe vào `gap_events`.
  - Parse JSON từ payload để lấy `{unit, sequence, count}`.
  - Gửi Gap Request như trước, giữ logic TCP và Heartbeat.

##### **main.go** - Khởi Chạy
```go
package main

import (
	"log"
)

func main() {
	seqChan := make(chan uint32, 100)

	// Khởi động receiver
	go StartReceiver(seqChan)

	// Khởi động processor
	go StartProcessor(seqChan)

	// Khởi động GRP client
	go StartGRPClient()

	log.Println("PITCH trading system started")
	select {}
}
```

---

### Hiệu Năng và Low Latency
- **Receiver**: Nhận UDP (~1µs) + Publish Redis (~1ms localhost) = ~1-2ms.
- **Processor**: Kiểm tra sequence (~1µs) + Publish gap event (~1ms nếu có gap) = ~1-2ms.
- **GRP Client**: Subscribe (~1ms) + TCP request (~10-50ms tùy mạng) = không ảnh hưởng luồng chính vì bất đồng bộ.
- **Tổng độ trễ**:
  - Không gap: ~2-3ms từ nhận UDP đến publish message.
  - Có gap: ~2-3ms cho message hiện tại, ~10-50ms để nhận retransmission (nhưng không block xử lý message khác).
- **Ưu điểm Pub/Sub**:
  - Loại bỏ lưu trữ Redis, giảm overhead.
  - Cho phép mở rộng: Nhiều consumer có thể subscribe vào `pitch_messages` hoặc `gap_events`.

---

### Xử Lý Gói Tin Bị Mất
- **Phát hiện**: Processor so sánh sequence nhận được với `last_processed + 1`. Ví dụ, nếu `last_processed=310172` và nhận `310176`, gap là 310173-310175.
- **Publish sự kiện**: Gửi `{unit: 1, sequence: 310173, count: 3}` vào `gap_events`.
- **GRP xử lý**: Nhận sự kiện, gửi Gap Request `{Unit=1, Sequence=310173, Count=3}`.
- **Retransmission**: Receiver nhận message retransmission qua multicast, publish lại vào `pitch_messages`, Processor cập nhật `last_processed`.

---

### Cách Chạy
1. Cài Redis: `redis-server`.
2. Cấu hình mạng: Đảm bảo nhận được multicast UDP (Section 8 tài liệu PITCH).
3. Build và chạy:
   ```
   go mod init pitch-trading
   go get github.com/redis/go-redis/v9
   go run .
   ```
4. (Tùy chọn) Consumer mẫu cho `pitch_messages`:
   ```go
   package main

   import (
       "context"
       "log"
       "github.com/redis/go-redis/v9"
   )

   func main() {
       client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
       pubsub := client.Subscribe(context.Background(), "pitch_messages")
       defer pubsub.Close()

       for {
           msg, err := pubsub.ReceiveMessage(context.Background())
           if err != nil {
               log.Println("Receive failed:", err)
               continue
           }
           log.Printf("Received message: %x", msg.Payload)
       }
   }
   ```

---

### Lưu Ý
- **Độ tin cậy**: Redis Pub/Sub không đảm bảo delivery (fire-and-forget). Nếu cần độ tin cậy cao, cân nhắc dùng Redis Streams thay Pub/Sub.
- **Spin Server**: Nếu muốn tích hợp để lấy snapshot khi khởi động, tôi có thể thêm code.
- **Node.js**: Nếu muốn dùng Node.js với `redis` và `node-dgram`, tôi có thể cung cấp code tương ứng.

Bạn có cần thêm tính năng nào (như Spin Server) hoặc muốn tôi tối ưu thêm không?
