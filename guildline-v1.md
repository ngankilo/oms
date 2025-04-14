Để tích hợp tính năng đón nhận và xử lý các message từ **Cboe Titanium Cboe Australia Multicast Depth of Book (PITCH)** dựa trên tài liệu đã cung cấp, đồng thời xây dựng một hệ thống lưu trữ gói tin và phát hiện gói tin bị mất, tôi sẽ đề xuất một giải pháp sử dụng **Golang** (vì bạn đã hỏi về Go trước đó và nó phù hợp với yêu cầu hiệu năng cao của hệ thống trading). Nếu bạn muốn dùng Node.js, tôi cũng có thể điều chỉnh.

Giải pháp sẽ:
1. **Nhận message PITCH qua multicast UDP**: Lắng nghe các feed A, B, hoặc E theo cấu hình multicast.
2. **Lưu trữ gói tin**: Sử dụng Redis để lưu trữ các gói tin và sequence number nhằm dễ dàng quản lý thứ tự.
3. **Phát hiện gói tin bị mất**: Dựa trên `Hdr Sequence` trong `Sequenced Unit Header` để kiểm tra gap.
4. **Yêu cầu retransmission**: Kết nối TCP tới Gap Request Proxy (GRP) để yêu cầu các gói tin bị mất.
5. **Hỗ trợ Spin Server**: Lấy snapshot order book khi cần thiết để đồng bộ trạng thái.

Dưới đây là thiết kế chi tiết và code triển khai bằng Go.

---

### Thiết Kế Hệ Thống

#### Yêu Cầu Từ Tài Liệu PITCH
- **Giao thức**: Multicast UDP cho real-time data, TCP cho GRP và Spin Server.
- **Message format**: Mỗi gói tin bắt đầu bằng `Sequenced Unit Header` (8 bytes) chứa `Hdr Sequence`, `Hdr Count`, và `Hdr Unit`, theo sau là các message (Add Order, Trade, v.v.).
- **Sequence number**: Tăng dần cho mỗi unit, dùng để phát hiện gap.
- **Gap Request Proxy (GRP)**: Yêu cầu retransmission khi phát hiện gap qua TCP.
- **Spin Server**: Cung cấp snapshot order book để đồng bộ trạng thái.
- **Latency**: Yêu cầu độ trễ thấp, đặc biệt khi không có gói tin bị mất.
- **Xử lý gói tin bị mất**: Gửi Gap Request khi phát hiện khoảng trống, nhận Gap Response qua multicast.

#### Kiến Trúc Đề Xuất
1. **Receiver Layer**:
   - Nhiều goroutines nhận multicast UDP từ các feed (A, B, E).
   - Giải mã `Sequenced Unit Header` và lưu message vào Redis.
2. **Sequence Manager**:
   - Redis lưu trữ:
     - `received_sequences:{unit}` (sorted set): Lưu sequence number đã nhận.
     - `packets:{unit}` (hash): Lưu dữ liệu gói tin theo sequence.
     - `last_processed:{unit}` (string): Sequence cuối cùng đã xử lý.
3. **Gap Detector**:
   - Goroutine riêng kiểm tra sequence tiếp theo (`last_processed + 1`) có trong Redis không.
   - Nếu thiếu, gửi Gap Request qua TCP tới GRP.
4. **GRP Client**:
   - Kết nối TCP tới GRP, gửi Login, Heartbeat, và Gap Request.
   - Nhận Gap Response và xử lý retransmission.
5. **Order Processor**:
   - Xử lý message tuần tự từ Redis, gửi vào channel in-memory cho logic giao dịch.
6. **Spin Client** (tùy chọn):
   - Kết nối TCP tới Spin Server để lấy snapshot khi khởi động hoặc khi gap quá lớn.

#### Luồng Xử Lý
1. Nhận multicast UDP → Giải mã `Sequenced Unit Header` → Lưu sequence và message vào Redis.
2. Kiểm tra sequence tiếp theo → Nếu tuần tự, xử lý message; nếu có gap, gửi Gap Request.
3. Nhận retransmission qua multicast → Lưu lại Redis → Tiếp tục xử lý.
4. Logic giao dịch tiêu thụ message từ channel để xây dựng order book.

---

### Triển Khai Bằng Golang

#### Cấu Hình Dự Án
- **Thư viện**:
  - `github.com/redis/go-redis/v9`: Kết nối Redis.
  - `encoding/binary`: Giải mã binary message.
  - `net`: Xử lý UDP và TCP.
- **Cấu trúc thư mục**:
  ```
  pitch-trading/
  ├── main.go
  ├── receiver.go
  ├── processor.go
  ├── grp.go
  ├── config.go
  └── go.mod
  ```

#### Code Chi Tiết

##### **config.go** - Cấu Hình
```go
package main

import (
	"github.com/redis/go-redis/v9"
)

type Config struct {
	MulticastAddr string // e.g., "233.218.133.80:30501" (Feed A, Unit 1)
	GRPAddr       string // e.g., "grp.cxa.com:12345"
	RedisAddr     string // e.g., "localhost:6379"
}

var config = Config{
	MulticastAddr: "233.218.133.80:30501",
	GRPAddr:       "localhost:12345", // Thay bằng GRP thực tế
	RedisAddr:     "localhost:6379",
}

var redisClient = redis.NewClient(&redis.Options{
	Addr: config.RedisAddr,
})

const unitID = 1 // Unit 1, điều chỉnh theo cấu hình
```

##### **receiver.go** - Nhận Multicast UDP
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
```

- **Giải thích**:
  - Lắng nghe multicast UDP trên địa chỉ/port từ cấu hình.
  - Giải mã `Sequenced Unit Header` để lấy sequence number và số lượng message.
  - Lưu mỗi message vào Redis: sequence number vào sorted set, dữ liệu vào hash.
  - Gửi sequence qua channel để processor kiểm tra.

##### **processor.go** - Xử Lý Thứ Tự và Phát Hiện Gap
```go
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
```

- **Giải thích**:
  - Theo dõi `last_processed` để biết sequence cuối cùng đã xử lý.
  - Kiểm tra sequence tiếp theo có trong Redis không.
  - Nếu có, xử lý message và cập nhật trạng thái; nếu không, thông báo gap qua channel.

##### **grp.go** - Kết Nối Gap Request Proxy
```go
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
```

- **Giải thích**:
  - Kết nối TCP tới GRP, gửi Login message và xử lý Login Response.
  - Gửi Heartbeat mỗi giây để giữ kết nối.
  - Khi nhận yêu cầu gap, gửi Gap Request và xử lý Gap Response bất đồng bộ.

##### **main.go** - Khởi Chạy
```go
package main

import (
	"log"
)

func main() {
	seqChan := make(chan uint32, 100)
	gapRequestChan := make(chan uint32, 100)

	// Khởi động receiver
	go StartReceiver(seqChan)

	// Khởi động processor
	go StartProcessor(seqChan, gapRequestChan)

	// Khởi động GRP client
	go StartGRPClient(gapRequestChan)

	log.Println("PITCH trading system started")
	select {}
}
```

---

### Xử Lý Gói Tin Bị Mất
- **Phát hiện gap**: Processor kiểm tra `last_processed + 1` không có trong `received_sequences`. Ví dụ, nếu nhận sequence 310171, 310172, rồi 310176, phát hiện gap tại 310173-310175.
- **Yêu cầu retransmission**: Gửi Gap Request qua GRP với `Unit=1`, `Sequence=310173`, `Count=3`.
- **Nhận retransmission**: Receiver nhận gói tin retransmission qua multicast gap response (địa chỉ riêng, ví dụ 233.218.133.81:30501), lưu vào Redis.
- **Tiếp tục xử lý**: Processor áp dụng các sequence 310173-310175, rồi xử lý các sequence đã cache (310176+).

---

### Tích Hợp Spin Server (Tùy Chọn)
Nếu cần đồng bộ order book khi khởi động hoặc gap quá lớn:
- Kết nối TCP tới Spin Server, gửi Login và Spin Request.
- Nhận Spin Response và các message (Trading Status, Add Order, Auction Update).
- Áp dụng message từ Spin Finished, sau đó xử lý multicast message từ sequence tiếp theo.

---

### Hiệu Năng và Low Latency
- **Receiver**: Nhận UDP và lưu Redis (~1-2ms localhost).
- **Processor**: Kiểm tra và xử lý sequence (~1ms).
- **GRP**: Gap Request qua TCP (~10-50ms tùy mạng), nhưng không block luồng chính nhờ goroutine.
- **Tổng độ trễ**: ~3-5ms khi không có gap, ~10-50ms khi có gap (tùy third-party).
- **Cải tiến**:
  - Dùng in-memory buffer thay Redis nếu cần <1ms.
  - Batch Gap Request để giảm số lần gọi GRP.
  - Tối ưu mạng (co-location với CXA).

---

### Cách Chạy
1. Cài Redis: `redis-server`.
2. Cấu hình multicast: Đảm bảo máy nhận được multicast từ địa chỉ/port trong tài liệu (Section 8).
3. Build và chạy:
   ```
   go mod init pitch-trading
   go get github.com/redis/go-redis/v9
   go run .
   ```

---

### So Sánh Với Node.js
Nếu dùng Node.js:
- **Ưu điểm**: Dễ viết hơn với `dgram` (UDP) và `net` (TCP), phù hợp nếu đội ngũ quen JavaScript.
- **Nhược điểm**: Hiệu năng thấp hơn Go (~20-30% chậm hơn), quản lý đồng thời phức tạp hơn (Promise vs goroutines).
- **Thư viện**: `redis`, `kafkajs` (nếu dùng Kafka), hoặc `events` thay channel.

Nếu bạn muốn triển khai bằng Node.js, tôi có thể cung cấp code tương ứng. Bạn có cần thêm chi tiết hoặc muốn tích hợp Spin Server không?
