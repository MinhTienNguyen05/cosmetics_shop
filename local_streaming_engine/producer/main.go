package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

// CosmeticsEvent là payload JSON đẩy lên topic ecommerce_events.
// Tất cả field giữ dạng string để bảo toàn đúng trạng thái nguồn (Bronze sẽ cast kiểu).
type CosmeticsEvent struct {
	EventTime    string `json:"event_time"`
	EventType    string `json:"event_type"`
	ProductID    string `json:"product_id"`
	CategoryID   string `json:"category_id"`
	CategoryCode string `json:"category_code"`
	Brand        string `json:"brand"`
	Price        string `json:"price"`
	UserID       string `json:"user_id"`
	UserSession  string `json:"user_session"`
}

const (
	offsetFile  = "/opt/airflow/data/producer_offset.txt"
	batchSize   = 100000
	sendBatchAt = 500 // ngưỡng số message trong 1 lô trước khi flush Kafka
)

// numColumns là số cột hợp lệ của 1 dòng CSV event.
const numColumns = 9

// parseRecord ánh xạ 1 dòng CSV → CosmeticsEvent và validate cơ bản.
// Trả về error khi dòng thiếu cột (malformed) — Bronze sẽ không bao giờ nhận các dòng này.
func parseRecord(record []string) (CosmeticsEvent, error) {
	if len(record) < numColumns {
		return CosmeticsEvent{}, fmt.Errorf("malformed row: expect %d columns, got %d", numColumns, len(record))
	}
	return CosmeticsEvent{
		EventTime:    record[0],
		EventType:    record[1],
		ProductID:    record[2],
		CategoryID:   record[3],
		CategoryCode: record[4],
		Brand:        record[5],
		Price:        record[6],
		UserID:       record[7],
		UserSession:  record[8],
	}, nil
}

// readCheckpoint đọc (fileIdx, recordOffset) đã lưu. File chưa có → (0,0).
// Hàm lấy path làm tham số để dễ test và không phụ thuộc filesystem toàn cục.
func readCheckpoint(path string) (int, int) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0
	}
	parts := strings.Split(strings.TrimSpace(string(data)), ",")
	if len(parts) != 2 {
		return 0, 0
	}
	fileIdx, _ := strconv.Atoi(parts[0])
	recordOffset, _ := strconv.Atoi(parts[1])
	return fileIdx, recordOffset
}

// saveCheckpoint ghi atomic-enough (ghi đè) trạng thái (fileIdx, recordOffset).
func saveCheckpoint(path string, fileIdx int, recordOffset int) error {
	state := fmt.Sprintf("%d,%d", fileIdx, recordOffset)
	return os.WriteFile(path, []byte(state), 0644)
}

// pickBroker lấy broker đầu tiên từ biến KAFKA_BROKERS (comma-separated),
// fallback về kafka-1:9092 khi chưa set.
func pickBroker(envBroker string) string {
	if envBroker == "" {
		return "kafka-1:9092"
	}
	return strings.TrimSpace(strings.Split(envBroker, ",")[0])
}

func main() {
	brokerAddr := pickBroker(os.Getenv("KAFKA_BROKERS"))

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddr),
		Topic:    "ecommerce_events",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	fileNames := []string{
		"/opt/airflow/data/2019-Oct.csv",
		"/opt/airflow/data/2019-Nov.csv",
		"/opt/airflow/data/2019-Dec.csv",
	}

	startFileIdx, recordOffset := readCheckpoint(offsetFile)

	if startFileIdx >= len(fileNames) {
		log.Println("Đã xử lý xong toàn bộ các file. Không còn dữ liệu để đẩy.")
		return
	}

	log.Printf("Bắt đầu chạy batch mới. Tiếp tục từ File Index: %d, Dòng: %d\n", startFileIdx, recordOffset)
	sentCount := 0

	for i := startFileIdx; i < len(fileNames); i++ {
		filePath := fileNames[i]
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("Bỏ qua file %s vì lỗi: %v\n", filePath, err)
			continue
		}

		reader := csv.NewReader(file)
		_, _ = reader.Read() // bỏ qua dòng header

		currentRecord := 0

		if i == startFileIdx {
			log.Printf("Đang tua nhanh qua %d dòng đã xử lý của file %s...\n", recordOffset, filePath)
			for currentRecord < recordOffset {
				_, err := reader.Read()
				if err != nil {
					break // Hết file
				}
				currentRecord++
			}
		} else {
			recordOffset = 0
		}

		log.Printf("Bắt đầu đẩy dữ liệu vào Kafka từ file: %s\n", filePath)

		var messageBatch []kafka.Message

		for {
			record, err := reader.Read()
			if err != nil {
				// Xả nốt lô dư khi hết file.
				if len(messageBatch) > 0 {
					if errWrite := writer.WriteMessages(context.Background(), messageBatch...); errWrite != nil {
						log.Printf("Lỗi khi gửi message dư cuối file %s: %v\n", filePath, errWrite)
					}
					messageBatch = nil
				}
				break
			}

			event, err := parseRecord(record)
			if err != nil {
				// Bỏ qua dòng malformed (thiếu cột) — không đẩy rác vào Kafka.
				log.Printf("Bỏ qua dòng lỗi tại file %s: %v\n", filePath, err)
				continue
			}

			eventJSON, _ := json.Marshal(event)

			messageBatch = append(messageBatch, kafka.Message{
				Key:   []byte(event.UserID),
				Value: eventJSON,
			})

			currentRecord++
			sentCount++

			if len(messageBatch) >= sendBatchAt || sentCount >= batchSize {
				if errWrite := writer.WriteMessages(context.Background(), messageBatch...); errWrite != nil {
					log.Printf("Lỗi khi gửi message tại file %s: %v\n", filePath, errWrite)
				}
				messageBatch = nil
			}
			if sentCount >= batchSize {
				if err := saveCheckpoint(offsetFile, i, currentRecord); err != nil {
					log.Printf("Cảnh báo: không lưu được checkpoint: %v\n", err)
				}
				file.Close()
				log.Printf("Đã đẩy đủ %d sự kiện cho chu kỳ này. Lưu checkpoint và thoát.\n", sentCount)
				return
			}
		}

		file.Close()

		if err := saveCheckpoint(offsetFile, i+1, 0); err != nil {
			log.Printf("Cảnh báo: không lưu được checkpoint sau file %s: %v\n", filePath, err)
		}
		log.Printf("Đã xử lý xong toàn bộ file: %s\n", filePath)
	}

	log.Println("Hoàn tất toàn bộ dữ liệu của 3 tháng!")
}
