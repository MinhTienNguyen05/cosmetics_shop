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

const offsetFile = "/opt/airflow/data/producer_offset.txt"
const batchSize = 100000

func readCheckpoint() (int, int) {
	data, err := os.ReadFile(offsetFile)
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


func saveCheckpoint(fileIdx int, recordOffset int) {
	state := fmt.Sprintf("%d,%d", fileIdx, recordOffset)
	os.WriteFile(offsetFile, []byte(state), 0644)
}

func main() {
	brokerAddr := os.Getenv("KAFKA_BROKERS")
	if brokerAddr == "" {
		brokerAddr = "kafka-1:9092"
	} else {
		parts := strings.Split(brokerAddr, ",")
		brokerAddr = parts[0]
	}

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

	startFileIdx, recordOffset := readCheckpoint()

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
		_, _ = reader.Read()

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
				if len(messageBatch) > 0 {
					errWrite := writer.WriteMessages(context.Background(), messageBatch...)
					if errWrite != nil {
						log.Printf("Lỗi khi gửi message dư cuối file %s: %v\n", filePath, errWrite)
					}
					messageBatch = nil
				}
				break
			}

			event := CosmeticsEvent{
				EventTime:    record[0],
				EventType:    record[1],
				ProductID:    record[2],
				CategoryID:   record[3],
				CategoryCode: record[4],
				Brand:        record[5],
				Price:        record[6],
				UserID:       record[7],
				UserSession:  record[8],
			}

			eventJSON, _ := json.Marshal(event)

			messageBatch = append(messageBatch, kafka.Message{
				Key:   []byte(event.UserID),
				Value: eventJSON,
			})

			currentRecord++
			sentCount++

			if len(messageBatch) >= 500 || sentCount >= batchSize {
				errWrite := writer.WriteMessages(context.Background(), messageBatch...)
				if errWrite != nil {
					log.Printf("Lỗi khi gửi message tại file %s: %v\n", filePath, errWrite)
				}
				messageBatch = nil
			}
			if sentCount >= batchSize {
				saveCheckpoint(i, currentRecord)
				file.Close()
				log.Printf("Đã đẩy đủ %d sự kiện cho chu kỳ này. Lưu checkpoint và thoát.\n", sentCount)
				return
			}
		}

		file.Close()

		saveCheckpoint(i+1, 0)
		log.Printf("Đã xử lý xong toàn bộ file: %s\n", filePath)
	}

	log.Println("Hoàn tất toàn bộ dữ liệu của 3 tháng!")
}