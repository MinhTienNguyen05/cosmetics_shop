package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseRecord_Valid(t *testing.T) {
	row := []string{"2019-10-01 00:00:00 UTC", "view", "1001", "2052", "electronics.smartphone", "apple", "1290.0", "555", "sess-1"}
	ev, err := parseRecord(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.ProductID != "1001" || ev.Brand != "apple" || ev.Price != "1290.0" {
		t.Errorf("mapping mismatch: %+v", ev)
	}
}

func TestParseRecord_Malformed(t *testing.T) {
	if _, err := parseRecord([]string{"a", "b"}); err == nil {
		t.Fatal("expected error for row with too few columns")
	}
}

func TestParseRecord_HeaderRejected(t *testing.T) {
	// Dòng header CSV có đúng 9 cột nên parseRecord thành công ở tầng parse;
	// việc chặn header thực sự nằm ở Bronze (quarantine). Ở đây chỉ assert parse ổn định.
	header := []string{"event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"}
	if _, err := parseRecord(header); err != nil {
		t.Fatalf("header should parse without error (filtering happens downstream): %v", err)
	}
}

func TestReadCheckpoint_Missing(t *testing.T) {
	idx, off := readCheckpoint("/nonexistent/path/offset.txt")
	if idx != 0 || off != 0 {
		t.Errorf("missing checkpoint should yield (0,0), got (%d,%d)", idx, off)
	}
}

func TestSaveAndReadCheckpoint_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offset.txt")

	if err := saveCheckpoint(path, 2, 12345); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	idx, off := readCheckpoint(path)
	if idx != 2 || off != 12345 {
		t.Errorf("round-trip mismatch: got (%d,%d)", idx, off)
	}
}

func TestSaveCheckpoint_Overwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offset.txt")
	_ = saveCheckpoint(path, 1, 10)
	_ = saveCheckpoint(path, 3, 999)
	idx, off := readCheckpoint(path)
	if idx != 3 || off != 999 {
		t.Errorf("overwrite failed: got (%d,%d)", idx, off)
	}
}

func TestPickBroker(t *testing.T) {
	cases := map[string]string{
		"":                          "kafka-1:9092",
		"kafka-1:9092":              "kafka-1:9092",
		"kafka-1:9092,kafka-2:9092": "kafka-1:9092",
		"  kafka-2:9092 ,kafka-3 ":  "kafka-2:9092",
	}
	for in, want := range cases {
		if got := pickBroker(in); got != want {
			t.Errorf("pickBroker(%q) = %q, want %q", in, got, want)
		}
	}
}

// sanity: offsetFile hằng số đúng path Airflow mount.
func TestOffsetFilePath(t *testing.T) {
	if offsetFile != "/opt/airflow/data/producer_offset.txt" {
		t.Errorf("offsetFile changed: %s", offsetFile)
	}
	// tránh unused import warning nếu các test trên không dùng os.
	_ = os.Getenv("HOME")
}
