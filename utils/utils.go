package utils

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type Error struct {
	Code  string
	Msg   string
	Cause error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

func (e *Error) Unwrap() error {
	return e.Cause
}

type UploadResult struct {
	FileName    string    `json:"file_name"`
	FileSizeMB  int       `json:"file_size_mb"`
	ChunkSizeMB int       `json:"chunk_size_mb"`
	UploadTime  float64   `json:"upload_time_seconds"`
	ChunkSpeeds []float64 `json:"chunk_speeds_mb_per_s"`
	TimePeriods []float64 `json:"time_periods_seconds"`
}

func makeRandBuf(len int) []byte {
	buf := make([]byte, len)
	n, err := rand.Read(buf)
	if err != nil || n != len {
		panic("failed to make a random buffer")
	}
	return buf
}

func saveJson(x any) []byte {
	b, err := json.Marshal(x)
	if err != nil {
		panic("json.Marshal() failed")
	}
	return b
}

func CreateRandomFile(fileName string, sizeMB int) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	oneMB := make([]byte, 1<<20)
	for i := 0; i < sizeMB; i++ {
		_, err := rand.Read(oneMB)
		if err != nil {
			return fmt.Errorf("failed to generate random data: %w", err)
		}
		if _, err := file.Write(oneMB); err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
	}
	return nil
}

// appendToCSV appends data to the CSV file as a new column if the file exists.
func appendToCSV(filename string, data [][]string) {
	var existingRecords [][]string

	// Check if the CSV file already exists
	if _, err := os.Stat(filename); err == nil {
		// Read existing data from CSV
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("failed to open CSV file for reading: %v", err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		existingRecords, err = reader.ReadAll()
		if err != nil {
			log.Fatalf("failed to read CSV file: %v", err)
		}

		// Validate headers
		if len(existingRecords) > 0 && len(data) > 0 {
			if !equalHeaders(existingRecords[0], data[0]) {
				log.Fatalf("header mismatch: existing file headers differ from new data headers")
			}
		}

		// Add new rows from data to existing records
		existingRecords = append(existingRecords, data[1:]...)
	} else {
		// If file doesn't exist, use data as-is
		existingRecords = data
	}

	// Write updated data to the CSV
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, record := range existingRecords {
		if err := writer.Write(record); err != nil {
			log.Fatalf("failed to write record to CSV: %v", err)
		}
	}
}

// Helper function to compare headers
func equalHeaders(existingHeader, newHeader []string) bool {
	if len(existingHeader) != len(newHeader) {
		return false
	}
	for i := range existingHeader {
		if existingHeader[i] != newHeader[i] {
			return false
		}
	}
	return true
}

func CreateTmpDirectory(tmpDir string) {
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		if err := os.Mkdir(tmpDir, os.ModePerm); err != nil {
			log.Fatalf("failed to create tmp directory: %v", err)
		}
	}
}

func AppendToJSON(fileName string, results []UploadResult) error {
	var existingResults []UploadResult

	// Ensure that the directory path exists
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories for path %s: %w", dir, err)
	}

	// Check if the JSON file already exists
	if _, err := os.Stat(fileName); err == nil {
		// File exists, open it for reading
		file, err := os.Open(fileName)
		if err != nil {
			return fmt.Errorf("failed to open JSON file for reading: %w", err)
		}
		defer file.Close()

		err = json.NewDecoder(file).Decode(&existingResults)
		if err != nil {
			return fmt.Errorf("failed to decode JSON file: %w", err)
		}
	} else if os.IsNotExist(err) {
		// File does not exist, initialize empty slice
		existingResults = []UploadResult{}
	} else {
		// Some other error occurred while checking file
		return fmt.Errorf("failed to stat JSON file: %w", err)
	}

	// Append new results
	existingResults = append(existingResults, results...)

	// Write updated data to the JSON file
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(existingResults); err != nil {
		return fmt.Errorf("failed to write to JSON file: %w", err)
	}

	return nil
}
