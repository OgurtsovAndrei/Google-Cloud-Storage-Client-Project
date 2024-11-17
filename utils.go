package main

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

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

func createTmpDirectory(tmpDir string) {
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		if err := os.Mkdir(tmpDir, os.ModePerm); err != nil {
			log.Fatalf("failed to create tmp directory: %v", err)
		}
	}
}
