package main

import (
	"awesomeProject/utils"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	numFiles    = 8
	numThreads  = 1
	chunkSizeMB = 128
	tmpDir      = "./tmp"
	dataDir     = "data/onServer"
	chunkSize   = int64(chunkSizeMB * 1024 * 1024)
)

func runUploadAnMeasureResults() {
	fmt.Printf("Running main() ...\n")
	utils.CreateTmpDirectory(tmpDir)

	var results []utils.UploadResult

	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("tmpfile_%d.dat", i+1)
		filePath := filepath.Join(tmpDir, fileName)

		err := utils.CreateRandomFile(filePath, sizeMB) // sizeMB in MB
		if err != nil {
			log.Fatalf("Failed to create random file: %v", err)
		}
		defer os.Remove(filePath)

		result, err := uploadFileChunked(filePath, bucket)
		if err != nil {
			fmt.Printf("Failed to upload file chunked: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("File uploaded successfully!")
		os.Remove(filePath)

		results = append(results, result)
	}

	jsonFileName := fmt.Sprintf("%s/chunked_upload_results.json", dataDir)
	err := utils.AppendToJSON(jsonFileName, results)
	if err != nil {
		log.Fatalf("Failed to write results to JSON file: %v", err)
	}
	fmt.Printf("Upload results saved to %s\n", jsonFileName)
}

func uploadFileChunked(filePath, bucket string) (utils.UploadResult, error) {
	ctx := context.Background()

	if bucket == "" {
		return utils.UploadResult{}, fmt.Errorf("destination bucket must be specified")
	}

	gcsClient, err := utils.NewGcsClient(ctx)
	if err != nil {
		return utils.UploadResult{}, err
	}

	// Open the file to be uploaded
	file, err := os.Open(filePath)
	if err != nil {
		return utils.UploadResult{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Initiate a resumable upload session
	uploadUrl, err := gcsClient.NewUploadSession(ctx, bucket, filepath.Base(filePath))
	if err != nil {
		return utils.UploadResult{}, fmt.Errorf("failed to start upload session: %w", err)
	}

	buffer := make([]byte, chunkSize)
	offset := int64(0)
	last := false
	var chunkSpeeds []float64
	var timePeriods []float64
	chunkCount := 0
	start := time.Now()

	fmt.Printf("Starting upload of file: %s\n", filePath)
	fmt.Printf("Target bucket: %s\n", bucket)
	fmt.Printf("Chunk size: %d bytes (%.2f MB)\n", chunkSize, float64(chunkSize)/(1024*1024))

	for {
		readStart := time.Now()
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return utils.UploadResult{}, fmt.Errorf("failed to read chunk: %w", err)
		}
		if n == 0 {
			break // No more data to read from the file
		}

		chunkCount++
		fmt.Printf("Processing chunk #%d\n", chunkCount)
		fmt.Printf("Read %d bytes from file\n", n)

		writeStart := time.Now()
		// Check if this is the last chunk
		if n < int(chunkSize) {
			last = true
		}

		// Upload the chunk
		err = gcsClient.UploadObjectPart(ctx, uploadUrl, offset, bytes.NewReader(buffer[:n]), int64(n), last)
		if err != nil {
			return utils.UploadResult{}, fmt.Errorf("failed to upload chunk at offset %d: %w", offset, err)
		}
		writeDuration := time.Since(writeStart).Seconds()
		timePeriods = append(timePeriods, writeDuration)
		fmt.Printf("Chunk #%d written to bucket in %.2f seconds\n", chunkCount, writeDuration)

		readDuration := time.Since(readStart).Seconds()
		if readDuration < 0.05 {
			fmt.Printf("Chunk #%d speed calculation skipped (too short read duration: %.2f seconds)\n", chunkCount, readDuration)
			chunkSpeeds = append(chunkSpeeds, 0)
			continue
		}

		chunkSpeed := float64(n) / readDuration / (1024 * 1024) // MB/s
		fmt.Printf("Chunk #%d speed: %.2f MB/s (read duration: %.2f seconds)\n", chunkCount, chunkSpeed, readDuration)

		chunkSpeeds = append(chunkSpeeds, chunkSpeed)

		// Update offset
		offset += int64(n)
	}

	totalDuration := time.Since(start).Seconds()
	fmt.Printf("Upload complete for file: %s\n", filePath)
	fmt.Printf("Total upload time: %.2f seconds\n", totalDuration)

	return utils.UploadResult{
		FileName:    filepath.Base(filePath),
		FileSizeMB:  sizeMB,
		ChunkSizeMB: chunkSizeMB,
		UploadTime:  totalDuration,
		ChunkSpeeds: chunkSpeeds,
		TimePeriods: timePeriods,
	}, nil
}
