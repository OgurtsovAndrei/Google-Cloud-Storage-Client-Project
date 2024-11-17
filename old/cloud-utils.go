package old

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

func uploadObject(bucketName, objectName, filePath string) (time.Duration, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	f, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	start := time.Now()
	wc := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return 0, fmt.Errorf("failed to write data to bucket: %w", err)
	}
	if err := wc.Close(); err != nil {
		return 0, fmt.Errorf("failed to close writer: %w", err)
	}

	return time.Since(start), nil
}

func uploadObjectInChunksWithSpeed(bucketName, objectName, filePath string, chunkSize int64) (time.Duration, []string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	f, err := os.Open(filePath)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	start := time.Now()
	wc := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	buffer := make([]byte, chunkSize)
	var chunkSpeeds []string
	chunkCount := 0

	fmt.Printf("Starting upload of file: %s\n", filePath)
	fmt.Printf("Target bucket: %s, object name: %s\n", bucketName, objectName)
	fmt.Printf("Chunk size: %d bytes (%.2f MB)\n", chunkSize, float64(chunkSize)/(1024*1024))

	for {
		readStart := time.Now()
		n, err := f.Read(buffer)
		if err != nil && err != io.EOF {
			return 0, nil, fmt.Errorf("failed to read file: %w", err)
		}
		if n == 0 { // EOF
			break
		}

		chunkCount++
		fmt.Printf("Processing chunk #%d\n", chunkCount)
		fmt.Printf("Read %d bytes from file\n", n)

		writeStart := time.Now()
		if _, err := wc.Write(buffer[:n]); err != nil {
			return 0, nil, fmt.Errorf("failed to write chunk #%d to bucket: %w", chunkCount, err)
		}
		writeDuration := time.Since(writeStart).Seconds()
		fmt.Printf("Chunk #%d written to bucket in %.2f seconds\n", chunkCount, writeDuration)

		readDuration := time.Since(readStart).Seconds()
		chunkSpeed := float64(n) / readDuration / (1024 * 1024) // MB/s
		fmt.Printf("Chunk #%d speed: %.2f MB/s (read duration: %.2f seconds)\n", chunkCount, chunkSpeed, readDuration)

		chunkSpeeds = append(chunkSpeeds, fmt.Sprintf("%.2f", chunkSpeed))
	}

	if err := wc.Close(); err != nil {
		return 0, nil, fmt.Errorf("failed to close writer: %w", err)
	}

	totalDuration := time.Since(start)
	fmt.Printf("Upload complete for file: %s\n", filePath)
	fmt.Printf("Total upload time: %.2f seconds\n", totalDuration.Seconds())

	return totalDuration, chunkSpeeds, nil
}

func uploadObjectInChunksWithSpeedWithBounds(bucketName, objectName, filePath string, chunkSize int64) (time.Duration, []string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	f, err := os.Open(filePath)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	start := time.Now()
	wc := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	buffer := make([]byte, chunkSize)
	var chunkSpeeds []string
	chunkCount := 0

	fmt.Printf("Starting upload of file: %s\n", filePath)
	fmt.Printf("Target bucket: %s, object name: %s\n", bucketName, objectName)
	fmt.Printf("Chunk size: %d bytes (%.2f MB)\n", chunkSize, float64(chunkSize)/(1024*1024))

	for {
		readStart := time.Now()
		n, err := f.Read(buffer)
		if err != nil && err != io.EOF {
			return 0, nil, fmt.Errorf("failed to read file: %w", err)
		}
		if n == 0 { // EOF
			break
		}

		chunkCount++
		fmt.Printf("Processing chunk #%d\n", chunkCount)
		fmt.Printf("Read %d bytes from file\n", n)

		writeStart := time.Now()
		if _, err := wc.Write(buffer[:n]); err != nil {
			return 0, nil, fmt.Errorf("failed to write chunk #%d to bucket: %w", chunkCount, err)
		}
		writeDuration := time.Since(writeStart).Seconds()
		fmt.Printf("Chunk #%d written to bucket in %.2f seconds\n", chunkCount, writeDuration)

		readDuration := time.Since(readStart).Seconds()
		if readDuration < 0.05 { // Минимальный порог времени
			fmt.Printf("Chunk #%d speed calculation skipped (too short read duration: %.2f seconds)\n", chunkCount, readDuration)
			chunkSpeeds = append(chunkSpeeds, "N/A")
			continue
		}

		chunkSpeed := float64(n) / readDuration / (1024 * 1024) // MB/s
		fmt.Printf("Chunk #%d speed: %.2f MB/s (read duration: %.2f seconds)\n", chunkCount, chunkSpeed, readDuration)

		chunkSpeeds = append(chunkSpeeds, fmt.Sprintf("%.2f", chunkSpeed))
	}

	if err := wc.Close(); err != nil {
		return 0, nil, fmt.Errorf("failed to close writer: %w", err)
	}

	totalDuration := time.Since(start)
	fmt.Printf("Upload complete for file: %s\n", filePath)
	fmt.Printf("Total upload time: %.2f seconds\n", totalDuration.Seconds())

	return totalDuration, chunkSpeeds, nil
}
