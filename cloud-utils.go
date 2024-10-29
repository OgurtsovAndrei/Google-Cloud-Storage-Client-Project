package main

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
