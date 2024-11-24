package main

import (
	"awesomeProject/writers"
	"context"
	"fmt"
	"time"
)

func main() {
	filePath := "1GB_output_file.dat"

	unreliableWriter, err := writers.NewUnreliableLocalWriter(filePath)
	if err != nil {
		fmt.Println("Failed to create UnreliableLocalWriter:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	rw := writers.NewReliableWriterImpl(ctx, unreliableWriter)
	rw.MaxCacheSize = 64 * 1024 * 1024
	rw.MaxChunkSize = 16 * 1024 * 1024

	// Generate a 1 GB buffer
	var totalSize int64 = 1 * 1024 * 1024 * 1024
	chunkSize := 128 * 1024 * 1024
	data := make([]byte, chunkSize)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	fmt.Println("Starting to write 1 GB file...")

	for written := int64(0); written < totalSize; written += int64(len(data)) {
		isLast := written+int64(len(data)) >= totalSize
		err := rw.WriteAt(ctx, data, written)
		if err != nil {
			fmt.Println("Error during writing:", err)
			rw.Abort(ctx)
			return
		}

		if isLast {
			err = rw.Complete(ctx)
			if err != nil {
				fmt.Println("Error during completion:", err)
				return
			}
			break
		}
	}

	fmt.Println("File writing completed successfully.")
}
