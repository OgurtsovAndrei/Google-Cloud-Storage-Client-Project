package main

import (
	"awesomeProject/writers"
	"context"
	"fmt"
	"math/rand"
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

	config := writers.ReliableWriterConfig{
		MaxCacheSize: 64 * 1024 * 1024,
		MinChunkSize: 256 * 1024,
		MaxChunkSize: 16 * 1024 * 1024,
	}

	rw := writers.NewReliableWriterImpl(ctx, unreliableWriter, config)
	// Total size to write (1 GB)
	var totalSize int64 = 256 * 1024 * 1024

	// Possible chunk sizes (powers of 2 from 1 KB to 128 MB)
	chunkSizes := []int64{
		1 * 1024,          // 1 KB
		2 * 1024,          // 2 KB
		4 * 1024,          // 4 KB
		8 * 1024,          // 8 KB
		16 * 1024,         // 16 KB
		32 * 1024,         // 32 KB
		64 * 1024,         // 64 KB
		128 * 1024,        // 128 KB
		256 * 1024,        // 256 KB
		512 * 1024,        // 512 KB
		1 * 1024 * 1024,   // 1 MB
		2 * 1024 * 1024,   // 2 MB
		4 * 1024 * 1024,   // 4 MB
		8 * 1024 * 1024,   // 8 MB
		16 * 1024 * 1024,  // 16 MB
		32 * 1024 * 1024,  // 32 MB
		64 * 1024 * 1024,  // 64 MB
		128 * 1024 * 1024, // 128 MB
	}

	rand.Seed(time.Now().UnixNano())

	fmt.Println("Starting to write 1 GB file...")

	for written := int64(0); written < totalSize; {
		remaining := totalSize - written
		var chunkSize int64

		chunkSize = chunkSizes[rand.Intn(len(chunkSizes))]
		if chunkSize > remaining {
			chunkSize = remaining
		}

		// Generate the chunk with random data
		data := make([]byte, chunkSize)
		for i := 0; i < len(data); i++ {
			data[i] = byte(rand.Intn(256))
		}

		err := rw.WriteAt(ctx, data, written)

		if err != nil {
			fmt.Println("Error during writing:", err)
			rw.Abort(ctx)
			return
		}

		written += chunkSize
	}

	fmt.Println("Calling Complete")
	err = rw.Complete(ctx)
	if err != nil {
		fmt.Println("Error during completion:", err)
		return
	}

	fmt.Println("File writing completed successfully.")
}
