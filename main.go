package main

import (
	"awesomeProject/writers"
	"context"
	"fmt"
	"math/rand"
	"time"
)

var (
	bucket               = "another-eu-1-reg-bucket-finland"
	sizeMB               = 256
	totalSize            = int64(sizeMB * 1024 * 1024)
	fileName             = "1GB_output_file.dat"
	maxCacheSize  uint32 = 64 * 1024 * 1024
	minChunkSize  uint32 = 256 * 1024
	maxChunkSize  uint32 = 16 * 1024 * 1024
	listenAddress        = ":9000"
)

func main() {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	go func() {
		if err := startGCSProxyServer(ctx, listenAddress); err != nil {
			fmt.Printf("Failed to start GCSProxyServer: %v\n", err)
			cancel()
		}
	}()

	time.Sleep(1 * time.Second)

	//unreliableWriter, err := writers.NewUnreliableLocalWriter(fileName)
	//unreliableWriter, err := writers.NewUnreliableGCSWriter(ctx, bucket, fileName)
	unreliableWriter, err := writers.NewUnreliableProxyWriter("localhost"+listenAddress, bucket, fileName)
	if err != nil {
		fmt.Println("Failed to create UnreliableProxyWriter:", err)
		return
	}

	config := writers.ReliableWriterConfig{
		MaxCacheSize: maxCacheSize,
		MinChunkSize: minChunkSize,
		MaxChunkSize: maxChunkSize,
	}

	rw := writers.NewReliableWriterImpl(ctx, unreliableWriter, config)

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

	rnd := rand.New(rand.NewSource(42))

	fmt.Println("Starting to write 1 GB file...")

	for written := int64(0); written < totalSize; {
		remaining := totalSize - written
		var chunkSize int64

		chunkSize = chunkSizes[rnd.Intn(len(chunkSizes))]
		if chunkSize > remaining {
			chunkSize = remaining
		}

		// Generate the chunk with random data
		data := make([]byte, chunkSize)
		for i := 0; i < len(data); i++ {
			data[i] = byte(rnd.Intn(256))
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
