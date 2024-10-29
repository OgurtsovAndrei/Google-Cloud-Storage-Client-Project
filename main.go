package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	bucket := "my-awesome-fs-project-bucket-hard-delete"
	sizeMB := 32
	numFiles := 100
	numThreads := 1
	results := [][]string{{"File Name", "Upload Time (seconds)"}}
	tmpDir := "./tmp"

	// Create tmp directory if it doesn't exist
	createTmpDirectory(tmpDir)

	log.Println("Start creating files...")

	// Step 1: Create all files
	filePaths := createFiles(tmpDir, sizeMB, numFiles)

	log.Println("Start uploading files...")

	// Step 2: Upload files concurrently using specified number of Goroutines
	uploadFilesConcurrently(bucket, filePaths, numThreads, &results)

	// Step 3: Save or update results to CSV
	csvFileName := fmt.Sprintf("upload_times_%dMB_%dThr.csv", sizeMB, numThreads)
	appendToCSV(csvFileName, results)
	fmt.Printf("Upload times saved to %s\n", csvFileName)
}

func uploadFilesConcurrently(bucket string, filePaths []string, numThreads int, results *[][]string) {
	var wg sync.WaitGroup
	uploadCh := make(chan string, numThreads) // Buffered channel to manage concurrent uploads
	resultCh := make(chan []string, len(filePaths))

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filePath := range uploadCh {
				objectName := filepath.Base(filePath)

				// Upload file
				uploadDuration, err := uploadObject(bucket, objectName, filePath)
				if err != nil {
					log.Printf("upload failed for %s: %v", filePath, err)
					continue
				}

				// Record results
				fmt.Printf("Uploaded %s in %.2f seconds\n", objectName, uploadDuration.Seconds())
				resultCh <- []string{objectName, fmt.Sprintf("%.2f", uploadDuration.Seconds())}

				// Delete local file to save space (optional)
				os.Remove(filePath)
			}
		}()
	}

	// Add files to the upload channel
	for _, filePath := range filePaths {
		uploadCh <- filePath
	}
	close(uploadCh)

	// Wait for all Goroutines to finish
	wg.Wait()
	close(resultCh)

	// Collect results from the result channel
	for result := range resultCh {
		*results = append(*results, result)
	}
}
