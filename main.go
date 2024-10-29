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
	sizeMB := 128
	numFiles := 100
	numThreads := 1
	results := [][]string{{"File Name", "Upload Time (seconds)"}}
	tmpDir := "./tmp"

	// Create tmp directory if it doesn't exist
	createTmpDirectory(tmpDir)

	log.Println("Start uploading files...")

	// Step 1: Upload files concurrently using specified number of Goroutines, creating files as needed
	uploadFilesConcurrently(bucket, tmpDir, sizeMB, numFiles, numThreads, &results)

	// Step 2: Save or update results to CSV
	csvFileName := fmt.Sprintf("upload_times_%dMB_%dThr.csv", sizeMB, numThreads)
	appendToCSV(csvFileName, results)
	fmt.Printf("Upload times saved to %s\n", csvFileName)
}

func uploadFilesConcurrently(bucket string, tmpDir string, sizeMB, numFiles, numThreads int, results *[][]string) {
	var wg sync.WaitGroup
	uploadCh := make(chan int, numThreads) // Buffered channel to manage concurrent uploads
	resultCh := make(chan []string, numFiles)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fileIndex := range uploadCh {
				fileName := fmt.Sprintf("file-u_%d", fileIndex)
				filePath := filepath.Join(tmpDir, fileName)

				if err := CreateRandomFile(filePath, sizeMB); err != nil {
					log.Printf("Error creating file %s: %v", filePath, err)
					continue
				}

				objectName := filepath.Base(filePath)
				uploadDuration, err := uploadObject(bucket, objectName, filePath)
				if err != nil {
					log.Printf("upload failed for %s: %v", filePath, err)
					os.Remove(filePath)
					continue
				}

				fmt.Printf("Uploaded %s in %.2f seconds\n", objectName, uploadDuration.Seconds())
				resultCh <- []string{objectName, fmt.Sprintf("%.2f", uploadDuration.Seconds())}

				os.Remove(filePath)
			}
		}()
	}

	// Add file indices to the upload channel
	for i := 1; i <= numFiles; i++ {
		uploadCh <- i
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
