package old

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	bucket := "another-eu-1-reg-bucket-finland"
	sizeGB := 1     // Размер файла в ГБ
	numFiles := 1   // Количество файлов
	numThreads := 1 // Количество потоков
	chunkSizeMB := 16
	chunkSize := int64(chunkSizeMB * 1024 * 1024) // Размер чанка (8 МБ)
	results := [][]string{{"File Name", "Upload Time (seconds)", "Chunk Speeds (MB/s)"}}
	tmpDir := "./tmp"

	dirLocalEu := "Local-EU"
	//dir_eu_eu := "EU-EU"
	//dir_us_eu := "US-EU"
	dataDir := fmt.Sprintf("data/%s", dirLocalEu)

	// Создаём временную директорию, если она не существует
	createTmpDirectory(tmpDir)

	log.Println("Start uploading large files in chunks...")

	// Шаг 1: Загрузка файлов большего размера по чанкам с использованием горутин
	uploadFilesChunkedConcurrently(bucket, tmpDir, sizeGB, numFiles, numThreads, chunkSize, &results)

	// Шаг 2: Сохраняем или обновляем результаты в CSV
	csvFileName := fmt.Sprintf("%s/chunked_upload_times_%dGB_%dMB_ChSize_%dThr.csv", dataDir, sizeGB, chunkSizeMB, numThreads)
	appendToCSV(csvFileName, results)
	fmt.Printf("Upload times saved to %s\n", csvFileName)
}

func mainBasic() {
	bucket := "my-awesome-fs-project-bucket-hard-delete"
	sizeMB := 128
	numFiles := 100
	numThreads := 1
	results := [][]string{{"File Name", "Upload Time (seconds)"}}
	tmpDir := "./tmp"

	//dir_local_eu := "Local-EU"
	//dir_eu_eu := "EU-EU"
	dir_us_eu := "US-EU"
	data_dir := fmt.Sprintf("data/%s", dir_us_eu)

	// Create tmp directory if it doesn't exist
	createTmpDirectory(tmpDir)

	log.Println("Start uploading files...")

	// Step 1: Upload files concurrently using specified number of Goroutines, creating files as needed
	uploadFilesConcurrently(bucket, tmpDir, sizeMB, numFiles, numThreads, &results)

	// Step 2: Save or update results to CSV
	csvFileName := fmt.Sprintf("%s/upload_times_%dMB_%dThr.csv", data_dir, sizeMB, numThreads)
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
				uploadDuration, err := old.uploadObject(bucket, objectName, filePath)
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

func uploadFilesChunkedConcurrently(bucket string, tmpDir string, sizeGB, numFiles, numThreads int, chunkSize int64, results *[][]string) {
	var wg sync.WaitGroup
	uploadCh := make(chan int, numThreads) // Buffered channel for concurrent uploads
	resultCh := make(chan []string, numFiles)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fileIndex := range uploadCh {
				fileName := fmt.Sprintf("file-chunked_%d", fileIndex)
				filePath := filepath.Join(tmpDir, fileName)

				if err := CreateRandomFile(filePath, sizeGB*1024); err != nil {
					log.Printf("Error creating file %s: %v", filePath, err)
					continue
				}

				objectName := filepath.Base(filePath)
				uploadDuration, chunkSpeeds, err := old.uploadObjectInChunksWithSpeed(bucket, objectName, filePath, chunkSize)
				if err != nil {
					log.Printf("upload failed for %s: %v", filePath, err)
					os.Remove(filePath)
					continue
				}

				resultRow := []string{objectName, fmt.Sprintf("%.2f", uploadDuration.Seconds())}
				resultRow = append(resultRow, chunkSpeeds...)
				resultCh <- resultRow

				os.Remove(filePath)
			}
		}()
	}

	for i := 1; i <= numFiles; i++ {
		uploadCh <- i
	}
	close(uploadCh)

	wg.Wait()
	close(resultCh)

	for result := range resultCh {
		*results = append(*results, result)
	}
}
