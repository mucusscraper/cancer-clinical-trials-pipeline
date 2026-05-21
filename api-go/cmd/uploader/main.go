package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return
	}
	client := s3.NewFromConfig(cfg)
	err = godotenv.Load()
	if err != nil {
		return
	}
	bucketName := os.Getenv("S3_BUCKET_NAME")
	rawDataDir := "./data/raw"
	conditions, err := os.ReadDir(rawDataDir)
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, condition := range conditions {
		conditionPath := fmt.Sprintf("%s/%s", rawDataDir, condition.Name())
		filesToUploadByCondition, err := os.ReadDir(conditionPath)
		if err != nil {
			return
		}
		for _, fileToUpload := range filesToUploadByCondition {
			fileToUploadPath := fmt.Sprintf("%s/%s", conditionPath, fileToUpload.Name())
			key := strings.TrimPrefix(fileToUploadPath, "./data/")
			wg.Add(1)
			go func(fileToUploadPath string, key string) {
				defer wg.Done()
				err = uploadFileToS3(bucketName, fileToUploadPath, key, client)
				if err != nil {
					fmt.Printf("failed upload %s: %v\n", key, err)
				}
			}(fileToUploadPath, key)
		}
	}
	wg.Wait()
}

func uploadFileToS3(bucketName, filePath, key string, client *s3.Client) error {
	if bucketName == "" {
		panic("Missing S3 Bucket Name")
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return err
	}
	return nil
}
