package main

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
)

func download(gcsFile GCSEvent, localDir string) (*os.File, error) {
	log.Printf("Downloading input file from GCS: bucket [%s], object [%s]", gcsFile.Bucket, gcsFile.Name)
	localPath := filepath.Join(localDir, gcsFile.Name)
	tmpInputFile, err1 := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY, 0777)
	if err1 != nil {
		return nil, err1
	}
	defer tmpInputFile.Close()
	ctx := context.Background()
	reader, err2 := storageClient.Bucket(gcsFile.Bucket).Object(gcsFile.Name).NewReader(ctx)
	if err2 != nil {
		return nil, err2
	}
	defer reader.Close()
	n, err3 := io.Copy(tmpInputFile, reader)
	if err3 != nil {
		return nil, err3
	}
	log.Printf("Downloaded %d bytes from gs://%s/%s to %s", n, gcsFile.Bucket, gcsFile.Name, localPath)
	return tmpInputFile, nil
}

func upload(localFilePath string, bucket string) error {
	objectName := filepath.Base(localFilePath)
	log.Printf("Uploading PDF result [%s] to GCS bucket [%s]", objectName, bucket)
	localFile, err1 := os.Open(localFilePath)
	if err1 != nil {
		return err1
	}
	ctx := context.Background()
	writer := storageClient.Bucket(bucket).Object(objectName).NewWriter(ctx)
	n, err2 := io.Copy(writer, localFile)
	if err2 != nil {
		return err2
	}
	err3 := writer.Close()
	if err3 != nil {
		return err3
	}
	log.Printf("Uploaded %d bytes from %s to gs://%s/%s", n, localFilePath, bucket, objectName)

	return nil
}

func deleteGCSFile(bucket, name string) error {
	log.Println("Deleting input file from GCS")
	ctx := context.Background()
	return storageClient.Bucket(bucket).Object(name).Delete(ctx)
}

var storageClient *storage.Client

func init() {
	ctx := context.Background()
	var err error
	storageClient, err = storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if os.Getenv("PDF_BUCKET") == "" {
		log.Fatal("Need env var PDF_BUCKET: target GCS bucket where generated PDF will be written")
	}
}
