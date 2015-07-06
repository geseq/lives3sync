package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func (s *Sync) Upload(sequence uint64, f string) error {
	key := f
	if s.Prefix != "" {
		key = path.Join(s.Prefix, key)
	}
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	inputFile, err := os.Open(f)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	stat, err := inputFile.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()

	s3Location := fmt.Sprintf("s3:///%s%s", s.Bucket, key)

	// do a HEAD request against s3 and see if the file is already there
	if s3size, err := s.head(key); err == nil {
		if s3size != -1 && size != s3size {
			log.Printf("[%s] size mismatch. overwriting s3 (locally: %d s3: %d) %q => %s", sequence, size, s3size, f, s3Location)
		} else if size == s3size {
			log.Printf("[%d] skipping. same file already exists %q => %s", sequence, f, s3Location)
			return nil
		}
	} else {
		return err
	}

	log.Printf("[%d] uploading %q => %s size:%d bytes", sequence, f, s3Location, size)
	start := time.Now().Truncate(time.Millisecond)

	if s.DryRun {
		log.Printf("[%d] dry run. not uploading", sequence)
		return nil
	}

	uploader := s3manager.NewUploader(&s3manager.UploadOptions{
		S3:       s.S3,
		PartSize: 1024 * 1024 * 10,
	})

	resp, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   inputFile,
	},
	)
	if err != nil {
		return err
	}
	duration := time.Now().Truncate(time.Millisecond).Sub(start)
	rate := float64(size) / (float64(duration) / float64(time.Second)) / 1024
	log.Printf("[%d] finished %s took:%s rate:%.fkB/s size:%d bytes", sequence, resp.Location, duration, rate, size)
	return nil
}

// head makes a HEAD request to get file metadata (if present)
// size -1 returned on missing files
func (s *Sync) head(key string) (size int64, err error) {
	var resp *s3.HeadObjectOutput
	resp, err = s.S3.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.RequestFailure); ok {
			switch awsErr.StatusCode() {
			case 404:
				err = nil
				size = -1
				return
			default:
				return
			}
		}
		return
	}
	size = *resp.ContentLength
	return
}
