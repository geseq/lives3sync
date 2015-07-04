package main

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Sync struct {
	Bucket string
	S3     *s3.S3
	Src    string
	Prefix string
	DryRun bool

	count      uint64
	queue      chan string
	pending    map[string]bool
	upload     chan string
	uploadDone chan bool

	sync.RWMutex
}

func NewSync() *Sync {
	s := &Sync{
		queue:      make(chan string, 100),
		pending:    make(map[string]bool),
		upload:     make(chan string),
		uploadDone: make(chan bool, 1),
	}
	return s
}

func (s *Sync) nextSequence() uint64 {
	return atomic.AddUint64(&s.count, 1)
}

func (s *Sync) firstPass() {
	handle := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		s.queue <- path
		return nil
	}
	err := filepath.Walk(s.Src, handle)
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("exiting firstPass. closing s.queue")
	close(s.queue)
}

func (s *Sync) Uploader(wg *sync.WaitGroup) {
	// goroutine that listens for files to upload
	for file := range s.upload {
		sequence := s.nextSequence()
		err := s.Upload(sequence, file)
		if err != nil {
			log.Printf("[%d] error: %s - %s", sequence, err, file)
		}
		select {
		case s.uploadDone <- true:
		default:
			continue
		}
	}
	log.Printf("Uploader Done")
	wg.Done()
}

func (s *Sync) Run() {
	// start fsnotify
	go s.firstPass()
	for f := range s.queue {
		s.upload <- f
	}
	log.Printf("exiting Run(). closing s.upload")
	close(s.upload)
}

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
	// TODO: size check against content already in S3
	log.Printf("[%d] - %q => s3:///%s%s size:%d bytes", sequence, f, s.Bucket, key, size)
	start := time.Now()

	if s.DryRun {
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
	duration := time.Since(start)
	log.Printf("[%d] - finished %s %s", resp.Location, duration)
	return nil
}

// resp, err := svc.ListObjects(&s3.ListObjectsInput{
// 	Bucket:  aws.String(settings.GetString("s3_bucket")),
// 	Prefix:  aws.String("data/"),
// 	MaxKeys: aws.Long(1000),
// })
// if awsErr, ok := err.(awserr.Error); ok {
// 	// A service error occurred.
// 	log.Fatalf("Error: %v %v", awsErr.Code, awsErr.Message)
// } else if err != nil {
// 	// A non-service error occurred.
// 	log.Fatalf("%v", err)
// }
