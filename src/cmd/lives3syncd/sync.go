package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Sync struct {
	Bucket  string
	S3      *s3.S3
	Src     string
	Prefix  string
	DryRun  bool
	Match   StringArray
	Exclude StringArray

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

func matchesAny(m []string, p string) (ok bool) {
	for _, mm := range m {
		if matched, err := path.Match(mm, p); err != nil {
			panic(err.Error())
		} else if matched {
			return true
		}
	}
	return
}

func (s *Sync) firstPass(q chan<- string) {
	log.Printf("Starting initial sync of %q", s.Src)
	var fileCount, excluded, nonMatch int64
	handle := func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasPrefix(path.Base(p), ".") {
			// skip .hidden_files
			return nil
		}
		if matchesAny(s.Exclude, p) {
			excluded++
			return nil
		}
		if len(s.Match) > 0 && !matchesAny(s.Match, p) {
			nonMatch++
			return nil
		}
		fileCount++
		q <- p
		return nil
	}
	err := filepath.Walk(s.Src, handle)
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("Finished initial sync of %q with %d files (excluded %d)", s.Src, fileCount, excluded+nonMatch)
}

func (s *Sync) Uploader(wg *sync.WaitGroup) {
	// goroutine that listens for files to upload
	for file := range s.upload {
		sequence := s.nextSequence()
		err := s.Upload(sequence, file)
		if err != nil {
			log.Printf("[%d] error uploading %q - %s", sequence, file, err)
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
	go func() {
		s.firstPass(s.queue)
		log.Printf("closing s.queue")
		close(s.queue)
	}()
	for f := range s.queue {
		s.upload <- f
	}
	log.Printf("exiting Run(). closing s.upload")
	close(s.upload)
}

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
