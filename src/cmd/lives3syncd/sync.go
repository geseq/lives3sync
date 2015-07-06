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
)

type Sync struct {
	Bucket  string
	S3      *s3.S3
	Src     string
	Prefix  string
	DryRun  bool
	Match   StringArray
	Exclude StringArray
	RunOnce bool

	count uint64

	queue      chan string
	pending    map[string]time.Duration
	upload     chan string
	uploadDone chan bool
	watcher    *Watcher

	sync.RWMutex
}

type FileUpdate struct {
	Name  string
	Delay time.Duration
}

func NewSync() *Sync {
	s := &Sync{
		queue:      make(chan string, 100),
		pending:    make(map[string]time.Duration),
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

func (s *Sync) firstPass(q chan<- string, dir chan<- string) {
	log.Printf("Starting initial sync of %q", s.Src)
	var fileCount, excluded, nonMatch, dirCount int64
	handle := func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirCount++
			dir <- p
			return nil
		}
		base := path.Base(p)
		if strings.HasPrefix(base, ".") {
			// skip .hidden_files
			return nil
		}
		if matchesAny(s.Exclude, base) {
			excluded++
			return nil
		}
		if len(s.Match) > 0 && !matchesAny(s.Match, base) {
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
	log.Printf("Finished initial sync of %q with %d files (excluded %d). Watching %d directories for future updates.", s.Src, fileCount, excluded+nonMatch, dirCount+1)
}

func (s *Sync) Uploader(wg *sync.WaitGroup) {
	// goroutine that listens for files to upload
	for file := range s.upload {
		sequence := s.nextSequence()
		err := s.Upload(sequence, file)
		if err != nil {
			log.Printf("[%d] error uploading %q - %s", sequence, file, err)
			// TODO: put back on pending queue
		}

		select {
		case s.uploadDone <- true:
		default:
		}

	}
	log.Printf("Uploader Done")
	wg.Done()
}

func (s *Sync) Run() {
	// start fsnotify
	updates := make(chan string, 10)
	go func() {
		for f := range updates {
			base := path.Base(f)
			if strings.HasPrefix(base, ".") {
				// skip .hidden_files
				continue
			}
			if matchesAny(s.Exclude, base) {
				// excluded++
				continue
			}
			if len(s.Match) > 0 && !matchesAny(s.Match, base) {
				// nonMatch++
				continue
			}
			// TODO: is dir?
			s.queue <- f
		}
	}()
	s.watcher = NewWatcher(src, s.queue)
	directories := make(chan string)
	go func() {
		for d := range directories {
			s.watcher.Watch(d)
		}
	}()

	go func() {
		s.firstPass(s.queue, directories)
		close(directories)
		if s.RunOnce {
			close(s.queue)
		}
	}()

	for f := range s.queue {
		s.upload <- f
	}
	log.Printf("exiting Run(). closing s.upload")
	close(s.upload)
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
