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

	pending      map[string]*PendingSync
	pqueue       PriorityQueue
	queue        chan *PendingSync
	upload       chan *PendingSync
	uploadNotify chan bool
	watcher      *Watcher

	sync.RWMutex
}

type FileUpdate struct {
	Name  string
	Delay time.Duration
}

func NewSync() *Sync {
	s := &Sync{
		queue:      make(chan *PendingSync, 100),
		pending:    make(map[string]*PendingSync),
		upload:     make(chan *PendingSync),
		uploadDone: make(chan bool, 1),
	}
	return s
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

// firstPass reads the filesystem and streams out files and directories
func (s *Sync) firstPass(q chan<- *PendingSync, dir chan<- string) {
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

		q <- &PendingSync{
			Name:  p,
			Mtime: info.ModTime().Unix(),
			Size:  info.Size(),
		}
		return nil
	}
	err := filepath.Walk(s.Src, handle)
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("Initial sync of %q found %d files (excluded %d). Watching %d directories for future updates.", s.Src, fileCount, excluded+nonMatch, dirCount+1)
}

func (s *Sync) queueLoop() {
	for p := range self.queue {
		s.Lock()
		if existing, ok := s.pending[p.Name]; ok {
			// update priority queue
			if p.Mtime > existing.Mtime {
				s.pending[p.Name] = p
				s.pqueue.update(existing, p.Mtime, p.Size)
			} else {
				// skipping
			}
		} else {
			s.pending[p.Name] = p
			hep.Push(&s.pqueue, p)
		}
		s.Unlock()
		s.notifyUpload()
	}
}

func (s *Sync) uplaodLoop() {
	for {
	}
}

func (s *Sync) Run() {
	go s.queueLoop()

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
			info, err := os.Stat(f)
			if err != nil {
				log.Printf("%s", err)
				continue
			}
			if info.IsDir() {
				continue
			}
			s.queue <- &PendingSync{
				Name:  f,
				Mtime: info.ModTime().Unix(),
				Size:  info.Size(),
			}
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

	log.Printf("exiting Run(). closing s.upload")
	close(s.upload)
}

func (s *Sync) notifyUpload() {
	select {
	case s.uploadNotify <- true:
	default:
	}
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
