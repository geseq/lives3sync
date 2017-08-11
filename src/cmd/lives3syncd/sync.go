package main

import (
	"container/heap"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Sync struct {
	Bucket       string
	S3           *s3.S3
	sess         client.ConfigProvider
	Src          string
	Prefix       string
	DryRun       bool
	Match        StringArray
	Exclude      StringArray
	IgnoreHidden bool
	BaseMatch    bool
	RunOnce      bool

	count uint64

	pending      map[string]*PendingSync
	pqueue       PriorityQueue
	queue        chan *PendingSync
	upload       chan *PendingSync
	uploadNotify chan bool
	exitChan     chan bool
	flushUploads bool
	watcher      *Watcher

	sync.RWMutex
}

func NewSync() *Sync {
	s := &Sync{
		queue:        make(chan *PendingSync, 100),
		pending:      make(map[string]*PendingSync),
		upload:       make(chan *PendingSync, 1),
		uploadNotify: make(chan bool, 1),
		exitChan:     make(chan bool),
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

// shouldSync checks the relative path p against the match and exclude lists
// It conditionally ignores "hidden" files
func shouldSync(p string, exclude, match []string, ignoreHidden, baseMatch bool) bool {
	if baseMatch {
		p = filepath.Base(p)
	}
	switch {
	case strings.HasPrefix(filepath.Base(p), "."):
		if ignoreHidden {
			// skip .hidden_files
			return false
		}
	case matchesAny(exclude, p):
		return false
	case len(match) > 0 && !matchesAny(match, p):
		return false
	}
	return true
}

// initialDirSync reads the filesystem and streams out files and directories
func (s *Sync) initialDirSync(dir chan<- string) {
	log.Printf("Starting initial sync of %q", s.Src)
	var fileCount, skipped, dirCount int64
	handle := func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirCount++
			if dir != nil {
				dir <- p
			}
			return nil
		}
		if !shouldSync(p, s.Exclude, s.Match, s.IgnoreHidden, s.BaseMatch) {
			skipped++
			return nil
		}
		fileCount++
		mtime := info.ModTime().Unix()
		size := info.Size()
		log.Printf("queueing %q %d %d", p, mtime, size)
		s.queue <- &PendingSync{
			Name:  p,
			Mtime: mtime,
			Size:  size,
		}
		return nil
	}
	err := filepath.Walk(s.Src, handle)
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("Initial sync of %q found %d files (skipped %d). Watching %d directories for future updates.", s.Src, fileCount, skipped, dirCount+1)
}

// queueLoop reads off new files to be synced and captures metdata and notifies the upload queue
func (s *Sync) queueLoop() {
	var notify bool
	for p := range s.queue {
		notify = false
		s.Lock()
		if existing, ok := s.pending[p.Name]; ok {
			// update priority queue
			switch {
			case p.Mtime > existing.Mtime:
				s.pending[p.Name] = p
				s.pqueue.update(existing, p.Mtime, p.Size)
				notify = true
			case p.Mtime == existing.Mtime:
				// skipping
			case p.Mtime < existing.Mtime:
				log.Printf("discarding update w/ older mtime %v than pending %v for %v", p.Mtime, existing.Mtime, existing.Name)
			}
		} else {
			s.pending[p.Name] = p
			heap.Push(&s.pqueue, p)
			notify = true
		}
		s.Unlock()
		if notify {
			s.notifyUpload()
		}
	}
	s.flushUploads = true
	s.notifyUpload()
	log.Printf("exiting queueLoop()")
}

func (s *Sync) uploadLoop() {
	// var ticker *time.Ticker
	var delay <-chan time.Time
	for {
		select {
		case <-s.uploadNotify:
			// log.Printf("upload Notify")
		case <-delay:
			log.Printf("delay elapsed")
		}

	next:

		var item *PendingSync
		var pendingCount int
		s.Lock()
		pendingCount = len(s.pending)
		if pendingCount > 0 {
			item = heap.Pop(&s.pqueue).(*PendingSync)
			delete(s.pending, item.Name)
			// TODO: still track while upload is in progress?
		}
		s.Unlock()
		if item != nil {
			s.upload <- item
			goto next
		}

		if s.flushUploads && pendingCount == 0 {
			break
		}
		// get the top item on the stack. If it's ready to sync, write to the upload channel
		// jump to calc
		// else
		// calculate delay timer
		// if ticker != nil {
		// ticker.Stop()
		// }

	}
	log.Printf("exiting uploadLoop()")
	close(s.upload)
}

func (s *Sync) handleWatchEvents(updates <-chan string) {
	for f := range updates {
		if !shouldSync(f, s.Exclude, s.Match, s.IgnoreHidden, s.BaseMatch) {
			continue
		}
		info, err := os.Lstat(f)
		if err != nil {
			log.Printf("%s", err)
			continue
		}
		if info.IsDir() {
			continue
		}
		mtime := info.ModTime().Unix()
		size := info.Size()
		log.Printf("queueing %q %s %s", f, mtime, size)
		s.queue <- &PendingSync{
			Name:  f,
			Mtime: mtime,
			Size:  size,
		}
	}
}

func (s *Sync) Run(wg *sync.WaitGroup) {
	go s.queueLoop()
	go s.uploadLoop()

	// start fsnotify
	// for any directory we discover in our initial sync, start watching it
	directories := make(chan string)
	if s.RunOnce {
		directories = nil
	} else {
		updates := make(chan string, 10)
		go s.handleWatchEvents(updates)
		s.watcher = NewWatcher(s.Src, updates)

		go func() {
			for d := range directories {
				log.Printf("adding %q to fsnotify", d)
				s.watcher.Watch(d)
			}
		}()
	}

	// start our initial sync
	go func() {
		s.initialDirSync(directories)
		if directories != nil {
			close(directories)
		}
		if s.RunOnce {
			close(s.queue)
		}
	}()

	wg.Done()
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
