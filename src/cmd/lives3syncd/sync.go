package main

import (
	"fmt"
	"io"
	"log"
	"path"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)


type Sync struct{
	Bucket: string
	Svc    *s3.S3
	Src string
	Prefix string
	DryRun bool
	queue chan string
	pending map[string]bool
	sync.RWMutex
}

func NewSync() *Sync {
	s = &Sync{
		queue: make(chan string, 100)
		pending: make(map[string]bool)
	}
	return s
	
}

func (s *Sync) firstPass() {
	handle := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		s.queue <- handle
	}
	err := filepath.Walk(s.Src, handle)
	if err != nil {
		log.Fatalf("%s", err)
	}
}

func (s *Sync) Uploader(){ 
	// goroutine that listens for files to upload
}

func (s *Sync) Run() {
	// start fsnotify
	s.firstPass()
}

func (p *Proxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	key := req.URL.Path
	if key == "/" {
		key = "/index.html"
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(key),
	}
	if v := req.Header.Get("If-None-Match"); v != "" {
		input.IfNoneMatch = aws.String(v)
	}

	// awsReq, resp := p.Svc.GetObjectRequest(input)
	// log.Printf("request: %#v", awsReq)
	// err := awsReq.Send()
	// log.Printf("response: %#v", )

	var is304 bool
	resp, err := p.Svc.GetObject(input)
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case "NoSuchKey":
			http.Error(rw, "Page Not Found", 404)
			return
		case "304NotModified":
			is304 = true
			// continue so other headers get set appropriately
		default:
			log.Printf("Error: %v %v", awsErr.Code(), awsErr.Message())
			http.Error(rw, "Internal Error", 500)
			return
		}
	} else if err != nil {
		log.Printf("not aws error %v %s", err, err)
		http.Error(rw, "Internal Error", 500)
		return
	}

	var contentType string
	if resp.ContentType != nil {
		contentType = *resp.ContentType
	}

	if contentType == "" {
		ext := path.Ext(req.URL.Path)
		contentType = mime.TypeByExtension(ext)
	}

	if resp.ETag != nil && *resp.ETag != "" {
		rw.Header().Set("Etag", *resp.ETag)
	}

	if contentType != "" {
		rw.Header().Set("Content-Type", contentType)
	}
	if resp.ContentLength != nil && *resp.ContentLength > 0 {
		rw.Header().Set("Content-Length", fmt.Sprintf("%d", *resp.ContentLength))
	}

	if is304 {
		rw.WriteHeader(304)
	}

	io.Copy(rw, resp.Body)
	resp.Body.Close()
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
