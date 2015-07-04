package main

import (
	"flag"
	"log"
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name")
	region := flag.String("region", "us-east-1", "AWS S3 Region")
	src := flag.String("src", "", "source directory to sync")
	prefix := flag.String("prefix", "", "prefix for content in s3")
	dryRun := flag.Bool("dry-run", false, "dry run only - don't upload files")
	parallelUploads := flag.Int("prallel", -1, "paralell uploads (defaulst to num cores)")
	flag.Parse()

	if *bucket == "" {
		log.Fatalf("bucket name required")
	}

	svc := s3.New(&aws.Config{
		Region: *region,
	})

	s := NewSync()
	s.Bucket = *bucket
	s.S3 = svc
	s.Src = *src
	s.Prefix = *prefix
	s.DryRun = *dryRun

	// launch the right number of concurrent uploads
	var wg sync.WaitGroup
	n := *parallelUploads
	if n < 1 {
		n = runtime.GOMAXPROCS(0)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go s.Uploader(&wg)
	}

	s.Run()
	wg.Done()

}
