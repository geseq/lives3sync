package main

import (
	"flag"
	"log"
	"path"
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func validatePatterns(p ...string) error {
	for _, pp := range p {
		_, err := path.Match(pp, ".")
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	s := NewSync()

	showVersion := flag.Bool("version", false, "print version string")
	flag.StringVar(&s.Bucket, "bucket", "", "S3 bucket name")
	flag.StringVar(&s.Src, "src", "", "source directory to sync")
	flag.StringVar(&s.Prefix, "prefix", "", "prefix for content in s3")
	flag.BoolVar(&s.DryRun, "dry-run", false, "dry run only - don't upload files")
	flag.BoolVar(&s.RunOnce, "run-once", false, "exit after syncing existing files (ie: don't wait for updates)")

	flag.Var(&s.Match, "match", "pattern to match (may be given multiple times. Multiple patterns OR'd together)")
	flag.Var(&s.Exclude, "exclude", "pattern to exclude (may be given multiple times. Multiple patterns OR'd together)")

	region := flag.String("region", "us-east-1", "AWS S3 Region")
	parallelUploads := flag.Int("parallel", runtime.NumCPU(), "parallel uploads (defaults to number of available cores)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("lives3syncd v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	if *parallelUploads < 1 {
		log.Fatalf("Invalid setting for parallel (%d). Must be >= 1", *parallelUploads)
	}

	if err := validatePatterns(s.Match...); err != nil {
		log.Fatalf("Invalid Pattern: %s", err)
	}
	if err := validatePatterns(s.Exclude...); err != nil {
		log.Fatalf("Invalid Pattern: %s", err)
	}

	if s.Bucket == "" {
		log.Fatalf("bucket name required")
	}

	s.S3 = s3.New(&aws.Config{
		Region: *region,
	})

	var wg sync.WaitGroup
	log.Printf("Starting %d Concurrent Upload Threads", *parallelUploads)
	for i := 0; i < *parallelUploads; i++ {
		wg.Add(1)
		go s.Uploader(&wg)
	}

	wg.Add(1)
	s.Run(&wg)
	wg.Wait()
}
