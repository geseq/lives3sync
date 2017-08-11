package main

import (
	"flag"
	"fmt"
	"log"
	"path"
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
	flag.BoolVar(&s.IgnoreHidden, "ignore-hidden", false, "ignore hidden files (i.e. dot files)")
	flag.BoolVar(&s.BaseMatch, "base", false, "apply 'match' and 'exclude' against the file base name not the full path")

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

	for _, p := range s.Match {
		log.Printf("using match pattern %q", p)
	}
	for _, p := range s.Exclude {
		log.Printf("using exclude pattern %q", p)
	}

	s.sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: region,
		},
	}))
	s.S3 = s3.New(s.sess)

	var wg sync.WaitGroup
	var totalCount, totalUploadSize int64
	log.Printf("Starting %d Concurrent Upload Threads", *parallelUploads)
	for i := 0; i < *parallelUploads; i++ {
		wg.Add(1)
		go s.Uploader(&wg, &totalCount, &totalUploadSize)
	}

	wg.Add(1)
	s.Run(&wg)
	wg.Wait()
	log.Printf("Uploaded %d entries total size %d bytes", totalCount, totalUploadSize)
}
