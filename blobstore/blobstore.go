package blobstore

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// parallelism controls how many concurrent requests to S3 are permitted
const parallelism = 64

type S3Blobstore struct {
	s3     s3iface.S3API
	bucket string

	workerPool *s3WorkerPool
}

func NewS3Blobstore(s3 s3iface.S3API, bucket string) *S3Blobstore {
	return &S3Blobstore{
		s3:         s3,
		bucket:     bucket,
		workerPool: newS3WorkerPool(s3, parallelism),
	}
}

func (s *S3Blobstore) Write(a *schema.Alert) (string, error) {
	contents := bytes.NewBuffer(nil)
	err := a.Serialize(contents)
	if err != nil {
		return "", fmt.Errorf("unable to serialize alert id=%v: %v", a.ObjectId, err)
	}

	key := fmt.Sprintf("alerts/v1/%s/%d", a.ObjectId, a.Candid)
	url := fmt.Sprintf("s3://%s/%s", s.bucket, key)
	_, err = s.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(contents.Bytes()),
	})
	if err != nil {
		return "", fmt.Errorf("unable to add alert to s3: %w", err)
	}

	return url, nil
}

func (s *S3Blobstore) keyFor(a *schema.Alert) string {
	return fmt.Sprintf("alerts/v1/%s/%d", a.ObjectId, a.Candid)
}

func (s *S3Blobstore) urlFor(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.bucket, key)
}

func (s *S3Blobstore) parseURL(url string) (bucket, key string, err error) {
	if !strings.HasPrefix(url, "s3://") {
		return "", "", errors.New("s3 urls should start with s3://")
	}
	url = strings.TrimPrefix(url, "s3://")

	slashPos := strings.IndexRune(url, '/')
	if slashPos == -1 {
		return "", "", errors.New("malformed s3 URL is missing key")
	}
	bucket = url[:slashPos]
	key = url[slashPos:]
	return bucket, key, nil
}

func (s *S3Blobstore) Read(url string) (*schema.Alert, error) {
	bucket, key, err := s.parseURL(url)
	if err != nil {
		return nil, fmt.Errorf("unable to parse URL: %w", err)
	}
	worker := s.workerPool.take()
	defer s.workerPool.giveBack(worker)
	resp, err := worker.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to find alert in S3: %w", err)
	}
	defer resp.Body.Close()
	return schema.DeserializeAlert(resp.Body)
}

func (s *S3Blobstore) ReadMany(urls []string) *AlertIterator {
	ai := &AlertIterator{
		alerts: make(chan *schema.Alert, s.workerPool.parallelism),
		errors: make(chan error, s.workerPool.parallelism),
	}
	var wg sync.WaitGroup
	for _, u := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			bucket, key, err := s.parseURL(url)
			if err != nil {
				ai.errors <- err
				return
			}

			worker := s.workerPool.take()
			defer s.workerPool.giveBack(worker)

			log.Printf("fetching %v", url)
			resp, err := worker.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				ai.errors <- err
				return
			}

			defer resp.Body.Close()
			alert, err := schema.DeserializeAlert(resp.Body)
			if err != nil {
				ai.errors <- err
				return
			}

			ai.alerts <- alert
		}(u)
	}
	go func() {
		wg.Wait()
		close(ai.alerts)
	}()

	return ai
}

// AlertIterator provides access to a stream of alert messages.
//
// Example usage:
// for ai.Next() {
//     fmt.Println(ai.Value()
// }
// err := ai.Error()
// if err != nil {
//     handle(err)
// }
type AlertIterator struct {
	more    bool
	current *schema.Alert
	err     error

	alerts chan *schema.Alert
	errors chan error
}

// Next advances the iterator to the next alert message. It returns true if
// ai.Value() will return a valid alert message. If it returns false, then the
// caller should check ai.Error() for a non-nil value.
func (ai *AlertIterator) Next() bool {
	select {
	case a, ok := <-ai.alerts:
		if !ok {
			ai.current = nil
			return false
		}
		ai.current = a
		return true
	case err, ok := <-ai.errors:
		if !ok {
			ai.current = nil
			return false
		}
		ai.err = err
		return false
	}
}

// Value returns the alert at the current position of the iterator. It can be
// called multiple times without advancing.
func (ai *AlertIterator) Value() *schema.Alert {
	return ai.current
}

// Error returns the first error in the stream, if there is one.
func (ai *AlertIterator) Error() error {
	return ai.err
}

// s3WorkerPool provides access to a max-concurrency model for requests to S3.
// Callers can acquire a worker with take(), but must return it when done with
// giveBack().
type s3WorkerPool struct {
	parallelism int
	workers     chan *s3Worker
}

func newS3WorkerPool(s3c s3iface.S3API, n int) *s3WorkerPool {
	p := &s3WorkerPool{
		parallelism: n,
		workers:     make(chan *s3Worker, n),
	}
	for i := 0; i < n; i++ {
		p.workers <- &s3Worker{s3c}
	}
	return p
}

func (p *s3WorkerPool) take() *s3Worker {
	return <-p.workers
}

func (p *s3WorkerPool) giveBack(w *s3Worker) {
	p.workers <- w
}

// s3Worker is a named wrapper around an S3 client.
type s3Worker struct {
	s3iface.S3API
}
