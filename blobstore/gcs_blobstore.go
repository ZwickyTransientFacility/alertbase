package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/ZwickyTransientFacility/alertbase/schema"
)

type CloudStorageBlobstore struct {
	client *storage.Client
	bucket *storage.BucketHandle
}

func NewCloudStorageBlobstore(client *storage.Client, bucket string) *CloudStorageBlobstore {
	return &CloudStorageBlobstore{
		client: client,
		bucket: client.Bucket(bucket),
	}
}

func (s *CloudStorageBlobstore) Write(ctx context.Context, a *schema.Alert) (string, error) {
	key := fmt.Sprintf("alerts/v1/%s/%d", a.ObjectId, a.Candid)
	obj := s.bucket.Object(key)
	w := obj.NewWriter(ctx)
	err := a.Serialize(w)
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s", obj.BucketName(), key)
	return url, err
}

func (s *CloudStorageBlobstore) Read(ctx context.Context, url string) (*schema.Alert, error) {
	bucket, key, err := s.parseURL(url)
	if err != nil {
		return nil, fmt.Errorf("unable to parse URL: %w", err)
	}
	reader, err := s.client.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return schema.DeserializeAlert(reader)
}

func (s *CloudStorageBlobstore) ReadMany(ctx context.Context, urls []string) *AlertIterator {
	// TODO: parallelism
	ai := &AlertIterator{
		alerts: make(chan *schema.Alert, 1),
		errors: make(chan error, 1),
	}
	go func() {
		defer close(ai.alerts)
		for _, u := range urls {
			alert, err := s.Read(ctx, u)
			if err != nil {
				ai.errors <- err
			} else {
				ai.alerts <- alert
			}
		}
	}()
	return ai
}

func (s *CloudStorageBlobstore) parseURL(urlStr string) (bucket, key string, err error) {
	parsed, err := url.Parse(urlStr)
	split := strings.SplitN(parsed.Path, "/", 2)
	if len(split) != 2 {
		return "", "", errors.New("malformed GCP storage url is missing key")
	}
	return split[0], split[1], nil
}
