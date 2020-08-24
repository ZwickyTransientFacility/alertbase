package blobstore

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type S3Blobstore struct {
	s3     s3iface.S3API
	bucket string
}

func NewS3Blobstore(s3 s3iface.S3API, bucket string) *S3Blobstore {
	return &S3Blobstore{s3: s3, bucket: bucket}
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
	resp, err := s.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to find alert in S3: %w", err)
	}
	defer resp.Body.Close()
	return schema.DeserializeAlert(resp.Body)
}
