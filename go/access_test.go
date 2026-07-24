package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestAccess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// BlockPublicAcls와 BlockPublicPolicy 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
		{"test_block_public_acl_and_policy", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
			configuration := publicAccessConfiguration(true, false, true, false)
			putPublicAccessBlock(t, s, bucket, configuration)
			assertPublicAccessBlock(t, s, bucket, configuration)
			for _, acl := range []types.BucketCannedACL{types.BucketCannedACLPublicRead, types.BucketCannedACLPublicReadWrite, types.BucketCannedACLAuthenticatedRead} {
				_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: acl})
				assertS3Error(t, err, 403, "AccessDenied")
			}
		}},
		// BlockPublicAcls 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
		{"test_block_public_acls", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
			configuration := publicAccessConfiguration(true, false, false, false)
			putPublicAccessBlock(t, s, bucket, configuration)
			assertPublicAccessBlock(t, s, bucket, configuration)
			for _, acl := range []types.ObjectCannedACL{types.ObjectCannedACLPublicRead, types.ObjectCannedACLPublicReadWrite, types.ObjectCannedACLAuthenticatedRead} {
				_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("testBlockPublicAcls"), Body: bytes.NewReader([]byte("body")), ACL: acl})
				assertS3Error(t, err, 403, "AccessDenied")
			}
		}},
		// BlockPublicPolicy 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
		{"test_block_public_policy", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
			putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, false, true, false))
			policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}`, bucket)
			_, err := s.client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)})
			assertS3Error(t, err, 403, "AccessDenied")
		}},
		// 버킷의 접근 권한 블록 삭제 기능을 확인하는 테스트
		{"test_delete_public_block", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
			configuration := publicAccessConfiguration(true, true, true, false)
			putPublicAccessBlock(t, s, bucket, configuration)
			assertPublicAccessBlock(t, s, bucket, configuration)
			if _, err := s.client.DeletePublicAccessBlock(context.Background(), &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)}); err != nil {
				t.Fatalf("DeletePublicAccessBlock: %v", err)
			}
			_, err := s.client.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
			assertS3Error(t, err, 404, "NoSuchPublicAccessBlockConfiguration")
		}},
		// IgnorePublicAcls 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
		{"test_ignore_public_acls", func(t *testing.T) {
			s := newSuite(t)
			if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
				t.Skip("configure Alt User credentials in config.ini")
			}
			bucket := s.bucket(t)
			key := "testIgnorePublicAcls"
			_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPublicRead})
			if err != nil {
				t.Fatalf("PutObject public-read: %v", err)
			}
			alt := s3Client(s.cfg, s.cfg.Alt)
			out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			if err != nil {
				t.Fatalf("Alt GetObject before ignore: %v", err)
			}
			body, readErr := io.ReadAll(out.Body)
			out.Body.Close()
			if readErr != nil || string(body) != key {
				t.Fatalf("Alt body = %q, err=%v", body, readErr)
			}
			putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, true, false, false))
			if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead}); err != nil {
				t.Fatalf("PutBucketAcl: %v", err)
			}
			public := anonymousClient(s)
			_, err = public.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
			assertS3Error(t, err, 403, "AccessDenied")
			_, err = public.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			assertS3Error(t, err, 403, "AccessDenied")
		}},
		// 버킷의 접근 권한 블록 설정 기능을 확인하는 테스트
		{"test_put_public_block", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
			configuration := publicAccessConfiguration(true, true, true, false)
			putPublicAccessBlock(t, s, bucket, configuration)
			assertPublicAccessBlock(t, s, bucket, configuration)
			if _, err := s.client.DeletePublicAccessBlock(context.Background(), &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)}); err != nil {
				t.Fatalf("DeletePublicAccessBlock: %v", err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func publicAccessConfiguration(blockACLs, ignoreACLs, blockPolicy, restrict bool) *types.PublicAccessBlockConfiguration {
	return &types.PublicAccessBlockConfiguration{BlockPublicAcls: aws.Bool(blockACLs), IgnorePublicAcls: aws.Bool(ignoreACLs), BlockPublicPolicy: aws.Bool(blockPolicy), RestrictPublicBuckets: aws.Bool(restrict)}
}
func putPublicAccessBlock(t *testing.T, s *suite, bucket string, cfg *types.PublicAccessBlockConfiguration) {
	t.Helper()
	if _, err := s.client.PutPublicAccessBlock(context.Background(), &s3.PutPublicAccessBlockInput{Bucket: aws.String(bucket), PublicAccessBlockConfiguration: cfg}); err != nil {
		t.Fatalf("PutPublicAccessBlock: %v", err)
	}
}
func assertPublicAccessBlock(t *testing.T, s *suite, bucket string, want *types.PublicAccessBlockConfiguration) {
	t.Helper()
	out, err := s.client.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	got := out.PublicAccessBlockConfiguration
	if got == nil || aws.ToBool(got.BlockPublicAcls) != aws.ToBool(want.BlockPublicAcls) || aws.ToBool(got.IgnorePublicAcls) != aws.ToBool(want.IgnorePublicAcls) || aws.ToBool(got.BlockPublicPolicy) != aws.ToBool(want.BlockPublicPolicy) || aws.ToBool(got.RestrictPublicBuckets) != aws.ToBool(want.RestrictPublicBuckets) {
		t.Fatalf("public access block = %#v, want %#v", got, want)
	}
}
