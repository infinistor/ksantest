package s3tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestOwnership(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 소유권 조회 확인
		{"test_get_bucket_ownership", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced)
			if _, err := s.client.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)}); err != nil {
				t.Fatalf("GetBucketOwnershipControls: %v", err)
			}
		}},
		// 버킷을 생성할때 소유권 설정 확인
		{"test_create_bucket_with_ownership", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced)
			assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
		}},
		// 버킷의 소유권 변경 확인
		{"test_change_bucket_ownership", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced)
			assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
			putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerPreferred)
			assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerPreferred)
		}},
		// [BucketOwnerEnforced] 버킷 ACL 설정이 실패하는지 확인
		{"test_bucket_ownership_deny_acl", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced)
			_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead})
			assertS3Error(t, err, 403, "AccessDenied")
		}},
		// [BucketOwnerEnforced] 오브젝트 ACL 설정이 실패하는지 확인
		{"test_bucket_ownership_deny_object_acl", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced)
			key := "testBucketOwnershipDenyObjectACL"
			put(t, s, bucket, key, key, nil)
			_, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead})
			assertS3Error(t, err, 403, "AccessDenied")
		}},
		// ACL 설정된 오브젝트에 소유권을 BucketOwnerEnforced로 변경해도 접근 가능한지 확인
		{"test_object_ownership_deny_change", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			key := "testObjectOwnershipDenyChange"
			_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPublicRead})
			if err != nil {
				t.Fatal(err)
			}
			public := anonymousClient(s)
			headPublic(t, public, bucket, key)
			putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
			headPublic(t, public, bucket, key)
		}},
		// ACL 설정된 오브젝트에 소유권을 BucketOwnerEnforced로 변경할경우 ACL 설정이 실패하는지 확인
		{"test_object_ownership_deny_acl", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			key := "testObjectOwnershipDenyACL"
			_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPublicRead})
			if err != nil {
				t.Fatal(err)
			}
			putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
			_, err = s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate})
			assertS3Error(t, err, 400, "AccessControlListNotSupported")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func ownershipBucket(t *testing.T, s *suite, ownership types.ObjectOwnership) string {
	t.Helper()
	name := newBucketName(s.cfg.BucketPrefix)
	input := createBucketInput(s.cfg, name)
	input.ObjectOwnership = ownership
	_, err := s.client.CreateBucket(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if ownership != types.ObjectOwnershipBucketOwnerEnforced {
		disablePublicAccessBlock(t, s, name)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			removeOwnedBucket(s, name)
		}
	})
	return name
}

func disablePublicAccessBlock(t *testing.T, s *suite, bucket string) {
	t.Helper()
	_, err := s.client.PutPublicAccessBlock(context.Background(), &s3.PutPublicAccessBlockInput{
		Bucket: aws.String(bucket),
		PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
			BlockPublicAcls:       aws.Bool(false),
			IgnorePublicAcls:      aws.Bool(false),
			BlockPublicPolicy:     aws.Bool(false),
			RestrictPublicBuckets: aws.Bool(false),
		},
	})
	if err != nil {
		t.Fatalf("disable public access block: %v", err)
	}
}
func putOwnership(t *testing.T, s *suite, bucket string, ownership types.ObjectOwnership) {
	t.Helper()
	_, err := s.client.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String(bucket), OwnershipControls: &types.OwnershipControls{Rules: []types.OwnershipControlsRule{{ObjectOwnership: ownership}}}})
	if err != nil {
		t.Fatal(err)
	}
}
func assertOwnership(t *testing.T, s *suite, bucket string, want types.ObjectOwnership) {
	t.Helper()
	out, err := s.client.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.OwnershipControls == nil || len(out.OwnershipControls.Rules) != 1 || out.OwnershipControls.Rules[0].ObjectOwnership != want {
		t.Fatalf("ownership controls = %#v, want %q", out.OwnershipControls, want)
	}
}
func anonymousClient(s *suite) *s3.Client {
	options := s.client.Options()
	options.Credentials = aws.AnonymousCredentials{}
	return s3.New(options)
}
func headPublic(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	if _, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("anonymous HeadObject: %v", err)
	}
}
func removeOwnedBucket(s *suite, bucket string) {
	ctx := context.Background()
	listed, _ := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	var objects []types.ObjectIdentifier
	if listed != nil {
		for _, item := range listed.Versions {
			objects = append(objects, types.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
		}
		for _, item := range listed.DeleteMarkers {
			objects = append(objects, types.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
		}
	}
	if len(objects) != 0 {
		_, _ = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects}})
	}
	_, _ = s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
