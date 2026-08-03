package s3tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type aclMatrixCase struct {
	bucketACL  types.BucketCannedACL
	objectACL  types.ObjectCannedACL
	altUpload  bool
	ownerFirst bool
	privAfter  bool // alt 업로드 후 PutBucketAcl PRIVATE (Java uploadAltUser)
}

// [Bucket = private, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPrivateBucketAndObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLPrivate, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 1)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = private, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPrivateBucketPublicReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLPublicRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 2)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = private, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPrivateBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLPublicReadWrite, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 3)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = private, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPrivateBucketAuthenticatedReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLAuthenticatedRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 4)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = private, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPrivateBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLBucketOwnerRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 5)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = private, Object = bucket-owner-read] Alt 사용자가 업로드한 오브젝트에 접근 가능한지 확인하는 테스트
func TestPrivateBucketBucketOwnerReadObjectUploadAltUser(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerRead, true, false, true}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 6)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = private, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPrivateBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLBucketOwnerFullControl, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 7)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicReadBucketPrivateObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLPrivate, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 8)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicReadBucketAndObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLPublicRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 9)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicReadBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLPublicReadWrite, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 10)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicReadBucketAuthenticatedReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLAuthenticatedRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 11)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicReadBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLBucketOwnerRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 12)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicReadBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLBucketOwnerFullControl, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 13)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketPrivateObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPrivate, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 14)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = private, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketPrivateObjectByAltUser(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPrivate, true, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 15)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketPublicReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 16)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = public-read, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketPublicReadObjectByAltUser(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicRead, true, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 17)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicReadWrite, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 18)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = public-read-write, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketPublicRWObjectByAltUser(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicReadWrite, true, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 19)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketAuthenticatedReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLAuthenticatedRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 20)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = authenticated-read, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketAuthenticatedReadObjectByAltUser(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLAuthenticatedRead, true, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 21)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 22)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = bucket-owner-read, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketBucketOwnerReadObjectByAltUser(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerRead, true, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 23)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerFullControl, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 24)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, Object = bucket-owner-full-control, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketBucketOwnerFullControlObjectByAltUser(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerFullControl, true, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 25)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = public-read-write, BucketOwnerPreferred, Object = bucket-owner-full-control, AltUser] 오브젝트에 접근 가능한지 확인하는 테스트
func TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerFullControl, true, true, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 26)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = authenticated-read, Object = private] 오브젝트에 접근 가능한지 확인하는 테스트
func TestAuthenticatedReadBucketPrivateObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLPrivate, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 27)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = authenticated-read, Object = public-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestAuthenticatedReadBucketPublicReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLPublicRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 28)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = authenticated-read, Object = public-read-write] 오브젝트에 접근 가능한지 확인하는 테스트
func TestAuthenticatedReadBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLPublicReadWrite, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 29)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = authenticated-read, Object = authenticated-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestAuthenticatedReadBucketAndObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLAuthenticatedRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 30)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = authenticated-read, Object = bucket-owner-read] 오브젝트에 접근 가능한지 확인하는 테스트
func TestAuthenticatedReadBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLBucketOwnerRead, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 31)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = authenticated-read, Object = bucket-owner-full-control] 오브젝트에 접근 가능한지 확인하는 테스트
func TestAuthenticatedReadBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	tc := aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLBucketOwnerFullControl, false, false, false}
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership, 32)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: tc.bucketACL}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	alt, public := s3Client(s.cfg, s.cfg.Alt), anonymousClient(s)
	uploader := s.client
	if tc.altUpload {
		uploader = alt
	}
	keys := []string{"main-object", "alt-object", "public-object"}
	for _, key := range keys {
		_, err := uploader.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: tc.objectACL})
		if err != nil {
			t.Fatalf("setup PutObject(%s): %v", key, err)
		}
	}
	if tc.privAfter {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
			t.Fatalf("PutBucketAcl private: %v", err)
		}
		assertACLRead(t, alt, bucket, keys[0], true)
		assertACLRead(t, s.client, bucket, keys[1], true)
		assertACLRead(t, public, bucket, keys[2], false)
		assertACLOverwrite(t, alt, bucket, keys[0], false)
		assertACLOverwrite(t, s.client, bucket, keys[1], true)
		assertACLOverwrite(t, public, bucket, keys[2], false)
		return
	}
	mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate := aclExpectations(tc)
	assertACLRead(t, s.client, bucket, keys[0], mainGet)
	assertACLRead(t, alt, bucket, keys[1], altGet)
	assertACLRead(t, public, bucket, keys[2], pubGet)
	assertACLOverwrite(t, s.client, bucket, keys[0], mainPut)
	assertACLOverwrite(t, alt, bucket, keys[1], altPut)
	assertACLOverwrite(t, public, bucket, keys[2], pubPut)
	if altCreate {
		assertACLCreate(t, alt, bucket, "alt-new", true)
	}
	if pubCreate {
		assertACLCreate(t, public, bucket, "public-new", true)
	}
	if tc.altUpload && !tc.ownerFirst &&
		(tc.objectACL == types.ObjectCannedACLPrivate || tc.objectACL == types.ObjectCannedACLPublicRead) {
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[1])})
		_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[2])})
	}
}

// [Bucket = private] 오브젝트 목록 조회가 가능한지 확인하는 테스트
func TestPrivateBucketList(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 33)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
		t.Fatal(err)
	}
	want := []string{"list-1", "list-2", "list-3"}
	for _, key := range want {
		put(t, s, bucket, key, key, nil)
	}
	assertACLList(t, s.client, bucket, want, true)
	assertACLList(t, s3Client(s.cfg, s.cfg.Alt), bucket, want, false)
	assertACLList(t, anonymousClient(s), bucket, want, false)
}

// [Bucket = public-read] 오브젝트 목록 조회가 가능한지 확인하는 테스트
func TestPublicReadBucketList(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 34)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead}); err != nil {
		t.Fatal(err)
	}
	want := []string{"list-1", "list-2", "list-3"}
	for _, key := range want {
		put(t, s, bucket, key, key, nil)
	}
	assertACLList(t, s.client, bucket, want, true)
	assertACLList(t, s3Client(s.cfg, s.cfg.Alt), bucket, want, true)
	assertACLList(t, anonymousClient(s), bucket, want, true)
}

// [Bucket = public-read-write] 오브젝트 목록 조회가 가능한지 확인하는 테스트
func TestPublicRWBucketList(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 35)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
		t.Fatal(err)
	}
	want := []string{"list-1", "list-2", "list-3"}
	for _, key := range want {
		put(t, s, bucket, key, key, nil)
	}
	assertACLList(t, s.client, bucket, want, true)
	assertACLList(t, s3Client(s.cfg, s.cfg.Alt), bucket, want, true)
	assertACLList(t, anonymousClient(s), bucket, want, true)
}

// [Bucket = authenticated-read] 오브젝트 목록 조회가 가능한지 확인하는 테스트
func TestAuthenticatedReadBucketList(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 36)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLAuthenticatedRead}); err != nil {
		t.Fatal(err)
	}
	want := []string{"list-1", "list-2", "list-3"}
	for _, key := range want {
		put(t, s, bucket, key, key, nil)
	}
	assertACLList(t, s.client, bucket, want, true)
	assertACLList(t, s3Client(s.cfg, s.cfg.Alt), bucket, want, true)
	assertACLList(t, anonymousClient(s), bucket, want, false)
}

// [Bucket = FullControl] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
func TestBucketPermissionAltUserFullControl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 37)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionFullControl)}); err != nil {
		t.Fatal(err)
	}
	checkBucketPermission(t, s3Client(s.cfg, s.cfg.Alt), bucket, true, true, true, true)
}

// [Bucket = Read] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
func TestBucketPermissionAltUserRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 38)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionRead)}); err != nil {
		t.Fatal(err)
	}
	checkBucketPermission(t, s3Client(s.cfg, s.cfg.Alt), bucket, true, false, false, false)
}

// [Bucket = ReadAcp] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
func TestBucketPermissionAltUserReadAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 39)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionReadAcp)}); err != nil {
		t.Fatal(err)
	}
	checkBucketPermission(t, s3Client(s.cfg, s.cfg.Alt), bucket, false, true, false, false)
}

// [Bucket = Write] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
func TestBucketPermissionAltUserWrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 40)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionWrite)}); err != nil {
		t.Fatal(err)
	}
	checkBucketPermission(t, s3Client(s.cfg, s.cfg.Alt), bucket, false, false, true, false)
}

// [Bucket = WriteAcp] 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인하는 테스트
func TestBucketPermissionAltUserWriteAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 41)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionWriteAcp)}); err != nil {
		t.Fatal(err)
	}
	checkBucketPermission(t, s3Client(s.cfg, s.cfg.Alt), bucket, false, false, false, true)
}

// [Object = FullControl] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
func TestObjectPermissionAltUserFullControl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 42)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
		t.Fatal(err)
	}
	key := "object-permission"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionFullControl)}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		out.Body.Close()
	}
	expectACLStatus(t, err, true)
	_, err = alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	expectACLStatus(t, err, true)
	_, err = alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	expectACLStatus(t, err, false)
	_, err = alt.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicReadWrite})
	expectACLStatus(t, err, true)
}

// [Object = Read] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
func TestObjectPermissionAltUserRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 43)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
		t.Fatal(err)
	}
	key := "object-permission"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionRead)}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		out.Body.Close()
	}
	expectACLStatus(t, err, true)
	_, err = alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	expectACLStatus(t, err, false)
	_, err = alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	expectACLStatus(t, err, false)
	_, err = alt.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead})
	expectACLStatus(t, err, false)
}

// [Object = ReadAcp] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
func TestObjectPermissionAltUserReadAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 44)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
		t.Fatal(err)
	}
	key := "object-permission"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionReadAcp)}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		out.Body.Close()
	}
	expectACLStatus(t, err, false)
	_, err = alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	expectACLStatus(t, err, true)
	_, err = alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	expectACLStatus(t, err, false)
	_, err = alt.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead})
	expectACLStatus(t, err, false)
}

// [Object = Write] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
func TestObjectPermissionAltUserWrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 45)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
		t.Fatal(err)
	}
	key := "object-permission"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionWrite)}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		out.Body.Close()
	}
	expectACLStatus(t, err, false)
	_, err = alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	expectACLStatus(t, err, false)
	_, err = alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	expectACLStatus(t, err, false)
	_, err = alt.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead})
	expectACLStatus(t, err, false)
}

// [Object = WriteAcp] 설정한 acl정보대로 서브유저가 해당 오브젝트에 접근 가능한지 확인하는 테스트
func TestObjectPermissionAltUserWriteAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 46)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
		t.Fatal(err)
	}
	key := "object-permission"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionWriteAcp)}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		out.Body.Close()
	}
	expectACLStatus(t, err, false)
	_, err = alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	expectACLStatus(t, err, false)
	_, err = alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	expectACLStatus(t, err, false)
	_, err = alt.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicReadWrite})
	expectACLStatus(t, err, true)
}

// aclExpectations는 Java ACL.java Access 케이스(비-uploadAltUser 특수 경로)를 따릅니다.
func aclExpectations(tc aclMatrixCase) (mainGet, altGet, pubGet, mainPut, altPut, pubPut, altCreate, pubCreate bool) {
	objectPublic := tc.objectACL == types.ObjectCannedACLPublicRead || tc.objectACL == types.ObjectCannedACLPublicReadWrite
	objectAuth := objectPublic || tc.objectACL == types.ObjectCannedACLAuthenticatedRead
	switch {
	case tc.ownerFirst:

		return true, false, false, true, false, false, true, true
	case tc.altUpload:

		mainGet = tc.objectACL != types.ObjectCannedACLPrivate
		altGet = true
		pubGet = objectPublic
		mainPut, altPut, pubPut = true, true, false
		pubCreate = tc.bucketACL == types.BucketCannedACLPublicReadWrite
		return
	default:
		mainGet = true
		altGet = objectAuth
		pubGet = objectPublic
		mainPut, altPut, pubPut = true, false, false
		if tc.bucketACL == types.BucketCannedACLPublicReadWrite {
			altCreate, pubCreate = true, true
		}
		return
	}
}

func assertACLRead(t *testing.T, client *s3.Client, bucket, key string, allowed bool) {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if !allowed {
		assertS3Error(t, err, 403, "AccessDenied")
		return
	}
	if err != nil {
		t.Fatalf("GetObject allowed: %v", err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != key {
		t.Fatalf("body=%q err=%v, want %q", body, err, key)
	}
}

func assertACLOverwrite(t *testing.T, client *s3.Client, bucket, key string, allowed bool) {
	t.Helper()
	body := []byte(key)
	if !allowed {
		body = nil
	}
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
	if !allowed {
		assertS3Error(t, err, 403, "AccessDenied")
		return
	}
	if err != nil {
		t.Fatalf("PutObject overwrite allowed: %v", err)
	}
}

func assertACLCreate(t *testing.T, client *s3.Client, bucket, key string, allowed bool) {
	t.Helper()
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	if !allowed {
		assertS3Error(t, err, 403, "AccessDenied")
		return
	}
	if err != nil {
		t.Fatalf("PutObject create allowed: %v", err)
	}
}

func assertACLList(t *testing.T, client *s3.Client, bucket string, want []string, allowed bool) {
	t.Helper()
	out, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if !allowed {
		assertS3Error(t, err, 403, "AccessDenied")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Contents) != len(want) {
		t.Fatalf("object count=%d, want %d", len(out.Contents), len(want))
	}
	for i := range want {
		if aws.ToString(out.Contents[i].Key) != want[i] {
			t.Fatalf("objects=%#v, want=%v", out.Contents, want)
		}
	}
}

func requireAltUser(t *testing.T, s *suite) {
	t.Helper()
	if s.cfg.Alt.ID == "" || s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User ID and credentials in config.ini")
	}
}

func aclPolicy(s *suite, permission types.Permission) *types.AccessControlPolicy {
	return &types.AccessControlPolicy{Owner: &types.Owner{ID: aws.String(s.cfg.Main.ID), DisplayName: aws.String(s.cfg.Main.DisplayName)}, Grants: []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(s.cfg.Main.ID)}, Permission: types.PermissionFullControl}, {Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(s.cfg.Alt.ID)}, Permission: permission}}}
}

func checkBucketPermission(t *testing.T, client *s3.Client, bucket string, readOK, readACPOK, writeOK, writeACPOK bool) {
	t.Helper()
	_, err := client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	expectACLStatus(t, err, readOK)
	_, err = client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	expectACLStatus(t, err, readACPOK)
	key := "permission-write"
	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	expectACLStatus(t, err, writeOK)
	if writeOK {
		_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	}

	writeACPACL := types.BucketCannedACLPublicRead
	if writeACPOK {
		writeACPACL = types.BucketCannedACLPublicReadWrite
	}
	_, err = client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: writeACPACL})
	expectACLStatus(t, err, writeACPOK)
}

// expectACLStatus는 Java checkBucketAcl*/checkObjectAcl* deny 헬퍼와 일치: HTTP 403만 검사.
// HeadBucket에는 XML body가 없어 SDK ErrorCode가 "AccessDenied" 대신 "Forbidden"인 경우가 많음.
func expectACLStatus(t *testing.T, err error, allowed bool) {
	t.Helper()
	if allowed {
		if err != nil {
			t.Fatalf("operation should be allowed: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("operation succeeded, want S3 API error")
	}
	var responseErr *smithyhttp.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want HTTP response error: %v", err, err)
	}
	if got := responseErr.HTTPStatusCode(); got != 403 {
		t.Fatalf("HTTP status = %d, want 403", got)
	}
}
