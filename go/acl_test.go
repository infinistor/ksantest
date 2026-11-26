package s3tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sort"
	"strings"
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
	key := "TestObjectPermissionAltUserFullControl"
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
	key := "TestObjectPermissionAltUserRead"
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
	key := "TestObjectPermissionAltUserReadAcp"
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
	key := "TestObjectPermissionAltUserWrite"
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
	key := "TestObjectPermissionAltUserWriteAcp"
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

// region ACL 권한 범위 (AWS 실측 기준)

const (
	aclAllUsers           = "http://acs.amazonaws.com/groups/global/AllUsers"
	aclAuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)

func assertACLDenied(t *testing.T, err error) {
	t.Helper()
	assertS3Error(t, err, 403, "AccessDenied")
}

// aclGrant는 ACL 의 (대상, 권한) 한 쌍이다. 대상은 main, alt, 그룹 URI 중 하나다.
type aclGrant struct{ grantee, permission string }

// ACL 응답의 Grants 를 (대상, 권한) 목록으로 줄여 정렬한다.
func aclGrantSet(s *suite, grants []types.Grant) []aclGrant {
	set := make([]aclGrant, 0, len(grants))
	for _, grant := range grants {
		grantee := aws.ToString(grant.Grantee.URI)
		if grantee == "" {
			switch aws.ToString(grant.Grantee.ID) {
			case s.cfg.Main.ID:
				grantee = "main"
			case s.cfg.Alt.ID:
				grantee = "alt"
			default:
				grantee = "other"
			}
		}
		set = append(set, aclGrant{grantee, string(grant.Permission)})
	}
	sort.Slice(set, func(i, j int) bool {
		if set[i].grantee != set[j].grantee {
			return set[i].grantee < set[j].grantee
		}
		return set[i].permission < set[j].permission
	})
	return set
}

func assertACLGrants(t *testing.T, s *suite, grants []types.Grant, want ...aclGrant) {
	t.Helper()
	if got := aclGrantSet(s, grants); !slices.Equal(got, want) {
		t.Fatalf("grants = %v, want %v", got, want)
	}
}

func assertBucketACLGrants(t *testing.T, s *suite, client *s3.Client, bucket string, want ...aclGrant) {
	t.Helper()
	out, err := client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	assertACLGrants(t, s, out.Grants, want...)
}

// versionID 가 비어 있으면 버전을 지정하지 않는다.
func assertObjectACLGrants(t *testing.T, s *suite, bucket, key, versionID string, want ...aclGrant) {
	t.Helper()
	input := &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := s.client.GetObjectAcl(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	assertACLGrants(t, s, out.Grants, want...)
}

// 비공개 버킷에 서브 사용자 권한 하나만 준다.
func setupBucketPermission(t *testing.T, s *suite, permission types.Permission, id int) string {
	t.Helper()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, id)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, permission)}); err != nil {
		t.Fatal(err)
	}
	return bucket
}

// 비공개 버킷의 오브젝트에 서브 사용자 권한 하나만 준다.
func setupObjectGrant(t *testing.T, s *suite, key string, permission types.Permission, id int) string {
	t.Helper()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, id)
	put(t, s, bucket, key, "content", nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, permission)}); err != nil {
		t.Fatal(err)
	}
	return bucket
}

func createMultipartErr(client *s3.Client, bucket, key string) error {
	_, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func deleteObjectErr(client *s3.Client, bucket, key string) error {
	_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func getObjectTaggingErr(client *s3.Client, bucket, key string) error {
	_, err := client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

// 버전 목록과 미완료 멀티파트 목록의 개수를 확인한다.
func assertACLVersionAndUploadCount(t *testing.T, client *s3.Client, bucket string, versions, uploads int) {
	t.Helper()
	listed, err := client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Versions) != versions {
		t.Fatalf("versions = %d, want %d", len(listed.Versions), versions)
	}
	multiparts, err := client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(multiparts.Uploads) != uploads {
		t.Fatalf("uploads = %d, want %d", len(multiparts.Uploads), uploads)
	}
}

// [Bucket = Read] 서브유저에게 열리는 범위가 목록 종류뿐인지 확인
func TestBucketAclReadScope(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "test_bucket_acl_read_scope"
	bucket := setupBucketPermission(t, s, types.PermissionRead, 47)
	put(t, s, bucket, key, "content", nil)
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multi")})
	if err != nil {
		t.Fatal(err)
	}

	alt := s3Client(s.cfg, s.cfg.Alt)
	// READ 는 목록 종류를 모두 연다
	if _, err := alt.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, 1)
	assertACLVersionAndUploadCount(t, alt, bucket, 1, 1)
	// 목록 권한이 있으므로 없는 키는 404 다
	assertS3Error(t, getObjectErr(alt, bucket, "not-exist", ""), 404, "NoSuchKey")

	// 오브젝트 읽기·쓰기와 버킷 설정은 열지 않는다
	assertACLDenied(t, getObjectErr(alt, bucket, key, ""))
	assertACLDenied(t, putObjectErr(alt, bucket, "new", "x", nil))
	assertACLDenied(t, deleteObjectErr(alt, bucket, key))
	assertACLDenied(t, createMultipartErr(alt, bucket, "new"))
	_, err = alt.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(bucket), Key: aws.String("multi"), UploadId: created.UploadId})
	assertACLDenied(t, err)
	_, err = alt.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	_, err = alt.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	deleted, err := alt.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String(key)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted.Deleted) != 0 {
		t.Fatalf("deleted = %#v, want none", deleted.Deleted)
	}
	if len(deleted.Errors) == 0 || aws.ToString(deleted.Errors[0].Code) != "AccessDenied" {
		t.Fatalf("errors = %#v, want AccessDenied", deleted.Errors)
	}
}

// [Bucket = Write] 서브유저에게 쓰기·삭제만 열리고 읽기·목록은 닫혀 있는지 확인
func TestBucketAclWriteScope(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "test_bucket_acl_write_scope"
	bucket := setupBucketPermission(t, s, types.PermissionWrite, 48)
	put(t, s, bucket, key, "content", nil)
	put(t, s, bucket, "other", "content", nil)
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multi")})
	if err != nil {
		t.Fatal(err)
	}

	alt := s3Client(s.cfg, s.cfg.Alt)
	// WRITE 는 쓰기·덮어쓰기·삭제·멀티파트 시작을 연다(소유자가 올린 오브젝트도)
	if err := putObjectErr(alt, bucket, "new", "x", nil); err != nil {
		t.Fatal(err)
	}
	if err := putObjectErr(alt, bucket, key, "overwritten", nil); err != nil {
		t.Fatal(err)
	}
	if err := deleteObjectErr(alt, bucket, key); err != nil {
		t.Fatal(err)
	}
	deleted, err := alt.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("other")}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted.Deleted) != 1 || aws.ToString(deleted.Deleted[0].Key) != "other" {
		t.Fatalf("deleted = %#v, want [other]", deleted.Deleted)
	}
	altCreated, err := alt.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("alt-multi")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alt.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("alt-multi"), UploadId: altCreated.UploadId}); err != nil {
		t.Fatal(err)
	}
	assertACLList(t, s.client, bucket, []string{"new"}, true)

	// 읽기·목록은 열지 않는다. 목록 권한이 없어 없는 키도 403 이다
	_, err = alt.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	assertS3Status(t, err, 403)
	assertACLDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}))
	_, err = alt.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	assertACLDenied(t, getObjectErr(alt, bucket, "not-exist", ""))
	_, err = alt.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	// 다른 사용자가 시작한 멀티파트는 중단할 수 없다
	_, err = alt.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multi"), UploadId: created.UploadId})
	assertACLDenied(t, err)
}

// [Bucket = FullControl] 서브유저에게 버킷 설정과 삭제는 열리지 않는지 확인
func TestBucketAclFullControlScope(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	bucket := setupBucketPermission(t, s, types.PermissionFullControl, 49)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, 0)
	if err := putObjectErr(alt, bucket, "new", "x", nil); err != nil {
		t.Fatal(err)
	}
	if err := deleteObjectErr(alt, bucket, "new"); err != nil {
		t.Fatal(err)
	}
	assertBucketACLGrants(t, s, alt, bucket, aclGrant{"alt", "FULL_CONTROL"}, aclGrant{"main", "FULL_CONTROL"})
	if _, err := alt.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionFullControl)}); err != nil {
		t.Fatal(err)
	}

	// ACL 의 FULL_CONTROL 은 READ·WRITE·READ_ACP·WRITE_ACP 뿐이다. 버킷 설정과 삭제는 소유자만 한다
	_, err := alt.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	_, err = alt.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
	assertACLDenied(t, err)
	_, err = alt.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(bucket), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}})
	assertACLDenied(t, err)
	_, err = alt.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	_, err = alt.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
}

// [Object = Read] 서브유저에게 본문과 메타데이터만 열리는지 확인
func TestObjectAclReadScope(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "test_object_acl_read_scope"
	bucket := setupObjectGrant(t, s, key, types.PermissionRead, 50)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, key, "content")
	head, err := alt.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt64(head.ContentLength); got != 7 {
		t.Fatalf("ContentLength = %d, want 7", got)
	}
	// READ 는 본문과 메타데이터만 연다
	assertACLDenied(t, getObjectTaggingErr(alt, bucket, key))
	_, err = alt.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: aws.String(bucket), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag}})
	assertACLDenied(t, err)
	_, err = alt.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertACLDenied(t, err)
	_, err = alt.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate})
	assertACLDenied(t, err)
	assertACLDenied(t, putObjectErr(alt, bucket, key, "x", nil))
	assertACLDenied(t, deleteObjectErr(alt, bucket, key))
}

// [Object = FullControl] 서브유저에게 쓰기·삭제·태그는 열리지 않는지 확인
func TestObjectAclFullControlScope(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "test_object_acl_full_control_scope"
	bucket := setupObjectGrant(t, s, key, types.PermissionFullControl, 51)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, key, "content")
	out, err := alt.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if grants := aclGrantSet(s, out.Grants); !slices.Contains(grants, aclGrant{"alt", "FULL_CONTROL"}) {
		t.Fatalf("grants = %v, want alt FULL_CONTROL", grants)
	}
	// 오브젝트의 FULL_CONTROL 에는 쓰기가 없다. 덮어쓰기·삭제는 버킷 권한으로 판정한다
	assertACLDenied(t, putObjectErr(alt, bucket, key, "x", nil))
	assertACLDenied(t, deleteObjectErr(alt, bucket, key))
	assertACLDenied(t, getObjectTaggingErr(alt, bucket, key))
	_, err = alt.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}})
	assertACLDenied(t, err)
	// WRITE_ACP 가 있으므로 ACL 은 바꿀 수 있다
	if _, err := alt.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate}); err != nil {
		t.Fatal(err)
	}
}

// 복사본이 원본의 ACL 을 물려받지 않고 복사 요청의 ACL 을 따르는지 확인
func TestObjectAclNotCopied(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 52)
	source := aws.String(bucket + "/source")
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("source"), Body: strings.NewReader("content"), ACL: types.ObjectCannedACLPublicRead}); err != nil {
		t.Fatal(err)
	}
	assertObjectACLGrants(t, s, bucket, "source", "", aclGrant{aclAllUsers, "READ"}, aclGrant{"main", "FULL_CONTROL"})

	// 복사본은 원본의 ACL 을 물려받지 않는다
	if _, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: source}); err != nil {
		t.Fatal(err)
	}
	assertObjectACLGrants(t, s, bucket, "copy", "", aclGrant{"main", "FULL_CONTROL"})
	// 복사 요청에 준 ACL 이 적용된다
	if _, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("canned"), CopySource: source, ACL: types.ObjectCannedACLAuthenticatedRead}); err != nil {
		t.Fatal(err)
	}
	assertObjectACLGrants(t, s, bucket, "canned", "", aclGrant{aclAuthenticatedUsers, "READ"}, aclGrant{"main", "FULL_CONTROL"})
	// grant 헤더만 주면 소유자 전권은 붙지 않는다
	if _, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("grant"), CopySource: source, GrantRead: aws.String(`id="` + s.cfg.Alt.ID + `"`)}); err != nil {
		t.Fatal(err)
	}
	assertObjectACLGrants(t, s, bucket, "grant", "", aclGrant{"alt", "READ"})
}

// ACL 없이 덮어쓰면 오브젝트가 비공개로 돌아가는지 확인
func TestObjectAclResetOnOverwrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_object_acl_reset_on_overwrite"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 53)
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("content"), ACL: types.ObjectCannedACLPublicRead}); err != nil {
		t.Fatal(err)
	}
	assertClientObjectBody(t, freshAnonymousClient(s), bucket, key, "content")

	// ACL 없이 덮어쓰면 비공개로 돌아간다
	put(t, s, bucket, key, "content", nil)
	assertObjectACLGrants(t, s, bucket, key, "", aclGrant{"main", "FULL_CONTROL"})
	assertACLDenied(t, getObjectErr(freshAnonymousClient(s), bucket, key, ""))
}

// 멀티파트를 시작할 때 준 ACL 이 완성된 오브젝트에 적용되는지 확인
func TestObjectAclMultipart(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	key := "test_object_acl_multipart"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 54)
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("content")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	// 멀티파트를 시작할 때 준 ACL 이 완성된 오브젝트의 ACL 이 된다
	assertObjectACLGrants(t, s, bucket, key, "", aclGrant{aclAllUsers, "READ"}, aclGrant{"main", "FULL_CONTROL"})
	public := freshAnonymousClient(s)
	assertClientObjectBody(t, public, bucket, key, "content")
	if _, err := public.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
	// public-read 는 ACL 조회와 태그 조회를 열지 않는다
	_, err = public.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertACLDenied(t, err)
	assertACLDenied(t, getObjectTaggingErr(public, bucket, key))
}

// ACL 이 버전마다 따로 적용되는지 확인
func TestObjectAclPerVersion(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	key := "test_object_acl_per_version"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 55)
	checkConfigureVersioningRetry(t, s, bucket, types.BucketVersioningStatusEnabled)
	first, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("1"), ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	version1 := aws.ToString(first.VersionId)
	version2 := aws.ToString(put(t, s, bucket, key, "2", nil).VersionId)

	// ACL 은 버전마다 따로다
	assertObjectACLGrants(t, s, bucket, key, version1, aclGrant{aclAllUsers, "READ"}, aclGrant{"main", "FULL_CONTROL"})
	assertObjectACLGrants(t, s, bucket, key, version2, aclGrant{"main", "FULL_CONTROL"})
	public := freshAnonymousClient(s)
	out, err := public.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(version1)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	if body, err := io.ReadAll(out.Body); err != nil || string(body) != "1" {
		t.Fatalf("body=%q err=%v, want %q", body, err, "1")
	}
	assertACLDenied(t, getObjectErr(public, bucket, key, ""))
	assertACLDenied(t, getObjectErr(public, bucket, key, version2))
}

// bucket-owner-read, bucket-owner-full-control 이 소유자 전권만 남기는지 확인
func TestAclCannedBucketOwner(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	key := "test_acl_canned_bucket_owner"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 56)
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("content"), ACL: types.ObjectCannedACLPublicRead}); err != nil {
		t.Fatal(err)
	}
	// 버킷 소유자가 곧 오브젝트 소유자면 bucket-owner-* 는 소유자 전권만 남긴다
	for _, acl := range []types.ObjectCannedACL{types.ObjectCannedACLBucketOwnerRead, types.ObjectCannedACLBucketOwnerFullControl} {
		if _, err := s.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: acl}); err != nil {
			t.Fatal(err)
		}
		assertObjectACLGrants(t, s, bucket, key, "", aclGrant{"main", "FULL_CONTROL"})
	}
	// 버킷에 주면 무시되고 비공개가 된다
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicRead)
	for _, acl := range []types.BucketCannedACL{"bucket-owner-read", "bucket-owner-full-control"} {
		setBucketCannedACL(t, s, bucket, acl)
		assertBucketACLGrants(t, s, s.client, bucket, aclGrant{"main", "FULL_CONTROL"})
	}
}

// [Bucket = public-read-write] 익명 사용자에게 열리는 범위를 확인
func TestPublicRWBucketAnonymousScope(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	key := "test_public_rw_bucket_anonymous_scope"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 57)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	put(t, s, bucket, key, "content", nil)
	if err := createMultipartErr(s.client, bucket, "multi"); err != nil {
		t.Fatal(err)
	}

	public := freshAnonymousClient(s)
	assertListV2KeyCount(t, public, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, 1)
	assertACLVersionAndUploadCount(t, public, bucket, 1, 1)
	if err := putObjectErr(public, bucket, "anonymous", "x", nil); err != nil {
		t.Fatal(err)
	}
	assertClientObjectBody(t, public, bucket, "anonymous", "x")

	// 버킷이 열려 있어도 비공개 오브젝트와 ACL 은 열리지 않는다
	assertACLDenied(t, getObjectErr(public, bucket, key, ""))
	_, err := public.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	assertACLDenied(t, err)
	_, err = public.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate})
	assertACLDenied(t, err)
	// 익명 사용자는 멀티파트 업로드를 시작할 수 없다
	assertACLDenied(t, createMultipartErr(public, bucket, "new"))
}

// endregion
