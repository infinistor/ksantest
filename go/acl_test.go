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
	privAfter  bool // PutBucketAcl PRIVATE after alt upload (Java uploadAltUser)
}

func TestPrivateBucketAndObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLPrivate, false, false, false})
}
func TestPrivateBucketPublicReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLPublicRead, false, false, false})
}
func TestPrivateBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLPublicReadWrite, false, false, false})
}
func TestPrivateBucketAuthenticatedReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLAuthenticatedRead, false, false, false})
}
func TestPrivateBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLBucketOwnerRead, false, false, false})
}
func TestPrivateBucketBucketOwnerReadObjectUploadAltUser(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerRead, true, false, true})
}
func TestPrivateBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPrivate, types.ObjectCannedACLBucketOwnerFullControl, false, false, false})
}
func TestPublicReadBucketPrivateObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLPrivate, false, false, false})
}
func TestPublicReadBucketAndObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLPublicRead, false, false, false})
}
func TestPublicReadBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLPublicReadWrite, false, false, false})
}
func TestPublicReadBucketAuthenticatedReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLAuthenticatedRead, false, false, false})
}
func TestPublicReadBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLBucketOwnerRead, false, false, false})
}
func TestPublicReadBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicRead, types.ObjectCannedACLBucketOwnerFullControl, false, false, false})
}
func TestPublicRWBucketPrivateObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPrivate, false, false, false})
}
func TestPublicRWBucketPrivateObjectByAltUser(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPrivate, true, false, false})
}
func TestPublicRWBucketPublicReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicRead, false, false, false})
}
func TestPublicRWBucketPublicReadObjectByAltUser(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicRead, true, false, false})
}
func TestPublicRWBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicReadWrite, false, false, false})
}
func TestPublicRWBucketPublicRWObjectByAltUser(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLPublicReadWrite, true, false, false})
}
func TestPublicRWBucketAuthenticatedReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLAuthenticatedRead, false, false, false})
}
func TestPublicRWBucketAuthenticatedReadObjectByAltUser(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLAuthenticatedRead, true, false, false})
}
func TestPublicRWBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerRead, false, false, false})
}
func TestPublicRWBucketBucketOwnerReadObjectByAltUser(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerRead, true, false, false})
}
func TestPublicRWBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerFullControl, false, false, false})
}
func TestPublicRWBucketBucketOwnerFullControlObjectByAltUser(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerFullControl, true, false, false})
}
func TestPublicRWBucketBucketOwnerFullControlObjectByAltUserBucketOwnerPreferred(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLPublicReadWrite, types.ObjectCannedACLBucketOwnerFullControl, true, true, false})
}
func TestAuthenticatedReadBucketPrivateObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLPrivate, false, false, false})
}
func TestAuthenticatedReadBucketPublicReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLPublicRead, false, false, false})
}
func TestAuthenticatedReadBucketPublicRWObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLPublicReadWrite, false, false, false})
}
func TestAuthenticatedReadBucketAndObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLAuthenticatedRead, false, false, false})
}
func TestAuthenticatedReadBucketBucketOwnerReadObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLBucketOwnerRead, false, false, false})
}
func TestAuthenticatedReadBucketBucketOwnerFullControlObject(t *testing.T) {
	t.Parallel()

	runACLMatrix(t, aclMatrixCase{types.BucketCannedACLAuthenticatedRead, types.ObjectCannedACLBucketOwnerFullControl, false, false, false})
}
func TestPrivateBucketList(t *testing.T) {
	t.Parallel()

	runACLList(t, types.BucketCannedACLPrivate, false, false)
}
func TestPublicReadBucketList(t *testing.T) {
	t.Parallel()

	runACLList(t, types.BucketCannedACLPublicRead, true, true)
}
func TestPublicRWBucketList(t *testing.T) {
	t.Parallel()

	runACLList(t, types.BucketCannedACLPublicReadWrite, true, true)
}
func TestAuthenticatedReadBucketList(t *testing.T) {
	t.Parallel()

	runACLList(t, types.BucketCannedACLAuthenticatedRead, true, false)
}
func TestBucketPermissionAltUserFullControl(t *testing.T) {
	t.Parallel()

	runBucketPermission(t, types.PermissionFullControl, true, true, true, true)
}
func TestBucketPermissionAltUserRead(t *testing.T) {
	t.Parallel()

	runBucketPermission(t, types.PermissionRead, true, false, false, false)
}
func TestBucketPermissionAltUserReadAcp(t *testing.T) {
	t.Parallel()

	runBucketPermission(t, types.PermissionReadAcp, false, true, false, false)
}
func TestBucketPermissionAltUserWrite(t *testing.T) {
	t.Parallel()

	runBucketPermission(t, types.PermissionWrite, false, false, true, false)
}
func TestBucketPermissionAltUserWriteAcp(t *testing.T) {
	t.Parallel()

	runBucketPermission(t, types.PermissionWriteAcp, false, false, false, true)
}
func TestObjectPermissionAltUserFullControl(t *testing.T) {
	t.Parallel()

	runObjectPermission(t, types.PermissionFullControl, true, true, true)
}
func TestObjectPermissionAltUserRead(t *testing.T) {
	t.Parallel()

	runObjectPermission(t, types.PermissionRead, true, false, false)
}
func TestObjectPermissionAltUserReadAcp(t *testing.T) {
	t.Parallel()

	runObjectPermission(t, types.PermissionReadAcp, false, true, false)
}
func TestObjectPermissionAltUserWrite(t *testing.T) {
	t.Parallel()

	runObjectPermission(t, types.PermissionWrite, false, false, false)
}
func TestObjectPermissionAltUserWriteAcp(t *testing.T) {
	t.Parallel()

	runObjectPermission(t, types.PermissionWriteAcp, false, false, true)
}

// aclExpectations mirrors Java ACL.java Access cases (non-uploadAltUser special path).
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

func runACLMatrix(t *testing.T, tc aclMatrixCase) {
	t.Helper()
	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	ownership := types.ObjectOwnershipObjectWriter
	if tc.ownerFirst {
		ownership = types.ObjectOwnershipBucketOwnerPreferred
	}
	bucket := ownershipBucket(t, s, ownership)
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

func runACLList(t *testing.T, acl types.BucketCannedACL, altAllowed, publicAllowed bool) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: acl}); err != nil {
		t.Fatal(err)
	}
	want := []string{"list-1", "list-2", "list-3"}
	for _, key := range want {
		put(t, s, bucket, key, key, nil)
	}
	assertACLList(t, s.client, bucket, want, true)
	assertACLList(t, s3Client(s.cfg, s.cfg.Alt), bucket, want, altAllowed)
	assertACLList(t, anonymousClient(s), bucket, want, publicAllowed)
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
func runBucketPermission(t *testing.T, permission types.Permission, readOK, readACPOK, writeOK, writeACPOK bool) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, permission)}); err != nil {
		t.Fatal(err)
	}
	checkBucketPermission(t, s3Client(s.cfg, s.cfg.Alt), bucket, readOK, readACPOK, writeOK, writeACPOK)
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
func runObjectPermission(t *testing.T, permission types.Permission, readOK, readACPOK, writeACPOK bool) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
		t.Fatal(err)
	}
	key := "object-permission"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, permission)}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		out.Body.Close()
	}
	expectACLStatus(t, err, readOK)
	_, err = alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	expectACLStatus(t, err, readACPOK)
	_, err = alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	expectACLStatus(t, err, false)
	writeACPACL := types.ObjectCannedACLPublicRead
	if writeACPOK {
		writeACPACL = types.ObjectCannedACLPublicReadWrite
	}
	_, err = alt.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: writeACPACL})
	expectACLStatus(t, err, writeACPOK)
}

// expectACLStatus matches Java checkBucketAcl*/checkObjectAcl* deny helpers: HTTP 403 only.
// HeadBucket has no XML body, so SDK ErrorCode is often "Forbidden" rather than "AccessDenied".
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
