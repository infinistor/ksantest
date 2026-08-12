package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	backendVersionHeader      = "x-ifs-version-id"
	backendKSANVersionHeader  = "x-ksan-version-id"
	backendDeleteMarkerHeader = "x-ifs-delete-marker-version-id"
	backendKSANDeleteHeader   = "x-ksan-delete-marker-version-id"
	backendMB                 = 1024 * 1024
	backendPartSize           = 5 * backendMB
	backendObjectSize         = 10 * backendMB
)

// [Versioning] PutObject가 정상 동작하는지 확인
func TestPutObjectVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, key, body := s.bucket(t, 11), "test_put_object_versioning", "test content"
	enableVersioning(t, s, bucket)
	out := mustBackendPut(t, backend, bucket, key, []byte(body), nil, "")
	versionID := aws.ToString(out.VersionId)
	requireVersionID(t, versionID)
	assertBackendBody(t, s.client, bucket, key, []byte(body), versionID)
}

// [Versioning] PutObject 버전 정보 추가시 정상 동작 확인
func TestPutObjectVersioningWithVersionId(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket := s.bucket(t, 12)
	sourceKey, targetKey := "test_put_object_versioning_with_version_id_source", "test_put_object_versioning_with_version_id_target"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, sourceKey, "test content", nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	mustBackendPut(t, backend, bucket, targetKey, []byte("test content2"), nil, versionID)
	assertBackendBody(t, s.client, bucket, targetKey, []byte("test content2"), versionID)
}

// [Versioning] GetObject가 정상 동작하는지 확인
func TestGetObjectVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, key, body := s.bucket(t, 13), "test_get_object_versioning", "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	assertBackendBody(t, backend, bucket, key, []byte(body), versionID)
}

// [Versioning] DeleteObject가 정상 동작하는지 확인
func TestDeleteObjectVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, key, body := s.bucket(t, 14), "test_delete_object_versioning", "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	marker, err := backend.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	requireVersionID(t, aws.ToString(marker.VersionId))
	listed := listBackendVersions(t, s.client, bucket)
	if len(listed.Versions) != 1 || len(listed.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d deleteMarkers=%d, want 1/1", len(listed.Versions), len(listed.DeleteMarkers))
	}
	if _, err := backend.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)}); err != nil {
		t.Fatal(err)
	}
	listed = listBackendVersions(t, s.client, bucket)
	if len(listed.Versions) != 0 || len(listed.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d deleteMarkers=%d, want 0/1", len(listed.Versions), len(listed.DeleteMarkers))
	}
}

// [Versioning] DeleteObjects가 정상 동작하는지 확인
func TestDeleteObjectsVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, prefix, body := s.bucket(t, 15), "test_delete_objects_versioning", "test content"
	enableVersioning(t, s, bucket)
	keys := make([]string, 5)
	objects := make([]types.ObjectIdentifier, 5)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s-%d", prefix, i)
		putOut := put(t, s, bucket, keys[i], body, nil)
		requireVersionID(t, aws.ToString(putOut.VersionId))
		objects[i] = types.ObjectIdentifier{Key: aws.String(keys[i])}
	}
	deleted, err := backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects}})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted.Deleted) != 5 {
		t.Fatalf("deleted=%d, want 5", len(deleted.Deleted))
	}
	listed := listBackendVersions(t, s.client, bucket)
	if len(listed.Versions) != 5 || len(listed.DeleteMarkers) != 5 {
		t.Fatalf("versions=%d deleteMarkers=%d, want 5/5", len(listed.Versions), len(listed.DeleteMarkers))
	}
	purge := make([]types.ObjectIdentifier, 0, 10)
	for _, version := range listed.Versions {
		purge = append(purge, types.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
	}
	for _, marker := range listed.DeleteMarkers {
		purge = append(purge, types.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
	}
	finalDelete, err := backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: purge}})
	if err != nil {
		t.Fatal(err)
	}
	if len(finalDelete.Deleted) != 10 {
		t.Fatalf("final deleted=%d, want 10", len(finalDelete.Deleted))
	}
	listed = listBackendVersions(t, s.client, bucket)
	if len(listed.Versions) != 0 || len(listed.DeleteMarkers) != 0 {
		t.Fatalf("versions=%d deleteMarkers=%d, want 0/0", len(listed.Versions), len(listed.DeleteMarkers))
	}
}

// [Versioning] HeadObject가 정상 동작하는지 확인
func TestHeadObjectVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, key, body := s.bucket(t, 16), "test_head_object_versioning", "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	head, err := backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToInt64(head.ContentLength) != int64(len(body)) || aws.ToString(head.VersionId) != versionID {
		t.Fatalf("length=%v version=%q", head.ContentLength, aws.ToString(head.VersionId))
	}
}

// [Versioning] CopyObject가 정상 동작하는지 확인
func TestCopyObjectVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	sourceBucket, targetBucket := s.bucket(t, 17), s.bucket(t, 17)
	sourceKey, intermediateKey, targetKey, body := "source_key", "source_key_2", "target_key", "test content"
	enableVersioning(t, s, sourceBucket)
	enableVersioning(t, s, targetBucket)
	source := put(t, s, sourceBucket, sourceKey, body, nil)
	sourceVersionID := aws.ToString(source.VersionId)
	requireVersionID(t, sourceVersionID)
	intermediate, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String(sourceBucket), Key: aws.String(intermediateKey),
		CopySource: aws.String(backendCopySource(sourceBucket, sourceKey, sourceVersionID)),
	})
	if err != nil {
		t.Fatal(err)
	}
	targetVersionID := aws.ToString(intermediate.VersionId)
	requireVersionID(t, targetVersionID)
	_, err = backend.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String(targetBucket), Key: aws.String(targetKey),
		CopySource: aws.String(backendCopySource(sourceBucket, intermediateKey, targetVersionID)),
	}, versionOption(targetVersionID))
	if err != nil {
		t.Fatal(err)
	}
	assertBackendBody(t, s.client, targetBucket, targetKey, []byte(body), targetVersionID)
}

// [Versioning] MultipartUpload가 정상 동작하는지 확인
func TestMultipartUploadVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, key := s.bucket(t, 18), "test_multipart_upload_versioning"
	enableVersioning(t, s, bucket)
	body := backendPayload(backendObjectSize)
	versionID := backendMultipart(t, backend, bucket, key, body, nil, "")
	requireVersionID(t, versionID)
	assertBackendHead(t, s.client, bucket, key, versionID, int64(len(body)), nil)
	assertBackendRanges(t, s.client, bucket, key, versionID, body, backendMB)
}

// [Versioning] PutObjectAcl가 정상 동작하는지 확인
func TestPutObjectAclVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, key, body := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 19), "test_put_object_acl_versioning", "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	_, err := backend.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID), ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	assertBackendPublicReadACL(t, s.client, bucket, key, versionID)
}

// [Versioning] GetObjectAcl가 정상 동작하는지 확인
func TestGetObjectAclVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, key, body := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 20), "test_get_object_acl_versioning", "test content"
	enableVersioning(t, s, bucket)
	source, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(body)), ACL: types.ObjectCannedACLPublicRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	assertBackendPublicReadACL(t, backend, bucket, key, versionID)
}

// [Versioning] PutObjectTagging가 정상 동작하는지 확인
func TestPutObjectTaggingVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, key, body := s.bucket(t, 21), "test_put_object_tagging_versioning", "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	backendPutTag(t, backend, bucket, key, versionID)
	assertBackendTags(t, s.client, bucket, key, versionID, backendTags())
}

// [Versioning] GetObjectTagging가 정상 동작하는지 확인
func TestGetObjectTaggingVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, key, body := s.bucket(t, 22), "test_get_object_tagging_versioning", "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	backendPutTag(t, s.client, bucket, key, versionID)
	assertBackendTags(t, backend, bucket, key, versionID, backendTags())
}

// [Versioning] DeleteObjectTagging가 정상 동작하는지 확인
func TestDeleteObjectTaggingVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, key, body := s.bucket(t, 23), "test_delete_object_tagging_versioning", "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	requireVersionID(t, versionID)
	backendPutTag(t, s.client, bucket, key, versionID)
	if _, err := backend.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)}); err != nil {
		t.Fatal(err)
	}
	assertBackendTags(t, s.client, bucket, key, versionID, nil)
}

// [Versioning] PutObjectRetention가 정상 동작하는지 확인
func TestPutObjectRetentionVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket := backendLockBucket(t, s)
	key := "test_put_object_retention_versioning"
	putOut := put(t, s, bucket, key, "retained", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	until := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	retention := &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance, RetainUntilDate: &until}
	if _, err := backend.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
		Retention: retention, BypassGovernanceRetention: aws.Bool(true),
	}); err != nil {
		t.Fatal(err)
	}
}

// [Versioning] GetObjectRetention가 정상 동작하는지 확인
func TestGetObjectRetentionVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket := backendLockBucket(t, s)
	key := "test_get_object_retention_versioning"
	putOut := put(t, s, bucket, key, "retained", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	if out, err := backend.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
	}); err == nil {
		t.Fatalf("GetObjectRetention succeeded without retention: %#v", out.Retention)
	} else {
		assertAPIError(t, err)
	}
}

// [Versioning] PutObjectRetention 후 GetObjectRetention으로 조회가 정상 동작하는지 확인
func TestPutAndGetObjectRetentionVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket := backendLockBucket(t, s)
	key := "test_put_and_get_object_retention_versioning"
	putOut := put(t, s, bucket, key, "retained", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	until := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	retention := &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance, RetainUntilDate: &until}
	if _, err := backend.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
		Retention: retention, BypassGovernanceRetention: aws.Bool(true),
	}); err != nil {
		t.Fatal(err)
	}
	out, err := backend.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Retention == nil || out.Retention.Mode != types.ObjectLockRetentionModeGovernance ||
		out.Retention.RetainUntilDate == nil || !out.Retention.RetainUntilDate.Equal(until) {
		t.Fatalf("retention=%#v", out.Retention)
	}
}

// PutObject 복제가 정상 동작하는지 확인
func TestPutObjectReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	sourceBucket, targetBucket, key := s.bucket(t, 27), s.bucket(t, 27), "test_backend_replication"
	enableVersioning(t, s, sourceBucket)
	enableVersioning(t, s, targetBucket)
	putOut := put(t, s, sourceBucket, key, "test content", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	backendReplicatePut(t, backend, sourceBucket, key, targetBucket, key, versionID)
	assertBackendBody(t, s.client, targetBucket, key, []byte("test content"), versionID)
}

// PutObject 태그가 복제되는지 확인
func TestPutObjectWithTaggingReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	sourceBucket, targetBucket, key := s.bucket(t, 28), s.bucket(t, 28), "test_backend_replication_tagging"
	enableVersioning(t, s, sourceBucket)
	enableVersioning(t, s, targetBucket)
	putOut := put(t, s, sourceBucket, key, "test content", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	backendPutTag(t, s.client, sourceBucket, key, versionID)
	backendReplicatePut(t, backend, sourceBucket, key, targetBucket, key, versionID)
	assertBackendBody(t, s.client, targetBucket, key, []byte("test content"), versionID)
	assertBackendTags(t, s.client, targetBucket, key, versionID, backendTags())
}

// PutObject 헤더와 메타데이터가 복제되는지 확인
func TestPutObjectWithMetadataReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	sourceBucket, targetBucket, key := s.bucket(t, 29), s.bucket(t, 29), "test_backend_replication_metadata"
	enableVersioning(t, s, sourceBucket)
	enableVersioning(t, s, targetBucket)
	metadata := map[string]string{"test-key": "testValue"}
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(sourceBucket), Key: aws.String(key), Body: bytes.NewReader([]byte("test content")),
		Metadata: metadata, ContentType: aws.String("text/plain; charset=utf-8"), CacheControl: aws.String("no-cache"),
	})
	if err != nil {
		t.Fatal(err)
	}
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	backendReplicatePut(t, backend, sourceBucket, key, targetBucket, key, versionID)
	assertBackendBody(t, s.client, targetBucket, key, []byte("test content"), versionID)
	assertBackendHead(t, s.client, targetBucket, key, versionID, int64(len("test content")), metadata)
	assertBackendHeaders(t, s.client, targetBucket, key, versionID, "text/plain; charset=utf-8", "no-cache")
}

// CopyObject 복제가 정상 동작하는지 확인
func TestCopyObjectReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, sourceKey, intermediateKey, targetKey := s.bucket(t, 30), "source_key", "source_key_2", "target_key"
	enableVersioning(t, s, bucket)
	versionID := prepareBackendCopy(t, backendCopySetup{
		client: s.client, bucket: bucket, sourceKey: sourceKey, targetKey: intermediateKey, body: []byte("test content"),
	})
	copyInput := &s3.CopyObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(targetKey),
		CopySource: aws.String(backendCopySource(bucket, intermediateKey, versionID)),
	}
	if _, err := backend.CopyObject(ctx, copyInput, versionOption(versionID)); err != nil {
		t.Fatal(err)
	}
	assertBackendBody(t, s.client, bucket, targetKey, []byte("test content"), versionID)
}

// CopyObject 태그가 복제되는지 확인
func TestCopyObjectWithTaggingReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, sourceKey, intermediateKey, targetKey := s.bucket(t, 31), "source_key", "source_key_2", "target_key"
	enableVersioning(t, s, bucket)
	versionID := prepareBackendCopy(t, backendCopySetup{
		client: s.client, bucket: bucket, sourceKey: sourceKey, targetKey: intermediateKey,
		body: []byte("test content"), tagged: true,
	})
	copyInput := &s3.CopyObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(targetKey),
		CopySource: aws.String(backendCopySource(bucket, intermediateKey, versionID)),
	}
	if _, err := backend.CopyObject(ctx, copyInput, versionOption(versionID)); err != nil {
		t.Fatal(err)
	}
	assertBackendBody(t, s.client, bucket, targetKey, []byte("test content"), versionID)
	assertBackendTags(t, s.client, bucket, targetKey, versionID, backendTags())
}

// CopyObject 헤더와 메타데이터가 복제되는지 확인
func TestCopyObjectWithMetadataReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, sourceKey, intermediateKey, targetKey := s.bucket(t, 32), "source_key", "source_key_2", "target_key"
	enableVersioning(t, s, bucket)
	metadata := map[string]string{"test-key": "testValue"}
	versionID := prepareBackendCopy(t, backendCopySetup{
		client: s.client, bucket: bucket, sourceKey: sourceKey, targetKey: intermediateKey,
		body: []byte("test content"), metadata: metadata,
	})
	copyInput := &s3.CopyObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(targetKey),
		CopySource: aws.String(backendCopySource(bucket, intermediateKey, versionID)),
	}
	if _, err := backend.CopyObject(ctx, copyInput, versionOption(versionID)); err != nil {
		t.Fatal(err)
	}
	assertBackendBody(t, s.client, bucket, targetKey, []byte("test content"), versionID)
	assertBackendHead(t, s.client, bucket, targetKey, versionID, int64(len("test content")), metadata)
}

// CopyObject 메타데이터가 Replace되었을 경우 복제되는지 확인
func TestCopyObjectMetadataReplaceReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, sourceKey, intermediateKey, targetKey := s.bucket(t, 33), "source_key", "source_key_2", "target_key"
	enableVersioning(t, s, bucket)
	replacement := map[string]string{"test-key2": "testValue2"}
	versionID := prepareBackendCopyReplace(t, backendCopySetup{
		client: s.client, bucket: bucket, sourceKey: sourceKey, targetKey: intermediateKey,
		body: []byte("test content"), metadata: map[string]string{"test-key": "testValue"},
	}, replacement)
	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(targetKey),
		CopySource:        aws.String(backendCopySource(bucket, intermediateKey, versionID)),
		MetadataDirective: types.MetadataDirectiveCopy,
	}
	if _, err := backend.CopyObject(ctx, copyInput, versionOption(versionID)); err != nil {
		t.Fatal(err)
	}
	assertBackendBody(t, s.client, bucket, targetKey, []byte("test content"), versionID)
	assertBackendHead(t, s.client, bucket, targetKey, versionID, int64(len("test content")), replacement)
}

// MultipartUpload 복제가 정상 동작하는지 확인
func TestMultipartUploadReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, sourceKey, targetKey := s.bucket(t, 34), "test_multipart_upload_replication-source", "test_multipart_upload_replication-target"
	enableVersioning(t, s, bucket)
	body := backendPayload(backendObjectSize)
	versionID := backendMultipart(t, s.client, bucket, sourceKey, body, nil, "")
	backendReplicateMultipart(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	assertBackendHead(t, s.client, bucket, targetKey, versionID, int64(len(body)), nil)
	assertBackendRanges(t, s.client, bucket, targetKey, versionID, body, backendMB)
}

// MultipartUpload 태그가 복제되는지 확인
func TestMultipartUploadWithTaggingReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, sourceKey, targetKey := s.bucket(t, 35), "test_multipart_upload_with_tagging_replication-source", "test_multipart_upload_with_tagging_replication-target"
	enableVersioning(t, s, bucket)
	body := backendPayload(backendObjectSize)
	versionID := backendMultipart(t, s.client, bucket, sourceKey, body, nil, backendTaggingQuery())
	assertBackendTags(t, s.client, bucket, sourceKey, versionID, backendTags())
	backendReplicateMultipart(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	assertBackendHead(t, s.client, bucket, targetKey, versionID, int64(len(body)), nil)
	assertBackendTags(t, s.client, bucket, targetKey, versionID, backendTags())
	assertBackendRanges(t, s.client, bucket, targetKey, versionID, body, backendMB)
}

// MultipartUpload 헤더와 메타데이터가 복제되는지 확인
func TestMultipartUploadWithMetadataReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, sourceKey, targetKey := s.bucket(t, 36), "test_multipart_upload_with_metadata_replication-source", "test_multipart_upload_with_metadata_replication-target"
	enableVersioning(t, s, bucket)
	body := backendPayload(backendObjectSize)
	metadata := map[string]string{"test-key": "testValue"}
	versionID := backendMultipart(t, s.client, bucket, sourceKey, body, metadata, "")
	assertBackendHead(t, s.client, bucket, sourceKey, versionID, int64(len(body)), metadata)
	backendReplicateMultipart(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	assertBackendHead(t, s.client, bucket, targetKey, versionID, int64(len(body)), metadata)
	assertBackendHeaders(t, s.client, bucket, targetKey, versionID, "application/octet-stream", "")
	assertBackendRanges(t, s.client, bucket, targetKey, versionID, body, backendMB)
}

// PutObjectAcl 복제가 정상 동작하는지 확인
func TestPutObjectAclReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, sourceKey, targetKey := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 37), "test_put_object_acl_replication_source", "test_put_object_acl_replication_target"
	enableVersioning(t, s, bucket)
	putOut := put(t, s, bucket, sourceKey, "test content", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	if _, err := s.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(sourceKey), VersionId: aws.String(versionID), ACL: types.ObjectCannedACLPublicRead}); err != nil {
		t.Fatal(err)
	}
	backendReplicatePut(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	backendReplicateACL(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	assertBackendBody(t, s.client, bucket, targetKey, []byte("test content"), versionID)
	assertBackendPublicReadACL(t, s.client, bucket, targetKey, versionID)
}

// putObjectTagging 복제가 정상 동작하는지 확인
func TestPutObjectTaggingReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	bucket, sourceKey, targetKey := s.bucket(t, 38), "test_put_object_tagging_replication-source", "test_put_object_tagging_replication-target"
	enableVersioning(t, s, bucket)
	putOut := put(t, s, bucket, sourceKey, "test content", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	backendPutTag(t, s.client, bucket, sourceKey, versionID)
	backendReplicatePut(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	backendReplicateTags(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	assertBackendBody(t, s.client, bucket, targetKey, []byte("test content"), versionID)
	assertBackendTags(t, s.client, bucket, targetKey, versionID, backendTags())
}

// deleteObject 복제가 정상 동작하는지 확인
func TestDeleteObjectReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, sourceKey, targetKey := s.bucket(t, 39), "test_delete_object_replication-source", "test_delete_object_replication-target"
	enableVersioning(t, s, bucket)
	putOut := put(t, s, bucket, sourceKey, "test content", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	backendReplicatePut(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	deleted, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(sourceKey)})
	if err != nil {
		t.Fatal(err)
	}
	markerID := aws.ToString(deleted.VersionId)
	requireVersionID(t, markerID)
	if _, err := backend.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), Body: bytes.NewReader(nil)}, deleteMarkerOption(markerID)); err != nil {
		t.Fatal(err)
	}
	versions := listBackendVersions(t, s.client, bucket)
	if len(versions.DeleteMarkers) != 2 || len(versions.Versions) != 2 {
		t.Fatalf("versions=%d deleteMarkers=%d, want 2/2", len(versions.Versions), len(versions.DeleteMarkers))
	}
	for _, marker := range versions.DeleteMarkers {
		if aws.ToString(marker.VersionId) != markerID {
			t.Fatalf("delete marker version=%q, want %q", aws.ToString(marker.VersionId), markerID)
		}
	}
	for _, version := range versions.Versions {
		if aws.ToString(version.VersionId) != versionID {
			t.Fatalf("object version=%q, want %q", aws.ToString(version.VersionId), versionID)
		}
	}
}

// deleteObjectTagging 복제가 정상 동작하는지 확인
func TestDeleteObjectTaggingReplication(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("Backend API is unavailable on AWS")
	}
	backend := newBackendClient(t, s)

	ctx := context.Background()
	bucket, sourceKey, targetKey := s.bucket(t, 40), "test_delete_object_tagging_replication-source", "test_delete_object_tagging_replication-target"
	enableVersioning(t, s, bucket)
	putOut := put(t, s, bucket, sourceKey, "test content", nil)
	versionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, versionID)
	backendReplicatePut(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	backendPutTag(t, s.client, bucket, sourceKey, versionID)
	backendReplicateTags(t, backend, bucket, sourceKey, bucket, targetKey, versionID)
	assertBackendTags(t, s.client, bucket, sourceKey, versionID, backendTags())
	assertBackendTags(t, s.client, bucket, targetKey, versionID, backendTags())
	if _, err := s.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(targetKey)}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), VersionId: aws.String(versionID)}); err != nil {
		t.Fatal(err)
	}
	assertBackendTags(t, s.client, bucket, sourceKey, versionID, backendTags())
	assertBackendTags(t, s.client, bucket, targetKey, versionID, nil)
}

func newBackendClient(t *testing.T, s *suite) *s3.Client {
	t.Helper()
	user := s.cfg.Backend
	if user.AccessKey == "" || user.SecretKey == "" {
		t.Fatal("config.ini의 Backend User 자격 증명을 설정하세요")
	}
	options := s3.Options{
		Region: s.cfg.Region, BaseEndpoint: aws.String(s.cfg.Endpoint()), UsePathStyle: true,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(user.AccessKey, user.SecretKey, "")),
	}
	options.APIOptions = append(options.APIOptions, backendHeaders("backend-headers", map[string]string{
		"x-ifs-admin": "NONE", "x-ifs-backend": "NONE", "x-ksan-backend": "NONE", "x-ifs-replication": "NONE",
	}))
	return s3.New(options)
}

func backendHeaders(name string, headers map[string]string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc(name, func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
			if request, ok := in.Request.(*smithyhttp.Request); ok {
				for key, value := range headers {
					request.Header.Set(key, value)
				}
			}
			return next.HandleBuild(ctx, in)
		}), middleware.After)
	}
}

func versionOption(versionID string) func(*s3.Options) {
	return func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, backendHeaders("backend-version-headers", map[string]string{backendVersionHeader: versionID, backendKSANVersionHeader: versionID}))
	}
}

func deleteMarkerOption(versionID string) func(*s3.Options) {
	return func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, backendHeaders("backend-delete-marker-headers", map[string]string{backendDeleteMarkerHeader: versionID, backendKSANDeleteHeader: versionID}))
	}
}

func mustBackendPut(t *testing.T, client *s3.Client, bucket, key string, body []byte, metadata map[string]string, versionID string) *s3.PutObjectOutput {
	t.Helper()
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), Metadata: metadata}
	var (
		out *s3.PutObjectOutput
		err error
	)
	if versionID == "" {
		out, err = client.PutObject(context.Background(), input)
	} else {
		out, err = client.PutObject(context.Background(), input, versionOption(versionID))
	}
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func backendReplicatePut(t *testing.T, client *s3.Client, sourceBucket, sourceKey, targetBucket, targetKey, versionID string) {
	t.Helper()
	ctx := context.Background()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(sourceKey), VersionId: aws.String(versionID)})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(out.Body)
	closeErr := out.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read err=%v close err=%v", readErr, closeErr)
	}
	tags := getBackendTags(t, client, sourceBucket, sourceKey, versionID)
	input := &s3.PutObjectInput{
		Bucket: aws.String(targetBucket), Key: aws.String(targetKey), Body: bytes.NewReader(body),
		Metadata: out.Metadata, Tagging: aws.String(encodeBackendTags(tags)),
		CacheControl: out.CacheControl, ContentDisposition: out.ContentDisposition, ContentEncoding: out.ContentEncoding,
		ContentLanguage: out.ContentLanguage, ContentType: out.ContentType, Expires: out.Expires,
	}
	if _, err := client.PutObject(ctx, input, versionOption(versionID)); err != nil {
		t.Fatal(err)
	}
}

func backendMultipart(t *testing.T, client *s3.Client, bucket, key string, body []byte, metadata map[string]string, tagging string) string {
	t.Helper()
	ctx := context.Background()
	create := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Metadata: metadata, ContentType: aws.String("application/octet-stream"),
	}
	if tagging != "" {
		create.Tagging = aws.String(tagging)
	}
	created, err := client.CreateMultipartUpload(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	abortUpload := true
	defer func() {
		if abortUpload {
			_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
			})
		}
	}()
	parts := make([]types.CompletedPart, 0, (len(body)+backendPartSize-1)/backendPartSize)
	for start, partNumber := 0, int32(1); start < len(body); start, partNumber = start+backendPartSize, partNumber+1 {
		end := min(start+backendPartSize, len(body))
		part, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
			PartNumber: aws.Int32(partNumber), Body: bytes.NewReader(body[start:end]),
		})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(partNumber)})
	}
	completed, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		t.Fatal(err)
	}
	abortUpload = false
	versionID := aws.ToString(completed.VersionId)
	requireVersionID(t, versionID)
	return versionID
}

func backendReplicateMultipart(t *testing.T, client *s3.Client, sourceBucket, sourceKey, targetBucket, targetKey, versionID string) {
	t.Helper()
	ctx := context.Background()
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(sourceKey), VersionId: aws.String(versionID)})
	if err != nil {
		t.Fatal(err)
	}
	tags := getBackendTags(t, client, sourceBucket, sourceKey, versionID)
	create := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(targetBucket), Key: aws.String(targetKey), Metadata: head.Metadata,
		Tagging: aws.String(encodeBackendTags(tags)), CacheControl: head.CacheControl,
		ContentDisposition: head.ContentDisposition, ContentEncoding: head.ContentEncoding,
		ContentLanguage: head.ContentLanguage, ContentType: head.ContentType, Expires: head.Expires,
	}
	created, err := client.CreateMultipartUpload(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	abortUpload := true
	defer func() {
		if abortUpload {
			_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(targetBucket), Key: aws.String(targetKey), UploadId: created.UploadId,
			})
		}
	}()
	parts := make([]types.CompletedPart, 0, int((aws.ToInt64(head.ContentLength)+backendPartSize-1)/backendPartSize))
	for start, partNumber := int64(0), int32(1); start < aws.ToInt64(head.ContentLength); start, partNumber = start+backendPartSize, partNumber+1 {
		end := min(start+backendPartSize, aws.ToInt64(head.ContentLength)) - 1
		source, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(sourceBucket), Key: aws.String(sourceKey), VersionId: aws.String(versionID),
			Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		})
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(source.Body)
		closeErr := source.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read err=%v close err=%v", readErr, closeErr)
		}
		part, uploadErr := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(targetBucket), Key: aws.String(targetKey), UploadId: created.UploadId,
			PartNumber: aws.Int32(partNumber), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		if uploadErr != nil {
			t.Fatal(uploadErr)
		}
		parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(partNumber)})
	}
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(targetBucket), Key: aws.String(targetKey), UploadId: created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}, versionOption(versionID))
	if err != nil {
		t.Fatal(err)
	}
	abortUpload = false
}

func backendPutTag(t *testing.T, client *s3.Client, bucket, key, versionID string) {
	t.Helper()
	input := &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: &types.Tagging{TagSet: backendTags()}}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	if _, err := client.PutObjectTagging(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

func backendReplicateTags(t *testing.T, client *s3.Client, sourceBucket, sourceKey, targetBucket, targetKey, versionID string) {
	t.Helper()
	tags := getBackendTags(t, client, sourceBucket, sourceKey, versionID)
	if _, err := client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(targetBucket), Key: aws.String(targetKey), VersionId: aws.String(versionID),
		Tagging: &types.Tagging{TagSet: tags},
	}); err != nil {
		t.Fatal(err)
	}
}

func backendReplicateACL(t *testing.T, client *s3.Client, sourceBucket, sourceKey, targetBucket, targetKey, versionID string) {
	t.Helper()
	acl, err := client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
		Bucket: aws.String(sourceBucket), Key: aws.String(sourceKey), VersionId: aws.String(versionID),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
		Bucket: aws.String(targetBucket), Key: aws.String(targetKey), VersionId: aws.String(versionID),
		AccessControlPolicy: &types.AccessControlPolicy{Owner: acl.Owner, Grants: acl.Grants},
	}); err != nil {
		t.Fatal(err)
	}
}

func assertBackendBody(t *testing.T, client *s3.Client, bucket, key string, want []byte, versionID string) {
	t.Helper()
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil || !bytes.Equal(body, want) || (versionID != "" && aws.ToString(out.VersionId) != versionID) {
		t.Fatalf("body=%q version=%q err=%v", body, aws.ToString(out.VersionId), err)
	}
}

func assertBackendTags(t *testing.T, client *s3.Client, bucket, key, versionID string, want []types.Tag) {
	t.Helper()
	got := getBackendTags(t, client, bucket, key, versionID)
	sortBackendTags(got)
	sortBackendTags(want)
	if len(got) != len(want) {
		t.Fatalf("tags=%v, want %v", got, want)
	}
	for i := range want {
		if aws.ToString(got[i].Key) != aws.ToString(want[i].Key) || aws.ToString(got[i].Value) != aws.ToString(want[i].Value) {
			t.Fatalf("tags=%v, want %v", got, want)
		}
	}
}

func assertBackendPublicReadACL(t *testing.T, client *s3.Client, bucket, key, versionID string) {
	t.Helper()
	input := &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := client.GetObjectAcl(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Grants) != 2 {
		t.Fatalf("grants=%v, want exactly 2", out.Grants)
	}
	var fullControl, publicRead bool
	for _, grant := range out.Grants {
		if grant.Permission == types.PermissionFullControl && grant.Grantee != nil && aws.ToString(grant.Grantee.ID) != "" {
			fullControl = true
		}
		if grant.Permission == types.PermissionRead && grant.Grantee != nil && aws.ToString(grant.Grantee.URI) == allUsersURI {
			publicRead = true
		}
	}
	if !fullControl || !publicRead {
		t.Fatalf("grants=%v err=%v", out.Grants, err)
	}
}

func assertBackendHead(t *testing.T, client *s3.Client, bucket, key, versionID string, size int64, metadata map[string]string) {
	t.Helper()
	out, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
	})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToInt64(out.ContentLength) != size || aws.ToString(out.VersionId) != versionID ||
		!equalBackendMetadata(out.Metadata, metadata) {
		t.Fatalf("length=%d version=%q metadata=%v", aws.ToInt64(out.ContentLength), aws.ToString(out.VersionId), out.Metadata)
	}
}

func assertBackendHeaders(t *testing.T, client *s3.Client, bucket, key, versionID, contentType, cacheControl string) {
	t.Helper()
	out, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
	})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(out.ContentType) != contentType || (cacheControl != "" && aws.ToString(out.CacheControl) != cacheControl) {
		t.Fatalf("contentType=%q cacheControl=%q, want %q/%q", aws.ToString(out.ContentType), aws.ToString(out.CacheControl), contentType, cacheControl)
	}
}

func assertBackendRanges(t *testing.T, client *s3.Client, bucket, key, versionID string, want []byte, step int) {
	t.Helper()
	for start := 0; start < len(want); start += step {
		end := min(start+step, len(want)) - 1
		out, err := client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
			Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		})
		if err != nil {
			t.Fatal(err)
		}
		got, readErr := io.ReadAll(out.Body)
		closeErr := out.Body.Close()
		if readErr != nil || closeErr != nil || aws.ToInt64(out.ContentLength) != int64(end-start+1) || !bytes.Equal(got, want[start:end+1]) {
			t.Fatalf("range=%d-%d length=%d err=%v closeErr=%v", start, end, len(got), readErr, closeErr)
		}
	}
}

type backendCopySetup struct {
	client               *s3.Client
	bucket               string
	sourceKey, targetKey string
	body                 []byte
	metadata             map[string]string
	tagged               bool
}

func prepareBackendCopy(t *testing.T, setup backendCopySetup) string {
	t.Helper()
	input := &s3.PutObjectInput{
		Bucket: aws.String(setup.bucket), Key: aws.String(setup.sourceKey),
		Body: bytes.NewReader(setup.body), Metadata: setup.metadata,
	}
	if setup.tagged {
		input.Tagging = aws.String(backendTaggingQuery())
	}
	putOut, err := setup.client.PutObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	sourceVersionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, sourceVersionID)
	copyOut, err := setup.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String(setup.bucket), Key: aws.String(setup.targetKey),
		CopySource: aws.String(backendCopySource(setup.bucket, setup.sourceKey, sourceVersionID)),
	})
	if err != nil {
		t.Fatal(err)
	}
	versionID := aws.ToString(copyOut.VersionId)
	requireVersionID(t, versionID)
	return versionID
}

func prepareBackendCopyReplace(t *testing.T, setup backendCopySetup, replacement map[string]string) string {
	t.Helper()
	putOut, err := setup.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(setup.bucket), Key: aws.String(setup.sourceKey),
		Body: bytes.NewReader(setup.body), Metadata: setup.metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceVersionID := aws.ToString(putOut.VersionId)
	requireVersionID(t, sourceVersionID)
	copyOut, err := setup.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String(setup.bucket), Key: aws.String(setup.targetKey),
		CopySource: aws.String(backendCopySource(setup.bucket, setup.sourceKey, sourceVersionID)),
		Metadata:   replacement, MetadataDirective: types.MetadataDirectiveReplace,
	})
	if err != nil {
		t.Fatal(err)
	}
	versionID := aws.ToString(copyOut.VersionId)
	requireVersionID(t, versionID)
	return versionID
}

func backendLockBucket(t *testing.T, s *suite) string {
	t.Helper()
	bucket := newBucketName(s.cfg.BucketPrefix)
	input := createBucketInput(s.cfg, bucket)
	input.ObjectLockEnabledForBucket = aws.Bool(true)
	if _, err := s.client.CreateBucket(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			cleanupBucket(t, s, bucket)
		}
	})
	enableVersioning(t, s, bucket)
	return bucket
}

func listBackendVersions(t *testing.T, client *s3.Client, bucket string) *s3.ListObjectVersionsOutput {
	t.Helper()
	out, err := client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func getBackendTags(t *testing.T, client *s3.Client, bucket, key, versionID string) []types.Tag {
	t.Helper()
	input := &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := client.GetObjectTagging(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return append([]types.Tag(nil), out.TagSet...)
}

func backendTags() []types.Tag {
	return []types.Tag{{Key: aws.String("testKey"), Value: aws.String("testValue")}}
}

func backendTaggingQuery() string {
	return encodeBackendTags(backendTags())
}

func encodeBackendTags(tags []types.Tag) string {
	values := url.Values{}
	for _, tag := range tags {
		values.Add(aws.ToString(tag.Key), aws.ToString(tag.Value))
	}
	return values.Encode()
}

func sortBackendTags(tags []types.Tag) {
	sort.Slice(tags, func(i, j int) bool {
		if aws.ToString(tags[i].Key) == aws.ToString(tags[j].Key) {
			return aws.ToString(tags[i].Value) < aws.ToString(tags[j].Value)
		}
		return aws.ToString(tags[i].Key) < aws.ToString(tags[j].Key)
	})
}

func backendCopySource(bucket, key, versionID string) string {
	source := url.PathEscape(bucket + "/" + key)
	if versionID != "" {
		source += "?versionId=" + url.QueryEscape(versionID)
	}
	return source
}

func backendPayload(size int) []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	body := make([]byte, size)
	for i := range body {
		body[i] = alphabet[i%len(alphabet)]
	}
	return body
}

func requireVersionID(t *testing.T, versionID string) {
	t.Helper()
	if versionID == "" || versionID == "null" {
		t.Fatalf("invalid VersionId %q", versionID)
	}
}

func equalBackendMetadata(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}

func cleanupBucket(t *testing.T, s *suite, bucket string) {
	t.Helper()
	ctx := context.Background()
	listed, _ := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	var objects []types.ObjectIdentifier
	if listed != nil {
		for _, object := range listed.Versions {
			objects = append(objects, types.ObjectIdentifier{Key: object.Key, VersionId: object.VersionId})
		}
		for _, marker := range listed.DeleteMarkers {
			objects = append(objects, types.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
		}
	}
	if len(objects) != 0 {
		_, _ = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), BypassGovernanceRetention: aws.Bool(true), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)}})
	}
	_, _ = s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
