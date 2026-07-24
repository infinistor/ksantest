package s3tests

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"
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
)

func TestBackend(t *testing.T) {
	t.Parallel()
	// Java wrapper(java/src/test/.../Backend.java)와 동일하게 basic 테스트는 비활성.
	// "test_put_object", "test_get_object", "test_delete_object", "test_copy_object", "test_multipart_upload",
	// "test_put_object_acl", "test_get_object_acl", "test_put_object_tagging", "test_get_object_tagging", "test_delete_object_tagging",
	run := func(fn func(*testing.T, *suite, *s3.Client)) func(*testing.T) {
		return func(t *testing.T) {
			s := newSuite(t)
			if s.cfg.Endpoint() == "" {
				t.Skip("Backend API is unavailable on AWS")
			}
			fn(t, s, newBackendClient(t, s))
		}
	}
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// [Versioning] PutObject가 정상 동작하는지 확인
		{"test_put_object_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_put_object_versioning")
		})},
		// [Versioning] PutObject 버전 정보 추가시 정상 동작 확인
		{"test_put_object_versioning_with_version_id", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_put_object_versioning_with_version_id")
		})},
		// [Versioning] GetObject가 정상 동작하는지 확인
		{"test_get_object_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_get_object_versioning")
		})},
		// [Versioning] DeleteObject가 정상 동작하는지 확인
		{"test_delete_object_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_delete_object_versioning")
		})},
		// [Versioning] DeleteObjects가 정상 동작하는지 확인
		{"test_delete_objects_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_delete_objects_versioning")
		})},
		// [Versioning] HeadObject가 정상 동작하는지 확인
		{"test_head_object_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_head_object_versioning")
		})},
		// [Versioning] CopyObject가 정상 동작하는지 확인
		{"test_copy_object_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_copy_object_versioning")
		})},
		// [Versioning] MultipartUpload가 정상 동작하는지 확인
		{"test_multipart_upload_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_multipart_upload_versioning")
		})},
		// [Versioning] PutObjectAcl가 정상 동작하는지 확인
		{"test_put_object_acl_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_put_object_acl_versioning")
		})},
		// [Versioning] GetObjectAcl가 정상 동작하는지 확인
		{"test_get_object_acl_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_get_object_acl_versioning")
		})},
		// [Versioning] PutObjectTagging가 정상 동작하는지 확인
		{"test_put_object_tagging_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_put_object_tagging_versioning")
		})},
		// [Versioning] GetObjectTagging가 정상 동작하는지 확인
		{"test_get_object_tagging_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_get_object_tagging_versioning")
		})},
		// [Versioning] DeleteObjectTagging가 정상 동작하는지 확인
		{"test_delete_object_tagging_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_delete_object_tagging_versioning")
		})},
		// [Versioning] PutObjectRetention가 정상 동작하는지 확인
		{"test_put_object_retention_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_put_object_retention_versioning")
		})},
		// [Versioning] GetObjectRetention가 정상 동작하는지 확인
		{"test_get_object_retention_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_get_object_retention_versioning")
		})},
		// [Versioning] PutObjectRetention 후 GetObjectRetention으로 조회가 정상 동작하는지 확인
		{"test_put_and_get_object_retention_versioning", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendVersioning(t, s, backend, "test_put_and_get_object_retention_versioning")
		})},
		// PutObject 복제가 정상 동작하는지 확인
		{"test_put_object_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_put_object_replication")
		})},
		// PutObject 태그가 복제되는지 확인
		{"test_put_object_with_tagging_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_put_object_with_tagging_replication")
		})},
		// PutObject 헤더와 메타데이터가 복제되는지 확인
		{"test_put_object_with_metadata_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_put_object_with_metadata_replication")
		})},
		// CopyObject 복제가 정상 동작하는지 확인
		{"test_copy_object_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_copy_object_replication")
		})},
		// CopyObject 태그가 복제되는지 확인
		{"test_copy_object_with_tagging_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_copy_object_with_tagging_replication")
		})},
		// CopyObject 헤더와 메타데이터가 복제되는지 확인
		{"test_copy_object_with_metadata_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_copy_object_with_metadata_replication")
		})},
		// CopyObject 메타데이터가 Replace되었을 경우 복제되는지 확인
		{"test_copy_object_metadata_replace_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_copy_object_metadata_replace_replication")
		})},
		// MultipartUpload 복제가 정상 동작하는지 확인
		{"test_multipart_upload_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_multipart_upload_replication")
		})},
		// MultipartUpload 태그가 복제되는지 확인
		{"test_multipart_upload_with_tagging_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_multipart_upload_with_tagging_replication")
		})},
		// MultipartUpload 헤더와 메타데이터가 복제되는지 확인
		{"test_multipart_upload_with_metadata_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_multipart_upload_with_metadata_replication")
		})},
		// PutObjectAcl 복제가 정상 동작하는지 확인
		{"test_put_object_acl_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_put_object_acl_replication")
		})},
		// putObjectTagging 복제가 정상 동작하는지 확인
		{"test_put_object_tagging_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_put_object_tagging_replication")
		})},
		// deleteObject 복제가 정상 동작하는지 확인
		{"test_delete_object_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_delete_object_replication")
		})},
		// deleteObjectTagging 복제가 정상 동작하는지 확인
		{"test_delete_object_tagging_replication", run(func(t *testing.T, s *suite, backend *s3.Client) {
			runBackendReplication(t, s, backend, "test_delete_object_tagging_replication")
		})},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
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
	applyCompatibleS3Options(&options)
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

func runBackendVersioning(t *testing.T, s *suite, backend *s3.Client, name string) {
	t.Helper()
	ctx := context.Background()
	if strings.Contains(name, "retention") {
		runBackendRetention(t, s, backend, name)
		return
	}
	bucket, key, body := s.bucket(t), name, "test content"
	enableVersioning(t, s, bucket)
	source := put(t, s, bucket, key, body, nil)
	versionID := aws.ToString(source.VersionId)
	if versionID == "" {
		t.Fatal("missing source VersionId")
	}
	switch name {
	case "test_put_object_versioning", "test_put_object_versioning_with_version_id":
		mustBackendPut(t, backend, bucket, key+"-target", body, nil, versionID)
		assertBackendBody(t, s.client, bucket, key+"-target", body, versionID)
	case "test_get_object_versioning":
		assertBackendBody(t, backend, bucket, key, body, versionID)
	case "test_delete_object_versioning":
		if _, err := backend.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)}); err != nil {
			t.Fatal(err)
		}
		_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
		assertHTTPError(t, err, 404)
	case "test_delete_objects_versioning":
		_, err := backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String(key), VersionId: aws.String(versionID)}}}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
		assertHTTPError(t, err, 404)
	case "test_head_object_versioning":
		head, err := backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
		if err != nil || aws.ToInt64(head.ContentLength) != int64(len(body)) {
			t.Fatalf("head=%v err=%v", head.ContentLength, err)
		}
	case "test_copy_object_versioning":
		_, err := backend.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String(key + "-target"), CopySource: aws.String(url.PathEscape(bucket + "/" + key + "?versionId=" + versionID))}, versionOption(versionID))
		if err != nil {
			t.Fatal(err)
		}
		assertBackendBody(t, s.client, bucket, key+"-target", body, versionID)
	case "test_multipart_upload_versioning":
		backendMultipart(t, backend, bucket, key+"-target", []byte(body), versionID)
		assertBackendBody(t, s.client, bucket, key+"-target", body, versionID)
	case "test_put_object_acl_versioning":
		_, err := backend.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID), ACL: types.ObjectCannedACLPublicRead})
		if err != nil {
			t.Fatal(err)
		}
		assertBackendACL(t, s.client, bucket, key, versionID, 2)
	case "test_get_object_acl_versioning":
		assertBackendACL(t, backend, bucket, key, versionID, 1)
	case "test_put_object_tagging_versioning":
		backendPutTag(t, backend, bucket, key, versionID)
		assertBackendTag(t, s.client, bucket, key, versionID, 1)
	case "test_get_object_tagging_versioning":
		backendPutTag(t, s.client, bucket, key, versionID)
		assertBackendTag(t, backend, bucket, key, versionID, 1)
	case "test_delete_object_tagging_versioning":
		backendPutTag(t, s.client, bucket, key, versionID)
		if _, err := backend.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)}); err != nil {
			t.Fatal(err)
		}
		assertBackendTag(t, s.client, bucket, key, versionID, 0)
	}
}

func runBackendRetention(t *testing.T, s *suite, backend *s3.Client, name string) {
	t.Helper()
	ctx := context.Background()
	bucket := newBucketName(s.cfg.BucketPrefix)
	input := createBucketInput(s.cfg, bucket)
	input.ObjectLockEnabledForBucket = aws.Bool(true)
	_, err := s.client.CreateBucket(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			cleanupBucket(t, s, bucket)
		}
	})
	key := name
	putOut := put(t, s, bucket, key, "retained", nil)
	versionID, until := aws.ToString(putOut.VersionId), time.Now().Add(24*time.Hour)
	retention := &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance, RetainUntilDate: &until}
	if name != "test_get_object_retention_versioning" {
		if _, err := backend.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID), Retention: retention}); err != nil {
			t.Fatal(err)
		}
	} else if _, err := s.client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID), Retention: retention}); err != nil {
		t.Fatal(err)
	}
	if name != "test_put_object_retention_versioning" {
		out, err := backend.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
		if err != nil || out.Retention == nil || out.Retention.Mode != types.ObjectLockRetentionModeGovernance {
			t.Fatalf("retention=%#v err=%v", out.Retention, err)
		}
	}
}

func runBackendReplication(t *testing.T, s *suite, backend *s3.Client, name string) {
	t.Helper()
	ctx := context.Background()
	bucket, sourceKey, targetKey := s.bucket(t), name+"-source", name+"-target"
	enableVersioning(t, s, bucket)
	metadata := map[string]string(nil)
	if strings.Contains(name, "metadata") {
		metadata = map[string]string{"testkey": "testValue"}
	}
	putOut := put(t, s, bucket, sourceKey, "test content", metadata)
	versionID := aws.ToString(putOut.VersionId)
	if strings.Contains(name, "tagging") {
		backendPutTag(t, s.client, bucket, sourceKey, versionID)
	}

	switch {
	case name == "test_delete_object_replication":
		backendReplicatePut(t, backend, bucket, sourceKey, targetKey, versionID)
		deleted, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(sourceKey)})
		if err != nil {
			t.Fatal(err)
		}
		markerID := aws.ToString(deleted.VersionId)
		if _, err := backend.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), Body: bytes.NewReader(nil)}, deleteMarkerOption(markerID)); err != nil {
			t.Fatal(err)
		}
		versions, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		if err != nil || len(versions.DeleteMarkers) != 2 {
			t.Fatalf("delete markers=%v err=%v", versions.DeleteMarkers, err)
		}
	case name == "test_delete_object_tagging_replication":
		backendReplicatePut(t, backend, bucket, sourceKey, targetKey, versionID)
		backendPutTag(t, backend, bucket, targetKey, versionID)
		if _, err := backend.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), VersionId: aws.String(versionID)}); err != nil {
			t.Fatal(err)
		}
		assertBackendTag(t, s.client, bucket, sourceKey, versionID, 1)
		assertBackendTag(t, s.client, bucket, targetKey, versionID, 0)
	case strings.Contains(name, "multipart"):
		backendMultipart(t, backend, bucket, targetKey, []byte("test content"), versionID)
		if strings.Contains(name, "tagging") {
			backendPutTag(t, backend, bucket, targetKey, versionID)
			assertBackendTag(t, s.client, bucket, targetKey, versionID, 1)
		}
		assertBackendBody(t, s.client, bucket, targetKey, "test content", versionID)
	case strings.Contains(name, "copy_object"):
		copyMetadata := types.MetadataDirectiveCopy
		copyInput := &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), CopySource: aws.String(url.PathEscape(bucket + "/" + sourceKey + "?versionId=" + versionID)), MetadataDirective: copyMetadata}
		if strings.Contains(name, "replace") {
			copyInput.MetadataDirective = types.MetadataDirectiveReplace
			copyInput.Metadata = map[string]string{"testkey2": "testValue2"}
		}
		if _, err := backend.CopyObject(ctx, copyInput, versionOption(versionID)); err != nil {
			t.Fatal(err)
		}
		assertBackendBody(t, s.client, bucket, targetKey, "test content", versionID)
		if strings.Contains(name, "tagging") {
			backendPutTag(t, backend, bucket, targetKey, versionID)
			assertBackendTag(t, s.client, bucket, targetKey, versionID, 1)
		}
	case name == "test_put_object_acl_replication":
		backendReplicatePut(t, backend, bucket, sourceKey, targetKey, versionID)
		if _, err := backend.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), VersionId: aws.String(versionID), ACL: types.ObjectCannedACLPublicRead}); err != nil {
			t.Fatal(err)
		}
		assertBackendACL(t, s.client, bucket, targetKey, versionID, 2)
	default:
		backendReplicatePut(t, backend, bucket, sourceKey, targetKey, versionID)
		assertBackendBody(t, s.client, bucket, targetKey, "test content", versionID)
		if strings.Contains(name, "tagging") {
			backendPutTag(t, backend, bucket, targetKey, versionID)
			assertBackendTag(t, s.client, bucket, targetKey, versionID, 1)
		}
	}
}

func mustBackendPut(t *testing.T, client *s3.Client, bucket, key, body string, metadata map[string]string, versionID string) {
	t.Helper()
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(body)), Metadata: metadata}
	var err error
	if versionID == "" {
		_, err = client.PutObject(context.Background(), input)
	} else {
		_, err = client.PutObject(context.Background(), input, versionOption(versionID))
	}
	if err != nil {
		t.Fatal(err)
	}
}

func backendReplicatePut(t *testing.T, client *s3.Client, bucket, sourceKey, targetKey, versionID string) {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(sourceKey), VersionId: aws.String(versionID)})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	mustBackendPut(t, client, bucket, targetKey, string(body), out.Metadata, versionID)
}

func backendMultipart(t *testing.T, client *s3.Client, bucket, key string, body []byte, versionID string) {
	t.Helper()
	ctx := context.Background()
	created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	part, err := client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}}
	if versionID == "" {
		_, err = client.CompleteMultipartUpload(ctx, input)
	} else {
		_, err = client.CompleteMultipartUpload(ctx, input, versionOption(versionID))
	}
	if err != nil {
		t.Fatal(err)
	}
}

func backendPutTag(t *testing.T, client *s3.Client, bucket, key, versionID string) {
	t.Helper()
	input := &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("testKey"), Value: aws.String("testValue")}}}}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	if _, err := client.PutObjectTagging(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

func assertBackendBody(t *testing.T, client *s3.Client, bucket, key, want, versionID string) {
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
	if err != nil || string(body) != want || (versionID != "" && aws.ToString(out.VersionId) != versionID) {
		t.Fatalf("body=%q version=%q err=%v", body, aws.ToString(out.VersionId), err)
	}
}

func assertBackendTag(t *testing.T, client *s3.Client, bucket, key, versionID string, count int) {
	t.Helper()
	input := &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := client.GetObjectTagging(context.Background(), input)
	if err != nil || len(out.TagSet) != count {
		t.Fatalf("tags=%v err=%v", out.TagSet, err)
	}
}

func assertBackendACL(t *testing.T, client *s3.Client, bucket, key, versionID string, minimum int) {
	t.Helper()
	input := &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := client.GetObjectAcl(context.Background(), input)
	if err != nil || len(out.Grants) < minimum {
		t.Fatalf("grants=%v err=%v", out.Grants, err)
	}
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
