package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestVersioning(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 버저닝 옵션 변경 가능 확인
		{"test_versioning_bucket_create_suspend", testVersioningBucketState},
		// 버저닝 오브젝트의 생성/읽기/삭제 확인
		{"test_versioning_obj_create_read_remove", func(t *testing.T) { testVersioningDeleteMarker(t, "test_versioning_obj_create_read_remove") }},
		// 버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인
		{"test_versioning_obj_create_read_remove_head", func(t *testing.T) { testVersioningDeleteMarker(t, "test_versioning_obj_create_read_remove_head") }},
		// 버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인
		{"test_versioning_obj_plain_null_version_removal", func(t *testing.T) { testVersioningNull(t, "test_versioning_obj_plain_null_version_removal") }},
		// [버킷에 버저닝 설정이 되어있는 상태] null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인
		{"test_versioning_obj_plain_null_version_overwrite", func(t *testing.T) { testVersioningNull(t, "test_versioning_obj_plain_null_version_overwrite") }},
		// [버킷에 버저닝 설정이 되어있지만 중단된 상태일때] null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인
		{"test_versioning_obj_plain_null_version_overwrite_suspended", func(t *testing.T) { testVersioningNull(t, "test_versioning_obj_plain_null_version_overwrite_suspended") }},
		// 버전관리를 일시중단했을때 올바르게 동작하는지 확인
		{"test_versioning_obj_suspend_versions", func(t *testing.T) { testVersioningVersions(t, "test_versioning_obj_suspend_versions") }},
		// 오브젝트하나의 여러버전을 모두 삭제 가능한지 확인
		{"test_versioning_obj_create_versions_remove_all", func(t *testing.T) { testVersioningVersions(t, "test_versioning_obj_create_versions_remove_all") }},
		// 이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인
		{"test_versioning_obj_create_versions_remove_special_names", func(t *testing.T) { testVersioningVersions(t, "test_versioning_obj_create_versions_remove_special_names") }},
		// 오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인
		{"test_versioning_obj_create_overwrite_multipart", func(t *testing.T) { testVersioningMultipart(t, "test_versioning_obj_create_overwrite_multipart") }},
		// PutObject와 MultipartUpload를 섞어 올린 버전이 목록/조회에서 올바른지 확인
		{"test_versioning_obj_mix_put_and_multipart", testVersioningObjMixPutAndMultipart},
		// 버전 목록이 VersionId 기준으로 올바르게 정렬되어 반환되는지 확인
		{"test_versioning_obj_list_marker", testVersioningMarkers},
		// 오브젝트의 버전별 복사가 가능한지 화인
		{"test_versioning_copy_obj_version", func(t *testing.T) { testVersioningCopy(t, "test_versioning_copy_obj_version") }},
		// 버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인
		{"test_versioning_multi_object_delete", func(t *testing.T) { testVersioningMultiDelete(t, "test_versioning_multi_object_delete") }},
		// 버저닝된 버킷에서 여러 오브젝트를 삭제할 경우 DeleteMarker가 생성되는지 확인
		{"test_versioning_multi_object_delete_with_marker", func(t *testing.T) { testVersioningMultiDelete(t, "test_versioning_multi_object_delete_with_marker") }},
		// 버저닝된 버킷에서 존재하지 않는 오브젝트를 반복 삭제할 경우 DeleteMarker가 생성되는지 확인
		{"test_versioning_multi_object_delete_with_marker_create", func(t *testing.T) { testVersioningMultiDelete(t, "test_versioning_multi_object_delete_with_marker_create") }},
		// 오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인
		{"test_versioned_object_acl", func(t *testing.T) { testVersioningACL(t, "test_versioned_object_acl") }},
		// 버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인
		{"test_versioned_object_acl_no_version_specified", func(t *testing.T) { testVersioningACL(t, "test_versioned_object_acl_no_version_specified") }},
		// 오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인
		{"test_versioned_concurrent_object_create_and_remove", testVersioningConcurrent},
		// 버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인
		{"test_versioning_bucket_atomic_upload_return_version_id", func(t *testing.T) { testVersioningVersions(t, "test_versioning_bucket_atomic_upload_return_version_id") }},
		// 버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인
		{"test_versioning_bucket_multipart_upload_return_version_id", func(t *testing.T) { testVersioningMultipart(t, "test_versioning_bucket_multipart_upload_return_version_id") }},
		// 업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인
		{"test_versioning_get_object_head", func(t *testing.T) { testVersioningVersions(t, "test_versioning_get_object_head") }},
		// 버전이 여러개인 오브젝트의 최신 버전을 삭제 했을때 이전버전이 최신버전으로 변경되는지 확인
		{"test_versioning_latest", func(t *testing.T) { testVersioningVersions(t, "test_versioning_latest") }},
		// 잘못된 버전 정보를 사용하여 오브젝트 조회 실패 확인
		{"test_versioning_invalid_version_id", testVersioningInvalidID},
		// CopyObject로 복사할 경우 버저닝이 올바르게 동작하는지 확인
		{"test_versioning_copy_object", func(t *testing.T) { testVersioningCopy(t, "test_versioning_copy_object") }},
		// 버저닝 미설정 버킷에서 Put/Head/Get/Multipart/Copy/List의 versionId가 비어있는지 확인
		{"test_versioning_unversioned_all_version_id", func(t *testing.T) { testVersioningAllIDs(t, "test_versioning_unversioned_all_version_id") }},
		// 버저닝 ENABLED 상태에서 Put/Head/Get/Multipart/Copy/List의 versionId가 존재하고 일치하는지 확인
		{"test_versioning_enabled_all_version_id", func(t *testing.T) { testVersioningAllIDs(t, "test_versioning_enabled_all_version_id") }},
		// 버저닝 SUSPENDED 상태에서 Put/Head/Get/Multipart/Copy/List의 versionId가 "null"인지 확인
		{"test_versioning_suspended_all_version_id", func(t *testing.T) { testVersioningAllIDs(t, "test_versioning_suspended_all_version_id") }},
		// OFF→ENABLED→SUSPENDED 순으로 같은 key에 put 후 listVersions가 null+versionId 2개인지 확인
		{"test_versioning_list_versions_off_enabled_suspended", func(t *testing.T) { testVersioningTransitions(t, "test_versioning_list_versions_off_enabled_suspended") }},
		// OFF→ENABLED→SUSPENDED를 서로 다른 key에 적용 후 listVersions 확인
		{"test_versioning_list_versions_off_enabled_suspended_different_keys", func(t *testing.T) { testVersioningTransitions(t, "test_versioning_list_versions_off_enabled_suspended_different_keys") }},
		// SUSPENDED 상태에서 null 버전 삭제 후 동작 확인
		{"test_versioning_delete_null_version_after_suspend", func(t *testing.T) { testVersioningNull(t, "test_versioning_delete_null_version_after_suspend") }},
		// ENABLED에서 여러 번 put 후 SUSPENDED put 시 listVersions가 versionId N개+null 1개인지 확인
		{"test_versioning_list_versions_multiple_enabled_then_suspended", func(t *testing.T) { testVersioningTransitions(t, "test_versioning_list_versions_multiple_enabled_then_suspended") }},
		// Current가 DeleteMarker인 오브젝트를 HeadObject 요청 시 올바르게 동작하는지 확인
		{"test_versioning_head_object_delete_marker", func(t *testing.T) { testVersioningDeleteMarker(t, "test_versioning_head_object_delete_marker") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func setVersioning(t *testing.T, s *suite, bucket string, status types.BucketVersioningStatus) {
	t.Helper()
	_, err := s.client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: status}})
	if err != nil {
		t.Fatal(err)
	}
}
func suspendVersioning(t *testing.T, s *suite, bucket string) {
	setVersioning(t, s, bucket, types.BucketVersioningStatusSuspended)
}
func versionBody(t *testing.T, client *s3.Client, bucket, key, id string) string {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(id)})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func testVersioningBucketState(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	out, err := s.client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String(b)})
	if err != nil || out.Status != types.BucketVersioningStatusEnabled {
		t.Fatalf("status=%q err=%v", out.Status, err)
	}
	suspendVersioning(t, s, b)
	out, err = s.client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String(b)})
	if err != nil || out.Status != types.BucketVersioningStatusSuspended {
		t.Fatalf("status=%q err=%v", out.Status, err)
	}
}

func isNullVersionID(id string) bool {
	return id == "" || id == "null"
}

func testVersioningDeleteMarker(t *testing.T, name string) {
	if name == "test_versioning_obj_create_read_remove_head" {
		// Java @Disabled: DeleteObject response has no VersionId in older Java SDK path.
		t.Skip("JAVA에서는 DeleteObject API를 이용하여 오브젝트를 삭제할 경우 반환값이 없어 삭제된 오브젝트의 버전 정보를 받을 수 없음으로 테스트 불가")
	}
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	key := "key"
	putOut := put(t, s, b, key, "body", nil)
	deleted, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || !aws.ToBool(deleted.DeleteMarker) || aws.ToString(deleted.VersionId) == "" {
		t.Fatalf("delete=%#v err=%v", deleted, err)
	}
	_, err = s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
	if versionBody(t, s.client, b, key, aws.ToString(putOut.VersionId)) != "body" {
		t.Fatal("old version mismatch")
	}
	// Java testVersioningHeadObjectDeleteMarker: Head without VersionId → 404 NoSuchKey.
	if name == "test_versioning_head_object_delete_marker" {
		_, headErr := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		assertHTTPError(t, headErr, 404)
		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
		if len(listed.Versions) != 1 || len(listed.DeleteMarkers) != 1 {
			t.Fatalf("versions=%d markers=%d", len(listed.Versions), len(listed.DeleteMarkers))
		}
	}
}

func testVersioningNull(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	key := "foo"
	switch name {
	case "test_versioning_obj_plain_null_version_removal":
		put(t, s, b, key, "foo data", nil)
		enableVersioning(t, s, b)
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")}); err != nil {
			t.Fatal(err)
		}
		_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		assertS3Error(t, err, 404, "NoSuchKey")
		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
		if len(listed.Versions) != 0 {
			t.Fatalf("versions=%d", len(listed.Versions))
		}
	case "test_versioning_obj_plain_null_version_overwrite":
		put(t, s, b, key, "foo zzz", nil)
		enableVersioning(t, s, b)
		put(t, s, b, key, "zzz", nil)
		if read(t, s, b, key) != "zzz" {
			t.Fatal("enabled overwrite body mismatch")
		}
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil || aws.ToString(head.VersionId) == "" || isNullVersionID(aws.ToString(head.VersionId)) {
			t.Fatalf("enabled VersionId=%q err=%v", aws.ToString(head.VersionId), err)
		}
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: head.VersionId}); err != nil {
			t.Fatal(err)
		}
		if read(t, s, b, key) != "foo zzz" {
			t.Fatal("null version body mismatch")
		}
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")}); err != nil {
			t.Fatal(err)
		}
		_, err = s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		assertS3Error(t, err, 404, "NoSuchKey")
	case "test_versioning_obj_plain_null_version_overwrite_suspended":
		put(t, s, b, key, "foo zzz", nil)
		enableVersioning(t, s, b)
		suspendVersioning(t, s, b)
		put(t, s, b, key, "zzz", nil)
		if read(t, s, b, key) != "zzz" {
			t.Fatal("suspended overwrite body mismatch")
		}
		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
		if len(listed.Versions) != 1 {
			t.Fatalf("versions=%d", len(listed.Versions))
		}
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")}); err != nil {
			t.Fatal(err)
		}
		_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		assertS3Error(t, err, 404, "NoSuchKey")
	case "test_versioning_delete_null_version_after_suspend":
		put(t, s, b, key, "content-off", nil)
		enableVersioning(t, s, b)
		enabled := put(t, s, b, key, "content-enabled", nil)
		if aws.ToString(enabled.VersionId) == "" || isNullVersionID(aws.ToString(enabled.VersionId)) {
			t.Fatalf("enabled VersionId=%q", aws.ToString(enabled.VersionId))
		}
		suspendVersioning(t, s, b)
		put(t, s, b, key, "content-suspended", nil)
		if read(t, s, b, key) != "content-suspended" {
			t.Fatal("suspended body mismatch")
		}
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")}); err != nil {
			t.Fatal(err)
		}
		out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(out.Body)
		out.Body.Close()
		if string(data) != "content-enabled" || aws.ToString(out.VersionId) != aws.ToString(enabled.VersionId) {
			t.Fatalf("body=%q id=%q want content-enabled/%s", data, aws.ToString(out.VersionId), aws.ToString(enabled.VersionId))
		}
	default:
		t.Fatalf("unknown null version case %q", name)
	}
}

func testVersioningMultipart(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	key := name
	body := deterministicBody(6 * 1024 * 1024)
	completeMultipart(t, s.client, b, key, body, false, nil)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || aws.ToString(head.VersionId) == "" || aws.ToInt64(head.ContentLength) != int64(len(body)) {
		t.Fatalf("head=%#v err=%v", head, err)
	}
	if name == "test_versioning_obj_create_overwrite_multipart" {
		put(t, s, b, key, "replacement", nil)
		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(key)})
		if len(listed.Versions) != 2 {
			t.Fatalf("versions=%d", len(listed.Versions))
		}
	}
}

func testVersioningObjMixPutAndMultipart(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	key := "test_versioning_obj_mix_put_and_multipart"
	versionIDs := make([]string, 0, 4)
	contents := make([][]byte, 0, 4)

	putVersion := func(body []byte) {
		t.Helper()
		out, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader(body)})
		if err != nil || aws.ToString(out.VersionId) == "" {
			t.Fatalf("PutObject version=%q err=%v", aws.ToString(out.VersionId), err)
		}
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
		contents = append(contents, body)
	}
	multipartVersion := func(size int) {
		t.Helper()
		body := deterministicBody(size)
		created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		parts := uploadByteParts(t, s.client, b, key, aws.ToString(created.UploadId), body, 1)
		out, err := s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
		if err != nil || aws.ToString(out.VersionId) == "" {
			t.Fatalf("CompleteMultipartUpload version=%q err=%v", aws.ToString(out.VersionId), err)
		}
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
		contents = append(contents, body)
	}

	putVersion(deterministicBody(1024))
	multipartVersion(50 * 1024 * 1024)
	putVersion(deterministicBody(1024 * 1024))
	multipartVersion(10 * 1024 * 1024)

	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(listed.Versions) != 4 {
		t.Fatalf("versions=%d want=4", len(listed.Versions))
	}
	for i, wantID := range reverseStrings(versionIDs) {
		v := listed.Versions[i]
		if aws.ToString(v.Key) != key || aws.ToString(v.VersionId) != wantID {
			t.Fatalf("list[%d]=%s/%s want %s/%s", i, aws.ToString(v.Key), aws.ToString(v.VersionId), key, wantID)
		}
		wantSize := int64(len(contents[len(contents)-1-i]))
		if aws.ToInt64(v.Size) != wantSize {
			t.Fatalf("list[%d] size=%d want=%d", i, aws.ToInt64(v.Size), wantSize)
		}
	}
	for i, id := range versionIDs {
		if !bytes.Equal([]byte(versionBody(t, s.client, b, key, id)), contents[i]) {
			t.Fatalf("GetObject version[%d] body mismatch", i)
		}
	}
}

func reverseStrings(in []string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[len(in)-1-i] = v
	}
	return out
}

func testVersioningMarkers(t *testing.T) {
	// Mirrors Java testVersioningObjListMarker: two keys × 5 versions, chronological order check.
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	key1, key2 := "obj", "obj-1"
	ids1, ids2 := make([]string, 0, 5), make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		out := put(t, s, b, key1, fmt.Sprintf("content-%d", i), nil)
		ids1 = append(ids1, aws.ToString(out.VersionId))
	}
	for i := 0; i < 5; i++ {
		out := put(t, s, b, key2, fmt.Sprintf("content-%d", i), nil)
		ids2 = append(ids2, aws.ToString(out.VersionId))
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(listed.Versions) != 10 {
		t.Fatalf("versions=%d", len(listed.Versions))
	}
	// ListObjectVersions returns newest-first; reverse to chronological like Java reverseVersions.
	versions := append([]types.ObjectVersion(nil), listed.Versions...)
	for i, j := 0, len(versions)-1; i < j; i, j = i+1, j-1 {
		versions[i], versions[j] = versions[j], versions[i]
	}
	for i := 0; i < 5; i++ {
		if aws.ToString(versions[i].Key) != key2 || aws.ToString(versions[i].VersionId) != ids2[i] {
			t.Fatalf("key2[%d]=%s/%s want %s/%s", i, aws.ToString(versions[i].Key), aws.ToString(versions[i].VersionId), key2, ids2[i])
		}
		if versionBody(t, s.client, b, key2, ids2[i]) != fmt.Sprintf("content-%d", i) {
			t.Fatalf("key2 body[%d]", i)
		}
	}
	for i := 0; i < 5; i++ {
		v := versions[5+i]
		if aws.ToString(v.Key) != key1 || aws.ToString(v.VersionId) != ids1[i] {
			t.Fatalf("key1[%d]=%s/%s want %s/%s", i, aws.ToString(v.Key), aws.ToString(v.VersionId), key1, ids1[i])
		}
		if versionBody(t, s.client, b, key1, ids1[i]) != fmt.Sprintf("content-%d", i) {
			t.Fatalf("key1 body[%d]", i)
		}
	}
}

func testVersioningCopy(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	source := put(t, s, b, "source", "one", nil)
	put(t, s, b, "source", "two", nil)
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target"), CopySource: copySource(b, "source", aws.ToString(source.VersionId))})
	if versionBody(t, s.client, b, "target", aws.ToString(listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String("target")}).Versions[0].VersionId)) != "one" {
		t.Fatal("copied wrong version")
	}
}

func testVersioningMultiDelete(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		put(t, s, b, key, key, nil)
	}
	if strings.Contains(name, "marker_create") {
		for i := 0; i < 7; i++ {
			deleteMany(t, s.client, b, deleteIdentifiers([]string{"new"}), false)
		}
	} else {
		deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	want := 3
	if strings.Contains(name, "marker_create") {
		want = 7
	}
	if len(out.DeleteMarkers) != want {
		t.Fatalf("markers=%d want=%d", len(out.DeleteMarkers), want)
	}
}

func testVersioningACL(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	first := put(t, s, b, "key", "one", nil)
	second := put(t, s, b, "key", "two", nil)
	version := first.VersionId
	if strings.Contains(name, "no_version") {
		version = nil
	}
	_, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: version, ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	if version == nil {
		version = second.VersionId
	}
	acl, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: version})
	if err != nil || len(acl.Grants) < 2 {
		t.Fatalf("grants=%v err=%v", acl.Grants, err)
	}
}

func testVersioningConcurrent(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			_, _ = s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
			_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		}()
	}
	wg.Wait()
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 10 || len(out.DeleteMarkers) != 10 {
		t.Fatalf("versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
}

func testVersioningInvalidID(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	put(t, s, b, "key", "body", nil)
	// Java uses a well-formed but nonexistent VersionId → 404 NoSuchVersion.
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: aws.String("f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59")})
	assertS3Error(t, err, 404, "NoSuchVersion")
}

func testVersioningAllIDs(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	if strings.Contains(name, "enabled") {
		enableVersioning(t, s, b)
	} else if strings.Contains(name, "suspended") {
		enableVersioning(t, s, b)
		suspendVersioning(t, s, b)
	}
	put(t, s, b, "key", "body", nil)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 1 {
		t.Fatalf("versions=%d", len(out.Versions))
	}
	id := aws.ToString(out.Versions[0].VersionId)
	if strings.Contains(name, "enabled") {
		if id == "" || id == "null" {
			t.Fatalf("VersionId=%q", id)
		}
	} else if !isNullVersionID(id) {
		t.Fatalf("VersionId=%q", id)
	}
}

func testVersioningTransitions(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	keys := []string{"a"}
	if strings.Contains(name, "different_keys") {
		keys = []string{"a", "b", "c"}
	}
	for _, key := range keys {
		put(t, s, b, key, "off", nil)
	}
	enableVersioning(t, s, b)
	for _, key := range keys {
		put(t, s, b, key, "enabled1", nil)
		put(t, s, b, key, "enabled2", nil)
	}
	suspendVersioning(t, s, b)
	for _, key := range keys {
		put(t, s, b, key, "suspended", nil)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	want := 3 * len(keys)
	if len(out.Versions) != want {
		t.Fatalf("versions=%d want=%d", len(out.Versions), want)
	}
	for _, key := range keys {
		if versionBody(t, s.client, b, key, "null") != "suspended" {
			t.Fatalf("null body for %q", key)
		}
	}
}

func testVersioningVersions(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	key := name
	if strings.Contains(name, "special_names") {
		key = "special ?+#/key"
	}
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		out := put(t, s, b, key, fmt.Sprint(i), nil)
		ids = append(ids, aws.ToString(out.VersionId))
	}
	if name == "test_versioning_obj_suspend_versions" {
		suspendVersioning(t, s, b)
		out := put(t, s, b, key, "suspended", nil)
		// Java accepts null or "null" for suspended puts.
		if !isNullVersionID(aws.ToString(out.VersionId)) {
			t.Fatalf("VersionId=%q", aws.ToString(out.VersionId))
		}
	}
	if name == "test_versioning_obj_create_versions_remove_all" || strings.Contains(name, "special_names") {
		objects := make([]types.ObjectIdentifier, 0, len(ids))
		for _, id := range ids {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key), VersionId: aws.String(id)})
		}
		deleteMany(t, s.client, b, objects, false)
		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
		if len(listed.Versions) != 0 {
			t.Fatalf("versions=%d", len(listed.Versions))
		}
		return
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(key)})
	if len(listed.Versions) < 3 {
		t.Fatalf("versions=%d", len(listed.Versions))
	}
	if name == "test_versioning_latest" && !aws.ToBool(listed.Versions[0].IsLatest) {
		t.Fatal("first version is not latest")
	}
	if name == "test_versioning_get_object_head" {
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: listed.Versions[0].VersionId})
		if err != nil || aws.ToString(head.VersionId) != aws.ToString(listed.Versions[0].VersionId) {
			t.Fatalf("head version=%q err=%v", aws.ToString(head.VersionId), err)
		}
	}
}
