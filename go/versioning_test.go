package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷의 버저닝 옵션 변경 가능 확인
func TestVersioningBucketCreateSuspend(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 1)
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

// 버저닝 오브젝트의 생성/읽기/삭제 확인
func TestVersioningObjCreateReadRemove(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 2)
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
}

// 버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인
func TestVersioningObjCreateReadRemoveHead(t *testing.T) {
	t.Parallel()

	t.Skip("JAVA에서는 DeleteObject API를 이용하여 오브젝트를 삭제할 경우 반환값이 없어 삭제된 오브젝트의 버전 정보를 받을 수 없음으로 테스트 불가")
}

// 버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인
func TestVersioningObjPlainNullVersionRemoval(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 4)
	key := "foo"
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
}

// [버킷에 버저닝 설정이 되어있는 상태] null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인
func TestVersioningObjPlainNullVersionOverwrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 5)
	key := "foo"
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
}

// [버킷에 버저닝 설정이 되어있지만 중단된 상태일때] null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인
func TestVersioningObjPlainNullVersionOverwriteSuspended(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 6)
	key := "foo"
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
}

// 버전관리를 일시중단했을때 올바르게 동작하는지 확인
func TestVersioningObjSuspendVersions(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 7)
	enableVersioning(t, s, b)
	key := "obj"
	versionIDs := make([]string, 0, 8)
	contents := make([]string, 0, 8)
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("content-%d", i)
		out := put(t, s, b, key, body, nil)
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
		contents = append(contents, body)
	}
	assertObjectVersionState(t, s.client, b, key, versionIDs, contents)

	suspendVersioning(t, s, b)
	deleteSuspendedVersion(t, s.client, b, key, &versionIDs, &contents)
	deleteSuspendedVersion(t, s.client, b, key, &versionIDs, &contents)
	overwriteSuspendedVersion(t, s, b, key, "null content 1", &versionIDs, &contents)
	overwriteSuspendedVersion(t, s, b, key, "null content 2", &versionIDs, &contents)
	deleteSuspendedVersion(t, s.client, b, key, &versionIDs, &contents)
	overwriteSuspendedVersion(t, s, b, key, "null content 3", &versionIDs, &contents)
	deleteSuspendedVersion(t, s.client, b, key, &versionIDs, &contents)

	enableVersioning(t, s, b)
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf("content-%d", i)
		out := put(t, s, b, key, body, nil)
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
		contents = append(contents, body)
	}
	assertObjectVersionState(t, s.client, b, key, versionIDs, contents)

	for len(versionIDs) > 0 {
		id, body := versionIDs[0], contents[0]
		if versionBody(t, s.client, b, key, id) != body {
			t.Fatalf("version %q body mismatch before deletion", id)
		}
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(id),
		}); err != nil {
			t.Fatal(err)
		}
		versionIDs, contents = versionIDs[1:], contents[1:]
		assertObjectVersionState(t, s.client, b, key, versionIDs, contents)
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(key)})
	if len(listed.Versions) != 0 {
		t.Fatalf("versions after full removal=%d", len(listed.Versions))
	}
}

// 오브젝트하나의 여러버전을 모두 삭제 가능한지 확인
func TestVersioningObjCreateVersionsRemoveAll(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 8)
	enableVersioning(t, s, b)
	key := "test_versioning_obj_create_versions_remove_all"
	ids := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		out := put(t, s, b, key, fmt.Sprint(i), nil)
		ids = append(ids, aws.ToString(out.VersionId))
	}
	for _, id := range ids {
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(id)}); err != nil {
			t.Fatal(err)
		}
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(listed.Versions) != 0 {
		t.Fatalf("versions=%d", len(listed.Versions))
	}
}

// 이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인
func TestVersioningObjCreateVersionsRemoveSpecialNames(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 9)
	enableVersioning(t, s, b)
	for _, key := range []string{"_", ":", " "} {
		ids := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			out := put(t, s, b, key, fmt.Sprintf("content-%d", i), nil)
			ids = append(ids, aws.ToString(out.VersionId))
		}
		for _, id := range ids {
			if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(id)}); err != nil {
				t.Fatal(err)
			}
		}
		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
		if len(listed.Versions) != 0 {
			t.Fatalf("key=%q versions=%d", key, len(listed.Versions))
		}
	}
}

// 오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인
func TestVersioningObjCreateOverwriteMultipart(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 10)
	enableVersioning(t, s, b)
	key := "test_versioning_obj_create_overwrite_multipart"
	versionIDs := make([]string, 0, 3)
	contents := make([][]byte, 0, 3)
	for i := 0; i < 3; i++ {
		body := deterministicBody(15 * 1024 * 1024)
		body[0] = byte('0' + i)
		out := completeVersionedMultipart(t, s.client, b, key, body, nil)
		if aws.ToString(out.VersionId) == "" {
			t.Fatal("CompleteMultipartUpload returned an empty VersionId")
		}
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
		contents = append(contents, body)
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(key)})
	if len(listed.Versions) != 3 {
		t.Fatalf("versions=%d want=3", len(listed.Versions))
	}
	for i, id := range versionIDs {
		if !bytes.Equal([]byte(versionBody(t, s.client, b, key, id)), contents[i]) {
			t.Fatalf("version[%d] body mismatch", i)
		}
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(id)}); err != nil {
			t.Fatal(err)
		}
	}
	listed = listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(key)})
	if len(listed.Versions) != 0 {
		t.Fatalf("versions after deletion=%d", len(listed.Versions))
	}
}

// 버저닝 버킷에서 PutObject와 MultipartUpload를 섞어 업로드한 뒤 버전별 조회가 올바른지 확인
func TestVersioningObjMixPutAndMultipart(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 33)
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
		out := completeVersionedMultipart(t, s.client, b, key, body, nil)
		if aws.ToString(out.VersionId) == "" {
			t.Fatal("CompleteMultipartUpload returned an empty VersionId")
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

// 오브젝트의 해당 버전 정보가 올바른지 확인
func TestVersioningObjListMarker(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 11)
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

// 오브젝트의 버전별 복사가 가능한지 화인
func TestVersioningCopyObjVersion(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 12)
	enableVersioning(t, s, b)
	key := "obj"
	versionIDs := make([]string, 0, 3)
	contents := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf("content-%d", i)
		out := put(t, s, b, key, body, nil)
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
		contents = append(contents, body)
	}
	for i, id := range versionIDs {
		target := fmt.Sprintf("key_%d", i)
		copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, key, id)})
		if read(t, s, b, target) != contents[i] {
			t.Fatalf("same-bucket copy[%d] body mismatch", i)
		}
	}
	other := s.bucket(t, 12)
	for i, id := range versionIDs {
		target := fmt.Sprintf("key_%d", i)
		copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(other), Key: aws.String(target), CopySource: copySource(b, key, id)})
		if read(t, s, other, target) != contents[i] {
			t.Fatalf("cross-bucket copy[%d] body mismatch", i)
		}
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(other), Key: aws.String("newKey"), CopySource: copySource(b, key, "")})
	if read(t, s, other, "newKey") != contents[len(contents)-1] {
		t.Fatal("latest-version copy body mismatch")
	}
}

// 버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인
func TestVersioningMultiObjectDelete(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 13)
	enableVersioning(t, s, b)
	key := "key"
	objects := make([]types.ObjectIdentifier, 0, 2)
	for i := 0; i < 2; i++ {
		out := put(t, s, b, key, fmt.Sprintf("content-%d", i), nil)
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(key), VersionId: out.VersionId})
	}
	for _, object := range objects {
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: object.Key, VersionId: object.VersionId}); err != nil {
			t.Fatal(err)
		}
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 0 {
		t.Fatalf("versions=%d", len(out.Versions))
	}
	for _, object := range objects {
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: object.Key, VersionId: object.VersionId}); err != nil {
			t.Fatal(err)
		}
	}
	out = listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 0 {
		t.Fatalf("versions after repeated deletion=%d", len(out.Versions))
	}
}

// 버전이 여러개인 오브젝트에 대한 삭제마커가 올바르게 동작하는지 확인
func TestVersioningMultiObjectDeleteWithMarker(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 14)
	enableVersioning(t, s, b)
	key := "key"
	for i := 0; i < 2; i++ {
		put(t, s, b, key, fmt.Sprintf("content-%d", i), nil)
	}
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 2 || len(out.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
	objects := make([]types.ObjectIdentifier, 0, 3)
	for _, version := range out.Versions {
		objects = append(objects, types.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
	}
	for _, marker := range out.DeleteMarkers {
		objects = append(objects, types.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
	}
	for _, object := range objects {
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: object.Key, VersionId: object.VersionId}); err != nil {
			t.Fatal(err)
		}
	}
	out = listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 0 || len(out.DeleteMarkers) != 0 {
		t.Fatalf("after cleanup versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
	for _, object := range objects {
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: object.Key, VersionId: object.VersionId}); err != nil {
			t.Fatal(err)
		}
	}
	out = listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 0 || len(out.DeleteMarkers) != 0 {
		t.Fatalf("after repeated cleanup versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
}

// 존재하지않는 오브젝트를 삭제할경우 삭제마커가 생성되는지 확인
func TestVersioningMultiObjectDeleteWithMarkerCreate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 15)
	enableVersioning(t, s, b)
	key := "key"
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.DeleteMarkers) != 1 || aws.ToString(out.DeleteMarkers[0].Key) != key {
		t.Fatalf("delete markers=%#v", out.DeleteMarkers)
	}
}

// 오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인
func TestVersionedObjectAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 16)
	enableVersioning(t, s, b)
	versionIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		out := put(t, s, b, "xyz", fmt.Sprintf("content-%d", i), nil)
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
	}
	acl, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(b), Key: aws.String("xyz"), VersionId: aws.String(versionIDs[1])})
	if err != nil {
		t.Fatal(err)
	}
	if acl.Owner == nil || aws.ToString(acl.Owner.ID) != s.cfg.Main.ID {
		t.Fatalf("owner=%#v want ID=%q", acl.Owner, s.cfg.Main.ID)
	}
	assertCanonicalGrant(t, acl.Grants, s.cfg.Main.ID, types.PermissionFullControl)
	if len(acl.Grants) != 1 {
		t.Fatalf("grants=%#v want owner FULL_CONTROL only", acl.Grants)
	}
}

// 버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인
func TestVersionedObjectAclNoVersionSpecified(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 17)
	enableVersioning(t, s, b)
	for i := 0; i < 3; i++ {
		put(t, s, b, "xyz", fmt.Sprintf("content-%d", i), nil)
	}
	before, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(b), Key: aws.String("xyz")})
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, before.Grants, s.cfg.Main.ID, types.PermissionFullControl)
	_, err = s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(b), Key: aws.String("xyz"), ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	acl, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(b), Key: aws.String("xyz")})
	if err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, acl.Grants, s.cfg.Main.ID, types.PermissionFullControl)
	assertGrants(t, acl.Grants, "http://acs.amazonaws.com/groups/global/AllUsers", []types.Permission{types.PermissionRead})
	if len(acl.Grants) != 2 {
		t.Fatalf("grants=%#v want owner FULL_CONTROL and AllUsers READ", acl.Grants)
	}
}

// 오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인
func TestVersionedConcurrentObjectCreateAndRemove(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 18)
	enableVersioning(t, s, b)
	key := "my_obj"
	for round := 0; round < 3; round++ {
		var createWG sync.WaitGroup
		errs := make(chan error, 3)
		for i := 0; i < 3; i++ {
			i := i
			createWG.Add(1)
			go func() {
				defer createWG.Done()
				_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(fmt.Sprintf("data %d", i)))})
				errs <- err
			}()
		}
		createWG.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}

		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
		var clearWG sync.WaitGroup
		clearErrs := make(chan error, len(listed.Versions))
		for _, version := range listed.Versions {
			version := version
			clearWG.Add(1)
			go func() {
				defer clearWG.Done()
				_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: version.Key, VersionId: version.VersionId})
				clearErrs <- err
			}()
		}
		clearWG.Wait()
		close(clearErrs)
		for err := range clearErrs {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 0 || len(out.DeleteMarkers) != 0 {
		t.Fatalf("versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
}

// 버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인
func TestVersioningBucketAtomicUploadReturnVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 19)
	enableVersioning(t, s, b)
	key := "test_versioning_bucket_atomic_upload_return_version_id"
	putOut := put(t, s, b, key, "bar", nil)
	if aws.ToString(putOut.VersionId) == "" {
		t.Fatal("PutObject returned an empty VersionId")
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(key)})
	if len(listed.Versions) != 1 || aws.ToString(listed.Versions[0].VersionId) != aws.ToString(putOut.VersionId) {
		t.Fatalf("versions=%#v want VersionId=%q", listed.Versions, aws.ToString(putOut.VersionId))
	}
}

// 버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인
func TestVersioningBucketMultipartUploadReturnVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 20)
	enableVersioning(t, s, b)
	key := "test_versioning_bucket_multipart_upload_return_version_id"
	out := completeVersionedMultipart(t, s.client, b, key, deterministicBody(50*1024*1024), map[string]string{"foo": "baz"})
	if aws.ToString(out.VersionId) == "" {
		t.Fatal("CompleteMultipartUpload returned an empty VersionId")
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(key)})
	if len(listed.Versions) != 1 || aws.ToString(listed.Versions[0].VersionId) != aws.ToString(out.VersionId) {
		t.Fatalf("versions=%#v want VersionId=%q", listed.Versions, aws.ToString(out.VersionId))
	}
}

// 업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인
func TestVersioningGetObjectHead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 21)
	enableVersioning(t, s, b)
	key := "test_versioning_get_object_head"
	versionIDs := make([]string, 0, 5)
	for size := 1; size <= 5; size++ {
		out := put(t, s, b, key, string(deterministicBody(size)), nil)
		versionIDs = append(versionIDs, aws.ToString(out.VersionId))
	}
	for i, id := range versionIDs {
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(id)})
		if err != nil || aws.ToString(head.VersionId) != id || aws.ToInt64(head.ContentLength) != int64(i+1) {
			t.Fatalf("head[%d] version=%q length=%d err=%v", i, aws.ToString(head.VersionId), aws.ToInt64(head.ContentLength), err)
		}
	}
}

// 버전이 여러개인 오브젝트의 최신 버전을 삭제 했을때 이전버전이 최신버전으로 변경되는지 확인
func TestVersioningLatest(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 22)
	enableVersioning(t, s, b)
	key := "test_versioning_latest"
	versionIDs := make([]string, 0, 5)
	for size := 1; size <= 5; size++ {
		out := put(t, s, b, key, string(deterministicBody(size)), nil)
		versionIDs = append([]string{aws.ToString(out.VersionId)}, versionIDs...)
	}
	for len(versionIDs) > 1 {
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(versionIDs[0])}); err != nil {
			t.Fatal(err)
		}
		versionIDs = versionIDs[1:]
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil || aws.ToString(head.VersionId) != versionIDs[0] {
			t.Fatalf("latest=%q want=%q err=%v", aws.ToString(head.VersionId), versionIDs[0], err)
		}
	}
}

// 잘못된 버전 정보를 사용하여 오브젝트 조회 실패 확인
func TestVersioningInvalidVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 23)
	enableVersioning(t, s, b)
	put(t, s, b, "key", "body", nil)

	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: aws.String("f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59")})
	assertS3Error(t, err, 404, "NoSuchVersion")
}

// CopyObject로 복사할 경우 버저닝이 올바르게 동작하는지 확인
func TestVersioningCopyObject(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 24)
	enableVersioning(t, s, b)
	sourceKey, targetKey, content := "source", "target", "content-version1"
	source := put(t, s, b, sourceKey, content, nil)
	expected := []string{aws.ToString(source.VersionId)}

	first := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(targetKey), CopySource: copySource(b, sourceKey, "")})
	expected = append(expected, aws.ToString(first.VersionId))
	assertVersionedCopy(t, s, b, targetKey, content, aws.ToString(first.VersionId))
	assertListedVersionIDs(t, s.client, b, expected)

	second := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(targetKey), CopySource: copySource(b, sourceKey, "")})
	expected = append(expected, aws.ToString(second.VersionId))
	assertVersionedCopy(t, s, b, targetKey, content, aws.ToString(second.VersionId))
	assertListedVersionIDs(t, s.client, b, expected)

	third := copyCall(t, s.client, &s3.CopyObjectInput{
		Bucket: aws.String(b), Key: aws.String(targetKey), CopySource: copySource(b, sourceKey, ""),
		ContentType: aws.String("text/plain"), Metadata: map[string]string{"test-key": "test-value"}, MetadataDirective: types.MetadataDirectiveReplace,
	})
	expected = append(expected, aws.ToString(third.VersionId))
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(targetKey)})
	if err != nil || head.Metadata["test-key"] != "test-value" || aws.ToString(head.ContentType) != "text/plain" || aws.ToString(head.VersionId) != aws.ToString(third.VersionId) {
		t.Fatalf("metadata copy head=%#v err=%v", head, err)
	}
	assertListedVersionIDs(t, s.client, b, expected)

	suspendVersioning(t, s, b)
	fourth := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(targetKey), CopySource: copySource(b, sourceKey, "")})
	if !isNullVersionID(aws.ToString(fourth.VersionId)) {
		t.Fatalf("suspended copy VersionId=%q", aws.ToString(fourth.VersionId))
	}
	expected = append(expected, "null")
	if read(t, s, b, targetKey) != content {
		t.Fatal("suspended copy body mismatch")
	}
	assertListedVersionIDs(t, s.client, b, expected)

	fifth := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(targetKey), CopySource: copySource(b, sourceKey, "")})
	if !isNullVersionID(aws.ToString(fifth.VersionId)) {
		t.Fatalf("suspended overwrite VersionId=%q", aws.ToString(fifth.VersionId))
	}
	assertVersionedCopy(t, s, b, targetKey, content, "null")
	assertListedVersionIDs(t, s.client, b, expected)
}

// 버저닝 미설정 버킷에서 Put/Head/Get/Multipart/Copy/List의 versionId가 비어있는지 확인
func TestVersioningUnversionedAllVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 25)
	key := "test_versioning_unversioned_all_version_id"
	multipartKey, copyKey, content := key+"-multipart", key+"-copy", "testContent"
	putOut := put(t, s, b, key, content, nil)
	if putOut.VersionId != nil {
		t.Fatalf("PutObject VersionId=%q want nil", aws.ToString(putOut.VersionId))
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || head.VersionId != nil {
		t.Fatalf("HeadObject VersionId=%q err=%v", aws.ToString(head.VersionId), err)
	}
	get, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(get.Body)
	get.Body.Close()
	if readErr != nil || get.VersionId != nil || string(body) != content {
		t.Fatalf("GetObject VersionId=%q body=%q err=%v", aws.ToString(get.VersionId), body, readErr)
	}
	multipart := completeVersionedMultipart(t, s.client, b, multipartKey, deterministicBody(5*1024*1024), nil)
	if multipart.VersionId != nil {
		t.Fatalf("CompleteMultipartUpload VersionId=%q want nil", aws.ToString(multipart.VersionId))
	}
	copied := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(copyKey), CopySource: copySource(b, key, "")})
	if copied.VersionId != nil {
		t.Fatalf("CopyObject VersionId=%q want nil", aws.ToString(copied.VersionId))
	}
	objects, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(b)})
	if err != nil || len(objects.Contents) != 3 {
		t.Fatalf("ListObjects contents=%d err=%v", len(objects.Contents), err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 3 {
		t.Fatalf("versions=%d", len(out.Versions))
	}
	for _, version := range out.Versions {
		if aws.ToString(version.VersionId) != "null" {
			t.Fatalf("listed VersionId=%q want null", aws.ToString(version.VersionId))
		}
	}
}

// 버저닝 ENABLED 상태에서 Put/Head/Get/Multipart/Copy/List의 versionId가 존재하고 일치하는지 확인
func TestVersioningEnabledAllVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 26)
	enableVersioning(t, s, b)
	key := "test_versioning_enabled_all_version_id"
	multipartKey, copyKey, content := key+"-multipart", key+"-copy", "testContent"
	putOut := put(t, s, b, key, content, nil)
	putVersionID := aws.ToString(putOut.VersionId)
	if putVersionID == "" {
		t.Fatal("PutObject returned an empty VersionId")
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || aws.ToString(head.VersionId) != putVersionID {
		t.Fatalf("HeadObject VersionId=%q want=%q err=%v", aws.ToString(head.VersionId), putVersionID, err)
	}
	get, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(get.Body)
	get.Body.Close()
	if readErr != nil || aws.ToString(get.VersionId) != putVersionID || string(body) != content {
		t.Fatalf("GetObject VersionId=%q body=%q err=%v", aws.ToString(get.VersionId), body, readErr)
	}
	multipart := completeVersionedMultipart(t, s.client, b, multipartKey, deterministicBody(5*1024*1024), nil)
	multipartVersionID := aws.ToString(multipart.VersionId)
	if multipartVersionID == "" {
		t.Fatal("CompleteMultipartUpload returned an empty VersionId")
	}
	copied := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(copyKey), CopySource: copySource(b, key, "")})
	copyVersionID := aws.ToString(copied.VersionId)
	if copyVersionID == "" {
		t.Fatal("CopyObject returned an empty VersionId")
	}
	objects, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(b)})
	if err != nil || len(objects.Contents) != 3 {
		t.Fatalf("ListObjects contents=%d err=%v", len(objects.Contents), err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 3 {
		t.Fatalf("versions=%d", len(out.Versions))
	}
	wanted := map[string]bool{putVersionID: false, multipartVersionID: false, copyVersionID: false}
	for _, version := range out.Versions {
		id := aws.ToString(version.VersionId)
		if _, ok := wanted[id]; !ok {
			t.Fatalf("unexpected listed VersionId=%q", id)
		}
		wanted[id] = true
	}
	for id, found := range wanted {
		if !found {
			t.Fatalf("listed VersionId %q not found", id)
		}
	}
}

// 버저닝 SUSPENDED 상태에서 Put/Head/Get/Multipart/Copy/List의 versionId가 "null"인지 확인
func TestVersioningSuspendedAllVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 27)
	suspendVersioning(t, s, b)
	key := "test_versioning_suspended_all_version_id"
	multipartKey, copyKey, content := key+"-multipart", key+"-copy", "testContent"
	putOut := put(t, s, b, key, content, nil)
	if putOut.VersionId != nil {
		t.Fatalf("PutObject VersionId=%q want nil", aws.ToString(putOut.VersionId))
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || aws.ToString(head.VersionId) != "null" {
		t.Fatalf("HeadObject VersionId=%q want null err=%v", aws.ToString(head.VersionId), err)
	}
	get, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(get.Body)
	get.Body.Close()
	if readErr != nil || aws.ToString(get.VersionId) != "null" || string(body) != content {
		t.Fatalf("GetObject VersionId=%q body=%q err=%v", aws.ToString(get.VersionId), body, readErr)
	}
	multipart := completeVersionedMultipart(t, s.client, b, multipartKey, deterministicBody(5*1024*1024), nil)
	if multipart.VersionId != nil {
		t.Fatalf("CompleteMultipartUpload VersionId=%q want nil", aws.ToString(multipart.VersionId))
	}
	copied := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(copyKey), CopySource: copySource(b, key, "")})
	if !isNullVersionID(aws.ToString(copied.VersionId)) {
		t.Fatalf("CopyObject VersionId=%q want null", aws.ToString(copied.VersionId))
	}
	objects, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(b)})
	if err != nil || len(objects.Contents) != 3 {
		t.Fatalf("ListObjects contents=%d err=%v", len(objects.Contents), err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 3 {
		t.Fatalf("versions=%d", len(out.Versions))
	}
	for _, version := range out.Versions {
		if aws.ToString(version.VersionId) != "null" {
			t.Fatalf("listed VersionId=%q want null", aws.ToString(version.VersionId))
		}
	}
}

// OFF→ENABLED→SUSPENDED 순으로 같은 key에 put 후 listVersions가 null+versionId 2개인지 확인
func TestVersioningListVersionsOffEnabledSuspended(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 28)
	key := "test_versioning_list_versions_off_enabled_suspended"
	contentOff, contentEnabled, contentSuspended := "content-off", "content-enabled", "content-suspended"
	off := put(t, s, b, key, contentOff, nil)
	if off.VersionId != nil {
		t.Fatalf("OFF PutObject VersionId=%q want nil", aws.ToString(off.VersionId))
	}

	enableVersioning(t, s, b)
	enabled := put(t, s, b, key, contentEnabled, nil)
	enabledVersionID := aws.ToString(enabled.VersionId)
	if enabledVersionID == "" {
		t.Fatal("ENABLED PutObject returned an empty VersionId")
	}

	suspendVersioning(t, s, b)
	suspended := put(t, s, b, key, contentSuspended, nil)
	if suspended.VersionId != nil {
		t.Fatalf("SUSPENDED PutObject VersionId=%q want nil", aws.ToString(suspended.VersionId))
	}

	objects, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects.Contents) != 1 || aws.ToString(objects.Contents[0].Key) != key {
		t.Fatalf("ListObjects contents=%#v err=%v", objects.Contents, err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 2 {
		t.Fatalf("versions=%d want=2", len(out.Versions))
	}
	foundEnabled, foundNull, foundLatest := false, false, false
	for _, version := range out.Versions {
		id := aws.ToString(version.VersionId)
		switch {
		case id == enabledVersionID:
			foundEnabled = true
			if aws.ToInt64(version.Size) != int64(len(contentEnabled)) {
				t.Fatalf("enabled size=%d want=%d", aws.ToInt64(version.Size), len(contentEnabled))
			}
		case isNullVersionID(id):
			foundNull = true
			if aws.ToBool(version.IsLatest) {
				foundLatest = true
			}
			if aws.ToInt64(version.Size) != int64(len(contentSuspended)) {
				t.Fatalf("latest null size=%d want=%d", aws.ToInt64(version.Size), len(contentSuspended))
			}
		default:
			t.Fatalf("unexpected VersionId=%q", id)
		}
	}
	if !foundEnabled || !foundNull || !foundLatest {
		t.Fatalf("enabled=%v null=%v latestNull=%v", foundEnabled, foundNull, foundLatest)
	}
	if read(t, s, b, key) != contentSuspended {
		t.Fatal("current suspended body mismatch")
	}
	if versionBody(t, s.client, b, key, enabledVersionID) != contentEnabled {
		t.Fatal("enabled version body mismatch")
	}
}

// OFF→ENABLED→SUSPENDED 순으로 서로 다른 key에 put 후 listVersions가 3개(null 2개+versionId 1개)인지 확인
func TestVersioningListVersionsOffEnabledSuspendedDifferentKeys(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 29)
	keyOff := "test_versioning_list_versions_off"
	keyEnabled := "test_versioning_list_versions_enabled"
	keySuspended := "test_versioning_list_versions_suspended"
	contentOff, contentEnabled, contentSuspended := "content-off", "content-enabled", "content-suspended"
	off := put(t, s, b, keyOff, contentOff, nil)
	if off.VersionId != nil {
		t.Fatalf("OFF PutObject VersionId=%q want nil", aws.ToString(off.VersionId))
	}

	enableVersioning(t, s, b)
	enabled := put(t, s, b, keyEnabled, contentEnabled, nil)
	enabledVersionID := aws.ToString(enabled.VersionId)
	if enabledVersionID == "" {
		t.Fatal("ENABLED PutObject returned an empty VersionId")
	}

	suspendVersioning(t, s, b)
	suspended := put(t, s, b, keySuspended, contentSuspended, nil)
	if suspended.VersionId != nil {
		t.Fatalf("SUSPENDED PutObject VersionId=%q want nil", aws.ToString(suspended.VersionId))
	}

	objects, err := s.client.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects.Contents) != 3 {
		t.Fatalf("ListObjects contents=%d err=%v", len(objects.Contents), err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 3 {
		t.Fatalf("versions=%d want=3", len(out.Versions))
	}
	versionByKey := make(map[string]string, 3)
	nullCount := 0
	for _, version := range out.Versions {
		id := aws.ToString(version.VersionId)
		versionByKey[aws.ToString(version.Key)] = id
		if isNullVersionID(id) {
			nullCount++
		}
	}
	if !isNullVersionID(versionByKey[keyOff]) || versionByKey[keyEnabled] != enabledVersionID || !isNullVersionID(versionByKey[keySuspended]) || nullCount != 2 {
		t.Fatalf("versionByKey=%v nullCount=%d", versionByKey, nullCount)
	}
	headOff, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(keyOff)})
	if err != nil {
		t.Fatal(err)
	}
	if !isNullVersionID(aws.ToString(headOff.VersionId)) || read(t, s, b, keyOff) != contentOff {
		t.Fatalf("OFF key Head VersionId=%q err=%v", aws.ToString(headOff.VersionId), err)
	}
	headEnabled, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(keyEnabled)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(headEnabled.VersionId) != enabledVersionID || read(t, s, b, keyEnabled) != contentEnabled {
		t.Fatalf("ENABLED key Head VersionId=%q want=%q err=%v", aws.ToString(headEnabled.VersionId), enabledVersionID, err)
	}
	headSuspended, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(keySuspended)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(headSuspended.VersionId) != "null" || read(t, s, b, keySuspended) != contentSuspended {
		t.Fatalf("SUSPENDED key Head VersionId=%q err=%v", aws.ToString(headSuspended.VersionId), err)
	}
}

// OFF→ENABLED→SUSPENDED 후 null 버전 삭제 시 current가 ENABLED 버전으로 바뀌는지 확인
func TestVersioningDeleteNullVersionAfterSuspend(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 30)
	key := "foo"
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
}

// ENABLED에서 여러 번 put 후 SUSPENDED put 시 listVersions가 versionId N개+null 1개인지 확인
func TestVersioningListVersionsMultipleEnabledThenSuspended(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 31)
	key := "test_versioning_list_versions_multiple_enabled_then_suspended"
	put(t, s, b, key, "content-off", nil)

	enableVersioning(t, s, b)
	enabledVersionIDs := make([]string, 0, 3)
	for i := 1; i <= 3; i++ {
		out := put(t, s, b, key, fmt.Sprintf("content-enabled-%d", i), nil)
		id := aws.ToString(out.VersionId)
		if id == "" {
			t.Fatalf("enabled put %d returned an empty VersionId", i)
		}
		enabledVersionIDs = append(enabledVersionIDs, id)
	}

	suspendVersioning(t, s, b)
	put(t, s, b, key, "content-suspended", nil)

	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 4 {
		t.Fatalf("versions=%d want=4", len(out.Versions))
	}
	found := make(map[string]bool, 3)
	foundNull, latestNull := false, false
	for _, version := range out.Versions {
		id := aws.ToString(version.VersionId)
		if isNullVersionID(id) {
			foundNull = true
			latestNull = latestNull || aws.ToBool(version.IsLatest)
			continue
		}
		found[id] = true
	}
	for _, id := range enabledVersionIDs {
		if !found[id] {
			t.Fatalf("enabled VersionId %q not listed", id)
		}
	}
	if !foundNull || !latestNull {
		t.Fatalf("null=%v latestNull=%v", foundNull, latestNull)
	}
	if read(t, s, b, key) != "content-suspended" {
		t.Fatal("current suspended body mismatch")
	}
}

// Current가 DeleteMarker인 오브젝트를 HeadObject 요청 시 올바르게 동작하는지 확인
func TestVersioningHeadObjectDeleteMarker(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 32)
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
	_, headErr := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertHTTPError(t, headErr, 404)
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(listed.Versions) != 1 || len(listed.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d markers=%d", len(listed.Versions), len(listed.DeleteMarkers))
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

func isNullVersionID(id string) bool {
	return id == "" || id == "null"
}

func reverseStrings(in []string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[len(in)-1-i] = v
	}
	return out
}

func completeVersionedMultipart(t *testing.T, client *s3.Client, bucket, key string, body []byte, metadata map[string]string) *s3.CompleteMultipartUploadOutput {
	t.Helper()
	created, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Metadata: metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := false
	defer func() {
		if !completed {
			_, _ = client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
			})
		}
	}()
	parts := uploadByteParts(t, client, bucket, key, aws.ToString(created.UploadId), body, 1)
	out, err := client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed = true
	return out
}

func deleteSuspendedVersion(t *testing.T, client *s3.Client, bucket, key string, versionIDs, contents *[]string) {
	t.Helper()
	if _, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}); err != nil {
		t.Fatal(err)
	}
	removeNullExpectedVersion(versionIDs, contents)
	assertObjectVersionState(t, client, bucket, key, *versionIDs, *contents)
}

func overwriteSuspendedVersion(t *testing.T, s *suite, bucket, key, body string, versionIDs, contents *[]string) {
	t.Helper()
	out := put(t, s, bucket, key, body, nil)
	if !isNullVersionID(aws.ToString(out.VersionId)) {
		t.Fatalf("suspended PutObject VersionId=%q", aws.ToString(out.VersionId))
	}
	removeNullExpectedVersion(versionIDs, contents)
	*versionIDs = append(*versionIDs, "null")
	*contents = append(*contents, body)
	assertObjectVersionState(t, s.client, bucket, key, *versionIDs, *contents)
}

func removeNullExpectedVersion(versionIDs, contents *[]string) {
	for i := len(*versionIDs) - 1; i >= 0; i-- {
		if isNullVersionID((*versionIDs)[i]) {
			*versionIDs = append((*versionIDs)[:i], (*versionIDs)[i+1:]...)
			*contents = append((*contents)[:i], (*contents)[i+1:]...)
		}
	}
}

func assertObjectVersionState(t *testing.T, client *s3.Client, bucket, key string, versionIDs, contents []string) {
	t.Helper()
	if len(versionIDs) != len(contents) {
		t.Fatalf("versionIDs=%d contents=%d", len(versionIDs), len(contents))
	}
	listed := listVersions(t, client, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(key)})
	if len(listed.Versions) != len(versionIDs) {
		t.Fatalf("listed versions=%d want=%d", len(listed.Versions), len(versionIDs))
	}
	expected := make(map[string]string, len(versionIDs))
	for i, id := range versionIDs {
		if isNullVersionID(id) {
			id = "null"
		}
		expected[id] = contents[i]
	}
	for _, version := range listed.Versions {
		id := aws.ToString(version.VersionId)
		if isNullVersionID(id) {
			id = "null"
		}
		want, ok := expected[id]
		if !ok {
			t.Fatalf("unexpected listed VersionId=%q", id)
		}
		if versionBody(t, client, bucket, key, id) != want {
			t.Fatalf("VersionId=%q body mismatch", id)
		}
		delete(expected, id)
	}
	if len(expected) != 0 {
		t.Fatalf("missing versions=%v", expected)
	}
}

func assertVersionedCopy(t *testing.T, s *suite, bucket, key, wantBody, wantVersionID string) {
	t.Helper()
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(out.Body)
	out.Body.Close()
	if readErr != nil || string(body) != wantBody || (wantVersionID != "null" && aws.ToString(out.VersionId) != wantVersionID) || (wantVersionID == "null" && !isNullVersionID(aws.ToString(out.VersionId))) {
		t.Fatalf("GetObject body=%q VersionId=%q want body=%q VersionId=%q err=%v", body, aws.ToString(out.VersionId), wantBody, wantVersionID, readErr)
	}
}

func assertListedVersionIDs(t *testing.T, client *s3.Client, bucket string, wanted []string) {
	t.Helper()
	listed := listVersions(t, client, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if len(listed.Versions) != len(wanted) {
		t.Fatalf("versions=%d want=%d", len(listed.Versions), len(wanted))
	}
	remaining := make(map[string]int, len(wanted))
	for _, id := range wanted {
		remaining[id]++
	}
	for _, version := range listed.Versions {
		id := aws.ToString(version.VersionId)
		if isNullVersionID(id) {
			id = "null"
		}
		if remaining[id] == 0 {
			t.Fatalf("unexpected listed VersionId=%q", id)
		}
		remaining[id]--
	}
	for id, count := range remaining {
		if count != 0 {
			t.Fatalf("listed VersionId=%q missing count=%d", id, count)
		}
	}
}
