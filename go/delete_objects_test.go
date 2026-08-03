package s3tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 존재하는 오브젝트 여러개를 한번에 삭제
func TestMultiObjectDelete(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	keys := []string{"test_multi_object_delete0", "test_multi_object_delete1", "test_multi_object_delete2"}
	for _, key := range keys {
		put(t, s, b, key, key, nil)
	}
	out := deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	if len(out.Deleted) != len(keys) {
		t.Fatalf("Deleted=%v", out.Deleted)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || len(listed.Contents) != 0 {
		t.Fatalf("contents=%v err=%v", listed.Contents, err)
	}
	again := deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	if len(again.Deleted) != len(keys) {
		t.Fatalf("second Deleted=%v", again.Deleted)
	}
}

// 버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)
func TestMultiObjectV2Delete(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	keys := []string{"test_multi_object_v2_delete0", "test_multi_object_v2_delete1", "test_multi_object_v2_delete2"}
	for _, key := range keys {
		put(t, s, b, key, key, nil)
	}
	out := deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	if len(out.Deleted) != len(keys) {
		t.Fatalf("Deleted=%v", out.Deleted)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || len(listed.Contents) != 0 {
		t.Fatalf("contents=%v err=%v", listed.Contents, err)
	}
	again := deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	if len(again.Deleted) != len(keys) {
		t.Fatalf("second Deleted=%v", again.Deleted)
	}
}

// 버킷에 존재하는 버저닝 오브젝트 여러개를 한번에 삭제
func TestMultiObjectDeleteVersions(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	keys := []string{"test_multi_object_delete_versions0", "test_multi_object_delete_versions1", "test_multi_object_delete_versions2"}
	enableVersioning(t, s, b)
	for _, key := range keys {
		for i := 0; i < 3; i++ {
			put(t, s, b, key, fmt.Sprint(i), nil)
		}
	}
	out := deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	if len(out.Deleted) != len(keys) {
		t.Fatalf("Deleted=%v", out.Deleted)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || len(listed.Contents) != 0 {
		t.Fatalf("contents=%v err=%v", listed.Contents, err)
	}
}

// quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제
func TestMultiObjectDeleteQuiet(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	keys := []string{"test_multi_object_delete_quiet0", "test_multi_object_delete_quiet1", "test_multi_object_delete_quiet2"}
	for _, key := range keys {
		put(t, s, b, key, key, nil)
	}
	out := deleteMany(t, s.client, b, deleteIdentifiers(keys), true)
	if len(out.Deleted) != 0 {
		t.Fatalf("Deleted=%v", out.Deleted)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || len(listed.Contents) != 0 {
		t.Fatalf("contents=%v err=%v", listed.Contents, err)
	}
	again := deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	if len(again.Deleted) != len(keys) {
		t.Fatalf("second Deleted=%v", again.Deleted)
	}
}

// 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
func TestDirectoryDelete(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	keys := []string{"a/", "a/one", "a/two", "b/", "b/one"}
	for _, key := range keys {
		put(t, s, b, key, key, nil)
	}
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("a/")})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || len(listed.Contents) != 4 {
		t.Fatalf("contents=%v err=%v", listed.Contents, err)
	}
}

// 버저닝 된 버킷에 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
func TestDirectoryDeleteVersions(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	keys := []string{"a/", "a/one", "a/two", "b/", "b/one"}
	enableVersioning(t, s, b)
	for _, key := range keys {
		for i := 0; i < 3; i++ {
			put(t, s, b, key, fmt.Sprint(i), nil)
		}
	}
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("a/")})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || len(listed.Contents) != 4 {
		t.Fatalf("contents=%v err=%v", listed.Contents, err)
	}
	versions := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(versions.Versions) != 15 || len(versions.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d markers=%d", len(versions.Versions), len(versions.DeleteMarkers))
	}
}

// 삭제한 오브젝트가 재대로 삭제 되었는지 확인
func TestDeleteObjects(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%03d", i)
		put(t, s, b, keys[i], keys[i], nil)
	}
	out := deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	if len(out.Deleted) != 100 {
		t.Fatalf("Deleted=%d", len(out.Deleted))
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || len(listed.Contents) != 0 {
		t.Fatalf("contents=%v err=%v", listed.Contents, err)
	}
}

// 버저닝 된 버켓에서 버전 정보를 포함한 삭제가 정상 동작하는지 확인
func TestDeleteObjectsWithVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	keys := []string{"a", "b", "c", "d", "e"}
	var oldest []types.ObjectIdentifier
	for _, key := range keys {
		first := put(t, s, b, key, "old", nil)
		put(t, s, b, key, "new", nil)
		oldest = append(oldest, types.ObjectIdentifier{Key: aws.String(key), VersionId: first.VersionId})
	}
	objects := append(deleteIdentifiers(keys), oldest...)
	out := deleteMany(t, s.client, b, objects, false)
	if len(out.Deleted) != 10 {
		t.Fatalf("Deleted=%d", len(out.Deleted))
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(listed.DeleteMarkers) != 5 || len(listed.Versions) != 5 {
		t.Fatalf("versions=%d markers=%d", len(listed.Versions), len(listed.DeleteMarkers))
	}
	var cleanup []types.ObjectIdentifier
	for _, v := range listed.Versions {
		cleanup = append(cleanup, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
	}
	for _, v := range listed.DeleteMarkers {
		cleanup = append(cleanup, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
	}
	deleteMany(t, s.client, b, cleanup, false)
	final := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(final.Versions)+len(final.DeleteMarkers) != 0 {
		t.Fatalf("remaining=%d", len(final.Versions)+len(final.DeleteMarkers))
	}
}

// 버저닝된 버킷에서 오브젝트를 삭제할 경우 DeleteMarker가 생성되는지 확인
func TestDeleteObjectsWithVersioningDeleteMarker(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	put(t, s, b, "key", "body", nil)
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("key")})
	if err != nil {
		t.Fatal(err)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 1 || len(out.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
}

// 버저닝된 버킷에서 여러 오브젝트를 삭제할 경우 DeleteMarker가 생성되는지 확인
func TestDeleteObjectsVersioningMultiObjectDeleteWithMarker(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		put(t, s, b, key, key, nil)
	}
	deleteMany(t, s.client, b, deleteIdentifiers(keys), false)
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 3 || len(out.DeleteMarkers) != 3 {
		t.Fatalf("versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
}

// 버저닝된 버킷에서 존재하지 않는 오브젝트를 반복 삭제할 경우 DeleteMarker가 생성되는지 확인
func TestDeleteObjectsVersioningMultiObjectDeleteWithMarkerCreate(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	for i := 0; i < 10; i++ {
		_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("key")})
		if err != nil {
			t.Fatal(err)
		}
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 0 || len(out.DeleteMarkers) != 10 {
		t.Fatalf("versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
}

// 버저닝된 버킷에서 존재하지 않는 여러개의 오브젝트를 삭제할 경우 DeleteMarker가 생성되는지 확인
func TestDeleteObjectsVersioningMultiObjectDeleteWithMarkerCreateObjects(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	for i := 0; i < 10; i++ {
		deleteMany(t, s.client, b, deleteIdentifiers([]string{"key"}), false)
	}
	out := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if len(out.Versions) != 0 || len(out.DeleteMarkers) != 10 {
		t.Fatalf("versions=%d markers=%d", len(out.Versions), len(out.DeleteMarkers))
	}
}

// 일치하는 If-Match 조건으로 오브젝트 삭제 성공 확인
func TestDeleteObjectIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	key := "test_delete_object_if_match_good"
	created := put(t, s, b, key, key, nil)
	input := &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)}
	input.IfMatch = created.ETag
	_, err := s.client.DeleteObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 0 {
		t.Fatalf("contents=%d want=%d", len(listed.Contents), 0)
	}
}

// 일치하지 않는 If-Match 조건으로 오브젝트 삭제 시 412 실패 확인
func TestDeleteObjectIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	key := "test_delete_object_if_match_failed"
	put(t, s, b, key, key, nil)
	input := &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)}
	input.IfMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	_, err := s.client.DeleteObject(context.Background(), input)
	assertHTTPError(t, err, 412)
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("contents=%d want=%d", len(listed.Contents), 1)
	}
}

// If-Match: * 조건으로 존재하는 오브젝트 삭제 성공 확인
func TestDeleteObjectIfMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	key := "test_delete_object_if_match_any"
	put(t, s, b, key, key, nil)
	input := &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)}
	input.IfMatch = aws.String("*")
	_, err := s.client.DeleteObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 0 {
		t.Fatalf("contents=%d want=%d", len(listed.Contents), 0)
	}
}

// If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
func TestDeleteObjectIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	key := "test_delete_object_if_match_and_if_none_match"
	created := put(t, s, b, key, key, nil)
	input := &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)}
	input.IfMatch = created.ETag
	_, err := s.client.DeleteObject(context.Background(), input, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, backendHeaders("delete-if-none-match-etag", map[string]string{"If-None-Match": aws.ToString(created.ETag)}))
	})
	assertHTTPError(t, err, 501)
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("contents=%d want=%d", len(listed.Contents), 1)
	}
}

// If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
func TestDeleteObjectIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	key := "test_delete_object_if_match_and_if_none_match_any"
	created := put(t, s, b, key, key, nil)
	input := &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)}
	input.IfMatch = created.ETag
	_, err := s.client.DeleteObject(context.Background(), input, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, backendHeaders("delete-if-none-match-any", map[string]string{"If-None-Match": "*"}))
	})
	assertHTTPError(t, err, 501)
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("contents=%d want=%d", len(listed.Contents), 1)
	}
}

// 모든 오브젝트의 ETag 조건이 일치하는 DeleteObjects 성공 확인
func TestDeleteObjectsIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	good, bad := "good", "bad"
	goodPut := put(t, s, b, good, good, nil)
	badPut := put(t, s, b, bad, bad, nil)
	objects := []types.ObjectIdentifier{
		{Key: aws.String(good), ETag: goodPut.ETag},
		{Key: aws.String(bad), ETag: badPut.ETag},
	}
	out := deleteMany(t, s.client, b, objects, false)
	if len(out.Deleted) != 2 {
		t.Fatalf("Deleted=%v", out.Deleted)
	}
}

// ETag 조건이 일치하지 않는 오브젝트만 삭제에 실패(PreconditionFailed)하는지 확인
func TestDeleteObjectsIfMatchMixed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	good, bad := "good", "bad"
	goodPut := put(t, s, b, good, good, nil)
	put(t, s, b, bad, bad, nil)
	objects := []types.ObjectIdentifier{
		{Key: aws.String(good), ETag: goodPut.ETag},
		{Key: aws.String(bad), ETag: aws.String(`"ABCDEFGHIJKLMNOPQRSTUVWXYZ"`)},
	}
	out := deleteMany(t, s.client, b, objects, false)
	if len(out.Deleted) != 1 || len(out.Errors) != 1 || aws.ToString(out.Errors[0].Code) != "PreconditionFailed" {
		t.Fatalf("deleted=%v errors=%v", out.Deleted, out.Errors)
	}
}

// If-Match와 If-None-Match를 함께 지정하면 DeleteObjects가 501로 거부되는지 확인
func TestDeleteObjectsIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	good := "good"
	goodPut := put(t, s, b, good, good, nil)
	objects := []types.ObjectIdentifier{{Key: aws.String(good)}}
	headers := map[string]string{"If-Match": aws.ToString(goodPut.ETag), "If-None-Match": aws.ToString(goodPut.ETag)}
	_, err := s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: &types.Delete{Objects: objects}}, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, backendHeaders("delete-objects-if-none-match", headers))
	})
	assertHTTPError(t, err, 501)
}

// If-Match와 If-None-Match: * 를 함께 지정하면 DeleteObjects가 501로 거부되는지 확인
func TestDeleteObjectsIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	good := "good"
	goodPut := put(t, s, b, good, good, nil)
	objects := []types.ObjectIdentifier{{Key: aws.String(good)}}
	headers := map[string]string{"If-Match": aws.ToString(goodPut.ETag), "If-None-Match": "*"}
	_, err := s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: &types.Delete{Objects: objects}}, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, backendHeaders("delete-objects-if-none-match", headers))
	})
	assertHTTPError(t, err, 501)
}

func deleteIdentifiers(keys []string) []types.ObjectIdentifier {
	result := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		result[i] = types.ObjectIdentifier{Key: aws.String(key)}
	}
	return result
}
func deleteMany(t *testing.T, client *s3.Client, bucket string, objects []types.ObjectIdentifier, quiet bool, options ...func(*s3.Options)) *s3.DeleteObjectsOutput {
	t.Helper()
	out, err := client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(quiet)}}, options...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
