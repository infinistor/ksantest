package s3tests

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 존재하는 오브젝트 여러개를 한번에 삭제
func TestMultiObjectDelete(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 1)
	keys := []string{"TestMultiObjectDelete-0", "TestMultiObjectDelete-1", "TestMultiObjectDelete-2"}
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
	b := s.bucket(t, 2)
	keys := []string{"TestMultiObjectV2Delete-0", "TestMultiObjectV2Delete-1", "TestMultiObjectV2Delete-2"}
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
	b := s.bucket(t, 3)
	keys := []string{"TestMultiObjectDeleteVersions-0", "TestMultiObjectDeleteVersions-1", "TestMultiObjectDeleteVersions-2"}
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
	b := s.bucket(t, 4)
	keys := []string{"TestMultiObjectDeleteQuiet-0", "TestMultiObjectDeleteQuiet-1", "TestMultiObjectDeleteQuiet-2"}
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
	b := s.bucket(t, 5)
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
	b := s.bucket(t, 6)
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
	b := s.bucket(t, 7)
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
	b := s.bucket(t, 8)
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
	b := s.bucket(t, 9)
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
	b := s.bucket(t, 10)
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
	b := s.bucket(t, 11)
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
	b := s.bucket(t, 12)
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
	b := s.bucket(t, 13)
	key := "TestDeleteObjectIfMatchGood"
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
	b := s.bucket(t, 14)
	key := "TestDeleteObjectIfMatchFailed"
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
	b := s.bucket(t, 15)
	key := "TestDeleteObjectIfMatchAny"
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
	b := s.bucket(t, 16)
	key := "TestDeleteObjectIfMatchAndIfNoneMatch"
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
	b := s.bucket(t, 17)
	key := "TestDeleteObjectIfMatchAndIfNoneMatchAny"
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
	b := s.bucket(t, 18)
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
	b := s.bucket(t, 19)
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
	b := s.bucket(t, 20)
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
	b := s.bucket(t, 21)
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

// 없는 키를 지워도 성공한다
func TestDeleteObjectNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	out, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(s.bucket(t, 22)), Key: aws.String("not_exist")})
	if err != nil {
		t.Fatal(err)
	}
	if status := responseStatus(t, out.ResultMetadata); status != 204 || out.DeleteMarker != nil || out.VersionId != nil {
		t.Fatalf("status=%d DeleteMarker=%v VersionId=%q", status, out.DeleteMarker, aws.ToString(out.VersionId))
	}
}

// 없는 버킷에 삭제를 요청하면 NoSuchBucket
func TestDeleteObjectBucketNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucketNameOnly(23)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("key")})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: &types.Delete{Objects: deleteIdentifiers([]string{"key"})}})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 권한이 없는 사용자는 지울 수 없다
func TestDeleteObjectAccessDenied(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	alt := altClientOrSkip(t, s)
	b, key := s.bucket(t, 24), "TestDeleteObjectAccessDenied"
	put(t, s, b, key, key, nil)
	_, err := alt.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 403, "AccessDenied")

	// DeleteObjects 는 요청 자체는 200 이고 키마다 AccessDenied 를 돌려준다
	out := deleteMany(t, alt, b, deleteIdentifiers([]string{key}), false)
	if len(out.Deleted) != 0 || len(out.Errors) != 1 || aws.ToString(out.Errors[0].Key) != key || aws.ToString(out.Errors[0].Code) != "AccessDenied" {
		t.Fatalf("deleted=%v errors=%v", out.Deleted, out.Errors)
	}

	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
}

// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전으로 지울 수 있다
func TestDeleteObjectVersionIdUnversioned(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 25), "TestDeleteObjectVersionIdUnversioned"
	put(t, s, b, key, key, nil)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}

	out, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(out.VersionId) != "null" || out.DeleteMarker != nil {
		t.Fatalf("VersionId=%q DeleteMarker=%v", aws.ToString(out.VersionId), out.DeleteMarker)
	}
	_, err = s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// DeleteObjects 는 없는 키도 삭제된 것으로 응답한다
func TestDeleteObjectsNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	keys := []string{"not_exist1", "not_exist2"}
	out := deleteMany(t, s.client, s.bucket(t, 26), deleteIdentifiers(keys), false)
	deleted := make([]string, 0, len(out.Deleted))
	for _, item := range out.Deleted {
		deleted = append(deleted, aws.ToString(item.Key))
	}
	sort.Strings(deleted)
	assertStringList(t, deleted, keys)
	if len(out.Errors) != 0 {
		t.Fatalf("errors=%v", out.Errors)
	}
}

// 키가 하나도 없는 DeleteObjects 는 거부된다
func TestDeleteObjectsEmpty(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	_, err := s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(s.bucket(t, 27)), Delete: &types.Delete{Objects: []types.ObjectIdentifier{}}})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 한 번에 1000개까지 지울 수 있다
func TestDeleteObjectsMaxKeys(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 28)
	keys := make([]string, 1001)
	for index := range keys {
		keys[index] = fmt.Sprintf("key%d", index)
	}
	out := deleteMany(t, s.client, b, deleteIdentifiers(keys[:1000]), false)
	if len(out.Deleted) != 1000 {
		t.Fatalf("deleted=%d errors=%v", len(out.Deleted), out.Errors)
	}
	_, err := s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: &types.Delete{Objects: deleteIdentifiers(keys)}})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 같은 키를 두 번 넣어도 한 번만 응답한다
func TestDeleteObjectsDuplicateKeys(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 29), "TestDeleteObjectsDuplicateKeys"
	put(t, s, b, key, key, nil)
	out := deleteMany(t, s.client, b, deleteIdentifiers([]string{key, key}), false)
	if len(out.Deleted) != 1 || aws.ToString(out.Deleted[0].Key) != key || len(out.Errors) != 0 {
		t.Fatalf("deleted=%v errors=%v", out.Deleted, out.Errors)
	}
}

// DeleteObjects 에 잘못된 버전 ID 를 주면 요청은 200 이고 키마다 NoSuchVersion 을 돌려준다
func TestDeleteObjectsInvalidVersionId(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 30), "TestDeleteObjectsInvalidVersionId"
	put(t, s, b, key, key, nil)
	checkNoSuchVersion := func() {
		t.Helper()
		out := deleteMany(t, s.client, b, []types.ObjectIdentifier{{Key: aws.String(key), VersionId: aws.String("invalid")}}, false)
		if len(out.Deleted) != 0 || len(out.Errors) != 1 {
			t.Fatalf("deleted=%v errors=%v", out.Deleted, out.Errors)
		}
		if failed := out.Errors[0]; aws.ToString(failed.Key) != key || aws.ToString(failed.VersionId) != "invalid" || aws.ToString(failed.Code) != "NoSuchVersion" {
			t.Fatalf("error key=%q version=%q code=%q", aws.ToString(failed.Key), aws.ToString(failed.VersionId), aws.ToString(failed.Code))
		}
	}

	checkNoSuchVersion()
	enableVersioning(t, s, b)
	checkNoSuchVersion()
	if _, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
}

// 버저닝 버킷에서는 없는 키를 지워도 삭제 마커가 생긴다
func TestDeleteObjectVersioningNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 31)
	enableVersioning(t, s, b)
	deleted, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("not_exist")})
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(deleted.DeleteMarker) || aws.ToString(deleted.VersionId) == "" {
		t.Fatalf("DeleteObject DeleteMarker=%v VersionId=%q", deleted.DeleteMarker, aws.ToString(deleted.VersionId))
	}

	out := deleteMany(t, s.client, b, deleteIdentifiers([]string{"not_exist2"}), false)
	if len(out.Deleted) != 1 || !aws.ToBool(out.Deleted[0].DeleteMarker) || aws.ToString(out.Deleted[0].DeleteMarkerVersionId) == "" {
		t.Fatalf("DeleteObjects deleted=%v errors=%v", out.Deleted, out.Errors)
	}

	listed, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	markers := make([]string, 0, len(listed.DeleteMarkers))
	for _, marker := range listed.DeleteMarkers {
		markers = append(markers, aws.ToString(marker.Key))
	}
	sort.Strings(markers)
	assertStringList(t, markers, []string{"not_exist", "not_exist2"})
}

// 버저닝 버킷에서 없는 null 버전을 지워도 성공하고 삭제 마커는 생기지 않는다
func TestDeleteObjectVersioningInvalidVersionId(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 32), "TestDeleteObjectVersioningInvalidVersionId"
	enableVersioning(t, s, b)
	versionID := aws.ToString(put(t, s, b, key, "content", nil).VersionId)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")

	out, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(out.VersionId) != "null" || out.DeleteMarker != nil {
		t.Fatalf("VersionId=%q DeleteMarker=%v", aws.ToString(out.VersionId), out.DeleteMarker)
	}

	listed, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Versions) != 1 || aws.ToString(listed.Versions[0].VersionId) != versionID || len(listed.DeleteMarkers) != 0 {
		t.Fatalf("versions=%v markers=%v want version=%q", listed.Versions, listed.DeleteMarkers, versionID)
	}
}

// 삭제 마커를 지우면 x-amz-delete-marker: true 로 응답하고 이전 버전이 다시 보인다
func TestDeleteObjectRemoveDeleteMarker(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 33), "TestDeleteObjectRemoveDeleteMarker"
	enableVersioning(t, s, b)
	versionID := aws.ToString(put(t, s, b, key, "content", nil).VersionId)
	marker, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")

	out, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: marker.VersionId})
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(out.DeleteMarker) || aws.ToString(out.VersionId) != aws.ToString(marker.VersionId) {
		t.Fatalf("DeleteMarker=%v VersionId=%q want=%q", out.DeleteMarker, aws.ToString(out.VersionId), aws.ToString(marker.VersionId))
	}
	got, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	got.Body.Close()
	if aws.ToString(got.VersionId) != versionID {
		t.Fatalf("VersionId=%q want=%q", aws.ToString(got.VersionId), versionID)
	}
}
