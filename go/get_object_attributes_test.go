package s3tests

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var basicObjectAttributes = []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesStorageClass, types.ObjectAttributesEtag}

func TestGetObjectAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 기본 GetObjectAttributes 테스트. ObjectSize/StorageClass/ETag를 요청하고 응답이 올바른지 확인.
		{"test_get_object_attributes_basic", func(t *testing.T) { testAttributesBasic(t, "test_get_object_attributes_basic") }},
		// 특정 속성만 요청하는 테스트. 요청하지 않은 속성은 nil이어야 함.
		{"test_get_object_attributes_specific_attributes", func(t *testing.T) { testAttributesBasic(t, "test_get_object_attributes_specific_attributes") }},
		// 멀티파트 업로드된 객체에 대한 GetObjectAttributes 테스트.
		{"test_get_object_attributes_multipart", func(t *testing.T) { testAttributesMultipart(t, "test_get_object_attributes_multipart") }},
		// 체크섬 알고리즘을 사용한 객체에 대한 GetObjectAttributes 테스트.
		{"test_get_object_attributes_with_checksum", func(t *testing.T) { testAttributesBasic(t, "test_get_object_attributes_with_checksum") }},
		// 존재하지 않는 객체에 대한 GetObjectAttributes 테스트.
		{"test_get_object_attributes_non_existent_object", func(t *testing.T) {
			s := newSuite(t)
			b := s.bucket(t)
			_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("missing"), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
			assertS3Error(t, err, 404, "NoSuchKey")
		}},
		// 존재하지 않는 버킷에 대한 GetObjectAttributes 테스트.
		{"test_get_object_attributes_non_existent_bucket", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t)), Key: aws.String("key"), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
			assertS3Error(t, err, 404, "NoSuchBucket")
		}},
		// 속성을 지정하지 않은 GetObjectAttributes 테스트.
		{"test_get_object_attributes_no_attributes", testAttributesNone},
		// 버전 ID를 사용한 GetObjectAttributes 테스트.
		{"test_get_object_attributes_with_version_id", func(t *testing.T) { testAttributesVersion(t, "test_get_object_attributes_with_version_id") }},
		// 잘못된 버전 ID를 사용한 GetObjectAttributes 테스트.
		{"test_get_object_attributes_invalid_version_id", func(t *testing.T) { testAttributesVersion(t, "test_get_object_attributes_invalid_version_id") }},
		// 대용량 멀티파트 업로드 객체에 대한 GetObjectAttributes 테스트.
		{"test_get_object_attributes_large_multipart", func(t *testing.T) { testAttributesMultipart(t, "test_get_object_attributes_large_multipart") }},
		// 메타데이터가 있는 객체에 대한 GetObjectAttributes 테스트. 메타데이터는 HeadObject로 확인.
		{"test_get_object_attributes_with_metadata", func(t *testing.T) { testAttributesBasic(t, "test_get_object_attributes_with_metadata") }},
		// SSE-S3 암호화된 객체에 대한 GetObjectAttributes 테스트. 암호화 정보는 HeadObject로 확인.
		{"test_get_object_attributes_with_sse_s3", func(t *testing.T) { testAttributesBasic(t, "test_get_object_attributes_with_sse_s3") }},
		// 비동기 클라이언트를 사용한 GetObjectAttributes 테스트. Go는 동기 클라이언트로 동일 시나리오 검증.
		{"test_get_object_attributes_async", func(t *testing.T) { testAttributesBasic(t, "test_get_object_attributes_async") }},
		// 비동기 클라이언트를 사용한 GetObjectAttributes 에러 테스트.
		{"test_get_object_attributes_async_error", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t)), Key: aws.String("key"), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
			assertS3Error(t, err, 404, "NoSuchBucket")
		}},
		// 모든 가능한 속성을 요청하는 GetObjectAttributes 테스트.
		{"test_get_object_attributes_all_attributes", func(t *testing.T) { testAttributesMultipart(t, "test_get_object_attributes_all_attributes") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func getAttributes(t *testing.T, client *s3.Client, input *s3.GetObjectAttributesInput) *s3.GetObjectAttributesOutput {
	t.Helper()
	out, err := client.GetObjectAttributes(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func testAttributesBasic(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	b, key := s.bucket(t), name
	input := &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key))}
	if name == "test_get_object_attributes_with_checksum" {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}
	if name == "test_get_object_attributes_with_metadata" {
		input.Metadata = map[string]string{"custom-key1": "custom-value1", "custom-key2": "custom-value2"}
	}
	if name == "test_get_object_attributes_with_sse_s3" {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	putOut, err := s.client.PutObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	attributes := basicObjectAttributes
	if name == "test_get_object_attributes_with_checksum" {
		attributes = []types.ObjectAttributes{types.ObjectAttributesChecksum}
	}
	if name == "test_get_object_attributes_specific_attributes" {
		size := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
		if aws.ToInt64(size.ObjectSize) != int64(len(key)) || size.Checksum != nil {
			t.Fatalf("size=%v checksum=%#v", size.ObjectSize, size.Checksum)
		}
		etag := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag}})
		if aws.ToString(etag.ETag) == "" || etag.ObjectSize != nil {
			t.Fatalf("etag=%q size=%v", aws.ToString(etag.ETag), etag.ObjectSize)
		}
		return
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: attributes})
	if name == "test_get_object_attributes_with_checksum" {
		if out.Checksum == nil || aws.ToString(out.Checksum.ChecksumSHA256) == "" {
			t.Fatalf("checksum=%#v", out.Checksum)
		}
		return
	}
	if aws.ToInt64(out.ObjectSize) != int64(len(key)) || aws.ToString(out.ETag) == "" || out.StorageClass != types.StorageClassStandard {
		t.Fatalf("attributes=%#v", out)
	}
	if strings.Trim(aws.ToString(putOut.ETag), "\"") != strings.Trim(aws.ToString(out.ETag), "\"") {
		t.Fatalf("ETag=%q want=%q", aws.ToString(out.ETag), aws.ToString(putOut.ETag))
	}
	if name == "test_get_object_attributes_with_metadata" {
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil || head.Metadata["custom-key1"] != "custom-value1" {
			t.Fatalf("metadata=%v err=%v", head.Metadata, err)
		}
	}
	if name == "test_get_object_attributes_with_sse_s3" {
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil || head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
			t.Fatalf("encryption=%q err=%v", head.ServerSideEncryption, err)
		}
	}
}

func testAttributesNone(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	b := s.bucket(t)
	put(t, s, b, "key", "body", nil)
	_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("key")})
	if err == nil {
		t.Fatal("request without attributes was accepted")
	}
	// Java reaches the service (HTTP 400). Go SDK validates ObjectAttributes as
	// required client-side (*smithy.OperationError) before the request is sent.
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		assertHTTPError(t, err, 400)
		return
	}
	var opErr *smithy.OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("want client validation or HTTP 400, got %T: %v", err, err)
	}
}

func testAttributesVersion(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	first := put(t, s, b, "key", "first", nil)
	second := put(t, s, b, "key", "second-version", nil)
	if name == "test_get_object_attributes_invalid_version_id" {
		// Java와 동일한 형태의 versionId 토큰 사용.
		// "invalid-version"처럼 형식이 다르면 AWS는 InvalidArgument를 반환함.
		_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
			Bucket: aws.String(b), Key: aws.String("key"),
			VersionId:        aws.String("f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59"),
			ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize},
		})
		assertS3Error(t, err, 404, "NoSuchVersion")
		return
	}
	one := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: first.VersionId, ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
	two := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: second.VersionId, ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
	if aws.ToInt64(one.ObjectSize) != 5 || aws.ToInt64(two.ObjectSize) != 14 || aws.ToString(one.VersionId) != aws.ToString(first.VersionId) || aws.ToString(two.VersionId) != aws.ToString(second.VersionId) {
		t.Fatalf("first=%#v second=%#v", one, two)
	}
}

func testAttributesMultipart(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	b, key := s.bucket(t), name
	partCount := 2
	if name == "test_get_object_attributes_large_multipart" {
		partCount = 20
	}
	algorithm := types.ChecksumAlgorithm("")
	checksumType := types.ChecksumType("")
	if name == "test_get_object_attributes_all_attributes" {
		partCount = 1
		algorithm = types.ChecksumAlgorithmCrc64nvme
		checksumType = types.ChecksumTypeFullObject
	}
	createInput := &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), ChecksumAlgorithm: algorithm, ChecksumType: checksumType}
	created, err := s.client.CreateMultipartUpload(context.Background(), createInput)
	if err != nil {
		t.Fatal(err)
	}
	partBody := bytes.Repeat([]byte("m"), 5*1024*1024)
	parts := make([]types.CompletedPart, 0, partCount)
	for i := 1; i <= partCount; i++ {
		input := &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(int32(i)), Body: bytes.NewReader(partBody), ChecksumAlgorithm: algorithm}
		part, partErr := s.client.UploadPart(context.Background(), input)
		if partErr != nil {
			t.Fatal(partErr)
		}
		parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(int32(i)), ChecksumCRC64NVME: part.ChecksumCRC64NVME})
	}
	completeInput := &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, ChecksumType: checksumType, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), completeInput); err != nil {
		t.Fatal(err)
	}
	attributes := []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesStorageClass, types.ObjectAttributesEtag, types.ObjectAttributesObjectParts}
	if name == "test_get_object_attributes_all_attributes" {
		attributes = append(attributes, types.ObjectAttributesChecksum)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: attributes})
	wantSize := int64(partCount * len(partBody))
	if aws.ToInt64(out.ObjectSize) != wantSize || out.ObjectParts == nil || aws.ToInt32(out.ObjectParts.TotalPartsCount) != int32(partCount) {
		t.Fatalf("size=%v parts=%#v", out.ObjectSize, out.ObjectParts)
	}
	if name == "test_get_object_attributes_all_attributes" && (out.Checksum == nil || aws.ToString(out.Checksum.ChecksumCRC64NVME) == "") {
		t.Fatalf("checksum=%#v", out.Checksum)
	}
}
