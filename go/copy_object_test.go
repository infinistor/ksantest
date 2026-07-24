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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestCopyObject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 오브젝트의 크기가 0일때 복사가 가능한지 확인하는 테스트
		{"test_object_copy_zero_size", func(t *testing.T) { testCopyCore(t, "test_object_copy_zero_size") }},
		// 동일한 버킷에서 오브젝트 복사가 가능한지 확인하는 테스트
		{"test_object_copy_same_bucket", func(t *testing.T) { testCopyCore(t, "test_object_copy_same_bucket") }},
		// ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인하는 테스트
		{"test_object_copy_verify_content_type", func(t *testing.T) { testCopyCore(t, "test_object_copy_verify_content_type") }},
		// 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러를 확인하는 테스트
		{"test_object_copy_to_itself", func(t *testing.T) { testCopyCore(t, "test_object_copy_to_itself") }},
		// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인하는 테스트
		{"test_object_copy_to_itself_with_metadata", func(t *testing.T) { testCopyCore(t, "test_object_copy_to_itself_with_metadata") }},
		// 다른 버킷으로 오브젝트 복사가 가능한지 확인하는 테스트
		{"test_object_copy_diff_bucket", func(t *testing.T) { testCopyCore(t, "test_object_copy_diff_bucket") }},
		// [bucket1:created main user, object:created main user / bucket2:created sub user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인하는 테스트
		{"test_object_copy_not_owned_bucket", func(t *testing.T) { testCopyCore(t, "test_object_copy_not_owned_bucket") }},
		// 다른유저의 버킷의 오브젝트를 권한이 충분할 경우 복사 가능한지 확인하는 테스트
		{"test_object_copy_not_owned_object_bucket", func(t *testing.T) { testCopyCore(t, "test_object_copy_not_owned_object_bucket") }},
		// 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인하는 테스트
		{"test_object_copy_canned_acl", func(t *testing.T) { testCopyCore(t, "test_object_copy_canned_acl") }},
		// 크고 작은 용량의 오브젝트가 복사되는지 확인하는 테스트
		{"test_object_copy_retaining_metadata", func(t *testing.T) { testCopyCore(t, "test_object_copy_retaining_metadata") }},
		// 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인하는 테스트
		{"test_object_copy_replacing_metadata", func(t *testing.T) { testCopyCore(t, "test_object_copy_replacing_metadata") }},
		// 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패를 확인하는 테스트
		{"test_object_copy_bucket_not_found", func(t *testing.T) { testCopyCore(t, "test_object_copy_bucket_not_found") }},
		// 존재하지않는 오브젝트 복사 실패를 확인하는 테스트
		{"test_object_copy_key_not_found", func(t *testing.T) { testCopyCore(t, "test_object_copy_key_not_found") }},
		// 버저닝된 오브젝트 복사를 확인하는 테스트
		{"test_object_copy_versioning_bucket", func(t *testing.T) { testCopyCore(t, "test_object_copy_versioning_bucket") }},
		// [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공을 확인하는 테스트
		{"test_object_copy_versioning_url_encoding", func(t *testing.T) { testCopyCore(t, "test_object_copy_versioning_url_encoding") }},
		// [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사를 확인하는 테스트
		{"test_object_copy_versioning_multipart_upload", func(t *testing.T) { testCopyCore(t, "test_object_copy_versioning_multipart_upload") }},
		// ifMatch 값을 추가하여 오브젝트를 복사할 경우 성공을 확인하는 테스트
		{"test_copy_object_if_match_good", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_match_good") }},
		// ifMatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패를 확인하는 테스트
		{"test_copy_object_if_match_failed", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_match_failed") }},
		// 소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 복사 성공 확인
		{"test_copy_object_if_none_match_good", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_none_match_good") }},
		// 소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 복사 시 412 실패 확인
		{"test_copy_object_if_none_match_failed", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_none_match_failed") }},
		// 소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 복사 성공 확인
		{"test_copy_object_if_modified_since_good", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_modified_since_good") }},
		// 소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 복사 시 412 실패 확인
		{"test_copy_object_if_modified_since_failed", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_modified_since_failed") }},
		// 소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 복사 성공 확인
		{"test_copy_object_if_unmodified_since_good", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_unmodified_since_good") }},
		// 소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 복사 시 412 실패 확인
		{"test_copy_object_if_unmodified_since_failed", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_unmodified_since_failed") }},
		// copy-source-if-match(일치)와 copy-source-if-unmodified-since(불일치)를 함께 사용할 경우 ETag 조건이 우선되어 복사에 성공하는지 확인
		{"test_copy_object_if_match_with_if_unmodified_since", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_match_with_if_unmodified_since") }},
		// copy-source-if-none-match(불일치)와 copy-source-if-modified-since(일치)를 함께 사용할 경우 ETag 조건이 우선되어 412가 반환되는지 확인
		{"test_copy_object_if_none_match_with_if_modified_since", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_none_match_with_if_modified_since") }},
		// copy-source-if-match와 copy-source-if-none-match에 동일한 ETag를 지정하면 412가 반환되는지 확인
		{"test_copy_object_if_match_and_if_none_match", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_match_and_if_none_match") }},
		// copy-source-if-match와 copy-source-if-none-match: * 를 함께 지정하면 412가 반환되는지 확인
		{"test_copy_object_if_match_and_if_none_match_any", func(t *testing.T) { testCopyConditions(t, "test_copy_object_if_match_and_if_none_match_any") }},
		// 대상 오브젝트와 일치하는 If-Match 조건으로 덮어쓰기 복사 성공 확인
		{"test_copy_object_destination_if_match_good", func(t *testing.T) { testCopyConditions(t, "test_copy_object_destination_if_match_good") }},
		// 대상 오브젝트와 일치하지 않는 If-Match 조건으로 덮어쓰기 복사 시 412 실패 확인
		{"test_copy_object_destination_if_match_failed", func(t *testing.T) { testCopyConditions(t, "test_copy_object_destination_if_match_failed") }},
		// 존재하지 않는 대상 키에 If-None-Match: * 조건으로 복사 성공 확인
		{"test_copy_object_destination_if_none_match_good", func(t *testing.T) { testCopyConditions(t, "test_copy_object_destination_if_none_match_good") }},
		// 이미 존재하는 대상 키에 If-None-Match: * 조건으로 복사 시 412 실패 확인
		{"test_copy_object_destination_if_none_match_failed", func(t *testing.T) { testCopyConditions(t, "test_copy_object_destination_if_none_match_failed") }},
		// 대상에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
		{"test_copy_object_destination_if_match_and_if_none_match", func(t *testing.T) { testCopyConditions(t, "test_copy_object_destination_if_match_and_if_none_match") }},
		// 대상에 If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
		{"test_copy_object_destination_if_match_and_if_none_match_any", func(t *testing.T) { testCopyConditions(t, "test_copy_object_destination_if_match_and_if_none_match_any") }},
		// 소스 If-Match와 대상 If-None-Match: * 를 함께 사용해 복사 성공 확인
		{"test_copy_object_source_if_match_with_destination_if_none_match", func(t *testing.T) { testCopyConditions(t, "test_copy_object_source_if_match_with_destination_if_none_match") }},
		// [source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_nor_src_to_nor_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_nor_src_to_nor_bucket_and_obj") }},
		// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_nor_src_to_nor_bucket_encryption_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_nor_src_to_nor_bucket_encryption_obj") }},
		// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_nor_src_to_encryption_bucket_nor_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_nor_src_to_encryption_bucket_nor_obj") }},
		// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_nor_src_to_encryption_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_nor_src_to_encryption_bucket_and_obj") }},
		// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_src_to_nor_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_src_to_nor_bucket_and_obj") }},
		// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_src_to_nor_bucket_encryption_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_src_to_nor_bucket_encryption_obj") }},
		// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_src_to_encryption_bucket_nor_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_src_to_encryption_bucket_nor_obj") }},
		// [source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_src_to_encryption_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_src_to_encryption_bucket_and_obj") }},
		// [source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj") }},
		// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj") }},
		// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj") }},
		// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj") }},
		// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj") }},
		// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj") }},
		// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj") }},
		// [source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
		{"test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj", func(t *testing.T) { testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj") }},
		// 일반 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
		{"test_copy_to_normal_source", func(t *testing.T) { testCopyEncryption(t, "test_copy_to_normal_source") }},
		// SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
		{"test_copy_to_sse_s3_source", func(t *testing.T) { testCopyEncryption(t, "test_copy_to_sse_s3_source") }},
		// SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트 SDK V1은 SSE-C 차단 해제(BlockedEncryptionTypes)를 지원하지 않아 V2만 테스트한다.
		{"test_copy_to_sse_c_source", func(t *testing.T) { testCopyEncryption(t, "test_copy_to_sse_c_source") }},
		// 삭제된 오브젝트 복사 실패를 확인하는 테스트
		{"test_copy_to_deleted_object", func(t *testing.T) { testCopyDeleted(t, "test_copy_to_deleted_object") }},
		// 버저닝된 버킷에서 삭제된 오브젝트 복사 실패를 확인하는 테스트
		{"test_copy_to_delete_marker_object", func(t *testing.T) { testCopyDeleted(t, "test_copy_to_delete_marker_object") }},
		// 버저닝된 버킷에서 copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 추가 가능한지 확인하는 테스트
		{"test_object_versioning_copy_to_itself_with_metadata", func(t *testing.T) { testCopyCore(t, "test_object_versioning_copy_to_itself_with_metadata") }},
		// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인하는 테스트
		{"test_object_copy_to_itself_with_metadata_overwrite", func(t *testing.T) { testCopyCore(t, "test_object_copy_to_itself_with_metadata_overwrite") }},
		// 버저닝된 버킷에서 copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인하는 테스트
		{"test_object_versioning_copy_to_itself_with_metadata_overwrite", func(t *testing.T) { testCopyCore(t, "test_object_versioning_copy_to_itself_with_metadata_overwrite") }},
		// sse-c로 암호화된 오브젝트를 복사할때 Algorithm을 누락하면 오류가 발생하는지 확인하는 테스트 SDK V1은 SSE-C 차단 해제(BlockedEncryptionTypes)를 지원하지 않아 V2만 테스트한다.
		{"test_copy_revoke_sse_algorithm", testCopyRevokedSSEC},
		// UseChunkEncoding을 사용하는 오브젝트 복사 시 체크섬 계산 및 검증을 확인하는 테스트
		{"test_copy_object_checksum_use_chunk_encoding", testCopyChecksums},
		// 메타데이터와 태그가 복사되는지 확인하는 테스트
		{"test_copy_object_metadata_and_tags", testCopyMetadataTags},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func copySource(bucket, key, version string) *string {
	value := bucket + "/" + key
	if version != "" {
		value += "?versionId=" + url.QueryEscape(version)
	}
	return aws.String(value)
}
func copyCall(t *testing.T, client *s3.Client, input *s3.CopyObjectInput) *s3.CopyObjectOutput {
	t.Helper()
	out, err := client.CopyObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func assertCopied(t *testing.T, client *s3.Client, bucket, key, want string, input func(*s3.GetObjectInput)) {
	t.Helper()
	request := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if input != nil {
		input(request)
	}
	out, err := client.GetObject(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil || string(body) != want {
		t.Fatalf("body=%q want=%q err=%v", body, want, err)
	}
}

func testCopyCore(t *testing.T, name string) {
	t.Helper()
	if name == "test_object_copy_versioning_multipart_upload" {
		testCopyVersionedMultipart(t)
		return
	}
	s := newSuite(t)
	ctx := context.Background()
	sourceBucket, targetBucket := s.bucket(t), ""
	if name == "test_object_copy_canned_acl" {
		sourceBucket = ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	}
	targetBucket = sourceBucket
	source, target := name+"-source", name+"-target"
	body := source
	metadata := map[string]string{"source": "value1", "target": "value2"}
	contentType := "audio/ogg"
	if name == "test_object_copy_diff_bucket" {
		targetBucket = s.bucket(t)
	}
	if name == "test_object_copy_zero_size" {
		body = ""
	}
	if name == "test_object_copy_versioning_url_encoding" {
		enableVersioning(t, s, sourceBucket)
		source = "source?encoded"
		target = "target&encoded"
	}
	// [Version] object copy versioning bucket
	if name == "test_object_copy_versioning_bucket" || name == "test_object_copy_versioning_multipart_upload" || strings.HasPrefix(name, "test_object_versioning_") {
		enableVersioning(t, s, sourceBucket)
	}
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}
	if strings.Contains(name, "metadata") || strings.Contains(name, "content_type") {
		putInput.Metadata = metadata
		putInput.ContentType = aws.String(contentType)
	}
	putOut, err := s.client.PutObject(ctx, putInput)
	if err != nil {
		t.Fatal(err)
	}
	if name == "test_object_copy_bucket_not_found" {
		_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket+"-fake", source, "")})
		assertS3Error(t, err, 404, "NoSuchBucket")
		return
	}
	if name == "test_object_copy_key_not_found" {
		_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, "missing", "")})
		assertS3Error(t, err, 404, "NoSuchKey")
		return
	}
	if name == "test_object_copy_to_itself" {
		_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), CopySource: copySource(sourceBucket, source, "")})
		assertS3Error(t, err, 400, "InvalidRequest")
		return
	}
	if name == "test_object_copy_not_owned_bucket" {
		alt := s3Client(s.cfg, s.cfg.Alt)
		if s.cfg.Alt.AccessKey == "" {
			t.Skip("Alt User credentials required")
		}
		altBucket := strings.ToLower("copy-alt-" + uniqueBucketSuffix(t))
		if _, err = alt.CreateBucket(ctx, createBucketInput(s.cfg, altBucket)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if !s.cfg.NotDelete {
				_, _ = alt.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(altBucket)})
			}
		})
		_, err = alt.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(altBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")})
		assertS3Error(t, err, 403, "AccessDenied")
		return
	}
	input := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	if strings.Contains(name, "to_itself_with_metadata") {
		input.Key = aws.String(source)
		input.Metadata = map[string]string{"foo": "bar2"}
		input.MetadataDirective = types.MetadataDirectiveReplace
		target = source
	}
	if name == "test_object_copy_replacing_metadata" {
		input.Metadata = map[string]string{"key3": "value3", "key4": "value4"}
		input.MetadataDirective = types.MetadataDirectiveReplace
		input.ContentType = aws.String(contentType)
	}
	if name == "test_object_copy_canned_acl" {
		input.ACL = types.ObjectCannedACLPublicRead
	}
	if strings.Contains(name, "versioning") && aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(sourceBucket, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertCopied(t, s.client, targetBucket, target, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	if name == "test_object_copy_verify_content_type" || name == "test_object_copy_retaining_metadata" {
		if aws.ToString(head.ContentType) != contentType {
			t.Fatalf("ContentType=%q", aws.ToString(head.ContentType))
		}
	}
	if name == "test_object_copy_replacing_metadata" && head.Metadata["key3"] != "value3" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
	if name == "test_object_copy_canned_acl" {
		acl, aclErr := s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(targetBucket), Key: aws.String(target)})
		if aclErr != nil || len(acl.Grants) < 2 {
			t.Fatalf("grants=%v err=%v", acl.Grants, aclErr)
		}
	}
	if strings.Contains(name, "to_itself_with_metadata") && head.Metadata["foo"] != "bar2" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
	// object versioning 
	if strings.HasPrefix(name, "test_object_versioning_") {
		listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(sourceBucket), Prefix: aws.String(source)})
		if len(listed.Versions) != 2 {
			t.Fatalf("versions=%d", len(listed.Versions))
		}
	}
}

func testCopyVersionedMultipart(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	enableVersioning(t, s, sourceBucket)
	enableVersioning(t, s, targetBucket)
	body := deterministicBody(6 * 1024 * 1024)
	completeMultipart(t, s.client, sourceBucket, "source", body, false, map[string]string{"foo": "bar"})
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String("source")})
	if err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String("target"), CopySource: copySource(sourceBucket, "source", aws.ToString(head.VersionId))})
	target, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String("target")})
	if err != nil || aws.ToInt64(target.ContentLength) != int64(len(body)) || target.Metadata["foo"] != "bar" || aws.ToString(target.VersionId) == "" {
		t.Fatalf("target=%#v err=%v", target, err)
	}
	assertObjectBytes(t, s.client, targetBucket, "target", body)
}

func testCopyConditions(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := name+"-source", name+"-target"
	src := put(t, s, b, source, source, nil)
	dst := put(t, s, b, target, "old", nil)
	past, future := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC), time.Date(2100, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")}
	status := 0
	switch name {
	case "test_copy_object_if_match_good":
		input.CopySourceIfMatch = src.ETag
	case "test_copy_object_if_match_failed":
		input.CopySourceIfMatch = aws.String("bad")
		status = 412
	case "test_copy_object_if_none_match_good":
		input.CopySourceIfNoneMatch = aws.String("bad")
	case "test_copy_object_if_none_match_failed":
		input.CopySourceIfNoneMatch = src.ETag
		status = 412
	case "test_copy_object_if_modified_since_good":
		input.CopySourceIfModifiedSince = &past
	case "test_copy_object_if_modified_since_failed":
		input.CopySourceIfModifiedSince = &future
		status = 412
	case "test_copy_object_if_unmodified_since_good":
		input.CopySourceIfUnmodifiedSince = &future
	case "test_copy_object_if_unmodified_since_failed":
		input.CopySourceIfUnmodifiedSince = &past
		status = 412
	case "test_copy_object_if_match_with_if_unmodified_since":
		input.CopySourceIfMatch, input.CopySourceIfUnmodifiedSince = src.ETag, &past
	case "test_copy_object_if_none_match_with_if_modified_since":
		input.CopySourceIfNoneMatch, input.CopySourceIfModifiedSince = src.ETag, &past
		status = 412
	case "test_copy_object_if_match_and_if_none_match":
		input.CopySourceIfMatch, input.CopySourceIfNoneMatch = src.ETag, src.ETag
		status = 412
	case "test_copy_object_if_match_and_if_none_match_any":
		input.CopySourceIfMatch, input.CopySourceIfNoneMatch = src.ETag, aws.String("*")
		status = 412
	case "test_copy_object_destination_if_match_good":
		input.IfMatch = dst.ETag
	case "test_copy_object_destination_if_match_failed":
		input.IfMatch = aws.String("bad")
		status = 412
	case "test_copy_object_destination_if_none_match_good":
		input.Key = aws.String(target + "-new")
		target += "-new"
		input.IfNoneMatch = aws.String("*")
	case "test_copy_object_destination_if_none_match_failed":
		input.IfNoneMatch = aws.String("*")
		status = 412
	case "test_copy_object_destination_if_match_and_if_none_match":
		input.IfMatch, input.IfNoneMatch = dst.ETag, aws.String("*")
		status = 501
	case "test_copy_object_destination_if_match_and_if_none_match_any":
		input.IfMatch, input.IfNoneMatch = dst.ETag, dst.ETag
		status = 501
	case "test_copy_object_source_if_match_with_destination_if_none_match":
		input.CopySourceIfMatch, input.IfNoneMatch = src.ETag, aws.String("*")
		input.Key = aws.String(target + "-new")
		target += "-new"
	}
	_, err := s.client.CopyObject(context.Background(), input)
	if status != 0 {
		assertHTTPError(t, err, status)
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

func testCopyEncryption(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	// copy to 
	if strings.HasPrefix(name, "test_copy_to_") {
		sourceMode := "normal"
		if strings.Contains(name, "sse_s3") {
			sourceMode = "sse-s3"
		}
		if strings.Contains(name, "sse_c") {
			sourceMode = "sse-c"
		}
		for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
			targetMode := targetMode
			t.Run(targetMode, func(t *testing.T) { copyEncryptionModes(t, s, sourceMode, targetMode, false, false) })
		}
		return
	}
	sourceBucketDefault := strings.Contains(name, "encryption_bucket")
	sourceExplicit := strings.Contains(name, "encryption_src") || strings.Contains(name, "encryption_bucket_and_obj")
	targetBucketDefault := strings.Contains(name, "_to_encryption_bucket")
	targetExplicit := strings.HasSuffix(name, "encryption_obj") || strings.HasSuffix(name, "and_obj")
	sourceMode, targetMode := "normal", "normal"
	if sourceExplicit {
		sourceMode = "sse-s3"
	}
	if targetExplicit {
		targetMode = "sse-s3"
	}
	copyEncryptionModes(t, s, sourceMode, targetMode, sourceBucketDefault, targetBucketDefault)
}

func copyEncryptionModes(t *testing.T, s *suite, sourceMode, targetMode string, sourceBucketDefault, targetBucketDefault bool) {
	t.Helper()
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	if sourceBucketDefault {
		putAESBucketEncryption(t, s.client, sourceBucket)
	}
	if targetBucketDefault {
		putAESBucketEncryption(t, s.client, targetBucket)
	}
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if sourceMode == "sse-s3" {
		putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	if sourceMode == "sse-c" {
		putInput = sseCPutInput(sourceBucket, source, body)
	}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	if sourceMode == "sse-c" {
		copyInput.CopySourceSSECustomerAlgorithm = aws.String(sseCAlgorithm)
		copyInput.CopySourceSSECustomerKey = aws.String(sseCKey)
		copyInput.CopySourceSSECustomerKeyMD5 = aws.String(sseCKeyMD5)
	}
	if targetMode == "sse-s3" {
		copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	if targetMode == "sse-c" {
		copyInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
		copyInput.SSECustomerKey = aws.String(sseCKey)
		copyInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
	}
	copyCall(t, s.client, copyInput)
	var getOptions func(*s3.GetObjectInput)
	if targetMode == "sse-c" {
		getOptions = func(in *s3.GetObjectInput) {
			in.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
			in.SSECustomerKey = aws.String(sseCKey)
			in.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		}
	}
	assertCopied(t, s.client, targetBucket, target, string(body), getOptions)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	if targetMode == "sse-c" {
		headInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
		headInput.SSECustomerKey = aws.String(sseCKey)
		headInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
	}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := targetMode != "normal" || targetBucketDefault
	if encrypted && targetMode != "sse-c" && head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

func testCopyDeleted(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	if strings.Contains(name, "marker") {
		enableVersioning(t, s, b)
	}
	put(t, s, b, "source", "body", nil)
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("source")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target"), CopySource: copySource(b, "source", "")})
	assertS3Error(t, err, 404, "NoSuchKey")
}

func testCopyChecksums(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	for _, algorithm := range []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256} {
		body := []byte(string(algorithm))
		input := &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("source-" + string(algorithm)), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm}
		setPutChecksum(input, algorithm, body, false)
		if _, err := s.client.PutObject(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		out := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target-" + string(algorithm)), CopySource: copySource(b, "source-"+string(algorithm), "")})
		if out.CopyObjectResult == nil {
			t.Fatal("missing CopyObjectResult")
		}
		assertCopied(t, s.client, b, "target-"+string(algorithm), string(body), nil)
	}
}

func testCopyMetadataTags(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "source", "target"
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source)), Metadata: map[string]string{"foo": "bar"}, Tagging: aws.String("tag1=value1")})
	if err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil || head.Metadata["foo"] != "bar" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
	tags, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil || len(tags.TagSet) != 1 || aws.ToString(tags.TagSet[0].Value) != "value1" {
		t.Fatalf("tags=%v err=%v", tags.TagSet, err)
	}
}

func testCopyRevokedSSEC(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	_, err := s.client.PutObject(context.Background(), sseCPutInput(b, "source", []byte("body")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target"), CopySource: copySource(b, "source", ""), CopySourceSSECustomerKey: aws.String(sseCKey), CopySourceSSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	assertHTTPError(t, err, 400)
}
