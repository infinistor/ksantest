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

func TestObjectCopyZeroSize(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_zero_size")
}
func TestObjectCopySameBucket(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_same_bucket")
}
func TestObjectCopyVerifyContentType(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_verify_content_type")
}
func TestObjectCopyToItself(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_to_itself")
}
func TestObjectCopyToItselfWithMetadata(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_to_itself_with_metadata")
}
func TestObjectCopyDiffBucket(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_diff_bucket")
}
func TestObjectCopyNotOwnedBucket(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_not_owned_bucket")
}
func TestObjectCopyNotOwnedObjectBucket(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_not_owned_object_bucket")
}
func TestObjectCopyCannedAcl(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_canned_acl")
}
func TestObjectCopyRetainingMetadata(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_retaining_metadata")
}
func TestObjectCopyReplacingMetadata(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_replacing_metadata")
}
func TestObjectCopyBucketNotFound(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_bucket_not_found")
}
func TestObjectCopyKeyNotFound(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_key_not_found")
}
func TestObjectCopyVersioningBucket(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_versioning_bucket")
}
func TestObjectCopyVersioningUrlEncoding(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_versioning_url_encoding")
}
func TestObjectCopyVersioningMultipartUpload(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_versioning_multipart_upload")
}
func TestCopyObjectIfMatchGood(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_match_good")
}
func TestCopyObjectIfMatchFailed(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_match_failed")
}
func TestCopyObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_none_match_good")
}
func TestCopyObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_none_match_failed")
}
func TestCopyObjectIfModifiedSinceGood(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_modified_since_good")
}
func TestCopyObjectIfModifiedSinceFailed(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_modified_since_failed")
}
func TestCopyObjectIfUnmodifiedSinceGood(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_unmodified_since_good")
}
func TestCopyObjectIfUnmodifiedSinceFailed(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_unmodified_since_failed")
}
func TestCopyObjectIfMatchWithIfUnmodifiedSince(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_match_with_if_unmodified_since")
}
func TestCopyObjectIfNoneMatchWithIfModifiedSince(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_none_match_with_if_modified_since")
}
func TestCopyObjectIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_match_and_if_none_match")
}
func TestCopyObjectIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_if_match_and_if_none_match_any")
}
func TestCopyObjectDestinationIfMatchGood(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_destination_if_match_good")
}
func TestCopyObjectDestinationIfMatchFailed(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_destination_if_match_failed")
}
func TestCopyObjectDestinationIfNoneMatchGood(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_destination_if_none_match_good")
}
func TestCopyObjectDestinationIfNoneMatchFailed(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_destination_if_none_match_failed")
}
func TestCopyObjectDestinationIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_destination_if_match_and_if_none_match")
}
func TestCopyObjectDestinationIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_destination_if_match_and_if_none_match_any")
}
func TestCopyObjectSourceIfMatchWithDestinationIfNoneMatch(t *testing.T) {
	t.Parallel()

	testCopyConditions(t, "test_copy_object_source_if_match_with_destination_if_none_match")
}
func TestCopyNorSrcToNorBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_nor_src_to_nor_bucket_and_obj")
}
func TestCopyNorSrcToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_nor_src_to_nor_bucket_encryption_obj")
}
func TestCopyNorSrcToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_nor_src_to_encryption_bucket_nor_obj")
}
func TestCopyNorSrcToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_nor_src_to_encryption_bucket_and_obj")
}
func TestCopyEncryptionSrcToNorBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_src_to_nor_bucket_and_obj")
}
func TestCopyEncryptionSrcToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_src_to_nor_bucket_encryption_obj")
}
func TestCopyEncryptionSrcToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_src_to_encryption_bucket_nor_obj")
}
func TestCopyEncryptionSrcToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_src_to_encryption_bucket_and_obj")
}
func TestCopyEncryptionBucketNorObjToNorBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj")
}
func TestCopyEncryptionBucketNorObjToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj")
}
func TestCopyEncryptionBucketNorObjToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj")
}
func TestCopyEncryptionBucketNorObjToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj")
}
func TestCopyEncryptionBucketAndObjToNorBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj")
}
func TestCopyEncryptionBucketAndObjToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj")
}
func TestCopyEncryptionBucketAndObjToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj")
}
func TestCopyEncryptionBucketAndObjToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj")
}
func TestCopyToNormalSource(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_to_normal_source")
}
func TestCopyToSseS3Source(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_to_sse_s3_source")
}
func TestCopyToSseCSource(t *testing.T) {
	t.Parallel()

	testCopyEncryption(t, "test_copy_to_sse_c_source")
}
func TestCopyToDeletedObject(t *testing.T) {
	t.Parallel()

	testCopyDeleted(t, "test_copy_to_deleted_object")
}
func TestCopyToDeleteMarkerObject(t *testing.T) {
	t.Parallel()

	testCopyDeleted(t, "test_copy_to_delete_marker_object")
}
func TestObjectVersioningCopyToItselfWithMetadata(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_versioning_copy_to_itself_with_metadata")
}
func TestObjectCopyToItselfWithMetadataOverwrite(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_copy_to_itself_with_metadata_overwrite")
}
func TestObjectVersioningCopyToItselfWithMetadataOverwrite(t *testing.T) {
	t.Parallel()

	testCopyCore(t, "test_object_versioning_copy_to_itself_with_metadata_overwrite")
}
func TestCopyRevokeSseAlgorithm(t *testing.T) {
	t.Parallel()

	testCopyRevokedSSEC(t)
}
func TestCopyObjectChecksumUseChunkEncoding(t *testing.T) {
	t.Parallel()

	testCopyChecksums(t)
}
func TestCopyObjectMetadataAndTags(t *testing.T) {
	t.Parallel()

	testCopyMetadataTags(t)
}

func copySource(bucket, key, version string) *string {

	encKey := strings.ReplaceAll(url.PathEscape(key), "%2F", "/")
	value := url.PathEscape(bucket) + "/" + encKey
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

		head := headObject(t, s.client, b, source)
		after := head.LastModified.Add(time.Second)
		input.CopySourceIfModifiedSince = &after
		time.Sleep(time.Second)
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

	if strings.HasPrefix(name, "test_copy_to_") {
		sourceMode := "normal"
		if strings.Contains(name, "sse_s3") {
			sourceMode = "sse-s3"
		}
		if strings.Contains(name, "sse_c") {
			sourceMode = "sse-c"
		}

		for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
			copyEncryptionModes(t, s, sourceMode, targetMode, false, false)
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
	if sourceMode == "sse-c" {
		unblockSseC(t, s, sourceBucket)
	}
	if targetMode == "sse-c" {
		unblockSseC(t, s, targetBucket)
	}
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
	b := ssecBucket(t, s)
	_, err := s.client.PutObject(context.Background(), sseCPutInput(b, "source", []byte("body")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target"), CopySource: copySource(b, "source", ""), CopySourceSSECustomerKey: aws.String(sseCKey), CopySourceSSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	assertHTTPError(t, err, 400)
}
