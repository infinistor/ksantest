package s3tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestLock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷을 생성한 후 오브젝트의 잠금 설정을 활성화 할 수 있는지 확인
		{"test_created_bucket_enable_object_lock", func(t *testing.T) { testLockConfiguration(t, "test_created_bucket_enable_object_lock") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인
		{"test_object_lock_put_obj_lock", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_put_obj_lock") }},
		// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패
		{"test_object_lock_put_obj_lock_invalid_bucket", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_put_obj_lock_invalid_bucket") }},
		// [버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패
		{"test_object_lock_put_obj_lock_with_days_and_years", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_put_obj_lock_with_days_and_years") }},
		// [버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패
		{"test_object_lock_put_obj_lock_invalid_days", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_put_obj_lock_invalid_days") }},
		// [버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패
		{"test_object_lock_put_obj_lock_invalid_years", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_put_obj_lock_invalid_years") }},
		// [버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패
		{"test_object_lock_put_obj_lock_invalid_mode", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_put_obj_lock_invalid_mode") }},
		// [버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패
		{"test_object_lock_put_obj_lock_invalid_status", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_put_obj_lock_invalid_status") }},
		// [버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패
		{"test_object_lock_suspend_versioning", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_suspend_versioning") }},
		// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인
		{"test_object_lock_get_obj_lock", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_get_obj_lock") }},
		// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 오브젝트 업로드가 정상적으로 이루어지는지 확인
		{"test_object_lock_put_object", func(t *testing.T) { testLockObject(t, "test_object_lock_put_object") }},
		// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 오브젝트 복제가 정상적으로 이루어지는지 확인
		{"test_object_lock_copy_object", testLockCopy},
		// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 멀티파트 업로드가 정상적으로 이루어지는지 확인
		{"test_object_lock_multipart", testLockMultipart},
		// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 오브젝트 업로드시 md5 값이 없을 경우 업로드 실패 확인
		{"test_object_lock_md5", testLockMD5},
		// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패
		{"test_object_lock_get_obj_lock_invalid_bucket", func(t *testing.T) { testLockConfiguration(t, "test_object_lock_get_obj_lock_invalid_bucket") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인
		{"test_object_lock_put_obj_retention", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention") }},
		// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패
		{"test_object_lock_put_obj_retention_invalid_bucket", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention_invalid_bucket") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패
		{"test_object_lock_put_obj_retention_invalid_mode", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention_invalid_mode") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인
		{"test_object_lock_get_obj_retention", func(t *testing.T) { testLockRetention(t, "test_object_lock_get_obj_retention") }},
		// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패
		{"test_object_lock_get_obj_retention_invalid_bucket", func(t *testing.T) { testLockRetention(t, "test_object_lock_get_obj_retention_invalid_bucket") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인
		{"test_object_lock_put_obj_retention_versionid", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention_versionid") }},
		// [버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인
		{"test_object_lock_put_obj_retention_override_default_retention", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention_override_default_retention") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인
		{"test_object_lock_put_obj_retention_increase_period", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention_increase_period") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인
		{"test_object_lock_put_obj_retention_shorten_period", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention_shorten_period") }},
		// [버킷의 Lock옵션을 활성화] 바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인
		{"test_object_lock_put_obj_retention_shorten_period_bypass", func(t *testing.T) { testLockRetention(t, "test_object_lock_put_obj_retention_shorten_period_bypass") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인
		{"test_object_lock_delete_object_with_retention", func(t *testing.T) { testLockRetention(t, "test_object_lock_delete_object_with_retention") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 Lock 유지기한이 있어도 바이패스를 통해 삭제가 가능한지 확인
		{"test_object_lock_delete_object_with_retention_bypass", func(t *testing.T) { testLockRetention(t, "test_object_lock_delete_object_with_retention_bypass") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 Lock 유지기한이 있어도 바이패스를 통해 삭제가 가능한지 확인
		{"test_object_lock_delete_objects_with_retention_bypass", func(t *testing.T) { testLockRetention(t, "test_object_lock_delete_objects_with_retention_bypass") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인
		{"test_object_lock_put_legal_hold", func(t *testing.T) { testLockLegalHold(t, "test_object_lock_put_legal_hold") }},
		// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인
		{"test_object_lock_put_legal_hold_invalid_bucket", func(t *testing.T) { testLockLegalHold(t, "test_object_lock_put_legal_hold_invalid_bucket") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold에 잘못된 값을 넣을 경우 실패 확인
		{"test_object_lock_put_legal_hold_invalid_status", func(t *testing.T) { testLockLegalHold(t, "test_object_lock_put_legal_hold_invalid_status") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인
		{"test_object_lock_get_legal_hold", func(t *testing.T) { testLockLegalHold(t, "test_object_lock_get_legal_hold") }},
		// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold설정 조회 실패 확인
		{"test_object_lock_get_legal_hold_invalid_bucket", func(t *testing.T) { testLockLegalHold(t, "test_object_lock_get_legal_hold_invalid_bucket") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인
		{"test_object_lock_delete_object_with_legal_hold_on", func(t *testing.T) { testLockLegalHold(t, "test_object_lock_delete_object_with_legal_hold_on") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인
		{"test_object_lock_delete_object_with_legal_hold_off", func(t *testing.T) { testLockLegalHold(t, "test_object_lock_delete_object_with_legal_hold_off") }},
		// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인
		{"test_object_lock_get_obj_metadata", func(t *testing.T) { testLockObject(t, "test_object_lock_get_obj_metadata") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func newLockBucket(t *testing.T, s *suite) string {
	t.Helper()
	name := newBucketName(s.cfg.BucketPrefix)
	input := createBucketInput(s.cfg, name)
	input.ObjectLockEnabledForBucket = aws.Bool(true)
	_, err := s.client.CreateBucket(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			cleanupBucket(t, s, name)
		}
	})
	return name
}
func lockConfiguration(mode types.ObjectLockRetentionMode, days, years int32) *types.ObjectLockConfiguration {
	retention := &types.DefaultRetention{Mode: mode}
	if days != 0 {
		retention.Days = aws.Int32(days)
	}
	if years != 0 {
		retention.Years = aws.Int32(years)
	}
	return &types.ObjectLockConfiguration{
		ObjectLockEnabled: types.ObjectLockEnabledEnabled,
		Rule:              &types.ObjectLockRule{DefaultRetention: retention},
	}
}
func putLockConfiguration(t *testing.T, s *suite, bucket string, cfg *types.ObjectLockConfiguration) error {
	t.Helper()
	_, err := s.client.PutObjectLockConfiguration(context.Background(), &s3.PutObjectLockConfigurationInput{Bucket: aws.String(bucket), ObjectLockConfiguration: cfg})
	return err
}

func testLockConfiguration(t *testing.T, name string) {
	s := newSuite(t)
	if name == "test_created_bucket_enable_object_lock" {
		b := s.bucket(t)
		enableVersioning(t, s, b)
		err := putLockConfiguration(t, s, b, &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled})
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if strings.Contains(name, "invalid_bucket") || name == "test_object_lock_get_obj_lock_invalid_bucket" {
		b := s.bucket(t)
		if name == "test_object_lock_get_obj_lock_invalid_bucket" {
			_, err := s.client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
			assertS3Error(t, err, 404, "ObjectLockConfigurationNotFoundError")
		} else {
			err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 0, 1))
			assertS3Error(t, err, 409, "InvalidBucketState")
		}
		return
	}
	b := newLockBucket(t, s)
	if name == "test_object_lock_suspend_versioning" {
		_, err := s.client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(b), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended}})
		assertS3Error(t, err, 409, "InvalidBucketState")
		return
	}
	cfg := lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)
	status, code := 0, ""
	switch name {
	case "test_object_lock_put_obj_lock":
		cfg = lockConfiguration(types.ObjectLockRetentionModeCompliance, 0, 1)
	case "test_object_lock_put_obj_lock_with_days_and_years":
		cfg = lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 1)
		status, code = 400, "MalformedXML"
	case "test_object_lock_put_obj_lock_invalid_days":
		cfg = lockConfiguration(types.ObjectLockRetentionModeGovernance, 0, 0)
		cfg.Rule.DefaultRetention.Days = aws.Int32(0)
		status, code = 400, "InvalidArgument"
	case "test_object_lock_put_obj_lock_invalid_years":
		cfg = lockConfiguration(types.ObjectLockRetentionModeGovernance, 0, -1)
		status, code = 400, "InvalidArgument"
	case "test_object_lock_put_obj_lock_invalid_mode":
		cfg = lockConfiguration(types.ObjectLockRetentionMode("invalid"), 0, 1)
		status, code = 400, "MalformedXML"
	case "test_object_lock_put_obj_lock_invalid_status":
		cfg.ObjectLockEnabled = types.ObjectLockEnabled("Disabled")
		status, code = 400, "MalformedXML"
	}
	err := putLockConfiguration(t, s, b, cfg)
	if status != 0 {
		assertS3Error(t, err, status, code)
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
	if err != nil || out.ObjectLockConfiguration == nil || out.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatalf("configuration=%#v err=%v", out.ObjectLockConfiguration, err)
	}
}

func putLockedObject(t *testing.T, s *suite, bucket, key string) (*s3.PutObjectOutput, time.Time) {
	t.Helper()
	until := time.Now().UTC().Add(48 * time.Hour)
	body := []byte(key)
	sum := md5.Sum(body)
	out, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(sum[:])), ObjectLockMode: types.ObjectLockModeGovernance, ObjectLockRetainUntilDate: &until})
	if err != nil {
		t.Fatal(err)
	}
	return out, until
}

func testLockObject(t *testing.T, name string) {
	s := newSuite(t)
	b := newLockBucket(t, s)
	if name == "test_object_lock_put_object" {
		if err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)); err != nil {
			t.Fatal(err)
		}
		out := putWithMD5(t, s, b, name, name)
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId})
		if err != nil || head.ObjectLockMode != types.ObjectLockModeGovernance || head.ObjectLockRetainUntilDate == nil {
			t.Fatalf("head=%#v err=%v", head, err)
		}
		_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId})
		assertS3Error(t, err, 403, "AccessDenied")
		_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)})
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	out, until := putLockedObject(t, s, b, name)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId})
	if err != nil || head.ObjectLockMode != types.ObjectLockModeGovernance || head.ObjectLockRetainUntilDate == nil {
		t.Fatalf("head=%#v err=%v", head, err)
	}
	if name == "test_object_lock_get_obj_metadata" && !head.ObjectLockRetainUntilDate.Truncate(time.Millisecond).Equal(until.Truncate(time.Millisecond)) {
		t.Fatalf("retain=%v want=%v", head.ObjectLockRetainUntilDate, until)
	}
	_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
}

func testLockCopy(t *testing.T) {
	s := newSuite(t)
	locked, plain := newLockBucket(t, s), s.bucket(t)
	if err := putLockConfiguration(t, s, locked, lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)); err != nil {
		t.Fatal(err)
	}
	lockedOut := putWithMD5(t, s, locked, "locked", "locked")
	put(t, s, plain, "plain", "plain", nil)
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(plain), Key: aws.String("from-locked"), CopySource: copySource(locked, "locked", "")})
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(locked), Key: aws.String("from-plain"), CopySource: copySource(plain, "plain", "")})
	plainHead, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(plain), Key: aws.String("from-locked")})
	if err != nil || plainHead.ObjectLockMode != "" {
		t.Fatalf("plain lock=%q err=%v", plainHead.ObjectLockMode, err)
	}
	lockedHead, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(locked), Key: aws.String("from-plain")})
	if err != nil || lockedHead.ObjectLockMode == "" {
		t.Fatalf("locked mode=%q err=%v", lockedHead.ObjectLockMode, err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(locked), Key: aws.String("locked"), VersionId: lockedOut.VersionId, BypassGovernanceRetention: aws.Bool(true)})
}

func putWithMD5(t *testing.T, s *suite, bucket, key, value string) *s3.PutObjectOutput {
	t.Helper()
	body := []byte(value)
	sum := md5.Sum(body)
	out, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		Body:       bytes.NewReader(body),
		ContentMD5: aws.String(base64.StdEncoding.EncodeToString(sum[:])),
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func testLockMultipart(t *testing.T) {
	s := newSuite(t)
	b := newLockBucket(t, s)
	if err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)); err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("m"), 1024*1024)
	sum := md5.Sum(body)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("key")})
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String("key"), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(body), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(sum[:]))})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("key"), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("key")})
	if err != nil || head.ObjectLockMode != types.ObjectLockModeGovernance {
		t.Fatalf("mode=%q err=%v", head.ObjectLockMode, err)
	}
}

func testLockMD5(t *testing.T) {
	s := newSuite(t)
	b := newLockBucket(t, s)
	if err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("key"), Body: bytes.NewReader([]byte("body"))})
	assertS3Error(t, err, 400, "InvalidRequest")
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("multipart")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String("multipart"), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("body"))})
	assertS3Error(t, err, 400, "InvalidRequest")
}

func retention(mode types.ObjectLockRetentionMode, until time.Time) *types.ObjectLockRetention {
	return &types.ObjectLockRetention{Mode: mode, RetainUntilDate: &until}
}
func testLockRetention(t *testing.T, name string) {
	s := newSuite(t)
	invalidBucket := strings.Contains(name, "invalid_bucket")
	b := s.bucket(t)
	if !invalidBucket {
		b = newLockBucket(t, s)
	}
	key := name
	var out *s3.PutObjectOutput
	if name == "test_object_lock_put_obj_retention_override_default_retention" {
		if err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)); err != nil {
			t.Fatal(err)
		}
		out = putWithMD5(t, s, b, key, key)
	} else {
		out = put(t, s, b, key, key, nil)
	}
	early, late := time.Now().UTC().Add(48*time.Hour), time.Now().UTC().Add(96*time.Hour)
	if name == "test_object_lock_put_obj_retention_override_default_retention" {
		// Java uses a far-future retain-until without VersionId / BypassGovernance.
		early = time.Date(2030, 2, 1, 0, 0, 0, 0, time.UTC)
	}
	if strings.Contains(name, "invalid_bucket") {
		if strings.Contains(name, "get_obj") {
			_, err := s.client.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key)})
			assertS3Error(t, err, 400, "InvalidRequest")
		} else {
			_, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), Retention: retention(types.ObjectLockRetentionModeGovernance, early)})
			assertS3Error(t, err, 400, "InvalidRequest")
		}
		return
	}
	if strings.Contains(name, "invalid_mode") {
		_, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), Retention: retention(types.ObjectLockRetentionMode("invalid"), early)})
		assertS3Error(t, err, 400, "MalformedXML")
		return
	}
	version := out.VersionId
	if strings.Contains(name, "versionid") {
		put(t, s, b, key, "second", nil)
	}
	firstDate := early
	if strings.Contains(name, "increase") || strings.Contains(name, "shorten") {
		firstDate = late
	}
	_, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(b), Key: aws.String(key),
		VersionId: func() *string {
			if name == "test_object_lock_put_obj_retention_override_default_retention" {
				return nil // Java omits VersionId here
			}
			return version
		}(),
		Retention: retention(types.ObjectLockRetentionModeGovernance, firstDate),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(name, "shorten_period") {
		input := &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}
		if strings.HasSuffix(name, "bypass") {
			input.BypassGovernanceRetention = aws.Bool(true)
		}
		_, err = s.client.PutObjectRetention(context.Background(), input)
		if !strings.HasSuffix(name, "bypass") {
			assertS3Error(t, err, 403, "AccessDenied")
		} else if err != nil {
			t.Fatal(err)
		}
	} else if strings.Contains(name, "increase") {
		_, err = s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, late)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(name, "get_obj_retention") || strings.Contains(name, "override") || strings.Contains(name, "increase") || strings.Contains(name, "shorten_period_bypass") {
		got, err := s.client.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
		if err != nil || got.Retention == nil {
			t.Fatalf("retention=%#v err=%v", got.Retention, err)
		}
	}
	if strings.Contains(name, "delete_object_with_retention") && !strings.HasSuffix(name, "bypass") {
		_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
		assertS3Error(t, err, 403, "AccessDenied")
	}
	if strings.Contains(name, "delete_objects_with_retention_bypass") {
		var objects []types.ObjectIdentifier
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("key-%d", i)
			p := put(t, s, b, k, k, nil)
			_, err = s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(k), VersionId: p.VersionId, Retention: retention(types.ObjectLockRetentionModeGovernance, early)})
			if err != nil {
				t.Fatal(err)
			}
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(k), VersionId: p.VersionId})
		}
		_, err = s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(b), BypassGovernanceRetention: aws.Bool(true), Delete: &types.Delete{Objects: objects}})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

func testLockLegalHold(t *testing.T, name string) {
	s := newSuite(t)
	invalidBucket := strings.Contains(name, "invalid_bucket")
	b := s.bucket(t)
	if !invalidBucket {
		b = newLockBucket(t, s)
	}
	key := name
	out := put(t, s, b, key, key, nil)
	if invalidBucket {
		if strings.Contains(name, "get_legal") {
			_, err := s.client.GetObjectLegalHold(context.Background(), &s3.GetObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key)})
			assertS3Error(t, err, 400, "InvalidRequest")
		} else {
			_, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}})
			assertS3Error(t, err, 400, "InvalidRequest")
		}
		return
	}
	status := types.ObjectLockLegalHoldStatusOn
	if strings.Contains(name, "invalid_status") {
		status = types.ObjectLockLegalHoldStatus("abc")
	}
	_, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: status}})
	if strings.Contains(name, "invalid_status") {
		assertS3Error(t, err, 400, "MalformedXML")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(name, "get_legal_hold") {
		got, err := s.client.GetObjectLegalHold(context.Background(), &s3.GetObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId})
		if err != nil || got.LegalHold == nil || got.LegalHold.Status != types.ObjectLockLegalHoldStatusOn {
			t.Fatalf("hold=%#v err=%v", got.LegalHold, err)
		}
	}
	if strings.Contains(name, "delete_object_with_legal_hold_on") {
		_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)})
		assertS3Error(t, err, 403, "AccessDenied")
	}
	_, err = s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
}
