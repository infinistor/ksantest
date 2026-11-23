package s3tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Object Lock 없이 생성한 버킷에 버저닝을 활성화한 뒤 오브젝트 잠금을 활성화할 수 있는지 확인
func TestCreatedBucketEnableObjectLock(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 1)
	enableVersioning(t, s, b)
	if err := putLockConfiguration(t, s, b, &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled}); err != nil {
		t.Fatal(err)
	}
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인
func TestObjectLockPutObjLock(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	cfg := lockConfiguration(types.ObjectLockRetentionModeCompliance, 0, 1)
	if err := putLockConfiguration(t, s, b, cfg); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
	if err != nil || out.ObjectLockConfiguration == nil || out.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatalf("configuration=%#v err=%v", out.ObjectLockConfiguration, err)
	}
}

// 버저닝을 활성화하지 않은 버킷에 오브젝트 잠금을 설정할 경우 실패
func TestObjectLockPutObjLockInvalidBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 3)
	err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 0, 1))
	assertS3Error(t, err, 409, "InvalidBucketState")
}

// [버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패
func TestObjectLockPutObjLockWithDaysAndYears(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	cfg := lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 1)
	err := putLockConfiguration(t, s, b, cfg)
	assertS3Error(t, err, 400, "MalformedXML")
}

// [버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패
func TestObjectLockPutObjLockInvalidDays(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	cfg := lockConfiguration(types.ObjectLockRetentionModeGovernance, 0, 0)
	cfg.Rule.DefaultRetention.Days = aws.Int32(0)
	err := putLockConfiguration(t, s, b, cfg)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// [버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패
func TestObjectLockPutObjLockInvalidYears(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	cfg := lockConfiguration(types.ObjectLockRetentionModeGovernance, 0, -1)
	err := putLockConfiguration(t, s, b, cfg)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// [버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패
func TestObjectLockPutObjLockInvalidMode(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	cfg := lockConfiguration(types.ObjectLockRetentionMode("invalid"), 0, 1)
	err := putLockConfiguration(t, s, b, cfg)
	assertS3Error(t, err, 400, "MalformedXML")
}

// [버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패
func TestObjectLockPutObjLockInvalidStatus(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	cfg := lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)
	cfg.ObjectLockEnabled = types.ObjectLockEnabled("Disabled")
	err := putLockConfiguration(t, s, b, cfg)
	assertS3Error(t, err, 400, "MalformedXML")
}

// [버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패
func TestObjectLockSuspendVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	_, err := s.client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(b), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended}})
	assertS3Error(t, err, 409, "InvalidBucketState")
}

// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인
func TestObjectLockGetObjLock(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	cfg := lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)
	if err := putLockConfiguration(t, s, b, cfg); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
	if err != nil || out.ObjectLockConfiguration == nil || out.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatalf("configuration=%#v err=%v", out.ObjectLockConfiguration, err)
	}
}

// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 오브젝트 업로드가 정상적으로 이루어지는지 확인
func TestObjectLockPutObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	name := "TestObjectLockPutObject"
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
}

// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 오브젝트 복제가 정상적으로 이루어지는지 확인
func TestObjectLockCopyObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	locked, plain := newLockBucket(t, s), s.bucket(t, 12)
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

// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 멀티파트 업로드가 정상적으로 이루어지는지 확인
func TestObjectLockMultipart(t *testing.T) {
	t.Parallel()
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
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("key"), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("key")})
	if err != nil || head.ObjectLockMode != types.ObjectLockModeGovernance {
		t.Fatalf("mode=%q err=%v", head.ObjectLockMode, err)
	}
}

// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 있을때 오브젝트 업로드시 md5 값이 없을 경우 업로드 실패 확인
func TestObjectLockMD5(t *testing.T) {
	t.Parallel()
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

// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패
func TestObjectLockGetObjLockInvalidBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 15)
	_, err := s.client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 404, "ObjectLockConfigurationNotFoundError")
}

// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인
func TestObjectLockPutObjRetention(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutObjRetention"
	out := put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
		t.Fatal(err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패
func TestObjectLockPutObjRetentionInvalidBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 17)
	key := "TestObjectLockPutObjRetentionInvalidBucket"
	put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	_, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), Retention: retention(types.ObjectLockRetentionModeGovernance, early)})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패
func TestObjectLockPutObjRetentionInvalidMode(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutObjRetentionInvalidMode"
	put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	_, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), Retention: retention(types.ObjectLockRetentionMode("invalid"), early)})
	assertS3Error(t, err, 400, "MalformedXML")
}

// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인
func TestObjectLockGetObjRetention(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockGetObjRetention"
	out := put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
		t.Fatal(err)
	}
	got, err := s.client.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
	if err != nil || got.Retention == nil {
		t.Fatalf("retention=%#v err=%v", got.Retention, err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패
func TestObjectLockGetObjRetentionInvalidBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 20)
	key := "TestObjectLockGetObjRetentionInvalidBucket"
	put(t, s, b, key, key, nil)
	_, err := s.client.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인
func TestObjectLockPutObjRetentionVersionid(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutObjRetentionVersionid"
	out := put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	version := out.VersionId
	put(t, s, b, key, "second", nil)
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
		t.Fatal(err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인
func TestObjectLockPutObjRetentionOverrideDefaultRetention(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutObjRetentionOverrideDefaultRetention"
	if err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)); err != nil {
		t.Fatal(err)
	}
	out := putWithMD5(t, s, b, key, key)
	version := out.VersionId
	early := time.Date(2030, 2, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: nil, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
		t.Fatal(err)
	}
	got, err := s.client.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
	if err != nil || got.Retention == nil {
		t.Fatalf("retention=%#v err=%v", got.Retention, err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인
func TestObjectLockPutObjRetentionIncreasePeriod(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutObjRetentionIncreasePeriod"
	out := put(t, s, b, key, key, nil)
	late := time.Now().UTC().Add(96 * time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, late)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, late)}); err != nil {
		t.Fatal(err)
	}
	got, err := s.client.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
	if err != nil || got.Retention == nil {
		t.Fatalf("retention=%#v err=%v", got.Retention, err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인
func TestObjectLockPutObjRetentionShortenPeriod(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutObjRetentionShortenPeriod"
	out := put(t, s, b, key, key, nil)
	early, late := time.Now().UTC().Add(48*time.Hour), time.Now().UTC().Add(96*time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, late)}); err != nil {
		t.Fatal(err)
	}
	input := &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}
	_, err := s.client.PutObjectRetention(context.Background(), input)
	assertS3Error(t, err, 403, "AccessDenied")
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인
func TestObjectLockPutObjRetentionShortenPeriodBypass(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutObjRetentionShortenPeriodBypass"
	out := put(t, s, b, key, key, nil)
	early, late := time.Now().UTC().Add(48*time.Hour), time.Now().UTC().Add(96*time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, late)}); err != nil {
		t.Fatal(err)
	}
	input := &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early), BypassGovernanceRetention: aws.Bool(true)}
	if _, err := s.client.PutObjectRetention(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	got, err := s.client.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
	if err != nil || got.Retention == nil {
		t.Fatalf("retention=%#v err=%v", got.Retention, err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인
func TestObjectLockDeleteObjectWithRetention(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockDeleteObjectWithRetention"
	out := put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
	assertS3Error(t, err, 403, "AccessDenied")
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 Lock 유지기한이 있어도 바이패스를 통해 삭제가 가능한지 확인
func TestObjectLockDeleteObjectWithRetentionBypass(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockDeleteObjectWithRetentionBypass"
	out := put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
		t.Fatal(err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 Lock 유지기한이 있어도 바이패스를 통해 삭제가 가능한지 확인
func TestObjectLockDeleteObjectsWithRetentionBypass(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockDeleteObjectsWithRetentionBypass"
	out := put(t, s, b, key, key, nil)
	early := time.Now().UTC().Add(48 * time.Hour)
	version := out.VersionId
	if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
		t.Fatal(err)
	}
	var objects []types.ObjectIdentifier
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key-%d", i)
		p := put(t, s, b, k, k, nil)
		if _, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(k), VersionId: p.VersionId, Retention: retention(types.ObjectLockRetentionModeGovernance, early)}); err != nil {
			t.Fatal(err)
		}
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(k), VersionId: p.VersionId})
	}
	if _, err := s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(b), BypassGovernanceRetention: aws.Bool(true), Delete: &types.Delete{Objects: objects}}); err != nil {
		t.Fatal(err)
	}
	_, _ = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인
func TestObjectLockPutLegalHold(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutLegalHold"
	out := put(t, s, b, key, key, nil)
	if _, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
}

// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인
func TestObjectLockPutLegalHoldInvalidBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 30)
	key := "TestObjectLockPutLegalHoldInvalidBucket"
	put(t, s, b, key, key, nil)
	_, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold에 잘못된 값을 넣을 경우 실패 확인
func TestObjectLockPutLegalHoldInvalidStatus(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockPutLegalHoldInvalidStatus"
	out := put(t, s, b, key, key, nil)
	_, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatus("abc")}})
	assertS3Error(t, err, 400, "MalformedXML")
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인
func TestObjectLockGetLegalHold(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockGetLegalHold"
	out := put(t, s, b, key, key, nil)
	if _, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.client.GetObjectLegalHold(context.Background(), &s3.GetObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId})
	if err != nil || got.LegalHold == nil || got.LegalHold.Status != types.ObjectLockLegalHoldStatusOn {
		t.Fatalf("hold=%#v err=%v", got.LegalHold, err)
	}
	if _, err = s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
}

// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold설정 조회 실패 확인
func TestObjectLockGetLegalHoldInvalidBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 33)
	key := "TestObjectLockGetLegalHoldInvalidBucket"
	put(t, s, b, key, key, nil)
	_, err := s.client.GetObjectLegalHold(context.Background(), &s3.GetObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인
func TestObjectLockDeleteObjectWithLegalHoldOn(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockDeleteObjectWithLegalHoldOn"
	out := put(t, s, b, key, key, nil)
	if _, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)})
	assertS3Error(t, err, 403, "AccessDenied")
	if _, err = s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인
func TestObjectLockDeleteObjectWithLegalHoldOff(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	key := "TestObjectLockDeleteObjectWithLegalHoldOff"
	out := put(t, s, b, key, key, nil)
	if _, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
}

// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인
func TestObjectLockGetObjMetadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := newLockBucket(t, s)
	name := "TestObjectLockGetObjMetadata"
	out, until := putLockedObject(t, s, b, name)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId})
	if err != nil || head.ObjectLockMode != types.ObjectLockModeGovernance || head.ObjectLockRetainUntilDate == nil {
		t.Fatalf("head=%#v err=%v", head, err)
	}
	if !head.ObjectLockRetainUntilDate.Truncate(time.Millisecond).Equal(until.Truncate(time.Millisecond)) {
		t.Fatalf("retain=%v want=%v", head.ObjectLockRetainUntilDate, until)
	}
	_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(name), VersionId: out.VersionId, BypassGovernanceRetention: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
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

func retention(mode types.ObjectLockRetentionMode, until time.Time) *types.ObjectLockRetention {
	return &types.ObjectLockRetention{Mode: mode, RetainUntilDate: &until}
}

var (
	lockRetentionDate = time.Date(2030, 2, 1, 0, 0, 0, 0, time.UTC)
	lockPastDate      = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
)

// lockBucket은 Object Lock 을 켠 버킷을 테스트 ID 가 들어간 이름으로 만든다.
func lockBucket(t *testing.T, s *suite, id int) string {
	t.Helper()
	name := s.bucketNameOnly(id)
	input := createBucketInput(s.cfg, name)
	input.ObjectLockEnabledForBucket = aws.Bool(true)
	if _, err := s.client.CreateBucket(context.Background(), input); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			cleanupBucket(t, s, name)
		}
	})
	return name
}

// 보존 기한·법적 보존을 설정한 적이 없는 오브젝트는 조회에 실패한다
func TestObjectLockGetNotConfigured(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestObjectLockGetNotConfigured"
	b := lockBucket(t, s, 37)
	put(t, s, b, key, "content", nil)
	_, err := s.client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchObjectLockConfiguration")
	_, err = s.client.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchObjectLockConfiguration")
}

// 지난 날짜로는 보존 기한을 설정할 수 없다
func TestObjectLockPutObjRetentionPastDate(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestObjectLockPutObjRetentionPastDate"
	b := lockBucket(t, s, 38)
	put(t, s, b, key, "content", nil)
	_, err := s.client.PutObjectRetention(context.Background(), &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), Retention: retention(types.ObjectLockRetentionModeGovernance, lockPastDate)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 없는 오브젝트에는 보존 기한·법적 보존을 설정·조회할 수 없다
func TestObjectLockKeyNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestObjectLockKeyNotExist"
	b := lockBucket(t, s, 39)
	_, err := s.client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), Retention: retention(types.ObjectLockRetentionModeGovernance, lockRetentionDate)})
	assertS3Error(t, err, 404, "NoSuchKey")
	_, err = s.client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
	_, err = s.client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}})
	assertS3Error(t, err, 404, "NoSuchKey")
	_, err = s.client.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 잠금 헤더가 잘못된 PutObject 는 거부된다
func TestObjectLockPutObjectInvalidHeaders(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestObjectLockPutObjectInvalidHeaders"
	b := lockBucket(t, s, 40)
	invalidHeaders := []s3.PutObjectInput{
		{ObjectLockMode: types.ObjectLockModeGovernance},                                                    // 모드만
		{ObjectLockRetainUntilDate: aws.Time(lockRetentionDate)},                                            // 기한만
		{ObjectLockMode: types.ObjectLockModeGovernance, ObjectLockRetainUntilDate: aws.Time(lockPastDate)}, // 지난 기한
		{ObjectLockMode: types.ObjectLockMode("INVALID"), ObjectLockRetainUntilDate: aws.Time(lockRetentionDate)},
		{ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatus("INVALID")},
	}
	for i := range invalidHeaders {
		input := &invalidHeaders[i]
		input.Bucket, input.Key, input.Body = aws.String(b), aws.String(key), bytes.NewReader([]byte("content"))
		_, err := s.client.PutObject(ctx, input)
		assertS3Error(t, err, 400, "InvalidArgument")
	}
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Status(t, err, 404)
}

// Object Lock 을 켜지 않은 버킷에는 잠금 헤더를 쓸 수 없다
func TestObjectLockPutObjectInvalidBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestObjectLockPutObjectInvalidBucket"
	b := s.bucket(t, 41)
	headers := []s3.PutObjectInput{
		{ObjectLockMode: types.ObjectLockModeGovernance, ObjectLockRetainUntilDate: aws.Time(lockRetentionDate)},
		{ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn},
	}
	for i := range headers {
		input := &headers[i]
		input.Bucket, input.Key, input.Body = aws.String(b), aws.String(key), bytes.NewReader([]byte("content"))
		_, err := s.client.PutObject(context.Background(), input)
		assertS3Error(t, err, 400, "InvalidRequest")
	}
}

// PutObject 의 법적 보존 헤더가 적용되고, 그 버전은 bypass 로도 지울 수 없다
func TestObjectLockLegalHoldOnPutObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestObjectLockLegalHoldOnPutObject"
	b := lockBucket(t, s, 42)
	// 잠금 헤더가 있는 PutObject 는 Content-MD5 나 체크섬이 있어야 한다
	created, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte("content")), ContentMD5: aws.String(md5Base64("content")), ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn})
	if err != nil {
		t.Fatal(err)
	}
	version := created.VersionId
	// 끝나면 법적 보존을 풀어 버킷을 지울 수 있게 한다
	defer func() {
		if _, err := s.client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}}); err != nil {
			t.Errorf("PutObjectLegalHold OFF: %v", err)
		}
	}()

	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if head.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn || head.ObjectLockMode != "" {
		t.Fatalf("legal hold=%q mode=%q, want ON and no mode", head.ObjectLockLegalHoldStatus, head.ObjectLockMode)
	}

	// 버전을 지정하지 않은 삭제(삭제 마커)와 덮어쓰기는 막지 않는다
	deleted, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(deleted.DeleteMarker) {
		t.Fatalf("DeleteMarker = %v, want true", deleted.DeleteMarker)
	}
	if overwritten := put(t, s, b, key, "new", nil); aws.ToString(overwritten.VersionId) == aws.ToString(version) {
		t.Fatalf("overwrite kept version %q", aws.ToString(version))
	}

	// 법적 보존 중인 버전은 bypass 를 줘도 지울 수 없다
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, BypassGovernanceRetention: aws.Bool(true)})
	assertS3Error(t, err, 403, "AccessDenied")
	multi, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String(key), VersionId: version}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(multi.Deleted) != 0 || len(multi.Errors) == 0 || aws.ToString(multi.Errors[0].Code) != "AccessDenied" {
		t.Fatalf("deleted=%#v errors=%#v, want AccessDenied error only", multi.Deleted, multi.Errors)
	}
	got, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	var body bytes.Buffer
	if _, err := body.ReadFrom(got.Body); err != nil || body.String() != "content" {
		t.Fatalf("body=%q err=%v, want %q", body.String(), err, "content")
	}
}

// 삭제 마커에는 보존 기한·법적 보존을 걸 수 없다(버전을 지정하든 않든)
func TestObjectLockDeleteMarker(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestObjectLockDeleteMarker"
	b := lockBucket(t, s, 43)
	put(t, s, b, key, "content", nil)
	marker, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []*string{marker.VersionId, nil} {
		_, err := s.client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Retention: retention(types.ObjectLockRetentionModeGovernance, lockRetentionDate)})
		assertS3Error(t, err, 405, "MethodNotAllowed")
		_, err = s.client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
		assertS3Error(t, err, 405, "MethodNotAllowed")
		_, err = s.client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}})
		assertS3Error(t, err, 405, "MethodNotAllowed")
	}
}

// 잘못된 기본 보존 규칙은 거부된다
func TestObjectLockPutObjLockInvalidRule(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := lockBucket(t, s, 44)
	governance := types.ObjectLockRetentionModeGovernance

	// ObjectLockEnabled 누락, 모드 누락
	err := putLockConfiguration(t, s, b, &types.ObjectLockConfiguration{Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{Mode: governance, Days: aws.Int32(1)}}})
	assertS3Error(t, err, 400, "MalformedXML")
	err = putLockConfiguration(t, s, b, lockConfiguration("", 1, 0))
	assertS3Error(t, err, 400, "MalformedXML")
	// 기간 누락
	err = putLockConfiguration(t, s, b, lockConfiguration(governance, 0, 0))
	assertS3Error(t, err, 400, "InvalidRequest")
	// 기본 보존 기간은 36500일(100년)까지
	err = putLockConfiguration(t, s, b, lockConfiguration(governance, 36501, 0))
	assertS3Error(t, err, 400, "InvalidArgument")
	err = putLockConfiguration(t, s, b, lockConfiguration(governance, 0, 101))
	assertS3Error(t, err, 400, "InvalidArgument")
	if err := putLockConfiguration(t, s, b, lockConfiguration(governance, 36500, 0)); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if out.ObjectLockConfiguration == nil || out.ObjectLockConfiguration.Rule == nil || out.ObjectLockConfiguration.Rule.DefaultRetention == nil {
		t.Fatalf("configuration=%#v, want default retention", out.ObjectLockConfiguration)
	}
	if got := out.ObjectLockConfiguration.Rule.DefaultRetention; got.Mode != governance || aws.ToInt32(got.Days) != 36500 || got.Years != nil {
		t.Fatalf("default retention=%#v, want GOVERNANCE 36500 days", got)
	}
}

// Rule 없이 다시 넣으면 기본 보존 규칙이 지워진다
func TestObjectLockPutObjLockRemoveRule(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := lockBucket(t, s, 45)
	if err := putLockConfiguration(t, s, b, lockConfiguration(types.ObjectLockRetentionModeGovernance, 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := putLockConfiguration(t, s, b, &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if out.ObjectLockConfiguration == nil || out.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled || out.ObjectLockConfiguration.Rule != nil {
		t.Fatalf("configuration=%#v, want Enabled without rule", out.ObjectLockConfiguration)
	}

	key := "TestObjectLockPutObjLockRemoveRule"
	put(t, s, b, key, "content", nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if head.ObjectLockMode != "" {
		t.Fatalf("mode=%q, want none", head.ObjectLockMode)
	}
}

// 다른 사용자와 없는 버킷은 Object Lock 설정을 조회할 수 없다
func TestObjectLockGetObjLockAccessDenied(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	alt := altClientOrSkip(t, s)
	b := lockBucket(t, s, 46)
	_, err := alt.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = s.client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String(s.bucketNameOnly(46))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}
