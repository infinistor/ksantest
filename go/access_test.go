package s3tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// BlockPublicAcls와 BlockPublicPolicy 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
func TestBlockPublicAclAndPolicy(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 1)
	configuration := publicAccessConfiguration(true, false, true, false)
	putPublicAccessBlock(t, s, bucket, configuration)
	assertPublicAccessBlock(t, s, bucket, configuration)
	for _, acl := range []types.BucketCannedACL{types.BucketCannedACLPublicRead, types.BucketCannedACLPublicReadWrite, types.BucketCannedACLAuthenticatedRead} {
		_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: acl})
		assertS3Error(t, err, 403, "AccessDenied")
	}
}

// BlockPublicAcls 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
func TestBlockPublicAcls(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 2)
	configuration := publicAccessConfiguration(true, false, false, false)
	putPublicAccessBlock(t, s, bucket, configuration)
	assertPublicAccessBlock(t, s, bucket, configuration)
	for _, acl := range []types.ObjectCannedACL{types.ObjectCannedACLPublicRead, types.ObjectCannedACLPublicReadWrite, types.ObjectCannedACLAuthenticatedRead} {
		_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("testBlockPublicAcls"), Body: bytes.NewReader([]byte("body")), ACL: acl})
		assertS3Error(t, err, 403, "AccessDenied")
	}
}

// BlockPublicPolicy 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
func TestBlockPublicPolicy(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 3)
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, false, true, false))
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}`, bucket)
	_, err := s.client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 버킷의 접근 권한 블록 삭제 기능을 확인하는 테스트
func TestDeletePublicBlock(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 4)
	configuration := publicAccessConfiguration(true, true, true, false)
	putPublicAccessBlock(t, s, bucket, configuration)
	assertPublicAccessBlock(t, s, bucket, configuration)
	if _, err := s.client.DeletePublicAccessBlock(context.Background(), &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeletePublicAccessBlock: %v", err)
	}
	_, err := s.client.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchPublicAccessBlockConfiguration")
}

// IgnorePublicAcls 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
func TestIgnorePublicAcls(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 5)
	key := "TestIgnorePublicAcls"
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatalf("PutObject public-read: %v", err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("Alt GetObject before ignore: %v", err)
	}
	body, readErr := io.ReadAll(out.Body)
	out.Body.Close()
	if readErr != nil || string(body) != key {
		t.Fatalf("Alt body = %q, err=%v", body, readErr)
	}
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, true, false, false))
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead}); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	public := anonymousClient(s)
	_, err = public.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = public.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 버킷의 접근 권한 블록 설정 기능을 확인하는 테스트
func TestPutPublicBlock(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 6)
	configuration := publicAccessConfiguration(true, true, true, false)
	putPublicAccessBlock(t, s, bucket, configuration)
	assertPublicAccessBlock(t, s, bucket, configuration)
	if _, err := s.client.DeletePublicAccessBlock(context.Background(), &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeletePublicAccessBlock: %v", err)
	}
}

// 새로 만든 버킷의 접근 권한 블록 기본값을 확인하는 테스트
func TestPublicBlockDefault(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	// s.bucket 은 퍼블릭 액세스 차단 설정을 건드리지 않고 기본값 그대로 버킷을 만든다
	bucket := s.bucket(t, 7)
	// 새 버킷은 네 가지 차단이 모두 켜진 상태로 만들어진다
	assertPublicAccessBlock(t, s, bucket, publicAccessConfiguration(true, true, true, true))
}

// 접근 권한 블록을 일부 항목만 지정해 설정할 수 있는지 확인하는 테스트
func TestPutPublicBlockPartial(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 8)
	// 지정하지 않은 항목은 false 로 저장된다
	putPublicAccessBlock(t, s, bucket, &types.PublicAccessBlockConfiguration{BlockPublicAcls: aws.Bool(true)})
	assertPublicAccessBlock(t, s, bucket, publicAccessConfiguration(true, false, false, false))
}

// 접근 권한 블록 설정이 없는 버킷에서 삭제가 성공하는지 확인하는 테스트
func TestDeletePublicBlockNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 9)
	if _, err := s.client.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeletePublicAccessBlock: %v", err)
	}
	_, err := s.client.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchPublicAccessBlockConfiguration")
	// 설정이 없어도 삭제는 성공한다
	if _, err := s.client.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeletePublicAccessBlock again: %v", err)
	}
}

// 존재하지 않는 버킷의 접근 권한 블록 조회·설정·삭제가 실패하는지 확인하는 테스트
func TestPublicBlockBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 10)
	_, err := s.client.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{Bucket: aws.String(bucket), PublicAccessBlockConfiguration: publicAccessConfiguration(false, false, false, false)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버킷 소유자가 아닌 사용자의 접근 권한 블록 조회·설정·삭제가 실패하는지 확인하는 테스트
func TestPublicBlockAltUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := s.bucket(t, 11)
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(true, false, false, false))
	_, err := alt.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{Bucket: aws.String(bucket), PublicAccessBlockConfiguration: publicAccessConfiguration(false, false, false, false)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// BlockPublicAcls 가 켜져 있어도 퍼블릭이 아닌 ACL 은 허용되는지 확인하는 테스트
func TestBlockPublicAclsAllowPrivate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	key := "test_block_public_acls_allow_private"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 12)
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(true, false, false, false))
	put(t, s, bucket, key, key, nil)

	// 퍼블릭 ACL 은 오브젝트 ACL 변경에서도 거부된다
	_, err := s.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead})
	assertS3Error(t, err, 403, "AccessDenied")
	// 퍼블릭이 아닌 ACL 은 허용된다
	if _, err := s.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate}); err != nil {
		t.Fatalf("PutObjectAcl private: %v", err)
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key + "2"), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPrivate}); err != nil {
		t.Fatalf("PutObject private: %v", err)
	}
	if _, err := s.client.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
		t.Fatalf("PutBucketAcl private: %v", err)
	}
}

// 버킷 정책의 퍼블릭 여부 판정을 확인하는 테스트
func TestBucketPolicyStatus(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	bucket := s.bucket(t, 13)
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, false, false, false))
	resource := bucketARN(bucket) + "/*"

	_, err := s.client.GetBucketPolicyStatus(ctx, &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucketPolicy")

	// 모든 사용자에게 여는 정책은 퍼블릭으로 판정된다
	if _, err := s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(makeJSONPolicy("s3:GetObject", resource, nil))}); err != nil {
		t.Fatalf("PutBucketPolicy public: %v", err)
	}
	assertPolicyIsPublic(t, s, bucket, true)

	// 특정 사용자에게만 여는 정책은 퍼블릭이 아니다
	principal := map[string]any{"CanonicalUser": s.cfg.Alt.ID}
	if _, err := s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(makeJSONPolicy("s3:GetObject", resource, principal))}); err != nil {
		t.Fatalf("PutBucketPolicy canonical user: %v", err)
	}
	assertPolicyIsPublic(t, s, bucket, false)
}

// BlockPublicPolicy 가 켜져 있어도 퍼블릭이 아닌 정책은 허용되는지 확인하는 테스트
func TestBlockPublicPolicyAllowNonPublic(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	bucket := s.bucket(t, 14)
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, false, true, false))
	resource := bucketARN(bucket) + "/*"

	// 퍼블릭 정책은 거부되지만 특정 사용자에게만 여는 정책은 허용된다
	_, err := s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(makeJSONPolicy("s3:GetObject", resource, nil))})
	assertS3Error(t, err, 403, "AccessDenied")
	principal := map[string]any{"CanonicalUser": s.cfg.Alt.ID}
	if _, err := s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(makeJSONPolicy("s3:GetObject", resource, principal))}); err != nil {
		t.Fatalf("PutBucketPolicy canonical user: %v", err)
	}
	out, err := s.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	if aws.ToString(out.Policy) == "" {
		t.Fatal("버킷 정책이 비어 있습니다")
	}
}

// RestrictPublicBuckets 접근 권한 블록이 정상적으로 동작하는지 확인하는 테스트
func TestRestrictPublicBuckets(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "test_restrict_public_buckets"
	alt := s3Client(s.cfg, s.cfg.Alt)
	public := anonymousClient(s)
	bucket := s.bucket(t, 15)
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, false, false, false))
	put(t, s, bucket, key, key, nil)
	setPublicReadPolicy(t, s, bucket)

	// 퍼블릭 정책으로 누구나 읽을 수 있다
	waitUntil(t, func() bool { return getObjectStatus(public, bucket, key) == 200 })
	waitUntil(t, func() bool { return getObjectStatus(alt, bucket, key) == 200 })

	// RestrictPublicBuckets 를 켜면 퍼블릭 정책이 있어도 버킷 소유자만 접근할 수 있다
	putPublicAccessBlock(t, s, bucket, publicAccessConfiguration(false, false, false, true))
	waitUntil(t, func() bool { return getObjectStatus(public, bucket, key) == 403 })
	waitUntil(t, func() bool { return getObjectStatus(alt, bucket, key) == 403 })
	if got := getObjectStatus(s.client, bucket, key); got != 200 {
		t.Fatalf("owner GetObject status = %d, want 200", got)
	}
}

// 설정 변경이 반영될 때까지 check 가 true 를 돌려주기를 기다린다. (최대 60초, 2초 간격)
func waitUntil(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if check() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatal("설정 변경이 제한 시간 안에 반영되지 않았습니다")
		}
		time.Sleep(2 * time.Second)
	}
}

// 요청 결과를 HTTP 상태 코드로 바꾼다. 성공은 200, HTTP 응답이 없는 에러는 0 이다.
func errorHTTPStatus(err error) int {
	if err == nil {
		return 200
	}
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.HTTPStatusCode()
	}
	return 0
}

// GetObject 의 HTTP 상태 코드를 돌려준다.
func getObjectStatus(client *s3.Client, bucket, key string) int {
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		out.Body.Close()
	}
	return errorHTTPStatus(err)
}

// 단일 Allow 문장으로 된 버킷 정책을 만든다. principal 이 nil 이면 모든 사용자({"AWS": "*"})다.
func makeJSONPolicy(action, resource string, principal map[string]any) string {
	if principal == nil {
		principal = map[string]any{"AWS": "*"}
	}
	statement := map[string]any{"Effect": "Allow", "Principal": principal, "Action": action, "Resource": resource}
	data, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{statement}})
	return string(data)
}

func assertPolicyIsPublic(t *testing.T, s *suite, bucket string, want bool) {
	t.Helper()
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketPolicyStatus: %v", err)
	}
	if out.PolicyStatus == nil || aws.ToBool(out.PolicyStatus.IsPublic) != want {
		t.Fatalf("policy status = %#v, want IsPublic=%v", out.PolicyStatus, want)
	}
}

func publicAccessConfiguration(blockACLs, ignoreACLs, blockPolicy, restrict bool) *types.PublicAccessBlockConfiguration {
	return &types.PublicAccessBlockConfiguration{BlockPublicAcls: aws.Bool(blockACLs), IgnorePublicAcls: aws.Bool(ignoreACLs), BlockPublicPolicy: aws.Bool(blockPolicy), RestrictPublicBuckets: aws.Bool(restrict)}
}
func putPublicAccessBlock(t *testing.T, s *suite, bucket string, cfg *types.PublicAccessBlockConfiguration) {
	t.Helper()
	if _, err := s.client.PutPublicAccessBlock(context.Background(), &s3.PutPublicAccessBlockInput{Bucket: aws.String(bucket), PublicAccessBlockConfiguration: cfg}); err != nil {
		t.Fatalf("PutPublicAccessBlock: %v", err)
	}
}
func assertPublicAccessBlock(t *testing.T, s *suite, bucket string, want *types.PublicAccessBlockConfiguration) {
	t.Helper()
	out, err := s.client.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	got := out.PublicAccessBlockConfiguration
	if got == nil || aws.ToBool(got.BlockPublicAcls) != aws.ToBool(want.BlockPublicAcls) || aws.ToBool(got.IgnorePublicAcls) != aws.ToBool(want.IgnorePublicAcls) || aws.ToBool(got.BlockPublicPolicy) != aws.ToBool(want.BlockPublicPolicy) || aws.ToBool(got.RestrictPublicBuckets) != aws.ToBool(want.RestrictPublicBuckets) {
		t.Fatalf("public access block = %#v, want %#v", got, want)
	}
}
