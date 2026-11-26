package s3tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 정책 설정이 올바르게 적용되는지 확인
func TestBucketPolicy(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := s.bucket(t, 1), "asdf"
	put(t, s, bucket, key, key, nil)
	policy := allowPolicy("s3:ListBucket", []string{bucketARN(bucket), bucketARN(bucket) + "/*"}, nil)
	putBucketPolicy(t, s, bucket, policy)
	alt := s3Client(s.cfg, s.cfg.Alt)
	listOut, err := alt.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listOut.Contents) != 1 {
		t.Fatalf("object count=%d, want 1", len(listOut.Contents))
	}
	policyOut, err := s.client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSONPolicy(aws.ToString(policyOut.Policy), policy) {
		t.Fatalf("policy mismatch: %s", aws.ToString(policyOut.Policy))
	}
}

// 버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)
func TestBucketV2Policy(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := s.bucket(t, 2), "asdf"
	put(t, s, bucket, key, key, nil)
	policy := allowPolicy("s3:ListBucket", []string{bucketARN(bucket), bucketARN(bucket) + "/*"}, nil)
	putBucketPolicy(t, s, bucket, policy)
	alt := s3Client(s.cfg, s.cfg.Alt)
	listOut, err := alt.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listOut.Contents) != 1 {
		t.Fatalf("object count=%d, want 1", len(listOut.Contents))
	}
	policyOut, err := s.client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSONPolicy(aws.ToString(policyOut.Policy), policy) {
		t.Fatalf("policy mismatch: %s", aws.ToString(policyOut.Policy))
	}
}

// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인
func TestBucketPolicyAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 3), "asdf"
	put(t, s, bucket, key, key, nil)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLAuthenticatedRead)
	policy := denyPolicy("s3:ListBucket", []string{bucketARN(bucket), bucketARN(bucket) + "/*"})
	putBucketPolicy(t, s, bucket, policy)
	alt := s3Client(s.cfg, s.cfg.Alt)
	_, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Error(t, err, 403, "AccessDenied")
	if _, err := s.client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
}

// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)
func TestBucketV2PolicyAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 4), "asdf"
	put(t, s, bucket, key, key, nil)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLAuthenticatedRead)
	policy := denyPolicy("s3:ListBucket", []string{bucketARN(bucket), bucketARN(bucket) + "/*"})
	putBucketPolicy(t, s, bucket, policy)
	alt := s3Client(s.cfg, s.cfg.Alt)
	_, err := alt.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	if _, err := s.client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
}

// 정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인
func TestGetTagsAclPublic(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := s.bucket(t, 5), "acl"
	put(t, s, bucket, key, key, nil)
	putBucketPolicy(t, s, bucket, allowPolicy("s3:GetObjectTagging", []string{bucketARN(bucket) + "/" + key}, nil))
	tagList := []types.Tag{{Key: aws.String("key0"), Value: aws.String("value0")}, {Key: aws.String("key1"), Value: aws.String("value1")}}
	tagging := &types.Tagging{TagSet: tagList}
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := s.client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: tagging}); err != nil {
		t.Fatal(err)
	}
	out, err := alt.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || len(out.TagSet) != len(tagList) {
		t.Fatalf("GetObjectTagging tags=%v err=%v", out, err)
	}
}

// 정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인
func TestPutTagsAclPublic(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := s.bucket(t, 6), "acl"
	put(t, s, bucket, key, key, nil)
	putBucketPolicy(t, s, bucket, allowPolicy("s3:PutObjectTagging", []string{bucketARN(bucket) + "/" + key}, nil))
	tagList := []types.Tag{{Key: aws.String("key0"), Value: aws.String("value0")}, {Key: aws.String("key1"), Value: aws.String("value1")}}
	tagging := &types.Tagging{TagSet: tagList}
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: tagging}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || len(out.TagSet) != len(tagList) {
		t.Fatalf("tags=%v err=%v", out, err)
	}
}

// 정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인
func TestDeleteTagsObjPublic(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := s.bucket(t, 7), "acl"
	put(t, s, bucket, key, key, nil)
	putBucketPolicy(t, s, bucket, allowPolicy("s3:DeleteObjectTagging", []string{bucketARN(bucket) + "/" + key}, nil))
	tagList := []types.Tag{{Key: aws.String("key0"), Value: aws.String("value0")}, {Key: aws.String("key1"), Value: aws.String("value1")}}
	tagging := &types.Tagging{TagSet: tagList}
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := s.client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: tagging}); err != nil {
		t.Fatal(err)
	}
	if _, err := alt.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || len(out.TagSet) != 0 {
		t.Fatalf("tags after delete=%v err=%v", out, err)
	}
}

// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyGetObjExistingTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 8)
	keys := []string{"publicTag", "privateTag", "invalidTag"}
	for _, k := range keys {
		put(t, s, bucket, k, k, nil)
	}
	putObjectTags(t, s, bucket, keys[0], "security", "public")
	putObjectTags(t, s, bucket, keys[1], "security", "private")
	putObjectTags(t, s, bucket, keys[2], "security1", "public")
	condition := map[string]any{"StringEquals": map[string]any{"s3:ExistingObjectTag/security": "public"}}
	putBucketPolicy(t, s, bucket, allowPolicy("s3:GetObject", []string{bucketARN(bucket) + "/*"}, condition))
	alt := s3Client(s.cfg, s.cfg.Alt)
	out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[0])})
	if err != nil {
		t.Fatal(err)
	}
	out.Body.Close()
	for _, k := range keys[1:] {
		_, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
		assertS3Error(t, err, 403, "AccessDenied")
	}
}

// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyGetObjTaggingExistingTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 9)
	keys := []string{"publicTag", "privateTag", "invalidTag"}
	for _, k := range keys {
		put(t, s, bucket, k, k, nil)
	}
	putObjectTags(t, s, bucket, keys[0], "security", "public")
	putObjectTags(t, s, bucket, keys[1], "security", "private")
	putObjectTags(t, s, bucket, keys[2], "security1", "public")
	condition := map[string]any{"StringEquals": map[string]any{"s3:ExistingObjectTag/security": "public"}}
	putBucketPolicy(t, s, bucket, allowPolicy("s3:GetObjectTagging", []string{bucketARN(bucket) + "/*"}, condition))
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(keys[0])}); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys[1:] {
		_, err := alt.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(k)})
		assertS3Error(t, err, 403, "AccessDenied")
	}
}

// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 PutObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyPutObjTaggingExistingTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 10)
	keys := []string{"publicTag", "privateTag", "invalidTag"}
	for _, k := range keys {
		put(t, s, bucket, k, k, nil)
	}
	putObjectTags(t, s, bucket, keys[0], "security", "public")
	putObjectTags(t, s, bucket, keys[1], "security", "private")
	putObjectTags(t, s, bucket, keys[2], "security1", "public")
	condition := map[string]any{"StringEquals": map[string]any{"s3:ExistingObjectTag/security": "public"}}
	putBucketPolicy(t, s, bucket, allowPolicy("s3:PutObjectTagging", []string{bucketARN(bucket) + "/*"}, condition))
	alt := s3Client(s.cfg, s.cfg.Alt)
	newTagging := &types.Tagging{TagSet: []types.Tag{{Key: aws.String("security"), Value: aws.String("public")}}}
	if _, err := alt.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(keys[0]), Tagging: newTagging}); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys[1:] {
		_, err := alt.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(k), Tagging: newTagging})
		assertS3Error(t, err, 403, "AccessDenied")
	}
}

// [복사하려는 경로명이 'bucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyPutObjCopySource(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	source := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 11)
	target := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 11)
	keys := []string{"public/foo", "public/bar", "private/foo"}
	for _, key := range keys {
		put(t, s, source, key, key, nil)
	}
	putBucketPolicy(t, s, source, allowPolicy("s3:GetObject", []string{bucketARN(source) + "/*"}, nil))
	condition := map[string]any{"StringLike": map[string]any{"s3:x-amz-copy-source": source + "/public/*"}}
	putBucketPolicy(t, s, target, allowPolicy("s3:PutObject", []string{bucketARN(target) + "/*"}, condition))
	alt := s3Client(s.cfg, s.cfg.Alt)
	for index, sourceKey := range keys[:2] {
		targetKey := []string{"newFoo", "newFoo2"}[index]
		_, err := alt.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(target), Key: aws.String(targetKey), CopySource: aws.String(source + "/" + sourceKey)})
		if err != nil {
			t.Fatalf("CopyObject(%s): %v", sourceKey, err)
		}
		assertClientObjectBody(t, alt, target, targetKey, sourceKey)
	}
	_, err := alt.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(target), Key: aws.String("denied"), CopySource: aws.String(source + "/" + keys[2])})
	assertS3Error(t, err, 403, "AccessDenied")
}

// [오브젝트의 메타데이터값이 'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyPutObjCopySourceMeta(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	source := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 12)
	target := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 12)
	for _, key := range []string{"public/foo", "public/bar"} {
		put(t, s, source, key, key, nil)
	}
	putBucketPolicy(t, s, source, allowPolicy("s3:GetObject", []string{bucketARN(source) + "/*"}, nil))
	condition := map[string]any{"StringEquals": map[string]any{"s3:x-amz-metadata-directive": "COPY"}}
	putBucketPolicy(t, s, target, allowPolicy("s3:PutObject", []string{bucketARN(target) + "/*"}, condition))
	alt := s3Client(s.cfg, s.cfg.Alt)
	_, err := alt.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(target), Key: aws.String("newFoo"), CopySource: aws.String(source + "/public/foo"), MetadataDirective: types.MetadataDirectiveCopy})
	if err != nil {
		t.Fatal(err)
	}
	assertClientObjectBody(t, alt, target, "newFoo", "public/foo")
	for _, directive := range []types.MetadataDirective{"", types.MetadataDirectiveReplace} {
		input := &s3.CopyObjectInput{Bucket: aws.String(target), Key: aws.String("denied"), CopySource: aws.String(source + "/public/bar")}
		if directive != "" {
			input.MetadataDirective = directive
		}
		_, err := alt.CopyObject(context.Background(), input)
		assertS3Error(t, err, 403, "AccessDenied")
	}
}

// [PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyPutObjAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 13)
	resource := []string{bucketARN(bucket) + "/*"}
	allow := policyStatement("Allow", "s3:PutObject", resource, nil)
	deny := policyStatement("Deny", "s3:PutObject", resource, map[string]any{"StringLike": map[string]any{"s3:x-amz-acl": "public*"}})
	putBucketPolicy(t, s, bucket, marshalPolicy(allow, deny))
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("private-key")}); err != nil {
		t.Fatal(err)
	}
	_, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("public-key"), ACL: types.ObjectCannedACLPublicRead})
	assertS3Error(t, err, 403, "AccessDenied")
}

// [오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyPutObjGrant(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	first := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 14)
	second := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 14)
	ownerGrant := "id=" + s.cfg.Main.ID
	condition := map[string]any{"StringEquals": map[string]any{"s3:x-amz-grant-full-control": ownerGrant}}
	putBucketPolicy(t, s, first, allowPolicy("s3:PutObject", []string{bucketARN(first) + "/*"}, condition))
	putBucketPolicy(t, s, second, allowPolicy("s3:PutObject", []string{bucketARN(second) + "/*"}, nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(first), Key: aws.String("key1"), GrantFullControl: aws.String(ownerGrant)}); err != nil {
		t.Fatal(err)
	}
	if _, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(second), Key: aws.String("key2")}); err != nil {
		t.Fatal(err)
	}
	firstACL, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(first), Key: aws.String("key1")})
	if err != nil {
		t.Fatal(err)
	}
	assertGrantID(t, firstACL.Grants, s.cfg.Main.ID)
	secondACL, err := alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(second), Key: aws.String("key2")})
	if err != nil {
		t.Fatal(err)
	}
	assertGrantID(t, secondACL.Grants, s.cfg.Alt.ID)
}

// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectACL허용] 조건부 정책설정시 올바르게 동작하는지 확인
func TestBucketPolicyGetObjAclExistingTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 15)
	keys := []string{"publicTag", "privateTag", "invalidTag"}
	for _, k := range keys {
		put(t, s, bucket, k, k, nil)
	}
	putObjectTags(t, s, bucket, keys[0], "security", "public")
	putObjectTags(t, s, bucket, keys[1], "security", "private")
	putObjectTags(t, s, bucket, keys[2], "security1", "public")
	condition := map[string]any{"StringEquals": map[string]any{"s3:ExistingObjectTag/security": "public"}}
	putBucketPolicy(t, s, bucket, allowPolicy("s3:GetObjectAcl", []string{bucketARN(bucket) + "/*"}, condition))
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(keys[0])}); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys[1:] {
		_, err := alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(k)})
		assertS3Error(t, err, 403, "AccessDenied")
	}
}

// 모든 사용자가 버킷에 접근 가능(public으으로 간주)
func TestBucketPolicyStatusWithAllUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 16)
	statement := map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "*"}, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*"}
	doc := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(doc)
	putBucketPolicy(t, s, bucket, string(data))
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.PolicyStatus == nil || !aws.ToBool(out.PolicyStatus.IsPublic) {
		t.Fatalf("policy status=%#v, want public=true", out.PolicyStatus)
	}
}

// 특정 사용자만 버킷에 접근 가능(private)
func TestBucketPolicyStatusWithSpecificUserAccess(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 17)
	statement := map[string]any{"Effect": "Allow", "Principal": map[string]any{"CanonicalUser": s.cfg.Main.ID}, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*"}
	doc := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(doc)
	putBucketPolicy(t, s, bucket, string(data))
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.PolicyStatus == nil || aws.ToBool(out.PolicyStatus.IsPublic) {
		t.Fatalf("policy status=%#v, want public=false", out.PolicyStatus)
	}
}

// 너무 넓은 IP 범위를 가진 정책 (public으으로 간주)
func TestBucketPolicyStatusWithWideIPRange(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 18)
	statement := map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "*"}, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*", "Condition": map[string]any{"IpAddress": map[string]any{"aws:SourceIp": "0.0.0.0/1"}}}
	doc := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(doc)
	putBucketPolicy(t, s, bucket, string(data))
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.PolicyStatus == nil || !aws.ToBool(out.PolicyStatus.IsPublic) {
		t.Fatalf("policy status=%#v, want public=true", out.PolicyStatus)
	}
}

// 특정 IP 범위를 가진 정책 (private)
func TestBucketPolicyStatusWithIPRange(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 19)
	statement := map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "*"}, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*", "Condition": map[string]any{"IpAddress": map[string]any{"aws:SourceIp": "192.168.1.0/24"}}}
	doc := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(doc)
	putBucketPolicy(t, s, bucket, string(data))
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.PolicyStatus == nil || aws.ToBool(out.PolicyStatus.IsPublic) {
		t.Fatalf("policy status=%#v, want public=false", out.PolicyStatus)
	}
}

// 매우 제한적인 시간에 대한 접근 허용 정책 (public으로 간주)
func TestBucketPolicyStatusWithTimeCondition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	start := time.Now().UTC().Add(10 * time.Minute)
	bucket := s.bucket(t, 20)
	statement := map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "*"}, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*", "Condition": map[string]any{"DateGreaterThan": map[string]any{"aws:CurrentTime": start.Format(time.RFC3339Nano)}, "DateLessThan": map[string]any{"aws:CurrentTime": start.Add(time.Second).Format(time.RFC3339Nano)}}}
	doc := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(doc)
	putBucketPolicy(t, s, bucket, string(data))
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.PolicyStatus == nil || !aws.ToBool(out.PolicyStatus.IsPublic) {
		t.Fatalf("policy status=%#v, want public=true", out.PolicyStatus)
	}
}

// 특정 태그를 가진 오브젝트에 대한 접근 허용용 정책 (public으로 간주)
func TestBucketPolicyStatusWithTagCondition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 21)
	statement := map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "*"}, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*", "Condition": map[string]any{"StringEquals": map[string]any{"s3:ExistingObjectTag/access": "restricted"}}}
	doc := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(doc)
	putBucketPolicy(t, s, bucket, string(data))
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.PolicyStatus == nil || !aws.ToBool(out.PolicyStatus.IsPublic) {
		t.Fatalf("policy status=%#v, want public=true", out.PolicyStatus)
	}
}

func assertObjectBody(t *testing.T, s *suite, bucket, key, want string) {
	t.Helper()
	assertClientObjectBody(t, s.client, bucket, key, want)
}

func assertClientObjectBody(t *testing.T, client *s3.Client, bucket, key, want string) {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil || string(data) != want {
		t.Fatalf("body=%q err=%v, want %q", data, err, want)
	}
}

func policyStatement(effect, action string, resources []string, condition map[string]any) map[string]any {
	statement := map[string]any{"Effect": effect, "Principal": "*", "Action": action, "Resource": resources}
	if condition != nil {
		statement["Condition"] = condition
	}
	return statement
}

func marshalPolicy(statements ...map[string]any) string {
	document := map[string]any{"Version": "2012-10-17", "Statement": statements}
	data, _ := json.Marshal(document)
	return string(data)
}

func putObjectTags(t *testing.T, s *suite, bucket, key, tagKey, value string) {
	t.Helper()
	_, err := s.client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String(tagKey), Value: aws.String(value)}}}})
	if err != nil {
		t.Fatal(err)
	}
}

func assertGrantID(t *testing.T, grants []types.Grant, want string) {
	t.Helper()
	for _, grant := range grants {
		if grant.Grantee != nil && aws.ToString(grant.Grantee.ID) == want {
			return
		}
	}
	t.Fatalf("grant for canonical ID %q not found", want)
}

func equalJSONPolicy(left, right string) bool {
	var a, b any
	if json.Unmarshal([]byte(left), &a) != nil || json.Unmarshal([]byte(right), &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}
func bucketARN(bucket string) string { return "arn:aws:s3:::" + bucket }
func allowPolicy(action string, resources []string, condition map[string]any) string {
	statement := map[string]any{"Effect": "Allow", "Principal": "*", "Action": action, "Resource": resources}
	if condition != nil {
		statement["Condition"] = condition
	}
	document := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(document)
	return string(data)
}
func denyPolicy(action string, resources []string) string {
	document := map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Deny", "Principal": map[string]any{"AWS": "*"}, "Action": action, "Resource": resources}}}
	data, _ := json.Marshal(document)
	return string(data)
}
func putBucketPolicy(t *testing.T, s *suite, bucket, policy string) {
	t.Helper()
	disablePublicAccessBlock(t, s, bucket)
	if _, err := s.client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}
}

// 모든 주체의 GetObject 를 거부하는 문장에 주어진 항목을 덮어쓴다. 값이 nil 이면 그 항목을 뺀다.
func denyGetObjectStatement(bucket string, overrides map[string]any) map[string]any {
	statement := map[string]any{"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*"}
	for name, value := range overrides {
		if value == nil {
			delete(statement, name)
			continue
		}
		statement[name] = value
	}
	return statement
}

func policyDocumentVersion(version string, statements ...map[string]any) string {
	if statements == nil {
		statements = []map[string]any{}
	}
	data, _ := json.Marshal(map[string]any{"Version": version, "Statement": statements})
	return string(data)
}

func assertMalformedPolicy(t *testing.T, s *suite, bucket, policy string) {
	t.Helper()
	_, err := s.client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)})
	assertS3Error(t, err, 400, "MalformedPolicy")
}

// 정책이 없는 버킷의 정책·정책 상태 조회는 실패한다
func TestBucketPolicyGetNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 22)
	_, err := s.client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucketPolicy")
	_, err = s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucketPolicy")
}

// 정책이 없어도 삭제는 성공한다
func TestBucketPolicyDeleteNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 23)
	out, err := s.client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if status := responseStatus(t, out.ResultMetadata); status != 204 {
		t.Fatalf("HTTP status = %d, want 204", status)
	}
}

// 정책을 설정·조회·삭제한다
func TestBucketPolicyPutGetDelete(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 24)
	policy := policyDocumentVersion("2012-10-17", denyGetObjectStatement(bucket, map[string]any{"Sid": "deny-get"}))
	if _, err := s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}

	got, err := s.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSONPolicy(aws.ToString(got.Policy), policy) {
		t.Fatalf("policy mismatch: %s", aws.ToString(got.Policy))
	}
	status, err := s.client.GetBucketPolicyStatus(ctx, &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if status.PolicyStatus == nil || aws.ToBool(status.PolicyStatus.IsPublic) {
		t.Fatalf("policy status=%#v, want public=false", status.PolicyStatus)
	}

	if _, err := s.client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	_, err = s.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucketPolicy")
}

// 없는 버킷의 정책 조회·설정·삭제는 실패한다
func TestBucketPolicyBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucketNameOnly(25)
	policy := policyDocumentVersion("2012-10-17", denyGetObjectStatement(bucket, nil))
	_, err := s.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 다른 사용자는 정책을 조회·설정·삭제할 수 없다
func TestBucketPolicyAccessDenied(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	alt := altClientOrSkip(t, s)
	ctx := context.Background()
	bucket := s.bucket(t, 26)
	policy := policyDocumentVersion("2012-10-17", denyGetObjectStatement(bucket, nil))
	if _, err := s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}

	_, err := alt.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	// 정책은 그대로 남아 있다
	got, err := s.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSONPolicy(aws.ToString(got.Policy), policy) {
		t.Fatalf("policy mismatch: %s", aws.ToString(got.Policy))
	}
}

// JSON 이 아니거나 Statement 가 비었거나 Version 이 잘못된 정책은 거부된다
func TestBucketPolicyInvalidJson(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 27)
	assertMalformedPolicy(t, s, bucket, "not json")
	// Statement 가 비어 있다
	assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17"))
	assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2020-01-01", denyGetObjectStatement(bucket, nil)))
}

// 문장의 항목 값이 잘못된 정책은 거부된다
func TestBucketPolicyInvalidElement(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 28)
	invalidOverrides := []map[string]any{
		{"Effect": "Maybe"},
		{"Action": "s3:Invalid"},
		{"Principal": map[string]any{"AWS": "arn:aws:iam::000000000000:user/not-exist"}},
		{"Condition": map[string]any{"StringEquals": map[string]any{"s3:invalid": "value"}}},      // 없는 조건 키
		{"Condition": map[string]any{"InvalidEquals": map[string]any{"aws:SourceIp": "1.1.1.1"}}}, // 없는 조건 연산자
	}
	for _, overrides := range invalidOverrides {
		assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17", denyGetObjectStatement(bucket, overrides)))
	}
	_, err := s.client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucketPolicy")
}

// 리소스가 맞지 않거나 필수 항목이 빠진 정책은 거부된다
func TestBucketPolicyInvalidResource(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 29)
	// 다른 버킷의 리소스
	assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17", denyGetObjectStatement(bucket, map[string]any{"Resource": "arn:aws:s3:::other-bucket-name/*"})))
	// 버킷 대상 액션에 오브젝트 리소스
	assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17", denyGetObjectStatement(bucket, map[string]any{"Action": "s3:ListBucket"})))
	// Resource, Principal 누락
	for _, missing := range []string{"Resource", "Principal"} {
		assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17", denyGetObjectStatement(bucket, map[string]any{missing: nil})))
	}
}

// 정책 문서는 20KB 까지
func TestBucketPolicyTooLarge(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 30)
	statements := make([]map[string]any, 0, 70)
	for i := range 70 {
		statements = append(statements, denyGetObjectStatement(bucket, map[string]any{
			"Sid":       fmt.Sprintf("statement%d", i),
			"Condition": map[string]any{"StringEquals": map[string]any{"aws:Referer": strings.Repeat("x", 180)}},
		}))
	}
	policy := policyDocumentVersion("2012-10-17", statements...)
	if len(policy) <= 20480 {
		t.Fatalf("policy size = %d, want > 20480", len(policy))
	}
	assertMalformedPolicy(t, s, bucket, policy)
}

// 정책 효과 (AWS 실측 기준)
// 정책을 바꾼 뒤에는 새 클라이언트로 확인한다(같은 연결은 바뀌기 전 권한으로 답할 수 있다).
// Deny 는 정리에 필요 없는 액션(GetObject, PutObject 등)으로만 건다.

func altPrincipal(s *suite) map[string]any {
	return map[string]any{"CanonicalUser": s.cfg.Alt.ID}
}

// 주체가 서브 사용자인 문장을 만든다. action, resource 가 nil 이면 그 항목을 넣지 않고,
// overrides 값이 nil 이면 그 항목을 뺀다(NotAction 등으로 바꿀 때).
func effectStatement(s *suite, effect string, action, resource any, overrides map[string]any) map[string]any {
	statement := map[string]any{"Effect": effect, "Principal": altPrincipal(s)}
	if action != nil {
		statement["Action"] = action
	}
	if resource != nil {
		statement["Resource"] = resource
	}
	for name, value := range overrides {
		if value == nil {
			delete(statement, name)
			continue
		}
		statement[name] = value
	}
	return statement
}

func putPolicyStatements(t *testing.T, s *suite, bucket string, statements ...map[string]any) {
	t.Helper()
	putPolicyDocument(t, s, bucket, policyDocumentVersion("2012-10-17", statements...))
}

func putPolicyDocument(t *testing.T, s *suite, bucket, policy string) {
	t.Helper()
	if _, err := s.client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}
}

func assertPolicyDenied(t *testing.T, err error) {
	t.Helper()
	assertS3Error(t, err, 403, "AccessDenied")
}

// GetObject 의 에러만 돌려준다. versionID 가 비어 있으면 버전을 지정하지 않는다.
func getObjectErr(client *s3.Client, bucket, key, versionID string) error {
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := client.GetObject(context.Background(), input)
	if err == nil {
		out.Body.Close()
	}
	return err
}

// 주어진 클라이언트로 오브젝트를 올리고 에러만 돌려준다.
func putObjectErr(client *s3.Client, bucket, key, body string, mutate func(*s3.PutObjectInput)) error {
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body)}
	if mutate != nil {
		mutate(input)
	}
	_, err := client.PutObject(context.Background(), input)
	return err
}

func listV2Err(client *s3.Client, input *s3.ListObjectsV2Input) error {
	_, err := client.ListObjectsV2(context.Background(), input)
	return err
}

// ListObjectsV2 가 성공하고 KeyCount 가 want 인지 확인한다.
func assertListV2KeyCount(t *testing.T, client *s3.Client, input *s3.ListObjectsV2Input, want int32) {
	t.Helper()
	out, err := client.ListObjectsV2(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt32(out.KeyCount); got != want {
		t.Fatalf("KeyCount = %d, want %d", got, want)
	}
}

func assertEmptyObjectTagging(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	out, err := client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.TagSet) != 0 {
		t.Fatalf("TagSet = %v, want empty", out.TagSet)
	}
}

// policyHTTPSClient는 Main User 로 HTTPS 요청을 보내는 클라이언트를 만든다.
func policyHTTPSClient(s *suite) *s3.Client {
	cfg := s.cfg
	cfg.Secure = true
	options := s3.Options{
		Region:       cfg.Region,
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cfg.Main.AccessKey, cfg.Main.SecretKey, "")),
		UsePathStyle: cfg.Endpoint() != "",
	}
	if endpoint := cfg.Endpoint(); endpoint != "" {
		options.BaseEndpoint = aws.String(endpoint)
		options.HTTPClient = insecureHTTPClient()
	}
	return s3.New(options)
}

// 특정 사용자에게 허용한 액션만 되고 나머지는 거부된다
func TestBucketPolicyAllowUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyAllowUser"
	bucket := s.bucket(t, 31)
	put(t, s, bucket, key, "content", nil)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/*", nil))
	assertPolicyIsPublic(t, s, bucket, false)

	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, key, "content")
	head, err := alt.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt64(head.ContentLength); got != 7 {
		t.Fatalf("ContentLength = %d, want 7", got)
	}
	// 허용한 액션 말고는 그대로 거부된다
	assertPolicyDenied(t, putObjectErr(alt, bucket, "new", "x", nil))
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}))
	_, err = alt.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertPolicyDenied(t, err)
	_, err = alt.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertPolicyDenied(t, err)
	_, err = alt.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertPolicyDenied(t, err)
	// 목록 권한이 없으면 없는 키도 403 이다
	assertPolicyDenied(t, getObjectErr(alt, bucket, "not-exist", ""))
	// 정책에 없는 주체
	assertPolicyDenied(t, getObjectErr(freshAnonymousClient(s), bucket, key, ""))
}

// 명시적 Deny 는 Allow 보다 우선한다
func TestBucketPolicyDenyOverridesAllow(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 32)
	for _, key := range []string{"public/a", "private/a"} {
		put(t, s, bucket, key, key, nil)
	}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/*", nil),
		effectStatement(s, "Deny", "s3:GetObject", bucketARN(bucket)+"/private/*", nil),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, "public/a", "public/a")
	assertPolicyDenied(t, getObjectErr(alt, bucket, "private/a", ""))
	// Deny 의 주체가 아닌 소유자는 읽을 수 있다
	assertClientObjectBody(t, s3Client(s.cfg, s.cfg.Main), bucket, "private/a", "private/a")
}

// 명시적 Deny 는 버킷 소유자에게도 적용된다
func TestBucketPolicyDenyOwner(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	key := "TestBucketPolicyDenyOwner"
	bucket := s.bucket(t, 33)
	put(t, s, bucket, key, "content", nil)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Deny", "s3:GetObject", bucketARN(bucket)+"/*", map[string]any{"Principal": "*"}))
	// 명시적 Deny 는 버킷 소유자에게도 적용된다
	owner := s3Client(s.cfg, s.cfg.Main)
	assertPolicyDenied(t, getObjectErr(owner, bucket, key, ""))
	_, err := owner.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Status(t, err, 403)
	_, err = owner.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: copySource(bucket, key, "")})
	assertPolicyDenied(t, err)
	// 거부하지 않은 액션은 그대로 된다
	if err := putObjectErr(owner, bucket, "new", "x", nil); err != nil {
		t.Fatal(err)
	}
	assertEmptyObjectTagging(t, owner, bucket, key)
}

// NotAction 을 쓴 Deny 는 적지 않은 액션을 모두 거부한다
func TestBucketPolicyNotActionDeny(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyNotActionDeny"
	bucket := s.bucket(t, 34)
	put(t, s, bucket, key, "content", nil)
	resources := []string{bucketARN(bucket), bucketARN(bucket) + "/*"}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", "s3:*", resources, nil),
		// GetObject, ListBucket 이 아닌 모든 액션을 거부
		effectStatement(s, "Deny", nil, resources, map[string]any{"NotAction": []string{"s3:GetObject", "s3:ListBucket"}}),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, key, "content")
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, 1)
	assertPolicyDenied(t, putObjectErr(alt, bucket, "new", "x", nil))
	_, err := alt.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertPolicyDenied(t, err)
	_, err = alt.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertPolicyDenied(t, err)
}

// NotAction 을 쓴 Allow 는 적지 않은 액션을 모두 허용한다
func TestBucketPolicyNotActionAllow(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicyNotActionAllow"
	bucket := s.bucket(t, 35)
	put(t, s, bucket, key, "content", nil)
	// PutObject, DeleteObject 가 아닌 모든 오브젝트 액션을 허용
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", nil, bucketARN(bucket)+"/*", map[string]any{"NotAction": []string{"s3:PutObject", "s3:DeleteObject"}}),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, key, "content")
	assertEmptyObjectTagging(t, alt, bucket, key)
	assertPolicyDenied(t, putObjectErr(alt, bucket, "new", "x", nil))
	_, err := alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertPolicyDenied(t, err)
}

// NotResource 를 쓴 Allow 는 적지 않은 리소스만 허용한다
func TestBucketPolicyNotResourceAllow(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 36)
	for _, key := range []string{"public/a", "private/a"} {
		put(t, s, bucket, key, key, nil)
	}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", "s3:GetObject", nil, map[string]any{"NotResource": bucketARN(bucket) + "/private/*"}),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, "public/a", "public/a")
	assertPolicyDenied(t, getObjectErr(alt, bucket, "private/a", ""))
}

// NotResource 를 쓴 Deny 는 적지 않은 리소스를 모두 거부한다
func TestBucketPolicyNotResourceDeny(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 37)
	for _, key := range []string{"public/a", "private/a"} {
		put(t, s, bucket, key, key, nil)
	}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/*", nil),
		// public/ 아래가 아닌 것은 모두 거부
		effectStatement(s, "Deny", "s3:GetObject", nil, map[string]any{"NotResource": bucketARN(bucket) + "/public/*"}),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, "public/a", "public/a")
	assertPolicyDenied(t, getObjectErr(alt, bucket, "private/a", ""))
}

// NotPrincipal 은 Deny 에만 쓸 수 있고, 적지 않은 주체를 모두 거부한다
func TestBucketPolicyNotPrincipal(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicyNotPrincipal"
	bucket := s.bucket(t, 38)
	put(t, s, bucket, key, "content", nil)
	resource := bucketARN(bucket) + "/*"
	notPrincipal := map[string]any{"Principal": nil, "NotPrincipal": altPrincipal(s)}
	// Allow 에는 NotPrincipal 을 쓸 수 없다
	assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17", effectStatement(s, "Allow", "s3:GetObject", resource, notPrincipal)))
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", "s3:GetObject", resource, nil),
		// 서브 사용자가 아닌 모든 주체의 GetObject 를 거부(소유자 포함)
		effectStatement(s, "Deny", "s3:GetObject", resource, notPrincipal),
	)
	assertClientObjectBody(t, s3Client(s.cfg, s.cfg.Alt), bucket, key, "content")
	assertPolicyDenied(t, getObjectErr(s3Client(s.cfg, s.cfg.Main), bucket, key, ""))
}

// 리소스의 * 와 ? 와일드카드
func TestBucketPolicyResourceWildcard(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 39)
	for _, key := range []string{"a1/file1.txt", "a1/file10.txt", "b1/file1.txt", "a1/sub/file2.txt", "a1/file1.TXT"} {
		put(t, s, bucket, key, key, nil)
	}
	// * 는 0자 이상(/ 포함), ? 는 정확히 한 글자, 대소문자를 구분한다
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/a*/file?.txt", nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	for _, key := range []string{"a1/file1.txt", "a1/sub/file2.txt"} {
		assertClientObjectBody(t, alt, bucket, key, key)
	}
	for _, key := range []string{"a1/file10.txt", "b1/file1.txt", "a1/file1.TXT"} {
		assertPolicyDenied(t, getObjectErr(alt, bucket, key, ""))
	}
}

// 액션의 * 와일드카드
func TestBucketPolicyActionWildcard(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyActionWildcard"
	bucket := s.bucket(t, 40)
	put(t, s, bucket, key, "content", nil)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:Get*", []string{bucketARN(bucket), bucketARN(bucket) + "/*"}, nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, key, "content")
	acl, err := alt.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if len(acl.Grants) != 1 {
		t.Fatalf("grants = %d, want 1", len(acl.Grants))
	}
	assertEmptyObjectTagging(t, alt, bucket, key)
	versioning, err := alt.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if versioning.Status != "" {
		t.Fatalf("versioning status = %q, want empty", versioning.Status)
	}
	if _, err := alt.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	if _, err := alt.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	// Get 으로 시작하지 않는 액션
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}))
	assertPolicyDenied(t, putObjectErr(alt, bucket, "new", "x", nil))
}

// 액션 이름은 대소문자를 구분하지 않는다
func TestBucketPolicyActionCaseInsensitive(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicyActionCaseInsensitive"
	bucket := s.bucket(t, 41)
	put(t, s, bucket, key, "content", nil)
	// 액션 이름은 대소문자를 구분하지 않는다
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:getobject", bucketARN(bucket)+"/*", nil))
	assertClientObjectBody(t, s3Client(s.cfg, s.cfg.Alt), bucket, key, "content")
}

// ListBucket 은 버킷 리소스에 걸고, 목록 조회만 허용한다
func TestBucketPolicyListBucketResource(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyListBucketResource"
	bucket := s.bucket(t, 42)
	put(t, s, bucket, key, "content", nil)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:ListBucket", bucketARN(bucket), nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, 1)
	listed, err := alt.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("object count=%d, want 1", len(listed.Contents))
	}
	if _, err := alt.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	// ListBucket 만으로는 읽을 수 없고, 버전·멀티파트 목록은 다른 액션이다
	assertPolicyDenied(t, getObjectErr(alt, bucket, key, ""))
	_, err = alt.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	assertPolicyDenied(t, err)
	_, err = alt.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	assertPolicyDenied(t, err)
	// 목록 권한이 있으면 없는 키는 404 다
	assertS3Error(t, getObjectErr(alt, bucket, "not-exist", ""), 404, "NoSuchKey")
}

// s3:prefix 조건으로 목록 조회 범위를 제한한다
func TestBucketPolicyListPrefixCondition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 43)
	for _, key := range []string{"public/a", "private/a"} {
		put(t, s, bucket, key, key, nil)
	}
	condition := map[string]any{"StringEquals": map[string]any{"s3:prefix": "public/"}}
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:ListBucket", bucketARN(bucket), map[string]any{"Condition": condition}))
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("public/")}, 1)
	listed, err := alt.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("public/")})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("object count=%d, want 1", len(listed.Contents))
	}
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}))
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("public/a")}))
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("private/")}))
}

// 조건 연산자가 여럿이면 모두 맞아야 한다
func TestBucketPolicyListConditionKeys(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 44)
	for _, key := range []string{"public/a", "private/a"} {
		put(t, s, bucket, key, key, nil)
	}
	// 조건 연산자가 여럿이면 모두 맞아야 한다
	condition := map[string]any{
		"StringLike":            map[string]any{"s3:prefix": "public/*"},
		"StringEquals":          map[string]any{"s3:delimiter": "/"},
		"NumericLessThanEquals": map[string]any{"s3:max-keys": "10"},
	}
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:ListBucket", bucketARN(bucket), map[string]any{"Condition": condition}))
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("public/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(10)}, 1)
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("public/"), MaxKeys: aws.Int32(10)}))
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("public/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(11)}))
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("public/"), Delimiter: aws.String("/")}))
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("private/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(10)}))
}

// s3:prefix 조건을 건 Deny 는 요청의 prefix 값만 본다
func TestBucketPolicyListDenyPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 45)
	for _, key := range []string{"public/a", "private/a"} {
		put(t, s, bucket, key, key, nil)
	}
	resource := bucketARN(bucket)
	condition := map[string]any{"StringLike": map[string]any{"s3:prefix": "private/*"}}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", "s3:ListBucket", resource, nil),
		effectStatement(s, "Deny", "s3:ListBucket", resource, map[string]any{"Condition": condition}),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("private/")}))
	// 조건은 요청의 prefix 값만 본다. 응답에 private/ 아래 키가 들어 있어도 막지 않는다
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, 2)
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("priv")}, 1)
}

// s3:max-keys 조건으로 목록 조회 크기를 제한한다
func TestBucketPolicyListMaxKeys(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 46)
	put(t, s, bucket, "a", "a", nil)
	condition := map[string]any{"NumericLessThanEquals": map[string]any{"s3:max-keys": "10"}}
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:ListBucket", bucketARN(bucket), map[string]any{"Condition": condition}))
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertListV2KeyCount(t, alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), MaxKeys: aws.Int32(10)}, 1)
	listed, err := alt.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket), MaxKeys: aws.Int32(5)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("object count=%d, want 1", len(listed.Contents))
	}
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), MaxKeys: aws.Int32(11)}))
	// max-keys 를 보내지 않으면 조건 키가 없어 맞지 않는다
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}))
}

// aws:SecureTransport 조건으로 http 요청을 거부한다
func TestBucketPolicySecureTransport(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "TestBucketPolicySecureTransport"
	bucket := s.bucket(t, 47)
	put(t, s, bucket, key, "content", nil)
	condition := map[string]any{"Bool": map[string]any{"aws:SecureTransport": "false"}}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Deny", "s3:GetObject", bucketARN(bucket)+"/*", map[string]any{"Principal": "*", "Condition": condition}),
	)
	httpClient := sseCHTTPClient(s)
	httpsClient := policyHTTPSClient(s)
	assertClientObjectBody(t, httpsClient, bucket, key, "content")
	assertPolicyDenied(t, getObjectErr(httpClient, bucket, key, ""))
	// 거부한 액션이 아니면 http 로도 된다
	if err := putObjectErr(httpClient, bucket, "new", "x", nil); err != nil {
		t.Fatal(err)
	}
}

// aws:SourceIp 조건(IpAddress, NotIpAddress)
func TestBucketPolicySourceIp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicySourceIp"
	// (조건, 서브 사용자의 GetObject 가 되는지). 192.0.2.0/24 는 문서용 대역이라 요청 주소가 될 수 없다
	cases := []struct {
		condition map[string]any
		allowed   bool
	}{
		{map[string]any{"IpAddress": map[string]any{"aws:SourceIp": "0.0.0.0/0"}}, true},
		{map[string]any{"IpAddress": map[string]any{"aws:SourceIp": "192.0.2.0/24"}}, false},
		{map[string]any{"NotIpAddress": map[string]any{"aws:SourceIp": "192.0.2.0/24"}}, true},
		{map[string]any{"NotIpAddress": map[string]any{"aws:SourceIp": "0.0.0.0/0"}}, false},
		{map[string]any{"IpAddress": map[string]any{"aws:SourceIp": []string{"192.0.2.0/24", "0.0.0.0/0"}}}, true},
	}
	for _, c := range cases {
		bucket := s.bucket(t, 48)
		put(t, s, bucket, key, "content", nil)
		putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/*", map[string]any{"Condition": c.condition}))
		alt := s3Client(s.cfg, s.cfg.Alt)
		if c.allowed {
			assertClientObjectBody(t, alt, bucket, key, "content")
		} else {
			assertPolicyDenied(t, getObjectErr(alt, bucket, key, ""))
		}
	}

	bucket := s.bucket(t, 48)
	condition := map[string]any{"IpAddress": map[string]any{"aws:SourceIp": "not-an-ip"}}
	assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17", effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/*", map[string]any{"Condition": condition})))
}

// 암호화 헤더가 AES256 이 아닌 업로드를 거부한다
func TestBucketPolicyDenyUnencryptedPut(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	key := "TestBucketPolicyDenyUnencryptedPut"
	bucket := s.bucket(t, 49)
	condition := map[string]any{"StringNotEquals": map[string]any{"s3:x-amz-server-side-encryption": "AES256"}}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Deny", "s3:PutObject", bucketARN(bucket)+"/*", map[string]any{"Principal": "*", "Condition": condition}),
	)
	owner := s3Client(s.cfg, s.cfg.Main)
	source := copySource(bucket, key, "")
	aes256 := func(input *s3.PutObjectInput) { input.ServerSideEncryption = types.ServerSideEncryptionAes256 }
	// 헤더가 없으면 값이 AES256 이 아닌 것으로 본다
	assertPolicyDenied(t, putObjectErr(owner, bucket, key, "content", nil))
	if err := putObjectErr(owner, bucket, key, "content", aes256); err != nil {
		t.Fatal(err)
	}
	// 멀티파트 시작과 복사도 PutObject 액션이다
	_, err := owner.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multi")})
	assertPolicyDenied(t, err)
	_, err = owner.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: source})
	assertPolicyDenied(t, err)
	if _, err := owner.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: source, ServerSideEncryption: types.ServerSideEncryptionAes256}); err != nil {
		t.Fatal(err)
	}
	assertClientObjectBody(t, owner, bucket, "copy", "content")
}

// Null 조건으로 암호화 헤더가 없는 업로드를 거부한다
func TestBucketPolicyNullCondition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "TestBucketPolicyNullCondition"
	bucket := s.bucket(t, 50)
	// 암호화 헤더가 없는(Null) 업로드를 거부
	condition := map[string]any{"Null": map[string]any{"s3:x-amz-server-side-encryption": "true"}}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Deny", "s3:PutObject", bucketARN(bucket)+"/*", map[string]any{"Principal": "*", "Condition": condition}),
	)
	owner := s3Client(s.cfg, s.cfg.Main)
	assertPolicyDenied(t, putObjectErr(owner, bucket, key, "content", nil))
	if err := putObjectErr(owner, bucket, key, "content", func(input *s3.PutObjectInput) { input.ServerSideEncryption = types.ServerSideEncryptionAes256 }); err != nil {
		t.Fatal(err)
	}
	head, err := owner.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("ServerSideEncryption = %q, want AES256", head.ServerSideEncryption)
	}
}

// 버전을 지정한 조회는 s3:GetObjectVersion 이 필요하다
func TestBucketPolicyGetObjectVersionAction(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicyGetObjectVersionAction"
	bucket := s.bucket(t, 51)
	enableVersioning(t, s, bucket)
	version1 := aws.ToString(put(t, s, bucket, key, "1", nil).VersionId)
	version2 := aws.ToString(put(t, s, bucket, key, "2", nil).VersionId)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/*", nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertClientObjectBody(t, alt, bucket, key, "2")
	// 버전을 지정한 조회는 s3:GetObjectVersion 이 필요하다(최신 버전이어도)
	for _, versionID := range []string{version1, version2} {
		assertPolicyDenied(t, getObjectErr(alt, bucket, key, versionID))
	}
	_, err := alt.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(version1)})
	assertS3Status(t, err, 403)
}

// s3:VersionId 조건으로 특정 버전만 허용한다
func TestBucketPolicyVersionIdCondition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicyVersionIdCondition"
	bucket := s.bucket(t, 52)
	enableVersioning(t, s, bucket)
	version1 := aws.ToString(put(t, s, bucket, key, "1", nil).VersionId)
	version2 := aws.ToString(put(t, s, bucket, key, "2", nil).VersionId)
	condition := map[string]any{"StringEquals": map[string]any{"s3:VersionId": version1}}
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObjectVersion", bucketARN(bucket)+"/*", map[string]any{"Condition": condition}))
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, body := getObjectBody(t, alt, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(version1)}); body != "1" {
		t.Fatalf("body = %q, want %q", body, "1")
	}
	assertPolicyDenied(t, getObjectErr(alt, bucket, key, version2))
	// 버전을 지정하지 않은 조회는 s3:GetObject 가 필요하다
	assertPolicyDenied(t, getObjectErr(alt, bucket, key, ""))
}

// 버전을 지정한 삭제는 s3:DeleteObjectVersion 이 필요하다
func TestBucketPolicyDeleteObjectVersionAction(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyDeleteObjectVersionAction"
	bucket := s.bucket(t, 53)
	enableVersioning(t, s, bucket)
	versionID := aws.ToString(put(t, s, bucket, key, "1", nil).VersionId)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:DeleteObject", bucketARN(bucket)+"/*", nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	// 버전을 지정한 삭제는 s3:DeleteObjectVersion 이 필요하다
	_, err := alt.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
	assertPolicyDenied(t, err)
	response, err := alt.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String(key), VersionId: aws.String(versionID)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Deleted) != 0 || len(response.Errors) == 0 || aws.ToString(response.Errors[0].Code) != "AccessDenied" {
		t.Fatalf("deleted=%v errors=%v, want AccessDenied error only", response.Deleted, response.Errors)
	}
	// 버전을 지정하지 않은 삭제(삭제 마커)는 된다
	deleted, err := alt.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(deleted.DeleteMarker) {
		t.Fatal("DeleteMarker = false, want true")
	}
	if _, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID)}); body != "1" {
		t.Fatalf("body = %q, want %q", body, "1")
	}
}

// s3:RequestObjectTagKeys 조건으로 요청의 태그 키를 제한한다
func TestBucketPolicyRequestObjectTagKeys(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 54)
	// 요청의 태그 키가 모두 project, team 안에 있어야 한다
	condition := map[string]any{"ForAllValues:StringEquals": map[string]any{"s3:RequestObjectTagKeys": []string{"project", "team"}}}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", []string{"s3:PutObject", "s3:PutObjectTagging"}, bucketARN(bucket)+"/*", map[string]any{"Condition": condition}),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	if err := putObjectErr(alt, bucket, "allowed", "x", func(input *s3.PutObjectInput) { input.Tagging = aws.String("project=a") }); err != nil {
		t.Fatal(err)
	}
	// 태그가 없으면 ForAllValues 는 참이다
	if err := putObjectErr(alt, bucket, "no-tag", "x", nil); err != nil {
		t.Fatal(err)
	}
	assertPolicyDenied(t, putObjectErr(alt, bucket, "denied", "x", func(input *s3.PutObjectInput) { input.Tagging = aws.String("project=a&owner=b") }))
	tagging, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String("allowed")})
	if err != nil {
		t.Fatal(err)
	}
	if len(tagging.TagSet) != 1 || aws.ToString(tagging.TagSet[0].Key) != "project" || aws.ToString(tagging.TagSet[0].Value) != "a" {
		t.Fatalf("TagSet = %v, want [project=a]", tagging.TagSet)
	}
}

// PutObject 액션이 허용하는 범위
func TestBucketPolicyPutObjectAction(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyPutObjectAction"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 55)
	put(t, s, bucket, key, "content", nil)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:PutObject", bucketARN(bucket)+"/*", nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	if err := putObjectErr(alt, bucket, "new", "x", nil); err != nil {
		t.Fatal(err)
	}
	// 멀티파트 시작·파트 업로드는 PutObject 로, 시작한 사용자는 파트 목록·중단도 할 수 있다
	upload, err := alt.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multi")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alt.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String("multi"), UploadId: upload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("x")}); err != nil {
		t.Fatal(err)
	}
	parts, err := alt.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(bucket), Key: aws.String("multi"), UploadId: upload.UploadId})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts.Parts) != 1 {
		t.Fatalf("parts = %d, want 1", len(parts.Parts))
	}
	if _, err := alt.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String("multi"), UploadId: upload.UploadId}); err != nil {
		t.Fatal(err)
	}

	// 태그를 붙이려면 PutObjectTagging, 복사하려면 원본의 GetObject 가 필요하다
	assertPolicyDenied(t, putObjectErr(alt, bucket, "tagged", "x", func(input *s3.PutObjectInput) { input.Tagging = aws.String("a=b") }))
	_, err = alt.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: copySource(bucket, key, "")})
	assertPolicyDenied(t, err)
	_, err = alt.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	assertPolicyDenied(t, err)
	assertPolicyDenied(t, getObjectErr(alt, bucket, "new", ""))
	_, err = alt.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("new")})
	assertPolicyDenied(t, err)
}

// 멀티파트 목록 액션(ListMultipartUploadParts, ListBucketMultipartUploads)
func TestBucketPolicyMultipartListActions(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyMultipartListActions"
	bucket := s.bucket(t, 56)
	upload, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: upload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("x")}); err != nil {
		t.Fatal(err)
	}
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", []string{"s3:ListMultipartUploadParts", "s3:ListBucketMultipartUploads"}, []string{bucketARN(bucket), bucketARN(bucket) + "/*"}, nil),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	parts, err := alt.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: upload.UploadId})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts.Parts) != 1 {
		t.Fatalf("parts = %d, want 1", len(parts.Parts))
	}
	uploads, err := alt.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads.Uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(uploads.Uploads))
	}
	_, err = alt.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: upload.UploadId})
	assertPolicyDenied(t, err)
	_, err = alt.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: upload.UploadId, PartNumber: aws.Int32(2), Body: strings.NewReader("x")})
	assertPolicyDenied(t, err)
}

// 버킷 설정 액션. 설정 삭제는 Put 액션으로 판정한다
func TestBucketPolicyBucketConfigActions(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	bucket := s.bucket(t, 57)
	actions := []string{
		"s3:GetBucketVersioning",
		"s3:GetBucketTagging",
		"s3:PutBucketTagging",
		"s3:GetBucketCORS",
		"s3:PutBucketCORS",
		"s3:GetBucketAcl",
		"s3:GetBucketLocation",
		"s3:ListBucketVersions",
	}
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", actions, bucketARN(bucket), nil))
	alt := s3Client(s.cfg, s.cfg.Alt)
	versioning, err := alt.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if versioning.Status != "" {
		t.Fatalf("versioning status = %q, want empty", versioning.Status)
	}
	if _, err := alt.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	if _, err := alt.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	versions, err := alt.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(versions.Versions) != 0 {
		t.Fatalf("versions = %d, want 0", len(versions.Versions))
	}
	// 권한은 있고 설정이 없으면 403 이 아니라 404 다
	_, err = alt.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchTagSet")
	_, err = alt.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchCORSConfiguration")
	// 설정 삭제는 Put 액션으로 판정한다
	if _, err := alt.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(bucket), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}}); err != nil {
		t.Fatal(err)
	}
	tagging, err := s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(tagging.TagSet) != 1 || aws.ToString(tagging.TagSet[0].Key) != "a" || aws.ToString(tagging.TagSet[0].Value) != "b" {
		t.Fatalf("TagSet = %v, want [a=b]", tagging.TagSet)
	}
	if _, err := alt.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	cors := &types.CORSConfiguration{CORSRules: []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}}
	if _, err := alt.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String(bucket), CORSConfiguration: cors}); err != nil {
		t.Fatal(err)
	}
	if _, err := alt.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}

	// 허용하지 않은 액션
	assertPolicyDenied(t, listV2Err(alt, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}))
	_, err = alt.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
	assertPolicyDenied(t, err)
	_, err = alt.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)})
	assertPolicyDenied(t, err)
	_, err = alt.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertPolicyDenied(t, err)
}

// 정책 문서의 여러 형식(Statement 객체, Version 생략, Id·Sid, 주체 여럿)
func TestBucketPolicyStatementForms(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	key := "TestBucketPolicyStatementForms"
	bucket := s.bucket(t, 58)
	put(t, s, bucket, key, "content", nil)
	resource := bucketARN(bucket) + "/*"
	statement := effectStatement(s, "Allow", "s3:GetObject", resource, nil)
	marshal := func(document map[string]any) string {
		data, _ := json.Marshal(document)
		return string(data)
	}
	type policyDocument struct {
		Version   string
		Id        string
		Statement []map[string]any
	}
	getPolicy := func() policyDocument {
		t.Helper()
		out, err := s.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatal(err)
		}
		var document policyDocument
		if err := json.Unmarshal([]byte(aws.ToString(out.Policy)), &document); err != nil {
			t.Fatalf("policy JSON: %v: %s", err, aws.ToString(out.Policy))
		}
		return document
	}

	// Statement 는 배열이 아니라 객체 하나여도 된다
	putPolicyDocument(t, s, bucket, marshal(map[string]any{"Version": "2012-10-17", "Statement": statement}))
	assertClientObjectBody(t, s3Client(s.cfg, s.cfg.Alt), bucket, key, "content")

	// Version 을 빼면 2008-10-17 로 저장된다
	putPolicyDocument(t, s, bucket, marshal(map[string]any{"Statement": []any{statement}}))
	if got := getPolicy().Version; got != "2008-10-17" {
		t.Fatalf("Version = %q, want 2008-10-17", got)
	}
	putPolicyDocument(t, s, bucket, policyDocumentVersion("2008-10-17", statement))

	// Id 와 Sid 는 그대로 돌려준다. Sid 가 겹쳐도 받는다
	putPolicyDocument(t, s, bucket, marshal(map[string]any{
		"Version": "2012-10-17",
		"Id":      "policy-id",
		"Statement": []any{
			effectStatement(s, "Allow", "s3:GetObject", resource, map[string]any{"Sid": "same"}),
			effectStatement(s, "Allow", "s3:GetObjectTagging", resource, map[string]any{"Sid": "same"}),
		},
	}))
	actual := getPolicy()
	if actual.Id != "policy-id" {
		t.Fatalf("Id = %q, want policy-id", actual.Id)
	}
	var sids, actions []any
	for _, item := range actual.Statement {
		sids = append(sids, item["Sid"])
		actions = append(actions, item["Action"])
	}
	if !reflect.DeepEqual(sids, []any{"same", "same"}) {
		t.Fatalf("Sid = %v, want [same same]", sids)
	}
	if !reflect.DeepEqual(actions, []any{"s3:GetObject", "s3:GetObjectTagging"}) {
		t.Fatalf("Action = %v, want [s3:GetObject s3:GetObjectTagging]", actions)
	}

	// 주체를 여럿 적을 수 있다
	principal := map[string]any{"CanonicalUser": []string{s.cfg.Alt.ID, s.cfg.Main.ID}}
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObject", resource, map[string]any{"Principal": principal}))
	assertClientObjectBody(t, s3Client(s.cfg, s.cfg.Alt), bucket, key, "content")
}

// 항목을 잘못 쓴 문장은 거부된다
func TestBucketPolicyInvalidStatement(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t, 59)
	resource := bucketARN(bucket) + "/*"
	invalidStatements := []map[string]any{
		// 같은 뜻의 항목을 함께 쓴다
		effectStatement(s, "Allow", "s3:GetObject", resource, map[string]any{"NotAction": "s3:PutObject"}),
		effectStatement(s, "Allow", "s3:GetObject", resource, map[string]any{"NotResource": bucketARN(bucket) + "/a/*"}),
		effectStatement(s, "Deny", "s3:GetObject", resource, map[string]any{"NotPrincipal": altPrincipal(s)}),
		// 필수 항목 누락
		effectStatement(s, "Allow", nil, resource, nil),
		effectStatement(s, "Allow", "s3:GetObject", resource, map[string]any{"Effect": nil}),
		{},
		// 값이 잘못됐다
		effectStatement(s, "Allow", "s3:GetObject", resource, map[string]any{"Unknown": "value"}),
		effectStatement(s, "Allow", []string{}, resource, nil),
		effectStatement(s, "Allow", "ec2:RunInstances", resource, nil),
		effectStatement(s, "Allow", "s3:GetObject", "*", nil),
		effectStatement(s, "Allow", "s3:GetObject", resource, map[string]any{"Principal": map[string]any{"CanonicalUser": strings.Repeat("0", 64)}}),
	}
	for _, statement := range invalidStatements {
		assertMalformedPolicy(t, s, bucket, policyDocumentVersion("2012-10-17", statement))
	}
	// 최상위가 객체가 아니다
	assertMalformedPolicy(t, s, bucket, "[]")
	_, err := s.client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucketPolicy")
}

// 정책을 지우면 허용했던 권한이 사라진다
func TestBucketPolicyDeleteRevokes(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicyDeleteRevokes"
	bucket := s.bucket(t, 60)
	put(t, s, bucket, key, "content", nil)
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/*", nil))
	assertClientObjectBody(t, s3Client(s.cfg, s.cfg.Alt), bucket, key, "content")

	if _, err := s.client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	assertPolicyDenied(t, getObjectErr(s3Client(s.cfg, s.cfg.Alt), bucket, key, ""))
}

// 다른 사용자가 올린 오브젝트도 버킷 소유자의 것이다
func TestBucketPolicyOtherUserWrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	key := "TestBucketPolicyOtherUserWrite"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 61)
	putPolicyStatements(t, s, bucket,
		effectStatement(s, "Allow", []string{"s3:PutObject", "s3:GetObject", "s3:DeleteObject"}, bucketARN(bucket)+"/*", nil),
	)
	alt := s3Client(s.cfg, s.cfg.Alt)
	if err := putObjectErr(alt, bucket, key, "content", nil); err != nil {
		t.Fatal(err)
	}
	// 다른 사용자가 올린 오브젝트도 버킷 소유자의 것이다
	assertObjectBody(t, s, bucket, key, "content")
	assertClientObjectBody(t, alt, bucket, key, "content")
	response, err := alt.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: deleteIdentifiers([]string{key, "not-exist"})}})
	if err != nil {
		t.Fatal(err)
	}
	deleted := map[string]bool{}
	for _, item := range response.Deleted {
		deleted[aws.ToString(item.Key)] = true
	}
	if len(response.Deleted) != 2 || !deleted[key] || !deleted["not-exist"] || len(response.Errors) != 0 {
		t.Fatalf("deleted=%v errors=%v", response.Deleted, response.Errors)
	}
	assertS3Error(t, getObjectErr(s.client, bucket, key, ""), 404, "NoSuchKey")
}

// 익명 사용자에게 접두사 아래만 읽기를 허용한다
func TestBucketPolicyAnonymousPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 62)
	for _, key := range []string{"public/a", "private/a"} {
		put(t, s, bucket, key, key, nil)
	}
	putPolicyStatements(t, s, bucket, effectStatement(s, "Allow", "s3:GetObject", bucketARN(bucket)+"/public/*", map[string]any{"Principal": "*"}))
	assertPolicyIsPublic(t, s, bucket, true)

	public := freshAnonymousClient(s)
	assertClientObjectBody(t, public, bucket, "public/a", "public/a")
	assertPolicyDenied(t, getObjectErr(public, bucket, "private/a", ""))
	assertPolicyDenied(t, putObjectErr(public, bucket, "public/b", "x", nil))
	assertPolicyDenied(t, listV2Err(public, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}))
}
