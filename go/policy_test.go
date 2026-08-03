package s3tests

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
