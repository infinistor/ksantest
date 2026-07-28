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

func TestBucketPolicy(t *testing.T) {
	t.Parallel()

	testListPolicy(t, false)
}
func TestBucketV2Policy(t *testing.T) {
	t.Parallel()

	testListPolicy(t, true)
}
func TestBucketPolicyAcl(t *testing.T) {
	t.Parallel()

	testPolicyOverridesACL(t, false)
}
func TestBucketV2PolicyAcl(t *testing.T) {
	t.Parallel()

	testPolicyOverridesACL(t, true)
}
func TestGetTagsAclPublic(t *testing.T) {
	t.Parallel()

	testTagPolicy(t, "s3:GetObjectTagging")
}
func TestPutTagsAclPublic(t *testing.T) {
	t.Parallel()

	testTagPolicy(t, "s3:PutObjectTagging")
}
func TestDeleteTagsObjPublic(t *testing.T) {
	t.Parallel()

	testTagPolicy(t, "s3:DeleteObjectTagging")
}
func TestBucketPolicyGetObjExistingTag(t *testing.T) {
	t.Parallel()

	testExistingTagCondition(t, "s3:GetObject")
}
func TestBucketPolicyGetObjTaggingExistingTag(t *testing.T) {
	t.Parallel()

	testExistingTagCondition(t, "s3:GetObjectTagging")
}
func TestBucketPolicyPutObjTaggingExistingTag(t *testing.T) {
	t.Parallel()

	testExistingTagCondition(t, "s3:PutObjectTagging")
}
func TestBucketPolicyPutObjCopySource(t *testing.T) {
	t.Parallel()

	testPolicyCopySource(t)
}
func TestBucketPolicyPutObjCopySourceMeta(t *testing.T) {
	t.Parallel()

	testPolicyCopySourceMetadata(t)
}
func TestBucketPolicyPutObjAcl(t *testing.T) {
	t.Parallel()

	testPolicyPutObjectACL(t)
}
func TestBucketPolicyPutObjGrant(t *testing.T) {
	t.Parallel()

	testPolicyPutObjectGrant(t)
}
func TestBucketPolicyGetObjAclExistingTag(t *testing.T) {
	t.Parallel()

	testPolicyGetObjectACLExistingTag(t)
}
func TestBucketPolicyStatusWithAllUser(t *testing.T) {
	t.Parallel()

	testPolicyStatus(t, map[string]any{"AWS": "*"}, nil, true)
}
func TestBucketPolicyStatusWithSpecificUserAccess(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	testPolicyStatusWithSuite(t, s, map[string]any{"CanonicalUser": s.cfg.Main.ID}, nil, false)
}
func TestBucketPolicyStatusWithWideIPRange(t *testing.T) {
	t.Parallel()

	testPolicyStatus(t, map[string]any{"AWS": "*"}, map[string]any{"IpAddress": map[string]any{"aws:SourceIp": "0.0.0.0/1"}}, true)
}
func TestBucketPolicyStatusWithIPRange(t *testing.T) {
	t.Parallel()

	testPolicyStatus(t, map[string]any{"AWS": "*"}, map[string]any{"IpAddress": map[string]any{"aws:SourceIp": "192.168.1.0/24"}}, false)
}
func TestBucketPolicyStatusWithTimeCondition(t *testing.T) {
	t.Parallel()

	start := time.Now().UTC().Add(10 * time.Minute)
	testPolicyStatus(t, map[string]any{"AWS": "*"}, map[string]any{"DateGreaterThan": map[string]any{"aws:CurrentTime": start.Format(time.RFC3339Nano)}, "DateLessThan": map[string]any{"aws:CurrentTime": start.Add(time.Second).Format(time.RFC3339Nano)}}, true)
}
func TestBucketPolicyStatusWithTagCondition(t *testing.T) {
	t.Parallel()

	testPolicyStatus(t, map[string]any{"AWS": "*"}, map[string]any{"StringEquals": map[string]any{"s3:ExistingObjectTag/access": "restricted"}}, true)
}

func testPolicyCopySource(t *testing.T) {
	s := newSuite(t)
	requireAltUser(t, s)
	source := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	target := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
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

func testPolicyCopySourceMetadata(t *testing.T) {
	s := newSuite(t)
	requireAltUser(t, s)
	source := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	target := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
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

func testPolicyPutObjectACL(t *testing.T) {
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
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

func testPolicyPutObjectGrant(t *testing.T) {
	s := newSuite(t)
	requireAltUser(t, s)
	first := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	second := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
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

func testPolicyGetObjectACLExistingTag(t *testing.T) {
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	keys := []string{"publicTag", "privateTag", "invalidTag"}
	for _, key := range keys {
		put(t, s, bucket, key, key, nil)
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
	for _, key := range keys[1:] {
		_, err := alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		assertS3Error(t, err, 403, "AccessDenied")
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

func testListPolicy(t *testing.T, v2 bool) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := s.bucket(t), "asdf"
	put(t, s, bucket, key, key, nil)
	policy := allowPolicy("s3:ListBucket", []string{bucketARN(bucket), bucketARN(bucket) + "/*"}, nil)
	putBucketPolicy(t, s, bucket, policy)
	alt := s3Client(s.cfg, s.cfg.Alt)
	var count int
	if v2 {
		out, err := alt.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatal(err)
		}
		count = len(out.Contents)
	} else {
		out, err := alt.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatal(err)
		}
		count = len(out.Contents)
	}
	if count != 1 {
		t.Fatalf("object count=%d, want 1", count)
	}
	out, err := s.client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSONPolicy(aws.ToString(out.Policy), policy) {
		t.Fatalf("policy mismatch: %s", aws.ToString(out.Policy))
	}
}

func equalJSONPolicy(left, right string) bool {
	var a, b any
	if json.Unmarshal([]byte(left), &a) != nil || json.Unmarshal([]byte(right), &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}
func testPolicyOverridesACL(t *testing.T, v2 bool) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "asdf"
	put(t, s, bucket, key, key, nil)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLAuthenticatedRead)
	policy := denyPolicy("s3:ListBucket", []string{bucketARN(bucket), bucketARN(bucket) + "/*"})
	putBucketPolicy(t, s, bucket, policy)
	alt := s3Client(s.cfg, s.cfg.Alt)
	var err error
	if v2 {
		_, err = alt.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	} else {
		_, err = alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	}
	assertS3Error(t, err, 403, "AccessDenied")
	if _, err := s.client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
}
func testTagPolicy(t *testing.T, action string) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := s.bucket(t), "acl"
	put(t, s, bucket, key, key, nil)
	putBucketPolicy(t, s, bucket, allowPolicy(action, []string{bucketARN(bucket) + "/" + key}, nil))
	tags := []types.Tag{{Key: aws.String("key0"), Value: aws.String("value0")}, {Key: aws.String("key1"), Value: aws.String("value1")}}
	tagging := &types.Tagging{TagSet: tags}
	alt := s3Client(s.cfg, s.cfg.Alt)
	switch action {
	case "s3:GetObjectTagging":
		if _, err := s.client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: tagging}); err != nil {
			t.Fatal(err)
		}
		out, err := alt.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil || len(out.TagSet) != len(tags) {
			t.Fatalf("GetObjectTagging tags=%v err=%v", out, err)
		}
	case "s3:PutObjectTagging":
		if _, err := alt.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: tagging}); err != nil {
			t.Fatal(err)
		}
		out, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil || len(out.TagSet) != len(tags) {
			t.Fatalf("tags=%v err=%v", out, err)
		}
	case "s3:DeleteObjectTagging":
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

func testExistingTagCondition(t *testing.T, action string) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := s.bucket(t)
	keys := []string{"publicTag", "privateTag", "invalidTag"}
	for _, key := range keys {
		put(t, s, bucket, key, key, nil)
	}
	putTags := func(key string, tags []types.Tag) {
		_, err := s.client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: &types.Tagging{TagSet: tags}})
		if err != nil {
			t.Fatal(err)
		}
	}
	putTags(keys[0], []types.Tag{{Key: aws.String("security"), Value: aws.String("public")}})
	putTags(keys[1], []types.Tag{{Key: aws.String("security"), Value: aws.String("private")}})
	putTags(keys[2], []types.Tag{{Key: aws.String("security1"), Value: aws.String("public")}})
	condition := map[string]any{"StringEquals": map[string]any{"s3:ExistingObjectTag/security": "public"}}
	putBucketPolicy(t, s, bucket, allowPolicy(action, []string{bucketARN(bucket) + "/*"}, condition))
	alt := s3Client(s.cfg, s.cfg.Alt)
	switch action {
	case "s3:GetObject":
		out, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(keys[0])})
		if err != nil {
			t.Fatal(err)
		}
		out.Body.Close()
		for _, key := range keys[1:] {
			_, err := alt.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			assertS3Error(t, err, 403, "AccessDenied")
		}
	case "s3:GetObjectTagging":
		if _, err := alt.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(keys[0])}); err != nil {
			t.Fatal(err)
		}
		for _, key := range keys[1:] {
			_, err := alt.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			assertS3Error(t, err, 403, "AccessDenied")
		}
	case "s3:PutObjectTagging":
		tags := &types.Tagging{TagSet: []types.Tag{{Key: aws.String("security"), Value: aws.String("public")}}}
		if _, err := alt.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(keys[0]), Tagging: tags}); err != nil {
			t.Fatal(err)
		}
		for _, key := range keys[1:] {
			_, err := alt.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: tags})
			assertS3Error(t, err, 403, "AccessDenied")
		}
	}
}
func testPolicyStatus(t *testing.T, principal any, condition map[string]any, want bool) {
	t.Helper()
	testPolicyStatusWithSuite(t, newSuite(t), principal, condition, want)
}
func testPolicyStatusWithSuite(t *testing.T, s *suite, principal any, condition map[string]any, want bool) {
	t.Helper()
	bucket := s.bucket(t)
	statement := map[string]any{"Effect": "Allow", "Principal": principal, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*"}
	if condition != nil {
		statement["Condition"] = condition
	}
	doc := map[string]any{"Version": "2012-10-17", "Statement": []any{statement}}
	data, _ := json.Marshal(doc)
	putBucketPolicy(t, s, bucket, string(data))
	out, err := s.client.GetBucketPolicyStatus(context.Background(), &s3.GetBucketPolicyStatusInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.PolicyStatus == nil || aws.ToBool(out.PolicyStatus.IsPublic) != want {
		t.Fatalf("policy status=%#v, want public=%v", out.PolicyStatus, want)
	}
}
