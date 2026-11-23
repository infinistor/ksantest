package s3tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷의 소유권 조회 확인
func TestGetBucketOwnership(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 1)
	if _, err := s.client.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("GetBucketOwnershipControls: %v", err)
	}
}

// 버킷을 생성할때 소유권 설정 확인
func TestCreateBucketWithOwnership(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 2)
	assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
}

// 버킷의 소유권 변경 확인
func TestChangeBucketOwnership(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 3)
	assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
	putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerPreferred)
	assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerPreferred)
}

// [BucketOwnerEnforced] 버킷 ACL 설정이 실패하는지 확인
func TestBucketOwnershipDenyACL(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 4)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead})
	assertS3Error(t, err, 403, "AccessDenied")
}

// [BucketOwnerEnforced] 오브젝트 ACL 설정이 실패하는지 확인
func TestBucketOwnershipDenyObjectACL(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 5)
	key := "TestBucketOwnershipDenyObjectACL"
	put(t, s, bucket, key, key, nil)
	_, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead})
	assertS3Error(t, err, 403, "AccessDenied")
}

// ACL 설정된 오브젝트에 소유권을 BucketOwnerEnforced로 변경하면 퍼블릭 접근이 막히는지 확인
func TestObjectOwnershipDenyChange(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 6)
	key := "TestObjectOwnershipDenyChange"
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	public := anonymousClient(s)
	headPublic(t, public, bucket, key)
	putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
	// BucketOwnerEnforced 로 바꾸면 오브젝트 ACL 이 무시되어 퍼블릭 접근이 막힌다.
	// AWS 는 이미 맺은 연결에서는 변경 전 권한으로 계속 응답하므로 새 연결로 확인한다.
	waitUntil(t, func() bool {
		_, err := freshAnonymousClient(s).HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		return errorHTTPStatus(err) == 403
	})
}

// ACL 설정된 오브젝트에 소유권을 BucketOwnerEnforced로 변경할경우 ACL 설정이 실패하는지 확인
func TestObjectOwnershipDenyACL(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 7)
	key := "TestObjectOwnershipDenyACL"
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
	_, err = s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate})
	assertS3Error(t, err, 400, "AccessControlListNotSupported")
}

// 소유권을 지정하지 않고 만든 버킷의 소유권 기본값 확인
func TestBucketOwnershipDefault(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	// s.bucket 은 소유권을 지정하지 않고 버킷을 만든다
	bucket := s.bucket(t, 8)
	// 소유권을 지정하지 않고 만든 버킷은 BucketOwnerEnforced 이다
	assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
}

// 버킷의 소유권을 모든 값으로 변경할 수 있는지 확인
func TestPutBucketOwnershipAllValues(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 9)
	for _, ownership := range []types.ObjectOwnership{types.ObjectOwnershipObjectWriter, types.ObjectOwnershipBucketOwnerPreferred, types.ObjectOwnershipBucketOwnerEnforced} {
		putOwnership(t, s, bucket, ownership)
		assertOwnership(t, s, bucket, ownership)
	}
}

// 버킷의 소유권을 잘못된 값으로 변경하면 실패하는지 확인
func TestPutBucketOwnershipInvalid(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 10)
	err := putOwnershipWith(s.client, bucket, types.ObjectOwnership("Invalid"))
	assertS3Error(t, err, 400, "MalformedXML")
}

// 버킷의 소유권 설정 삭제 확인
func TestDeleteBucketOwnership(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerPreferred, 11)
	if _, err := s.client.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucketOwnershipControls: %v", err)
	}
	_, err := s.client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "OwnershipControlsNotFoundError")
	// 설정이 없어도 삭제는 성공한다
	if _, err := s.client.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucketOwnershipControls again: %v", err)
	}
}

// 존재하지 않는 버킷의 소유권 조회·설정·삭제가 실패하는지 확인
func TestBucketOwnershipBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 12)
	_, err := s.client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	err = putOwnershipWith(s.client, bucket, types.ObjectOwnershipObjectWriter)
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버킷 소유자가 아닌 사용자의 소유권 조회·설정·삭제가 실패하는지 확인
func TestBucketOwnershipAltUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := s.bucket(t, 13)
	_, err := alt.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	err = putOwnershipWith(alt, bucket, types.ObjectOwnershipObjectWriter)
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// BucketOwnerEnforced 버킷을 ACL 과 함께 만들면 실패하는지 확인
func TestCreateBucketOwnershipEnforcedWithAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 14)
	input := createBucketInput(s.cfg, bucket)
	input.ObjectOwnership = types.ObjectOwnershipBucketOwnerEnforced
	input.ACL = types.BucketCannedACLPublicRead
	cleanupCreatedBucket(t, s, bucket)
	// BucketOwnerEnforced 버킷은 ACL 을 함께 지정해 만들 수 없다
	_, err := s.client.CreateBucket(context.Background(), input)
	assertS3Error(t, err, 400, "InvalidBucketAclWithObjectOwnership")
}

// 잘못된 소유권 값으로 버킷을 만들면 실패하는지 확인
func TestCreateBucketInvalidOwnership(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 15)
	input := createBucketInput(s.cfg, bucket)
	input.ObjectOwnership = types.ObjectOwnership("Invalid")
	cleanupCreatedBucket(t, s, bucket)
	_, err := s.client.CreateBucket(context.Background(), input)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷 ACL 에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced 로 바꿀 수 없는지 확인
func TestBucketOwnershipEnforcedWithAclGrants(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := altWritableBucket(t, s, 16)
	// 버킷 ACL 에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced 로 바꿀 수 없다
	err := putOwnershipWith(s.client, bucket, types.ObjectOwnershipBucketOwnerEnforced)
	assertS3Error(t, err, 400, "InvalidBucketAclWithObjectOwnership")
	// ACL 을 초기화하면 바꿀 수 있다
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate}); err != nil {
		t.Fatalf("PutBucketAcl private: %v", err)
	}
	putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
	assertOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerEnforced)
}

// [ObjectWriter] 올린 사용자가 오브젝트 소유자가 되는지 확인
func TestObjectOwnerObjectWriter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := altWritableBucket(t, s, 17)
	key := "test_object_owner_object_writer"

	// ObjectWriter: 올린 사용자가 오브젝트 소유자가 되고, 버킷 소유자도 읽을 수 없다
	if _, err := alt.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))}); err != nil {
		t.Fatalf("Alt PutObject: %v", err)
	}
	assertObjectOwnerID(t, s, bucket, key, s.cfg.Alt.ID)
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Error(t, err, 403, "AccessDenied")

	// bucket-owner-full-control 을 줘도 소유자는 올린 사용자다
	key2 := key + "2"
	if _, err := alt.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key2), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLBucketOwnerFullControl}); err != nil {
		t.Fatalf("Alt PutObject bucket-owner-full-control: %v", err)
	}
	assertObjectOwnerID(t, s, bucket, key2, s.cfg.Alt.ID)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key2)})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	out.Body.Close()
}

// [BucketOwnerPreferred] bucket-owner-full-control 로 올리면 버킷 소유자가 소유자가 되는지 확인
func TestObjectOwnerBucketOwnerPreferred(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := altWritableBucket(t, s, 18)
	putOwnership(t, s, bucket, types.ObjectOwnershipBucketOwnerPreferred)
	key := "test_object_owner_bucket_owner_preferred"

	// BucketOwnerPreferred: ACL 없이 올리면 올린 사용자가 소유자다
	if _, err := alt.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))}); err != nil {
		t.Fatalf("Alt PutObject: %v", err)
	}
	assertObjectOwnerID(t, s, bucket, key, s.cfg.Alt.ID)

	// bucket-owner-full-control 로 올리면 버킷 소유자가 소유자가 된다
	key2 := key + "2"
	if _, err := alt.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key2), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLBucketOwnerFullControl}); err != nil {
		t.Fatalf("Alt PutObject bucket-owner-full-control: %v", err)
	}
	assertObjectOwnerID(t, s, bucket, key2, s.cfg.Main.ID)
}

// [BucketOwnerEnforced] 누가 올려도 버킷 소유자가 오브젝트 소유자가 되는지 확인
func TestObjectOwnerBucketOwnerEnforced(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 19)
	key := "test_object_owner_bucket_owner_enforced"
	principal := map[string]any{"CanonicalUser": s.cfg.Alt.ID}
	policy := makeJSONPolicy("s3:PutObject", bucketARN(bucket)+"/*", principal)
	if _, err := s.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}

	// BucketOwnerEnforced: 누가 올려도 버킷 소유자가 소유자다
	waitUntil(t, func() bool {
		_, err := alt.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
		return errorHTTPStatus(err) == 200
	})
	assertObjectOwnerID(t, s, bucket, key, s.cfg.Main.ID)

	// bucket-owner-full-control 은 허용되고 그 밖의 ACL 은 거부된다
	if _, err := alt.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key + "2"), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLBucketOwnerFullControl}); err != nil {
		t.Fatalf("Alt PutObject bucket-owner-full-control: %v", err)
	}
	_, err := alt.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key + "3"), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLPrivate})
	assertS3Error(t, err, 400, "AccessControlListNotSupported")
}

// [BucketOwnerEnforced] ACL 조회는 가능하고 변경은 거부되는지 확인
func TestBucketOwnershipEnforcedAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 20)
	key := "test_bucket_ownership_enforced_acl"
	put(t, s, bucket, key, key, nil)

	// ACL 조회는 가능하고 소유자의 FULL_CONTROL 만 나온다
	bucketACL, err := s.client.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	objectACL, err := s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObjectAcl: %v", err)
	}
	for _, grants := range [][]types.Grant{bucketACL.Grants, objectACL.Grants} {
		if len(grants) != 1 || grants[0].Grantee == nil || aws.ToString(grants[0].Grantee.ID) != s.cfg.Main.ID || grants[0].Permission != types.PermissionFullControl {
			t.Fatalf("grants = %#v, want only owner %q FULL_CONTROL", grants, s.cfg.Main.ID)
		}
	}

	// ACL 변경은 거부된다
	_, err = s.client.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate})
	assertS3Error(t, err, 400, "AccessControlListNotSupported")
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key + "2"), Body: bytes.NewReader([]byte("a")), ACL: types.ObjectCannedACLPublicRead})
	assertS3Error(t, err, 400, "AccessControlListNotSupported")
	// 소유자의 bucket-owner-full-control 은 허용된다
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key + "3"), Body: bytes.NewReader([]byte("a")), ACL: types.ObjectCannedACLBucketOwnerFullControl}); err != nil {
		t.Fatalf("PutObject bucket-owner-full-control: %v", err)
	}
}

// 지정한 클라이언트로 버킷 소유권을 설정하고 에러를 그대로 돌려준다.
func putOwnershipWith(client *s3.Client, bucket string, ownership types.ObjectOwnership) error {
	_, err := client.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String(bucket), OwnershipControls: &types.OwnershipControls{Rules: []types.OwnershipControlsRule{{ObjectOwnership: ownership}}}})
	return err
}

// ACL 로 alt 사용자에게 쓰기 권한을 준 ObjectWriter 버킷을 만든다.
func altWritableBucket(t *testing.T, s *suite, id int) string {
	t.Helper()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, id)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), GrantFullControl: aws.String("id=" + s.cfg.Main.ID), GrantWrite: aws.String("id=" + s.cfg.Alt.ID)})
	if err != nil {
		t.Fatalf("PutBucketAcl grants: %v", err)
	}
	return bucket
}

// 오브젝트 목록에서 조회한 오브젝트 소유자 아이디를 확인한다.
func assertObjectOwnerID(t *testing.T, s *suite, bucket, key, want string) {
	t.Helper()
	out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(key), FetchOwner: aws.Bool(true)})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(out.Contents) == 0 || out.Contents[0].Owner == nil {
		t.Fatalf("contents = %#v, want object %q with owner", out.Contents, key)
	}
	if got := aws.ToString(out.Contents[0].Owner.ID); got != want {
		t.Fatalf("owner of %q = %q, want %q", key, got, want)
	}
}

// 생성이 실패해야 하는 버킷이 잘못 만들어졌을 때를 대비해 정리 함수를 등록한다.
func cleanupCreatedBucket(t *testing.T, s *suite, bucket string) {
	t.Helper()
	t.Cleanup(func() {
		if s.cfg.NotDelete {
			return
		}
		_, _ = s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
}

func ownershipBucket(t *testing.T, s *suite, ownership types.ObjectOwnership, id ...int) string {
	t.Helper()
	name := buildBucketName(s.cfg.BucketPrefix, s.suiteID, id...)
	input := createBucketInput(s.cfg, name)
	input.ObjectOwnership = ownership
	_, err := s.client.CreateBucket(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if ownership != types.ObjectOwnershipBucketOwnerEnforced {
		disablePublicAccessBlock(t, s, name)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			removeOwnedBucket(t, s, name)
		}
	})
	return name
}

func disablePublicAccessBlock(t *testing.T, s *suite, bucket string) {
	t.Helper()
	_, err := s.client.PutPublicAccessBlock(context.Background(), &s3.PutPublicAccessBlockInput{
		Bucket: aws.String(bucket),
		PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
			BlockPublicAcls:       aws.Bool(false),
			IgnorePublicAcls:      aws.Bool(false),
			BlockPublicPolicy:     aws.Bool(false),
			RestrictPublicBuckets: aws.Bool(false),
		},
	})
	if err != nil {
		t.Fatalf("disable public access block: %v", err)
	}
}
func putOwnership(t *testing.T, s *suite, bucket string, ownership types.ObjectOwnership) {
	t.Helper()
	_, err := s.client.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String(bucket), OwnershipControls: &types.OwnershipControls{Rules: []types.OwnershipControlsRule{{ObjectOwnership: ownership}}}})
	if err != nil {
		t.Fatal(err)
	}
}
func assertOwnership(t *testing.T, s *suite, bucket string, want types.ObjectOwnership) {
	t.Helper()
	out, err := s.client.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.OwnershipControls == nil || len(out.OwnershipControls.Rules) != 1 || out.OwnershipControls.Rules[0].ObjectOwnership != want {
		t.Fatalf("ownership controls = %#v, want %q", out.OwnershipControls, want)
	}
}
func anonymousClient(s *suite) *s3.Client {
	options := s.client.Options()
	options.Credentials = aws.AnonymousCredentials{}
	return s3.New(options)
}

// 연결 풀을 공유하지 않는 익명 클라이언트를 만든다.
func freshAnonymousClient(s *suite) *s3.Client {
	options := s.client.Options()
	options.Credentials = aws.AnonymousCredentials{}
	options.HTTPClient = awshttp.NewBuildableClient()
	return s3.New(options)
}
func headPublic(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	if _, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("anonymous HeadObject: %v", err)
	}
}
func removeOwnedBucket(t *testing.T, s *suite, bucket string) {
	t.Helper()
	ctx := context.Background()
	abortBucketMultipartUploads(t, s.client, bucket)
	listed, _ := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	var objects []types.ObjectIdentifier
	if listed != nil {
		for _, item := range listed.Versions {
			objects = append(objects, types.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
		}
		for _, item := range listed.DeleteMarkers {
			objects = append(objects, types.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
		}
	}
	if len(objects) != 0 {
		_, _ = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects}})
	}
	_, _ = s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
