package s3tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	allUsersURI  = "http://acs.amazonaws.com/groups/global/AllUsers"
	authUsersURI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)

// 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
func TestBucketAclDefault(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 1)
	assertGrants(t, getBucketGrants(t, s, bucket), "", nil)
}

// [bucket : private] 생성한 버킷의 acl정보가 올바른지 확인
func TestBucketAclPrivate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 3)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
	assertGrants(t, getBucketGrants(t, s, bucket), "", nil)
}

// [bucket : public-read] 생성한 버킷의 acl정보가 올바른지 확인
func TestBucketAclPublicRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 4)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicRead)
	assertGrants(t, getBucketGrants(t, s, bucket), allUsersURI, []types.Permission{types.PermissionRead})
}

// [bucket : public-read-write] 생성한 버킷의 acl정보가 올바른지 확인
func TestBucketAclPublicRW(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 5)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	assertGrants(t, getBucketGrants(t, s, bucket), allUsersURI, []types.Permission{types.PermissionRead, types.PermissionWrite})
}

// [bucket : authenticated-read] 생성한 버킷의 acl정보가 올바른지 확인
func TestBucketAclAuthenticatedRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 6)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLAuthenticatedRead)
	assertGrants(t, getBucketGrants(t, s, bucket), authUsersURI, []types.Permission{types.PermissionRead})
}

// [bucket : public-read => private] 권한을 변경할경우 올바르게 적용되는지 확인
func TestBucketAclChanged(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 2)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicRead)
	assertGrants(t, getBucketGrants(t, s, bucket), allUsersURI, []types.Permission{types.PermissionRead})
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
	assertGrants(t, getBucketGrants(t, s, bucket), "", nil)
}

// 권한을 설정하지 않고 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclDefault(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "object-canned"
	bucket := s.bucket(t, 7)
	put(t, s, bucket, key, key, nil)
	assertGrants(t, getObjectGrants(t, s, bucket, key), "", nil)
}

// [object:private] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclPrivate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "object-canned"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 9)
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPrivate)
	assertGrants(t, getObjectGrants(t, s, bucket, key), "", nil)
}

// [object:public-read] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclPublicRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "object-canned"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 10)
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPublicRead)
	assertGrants(t, getObjectGrants(t, s, bucket, key), allUsersURI, []types.Permission{types.PermissionRead})
}

// [object:public-read-write] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclPublicRW(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "object-canned"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 11)
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPublicReadWrite)
	assertGrants(t, getObjectGrants(t, s, bucket, key), allUsersURI, []types.Permission{types.PermissionRead, types.PermissionWrite})
}

// [object:authenticated-read] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclAuthenticatedRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "object-canned"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 12)
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLAuthenticatedRead)
	assertGrants(t, getObjectGrants(t, s, bucket, key), authUsersURI, []types.Permission{types.PermissionRead})
}

// [object:public-read => private] 오브젝트의 권한을 변경할경우 올바르게 적용되는지 확인
func TestObjectAclChange(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 8), "object-acl-change"
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPublicRead)
	assertGrants(t, getObjectGrants(t, s, bucket, key), allUsersURI, []types.Permission{types.PermissionRead})
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate}); err != nil {
		t.Fatal(err)
	}
	assertGrants(t, getObjectGrants(t, s, bucket, key), "", nil)
}

// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
func TestBucketPermissionFullControl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 19)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionFullControl)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getBucketGrants(t, s, bucket), s.cfg.Alt.ID, types.PermissionFullControl)
}

// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
func TestBucketPermissionWrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 20)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionWrite)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getBucketGrants(t, s, bucket), s.cfg.Alt.ID, types.PermissionWrite)
}

// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
func TestBucketPermissionWriteAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 21)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionWriteAcp)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getBucketGrants(t, s, bucket), s.cfg.Alt.ID, types.PermissionWriteAcp)
}

// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
func TestBucketPermissionRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 22)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionRead)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getBucketGrants(t, s, bucket), s.cfg.Alt.ID, types.PermissionRead)
}

// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
func TestBucketPermissionReadAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 23)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, types.PermissionReadAcp)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getBucketGrants(t, s, bucket), s.cfg.Alt.ID, types.PermissionReadAcp)
}

// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
func TestObjectPermissionFullControl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 24), "permission-object"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionFullControl)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getObjectGrants(t, s, bucket, key), s.cfg.Alt.ID, types.PermissionFullControl)
}

// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
func TestObjectPermissionWrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 25), "permission-object"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionWrite)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getObjectGrants(t, s, bucket, key), s.cfg.Alt.ID, types.PermissionWrite)
}

// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
func TestObjectPermissionWriteAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 26), "permission-object"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionWriteAcp)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getObjectGrants(t, s, bucket, key), s.cfg.Alt.ID, types.PermissionWriteAcp)
}

// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
func TestObjectPermissionRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 27), "permission-object"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionRead)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getObjectGrants(t, s, bucket, key), s.cfg.Alt.ID, types.PermissionRead)
}

// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
func TestObjectPermissionReadAcp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 28), "permission-object"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionReadAcp)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getObjectGrants(t, s, bucket, key), s.cfg.Alt.ID, types.PermissionReadAcp)
}

// [bucket:private] 버킷에 ACL 중복 설정이 가능한지 확인
func TestBucketAclDuplicated(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 18)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
}

// [object:bucket-owner-read] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclBucketOwnerRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 13)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	key := "owner-grant"
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLBucketOwnerRead}); err != nil {
		t.Fatal(err)
	}
	out, err := alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	wantOwner := s.cfg.Alt.ID
	if aws.ToString(out.Owner.ID) != wantOwner {
		t.Fatalf("owner=%q, want %q", aws.ToString(out.Owner.ID), wantOwner)
	}
	assertCanonicalGrant(t, out.Grants, s.cfg.Main.ID, types.PermissionRead)
}

// [ObjectWriter][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestBucketObjectWriterObjectOwnerFullControl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 14)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	key := "owner-grant"
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLBucketOwnerFullControl}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	wantOwner := s.cfg.Alt.ID
	if aws.ToString(out.Owner.ID) != wantOwner {
		t.Fatalf("owner=%q, want %q", aws.ToString(out.Owner.ID), wantOwner)
	}
	assertCanonicalGrant(t, out.Grants, s.cfg.Main.ID, types.PermissionFullControl)
}

// [BucketOwnerEnforced][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestBucketOwnerEnforcedObjectOwnerFullControl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerPreferred, 15)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	key := "owner-grant"
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: types.ObjectCannedACLBucketOwnerFullControl}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	wantOwner := s.cfg.Main.ID
	if aws.ToString(out.Owner.ID) != wantOwner {
		t.Fatalf("owner=%q, want %q", aws.ToString(out.Owner.ID), wantOwner)
	}
	assertCanonicalGrant(t, out.Grants, s.cfg.Main.ID, types.PermissionFullControl)
}

// [object: public-read-write => alt-user-full-control => alt-user-read-acl] 권한을 변경해도 소유주가 변경되지 않는지 확인
func TestObjectAclOwnerNotChange(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 16)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	key := "owner-not-change"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionFullControl)}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	policy := aclPolicy(s, types.PermissionReadAcp)
	if _, err := alt.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: policy}); err != nil {
		t.Fatal(err)
	}
	out, err := alt.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(out.Owner.ID) != s.cfg.Main.ID {
		t.Fatalf("owner changed to %q", aws.ToString(out.Owner.ID))
	}
	assertCanonicalGrant(t, out.Grants, s.cfg.Alt.ID, types.PermissionReadAcp)
}

// 권한을 변경해도 오브젝트에 영향을 주지 않는지 확인
func TestBucketAclChangeNotEffect(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 17), "acl-effect"
	put(t, s, bucket, key, key, nil)
	before, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, types.PermissionFullControl)}); err != nil {
		t.Fatal(err)
	}
	after, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(before.ETag) != aws.ToString(after.ETag) || aws.ToString(before.ContentType) != aws.ToString(after.ContentType) {
		t.Fatalf("object metadata changed after ACL update")
	}
}

// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
func TestBucketAclGrantNonExistUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 29)
	policy := &types.AccessControlPolicy{Owner: &types.Owner{ID: aws.String(s.cfg.Main.ID)}, Grants: []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String("Foo")}, Permission: types.PermissionFullControl}}}
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: policy})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
func TestBucketAclNoGrants(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 30), "no-grants"
	put(t, s, bucket, key, key, nil)
	out, err := s.client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: &types.AccessControlPolicy{Owner: out.Owner, Grants: []types.Grant{}}}); err != nil {
		t.Fatal(err)
	}
	put(t, s, bucket, key, "A", nil)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: &types.AccessControlPolicy{Owner: out.Owner, Grants: out.Grants}}); err != nil {
		t.Fatal(err)
	}
}

// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
func TestBucketAclMultiGrants(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 31)
	policy := multiGrantPolicy(s)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: policy}); err != nil {
		t.Fatal(err)
	}
	grants := getBucketGrants(t, s, bucket)
	for _, permission := range []types.Permission{types.PermissionRead, types.PermissionWrite, types.PermissionReadAcp, types.PermissionWriteAcp, types.PermissionFullControl} {
		assertCanonicalGrant(t, grants, s.cfg.Alt.ID, permission)
	}
}

// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
func TestObjectAclMultiGrants(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 32), "multi-grants"
	policy := multiGrantPolicy(s)
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: policy}); err != nil {
		t.Fatal(err)
	}
	grants := getObjectGrants(t, s, bucket, key)
	for _, permission := range []types.Permission{types.PermissionRead, types.PermissionWrite, types.PermissionReadAcp, types.PermissionWriteAcp, types.PermissionFullControl} {
		assertCanonicalGrant(t, grants, s.cfg.Alt.ID, permission)
	}
}

// 버킷의 acl 설정이 누락될 경우 실패함을 확인
func TestBucketAclRevokeAll(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 33), "revoke-owner"
	put(t, s, bucket, key, key, nil)
	emptyOwner := &types.Owner{}
	out, err := s.client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{
		Bucket:              aws.String(bucket),
		AccessControlPolicy: &types.AccessControlPolicy{Owner: emptyOwner, Grants: out.Grants},
	})
	assertS3Error(t, err, 400, "MalformedACLError")
}

// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
func TestObjectAclRevokeAll(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 34), "revoke-owner"
	put(t, s, bucket, key, key, nil)
	emptyOwner := &types.Owner{}
	out, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		AccessControlPolicy: &types.AccessControlPolicy{Owner: emptyOwner, Grants: out.Grants},
	})
	assertS3Error(t, err, 400, "MalformedACLError")
}

// 버킷의 acl 설정에 Id가 누락될 경우 실패함을 확인
func TestBucketAclRevokeAllId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 35)
	out, err := s.client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	policy := &types.AccessControlPolicy{Owner: out.Owner, Grants: []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser}, Permission: types.PermissionFullControl}}}
	_, err = s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: policy})
	assertS3Error(t, err, 400, "MalformedACLError")
}

func setBucketCannedACL(t *testing.T, s *suite, bucket string, acl types.BucketCannedACL) {
	t.Helper()
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: acl}); err != nil {
		t.Fatal(err)
	}
}

func putObjectWithACL(t *testing.T, s *suite, bucket, key string, acl types.ObjectCannedACL) {
	t.Helper()
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: acl}); err != nil {
		t.Fatal(err)
	}
}

func getBucketGrants(t *testing.T, s *suite, bucket string) []types.Grant {
	t.Helper()
	out, err := s.client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	return out.Grants
}

func getObjectGrants(t *testing.T, s *suite, bucket, key string) []types.Grant {
	t.Helper()
	out, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	return out.Grants
}

func assertGrants(t *testing.T, grants []types.Grant, uri string, permissions []types.Permission) {
	t.Helper()
	for _, permission := range permissions {
		found := false
		for _, grant := range grants {
			if grant.Grantee != nil && aws.ToString(grant.Grantee.URI) == uri && grant.Permission == permission {
				found = true
			}
		}
		if !found {
			t.Errorf("grant %s %s not found in %#v", uri, permission, grants)
		}
	}
}

func assertCanonicalGrant(t *testing.T, grants []types.Grant, id string, permission types.Permission) {
	t.Helper()
	for _, grant := range grants {
		if grant.Grantee != nil && aws.ToString(grant.Grantee.ID) == id && grant.Permission == permission {
			return
		}
	}
	t.Fatalf("canonical grant %s %s not found in %#v", id, permission, grants)
}

func multiGrantPolicy(s *suite) *types.AccessControlPolicy {
	permissions := []types.Permission{types.PermissionRead, types.PermissionWrite, types.PermissionReadAcp, types.PermissionWriteAcp, types.PermissionFullControl}
	grants := []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(s.cfg.Main.ID)}, Permission: types.PermissionFullControl}}
	for _, permission := range permissions {
		grants = append(grants, types.Grant{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(s.cfg.Alt.ID)}, Permission: permission})
	}
	return &types.AccessControlPolicy{Owner: &types.Owner{ID: aws.String(s.cfg.Main.ID)}, Grants: grants}
}
