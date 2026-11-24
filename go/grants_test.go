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
	key := "TestObjectAclDefault"
	bucket := s.bucket(t, 7)
	put(t, s, bucket, key, key, nil)
	assertGrants(t, getObjectGrants(t, s, bucket, key), "", nil)
}

// [object:private] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclPrivate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "TestObjectAclPrivate"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 9)
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPrivate)
	assertGrants(t, getObjectGrants(t, s, bucket, key), "", nil)
}

// [object:public-read] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclPublicRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "TestObjectAclPublicRead"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 10)
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPublicRead)
	assertGrants(t, getObjectGrants(t, s, bucket, key), allUsersURI, []types.Permission{types.PermissionRead})
}

// [object:public-read-write] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclPublicRW(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "TestObjectAclPublicRW"
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 11)
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPublicReadWrite)
	assertGrants(t, getObjectGrants(t, s, bucket, key), allUsersURI, []types.Permission{types.PermissionRead, types.PermissionWrite})
}

// [object:authenticated-read] 생성한 오브젝트의 acl정보가 올바른지 확인
func TestObjectAclAuthenticatedRead(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "TestObjectAclAuthenticatedRead"
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
	key := "TestObjectAclBucketOwnerRead"
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
	key := "TestBucketObjectWriterObjectOwnerFullControl"
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
	key := "TestBucketOwnerEnforcedObjectOwnerFullControl"
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
	key := "TestObjectAclOwnerNotChange"
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
// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
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
// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 가 Owner 를 비운 ACL 을 보내므로
// KSAN 은 요청자를 Owner 로 채워 받는다. AWS 는 400 MalformedACLError 다.
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

const logDeliveryURI = "http://acs.amazonaws.com/groups/s3/LogDelivery"

// 버킷 소유자를 Owner 로 하는 AccessControlPolicy. grants 가 없으면 소유자 FULL_CONTROL.
func ownerACLPolicy(t *testing.T, s *suite, bucket string, grants ...types.Grant) *types.AccessControlPolicy {
	t.Helper()
	owner := bucketOwnerID(t, s, bucket)
	if len(grants) == 0 {
		grants = []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(owner)}, Permission: types.PermissionFullControl}}
	}
	return &types.AccessControlPolicy{Owner: &types.Owner{ID: aws.String(owner)}, Grants: grants}
}

func bucketOwnerID(t *testing.T, s *suite, bucket string) string {
	t.Helper()
	out, err := s.client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if out.Owner == nil {
		t.Fatal("GetBucketAcl: owner missing")
	}
	return aws.ToString(out.Owner.ID)
}

// 없는 캔드 ACL 은 거부된다
func TestBucketAclInvalidCanned(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 36), "TestBucketAclInvalidCanned"
	put(t, s, bucket, key, "content", nil)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACL("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")
	_, err = s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACL("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 캔드 ACL 과 권한 부여 헤더는 함께 쓸 수 없다
// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 x-amz-acl 과 x-amz-grant-* 를 함께 보내면 둘을 합쳐 저장한다.
// AWS 는 400 InvalidRequest 다.
func TestAclCannedWithGrantHeader(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 37), "TestAclCannedWithGrantHeader"
	put(t, s, bucket, key, "content", nil)
	grant := aws.String(`uri="` + authUsersURI + `"`)
	_, err := s.client.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate, GrantRead: grant})
	assertS3Error(t, err, 400, "InvalidRequest")
	_, err = s.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate, GrantRead: grant})
	assertS3Error(t, err, 400, "InvalidRequest")
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key + "2"), Body: bytes.NewReader([]byte("content")), ACL: types.ObjectCannedACLPrivate, GrantRead: grant})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 캔드 ACL 과 본문(AccessControlPolicy)은 함께 쓸 수 없다
// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 x-amz-acl 이 있으면 ACL 본문은 읽지 않고 캔드 ACL 을 적용한다.
// AWS 는 400 UnexpectedContent 다.
func TestBucketAclCannedWithBody(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 38)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate, AccessControlPolicy: ownerACLPolicy(t, s, bucket)})
	assertS3Error(t, err, 400, "UnexpectedContent")
}

// 캔드 ACL, 권한 부여 헤더, 본문 중 아무것도 없으면 거부된다
func TestBucketAclMissing(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 39)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 400, "MissingSecurityHeader")
}

// 없는 권한은 거부된다
func TestBucketAclInvalidPermission(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 40)
	grant := types.Grant{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(bucketOwnerID(t, s, bucket))}, Permission: types.Permission("INVALID")}
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: ownerACLPolicy(t, s, bucket, grant)})
	assertS3Error(t, err, 400, "MalformedACLError")
}

// ACL 로 소유자를 바꿀 수 없다
// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 ACL 본문의 Owner 가 실제 소유자와 달라도 기존 소유자로 덮어써 받는다(소유자는 바뀌지 않는다).
// AWS 는 403 AccessDenied 다.
func TestBucketAclChangeOwner(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 41)
	policy := ownerACLPolicy(t, s, bucket)
	policy.Owner = &types.Owner{ID: aws.String(s.cfg.Alt.ID)}
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: policy})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 잘못된 권한 부여 헤더는 거부된다
func TestBucketAclInvalidGrantHeader(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 42)
	// 형식 오류, 없는 사용자 ID, 없는 그룹 URI
	for _, grant := range []string{"invalid", `id="invalid"`, `uri="http://example.com/invalid"`} {
		_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), GrantRead: aws.String(grant)})
		assertS3Error(t, err, 400, "InvalidArgument")
	}
}

// 없는 그룹 URI 는 거부된다
func TestBucketAclInvalidGroupUri(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 43)
	grant := types.Grant{Grantee: &types.Grantee{Type: types.TypeGroup, URI: aws.String("http://example.com/invalid")}, Permission: types.PermissionRead}
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: ownerACLPolicy(t, s, bucket, grant)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 권한 부여는 100개까지
func TestBucketAclMaxGrants(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 44)
	owner := bucketOwnerID(t, s, bucket)
	grants := make([]types.Grant, 101)
	for i := range grants {
		grants[i] = types.Grant{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(owner)}, Permission: types.PermissionFullControl}
	}
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: ownerACLPolicy(t, s, bucket, grants[:100]...)}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: ownerACLPolicy(t, s, bucket, grants...)})
	assertS3Error(t, err, 400, "MalformedACLError")
}

// 없는 버킷의 ACL 조회·설정은 실패한다
func TestBucketAclBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucketNameOnly(45)
	_, err := s.client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 다른 사용자는 버킷·오브젝트 ACL 을 조회·설정할 수 없다
func TestAclAccessDenied(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	alt := altClientOrSkip(t, s)
	ctx := context.Background()
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 46), "TestAclAccessDenied"
	put(t, s, bucket, key, "content", nil)
	_, err := alt.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPrivate})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 없는 오브젝트의 ACL 조회·설정은 실패한다
func TestObjectAclKeyNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 47), "TestObjectAclKeyNotExist"
	_, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
	_, err = s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
func TestObjectAclVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 48), "TestObjectAclVersionId"
	put(t, s, bucket, key, "content", nil)
	out, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String("null")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Grants) != 1 {
		t.Fatalf("grants=%#v, want 1", out.Grants)
	}
	_, err = s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 최신 버전이 삭제 마커이면 없는 키, 삭제 마커 버전을 지정하면 405
func TestObjectAclDeleteMarker(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 49), "TestObjectAclDeleteMarker"
	checkConfigureVersioningRetry(t, s, bucket, types.BucketVersioningStatusEnabled)
	version := put(t, s, bucket, key, "content", nil).VersionId
	marker, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
	_, err = s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: marker.VersionId})
	assertS3Error(t, err, 405, "MethodNotAllowed")
	_, err = s.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: marker.VersionId, ACL: types.ObjectCannedACLPrivate})
	assertS3Error(t, err, 405, "MethodNotAllowed")
	out, err := s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: version})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Grants) != 1 {
		t.Fatalf("grants=%#v, want 1", out.Grants)
	}
}

// 권한 부여 헤더만 주면 소유자 FULL_CONTROL 없이 지정한 권한만 남는다
func TestPutObjectWithGrantHeader(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 51), "TestPutObjectWithGrantHeader"
	altID := s.cfg.Alt.ID
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("content")), GrantRead: aws.String(`id="` + altID + `"`)}); err != nil {
		t.Fatal(err)
	}
	grants := getObjectGrants(t, s, bucket, key)
	if len(grants) != 1 || grants[0].Grantee == nil || aws.ToString(grants[0].Grantee.ID) != altID || grants[0].Permission != types.PermissionRead {
		t.Fatalf("grants=%#v, want only READ for %s", grants, altID)
	}
	assertClientObjectBody(t, s3Client(s.cfg, s.cfg.Alt), bucket, key, "content")
}
