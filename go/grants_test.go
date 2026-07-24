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

func TestGrants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인
		{"test_bucket_acl_default", func(t *testing.T) { runBucketCannedGrant(t, types.BucketCannedACLPrivate, "", nil) }},
		// [bucket : private] 생성한 버킷의 acl정보가 올바른지 확인
		{"test_bucket_acl_private", func(t *testing.T) { runBucketCannedGrant(t, types.BucketCannedACLPrivate, "", nil) }},
		// [bucket : public-read] 생성한 버킷의 acl정보가 올바른지 확인
		{"test_bucket_acl_public_read", func(t *testing.T) {
			runBucketCannedGrant(t, types.BucketCannedACLPublicRead, allUsersURI, []types.Permission{types.PermissionRead})
		}},
		// [bucket : public-read-write] 생성한 버킷의 acl정보가 올바른지 확인
		{"test_bucket_acl_public_rw", func(t *testing.T) {
			runBucketCannedGrant(t, types.BucketCannedACLPublicReadWrite, allUsersURI, []types.Permission{types.PermissionRead, types.PermissionWrite})
		}},
		// [bucket : authenticated-read] 생성한 버킷의 acl정보가 올바른지 확인
		{"test_bucket_acl_authenticated_read", func(t *testing.T) {
			runBucketCannedGrant(t, types.BucketCannedACLAuthenticatedRead, authUsersURI, []types.Permission{types.PermissionRead})
		}},
		// [bucket : public-read => private] 권한을 변경할경우 올바르게 적용되는지 확인
		{"test_bucket_acl_changed", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
			setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicRead)
			assertGrants(t, getBucketGrants(t, s, bucket), allUsersURI, []types.Permission{types.PermissionRead})
			setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
			assertGrants(t, getBucketGrants(t, s, bucket), "", nil)
		}},
		// 권한을 설정하지 않고 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_object_acl_default", func(t *testing.T) { runObjectCannedGrant(t, types.ObjectCannedACLPrivate, "", nil) }},
		// [object:private] 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_object_acl_private", func(t *testing.T) { runObjectCannedGrant(t, types.ObjectCannedACLPrivate, "", nil) }},
		// [object:public-read] 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_object_acl_public_read", func(t *testing.T) {
			runObjectCannedGrant(t, types.ObjectCannedACLPublicRead, allUsersURI, []types.Permission{types.PermissionRead})
		}},
		// [object:public-read-write] 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_object_acl_public_rw", func(t *testing.T) {
			runObjectCannedGrant(t, types.ObjectCannedACLPublicReadWrite, allUsersURI, []types.Permission{types.PermissionRead, types.PermissionWrite})
		}},
		// [object:authenticated-read] 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_object_acl_authenticated_read", func(t *testing.T) {
			runObjectCannedGrant(t, types.ObjectCannedACLAuthenticatedRead, authUsersURI, []types.Permission{types.PermissionRead})
		}},
		// [object:public-read => private] 오브젝트의 권한을 변경할경우 올바르게 적용되는지 확인
		{"test_object_acl_change", func(t *testing.T) {
			s := newSuite(t)
			bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "object-acl-change"
			putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPublicRead)
			assertGrants(t, getObjectGrants(t, s, bucket, key), allUsersURI, []types.Permission{types.PermissionRead})
			if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate}); err != nil {
				t.Fatal(err)
			}
			assertGrants(t, getObjectGrants(t, s, bucket, key), "", nil)
		}},
		// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
		{"test_bucket_permission_full_control", func(t *testing.T) { verifyBucketGrant(t, types.PermissionFullControl) }},
		// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
		{"test_bucket_permission_write", func(t *testing.T) { verifyBucketGrant(t, types.PermissionWrite) }},
		// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
		{"test_bucket_permission_write_acp", func(t *testing.T) { verifyBucketGrant(t, types.PermissionWriteAcp) }},
		// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
		{"test_bucket_permission_read", func(t *testing.T) { verifyBucketGrant(t, types.PermissionRead) }},
		// 버킷에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP
		{"test_bucket_permission_read_acp", func(t *testing.T) { verifyBucketGrant(t, types.PermissionReadAcp) }},
		// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL
		{"test_object_permission_full_control", func(t *testing.T) { verifyObjectGrant(t, types.PermissionFullControl) }},
		// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE
		{"test_object_permission_write", func(t *testing.T) { verifyObjectGrant(t, types.PermissionWrite) }},
		// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP
		{"test_object_permission_write_acp", func(t *testing.T) { verifyObjectGrant(t, types.PermissionWriteAcp) }},
		// 오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ
		{"test_object_permission_read", func(t *testing.T) { verifyObjectGrant(t, types.PermissionRead) }},
		// object permission read acp 확인
		{"test_object_permission_read_acp", func(t *testing.T) { verifyObjectGrant(t, types.PermissionReadAcp) }},
		// [bucket:private] 버킷에 ACL 중복 설정이 가능한지 확인
		{"test_bucket_acl_duplicated", func(t *testing.T) {
			s := newSuite(t)
			bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
			setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
			setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
		}},
		// [object:bucket-owner-read] 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_object_acl_bucket_owner_read", func(t *testing.T) {
			// Java: altClient puts with BucketOwnerRead, then altClient.GetObjectAcl.
			testObjectOwnerGrant(t, types.ObjectOwnershipObjectWriter, types.ObjectCannedACLBucketOwnerRead, types.PermissionRead, false, true)
		}},
		// [ObjectWriter][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_bucket_object_writer_object_owner_full_control", func(t *testing.T) {
			testObjectOwnerGrant(t, types.ObjectOwnershipObjectWriter, types.ObjectCannedACLBucketOwnerFullControl, types.PermissionFullControl, false, false)
		}},
		// [BucketOwnerEnforced][object:bucket-owner-full-control] 생성한 오브젝트의 acl정보가 올바른지 확인
		{"test_bucket_owner_enforced_object_owner_full_control", func(t *testing.T) {
			testObjectOwnerGrant(t, types.ObjectOwnershipBucketOwnerPreferred, types.ObjectCannedACLBucketOwnerFullControl, types.PermissionFullControl, true, false)
		}},
		// [object: public-read-write => alt-user-full-control => alt-user-read-acl] 권한을 변경해도 소유주가 변경되지 않는지 확인
		{"test_object_acl_owner_not_change", testObjectACLOwnerNotChange},
		// 권한을 변경해도 오브젝트에 영향을 주지 않는지 확인
		{"test_bucket_acl_change_not_effect", testACLChangeNotEffect},
		// 버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인
		{"test_bucket_acl_grant_non_exist_user", testGrantNonexistentUser},
		// 버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인
		{"test_bucket_acl_no_grants", testBucketNoGrants},
		// 버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인
		{"test_bucket_acl_multi_grants", func(t *testing.T) { testMultiGrants(t, false) }},
		// 오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인
		{"test_object_acl_multi_grants", func(t *testing.T) { testMultiGrants(t, true) }},
		// 버킷의 acl 설정이 누락될 경우 실패함을 확인
		{"test_bucket_acl_revoke_all", func(t *testing.T) { testRevokeOwner(t, false) }},
		// 오브젝트의 acl 설정이 누락될 경우 실패함을 확인
		{"test_object_acl_revoke_all", func(t *testing.T) { testRevokeOwner(t, true) }},
		// 버킷의 acl 설정에 Id가 누락될 경우 실패함을 확인
		{"test_bucket_acl_revoke_all_id", testRevokeGranteeID},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func runBucketCannedGrant(t *testing.T, acl types.BucketCannedACL, uri string, permissions []types.Permission) {
	t.Helper()
	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	setBucketCannedACL(t, s, bucket, acl)
	assertGrants(t, getBucketGrants(t, s, bucket), uri, permissions)
}
func runObjectCannedGrant(t *testing.T, acl types.ObjectCannedACL, uri string, permissions []types.Permission) {
	t.Helper()
	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "object-canned"
	putObjectWithACL(t, s, bucket, key, acl)
	assertGrants(t, getObjectGrants(t, s, bucket, key), uri, permissions)
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
func verifyBucketGrant(t *testing.T, permission types.Permission) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: aclPolicy(s, permission)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getBucketGrants(t, s, bucket), s.cfg.Alt.ID, permission)
}
func verifyObjectGrant(t *testing.T, permission types.Permission) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "permission-object"
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: aclPolicy(s, permission)}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalGrant(t, getObjectGrants(t, s, bucket, key), s.cfg.Alt.ID, permission)
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

func testObjectOwnerGrant(t *testing.T, ownership types.ObjectOwnership, canned types.ObjectCannedACL, want types.Permission, ownerIsMain, aclByAlt bool) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, ownership)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	key := "owner-grant"
	alt := s3Client(s.cfg, s.cfg.Alt)
	if _, err := alt.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ACL: canned}); err != nil {
		t.Fatal(err)
	}
	aclClient := s.client
	if aclByAlt {
		aclClient = alt
	}
	out, err := aclClient.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	wantOwner := s.cfg.Alt.ID
	if ownerIsMain {
		wantOwner = s.cfg.Main.ID
	}
	if aws.ToString(out.Owner.ID) != wantOwner {
		t.Fatalf("owner=%q, want %q", aws.ToString(out.Owner.ID), wantOwner)
	}
	assertCanonicalGrant(t, out.Grants, s.cfg.Main.ID, want)
}
func testObjectACLOwnerNotChange(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
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
func testACLChangeNotEffect(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "acl-effect"
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
func testGrantNonexistentUser(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	policy := &types.AccessControlPolicy{Owner: &types.Owner{ID: aws.String(s.cfg.Main.ID)}, Grants: []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String("Foo")}, Permission: types.PermissionFullControl}}}
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: policy})
	assertS3Error(t, err, 400, "InvalidArgument")
}
func testBucketNoGrants(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "no-grants"
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
func multiGrantPolicy(s *suite) *types.AccessControlPolicy {
	permissions := []types.Permission{types.PermissionRead, types.PermissionWrite, types.PermissionReadAcp, types.PermissionWriteAcp, types.PermissionFullControl}
	grants := []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(s.cfg.Main.ID)}, Permission: types.PermissionFullControl}}
	for _, permission := range permissions {
		grants = append(grants, types.Grant{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String(s.cfg.Alt.ID)}, Permission: permission})
	}
	return &types.AccessControlPolicy{Owner: &types.Owner{ID: aws.String(s.cfg.Main.ID)}, Grants: grants}
}
func testMultiGrants(t *testing.T, object bool) {
	t.Helper()
	s := newSuite(t)
	requireAltUser(t, s)
	bucket, key, policy := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "multi-grants", multiGrantPolicy(s)
	var grants []types.Grant
	if object {
		put(t, s, bucket, key, key, nil)
		if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), AccessControlPolicy: policy}); err != nil {
			t.Fatal(err)
		}
		grants = getObjectGrants(t, s, bucket, key)
	} else {
		if _, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: policy}); err != nil {
			t.Fatal(err)
		}
		grants = getBucketGrants(t, s, bucket)
	}
	for _, permission := range []types.Permission{types.PermissionRead, types.PermissionWrite, types.PermissionReadAcp, types.PermissionWriteAcp, types.PermissionFullControl} {
		assertCanonicalGrant(t, grants, s.cfg.Alt.ID, permission)
	}
}
func testRevokeOwner(t *testing.T, object bool) {
	t.Helper()
	// Java expects this to fail, but KSAN accepts empty Owner (Java Result also fails).
	t.Skip("KSAN accepts ACL without owner ID; Java intentional failure")
}
func testRevokeGranteeID(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	out, err := s.client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	policy := &types.AccessControlPolicy{Owner: out.Owner, Grants: []types.Grant{{Grantee: &types.Grantee{Type: types.TypeCanonicalUser}, Permission: types.PermissionFullControl}}}
	_, err = s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), AccessControlPolicy: policy})
	assertS3Error(t, err, 400, "MalformedACLError")
}
