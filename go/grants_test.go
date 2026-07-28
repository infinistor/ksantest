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

func TestBucketAclDefault(t *testing.T) {
	t.Parallel()

	runBucketCannedGrant(t, types.BucketCannedACLPrivate, "", nil)
}
func TestBucketAclPrivate(t *testing.T) {
	t.Parallel()

	runBucketCannedGrant(t, types.BucketCannedACLPrivate, "", nil)
}
func TestBucketAclPublicRead(t *testing.T) {
	t.Parallel()

	runBucketCannedGrant(t, types.BucketCannedACLPublicRead, allUsersURI, []types.Permission{types.PermissionRead})
}
func TestBucketAclPublicRW(t *testing.T) {
	t.Parallel()

	runBucketCannedGrant(t, types.BucketCannedACLPublicReadWrite, allUsersURI, []types.Permission{types.PermissionRead, types.PermissionWrite})
}
func TestBucketAclAuthenticatedRead(t *testing.T) {
	t.Parallel()

	runBucketCannedGrant(t, types.BucketCannedACLAuthenticatedRead, authUsersURI, []types.Permission{types.PermissionRead})
}
func TestBucketAclChanged(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicRead)
	assertGrants(t, getBucketGrants(t, s, bucket), allUsersURI, []types.Permission{types.PermissionRead})
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
	assertGrants(t, getBucketGrants(t, s, bucket), "", nil)
}
func TestObjectAclDefault(t *testing.T) {
	t.Parallel()

	runObjectCannedGrant(t, types.ObjectCannedACLPrivate, "", nil)
}
func TestObjectAclPrivate(t *testing.T) {
	t.Parallel()

	runObjectCannedGrant(t, types.ObjectCannedACLPrivate, "", nil)
}
func TestObjectAclPublicRead(t *testing.T) {
	t.Parallel()

	runObjectCannedGrant(t, types.ObjectCannedACLPublicRead, allUsersURI, []types.Permission{types.PermissionRead})
}
func TestObjectAclPublicRW(t *testing.T) {
	t.Parallel()

	runObjectCannedGrant(t, types.ObjectCannedACLPublicReadWrite, allUsersURI, []types.Permission{types.PermissionRead, types.PermissionWrite})
}
func TestObjectAclAuthenticatedRead(t *testing.T) {
	t.Parallel()

	runObjectCannedGrant(t, types.ObjectCannedACLAuthenticatedRead, authUsersURI, []types.Permission{types.PermissionRead})
}
func TestObjectAclChange(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "object-acl-change"
	putObjectWithACL(t, s, bucket, key, types.ObjectCannedACLPublicRead)
	assertGrants(t, getObjectGrants(t, s, bucket, key), allUsersURI, []types.Permission{types.PermissionRead})
	if _, err := s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPrivate}); err != nil {
		t.Fatal(err)
	}
	assertGrants(t, getObjectGrants(t, s, bucket, key), "", nil)
}
func TestBucketPermissionFullControl(t *testing.T) {
	t.Parallel()

	verifyBucketGrant(t, types.PermissionFullControl)
}
func TestBucketPermissionWrite(t *testing.T) {
	t.Parallel()

	verifyBucketGrant(t, types.PermissionWrite)
}
func TestBucketPermissionWriteAcp(t *testing.T) {
	t.Parallel()

	verifyBucketGrant(t, types.PermissionWriteAcp)
}
func TestBucketPermissionRead(t *testing.T) {
	t.Parallel()

	verifyBucketGrant(t, types.PermissionRead)
}
func TestBucketPermissionReadAcp(t *testing.T) {
	t.Parallel()

	verifyBucketGrant(t, types.PermissionReadAcp)
}
func TestObjectPermissionFullControl(t *testing.T) {
	t.Parallel()

	verifyObjectGrant(t, types.PermissionFullControl)
}
func TestObjectPermissionWrite(t *testing.T) {
	t.Parallel()

	verifyObjectGrant(t, types.PermissionWrite)
}
func TestObjectPermissionWriteAcp(t *testing.T) {
	t.Parallel()

	verifyObjectGrant(t, types.PermissionWriteAcp)
}
func TestObjectPermissionRead(t *testing.T) {
	t.Parallel()

	verifyObjectGrant(t, types.PermissionRead)
}
func TestObjectPermissionReadAcp(t *testing.T) {
	t.Parallel()

	verifyObjectGrant(t, types.PermissionReadAcp)
}
func TestBucketAclDuplicated(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPrivate)
}
func TestObjectAclBucketOwnerRead(t *testing.T) {
	t.Parallel()

	testObjectOwnerGrant(t, types.ObjectOwnershipObjectWriter, types.ObjectCannedACLBucketOwnerRead, types.PermissionRead, false, true)
}
func TestBucketObjectWriterObjectOwnerFullControl(t *testing.T) {
	t.Parallel()

	testObjectOwnerGrant(t, types.ObjectOwnershipObjectWriter, types.ObjectCannedACLBucketOwnerFullControl, types.PermissionFullControl, false, false)
}
func TestBucketOwnerEnforcedObjectOwnerFullControl(t *testing.T) {
	t.Parallel()

	testObjectOwnerGrant(t, types.ObjectOwnershipBucketOwnerPreferred, types.ObjectCannedACLBucketOwnerFullControl, types.PermissionFullControl, true, false)
}
func TestObjectAclOwnerNotChange(t *testing.T) {
	t.Parallel()

	testObjectACLOwnerNotChange(t)
}
func TestBucketAclChangeNotEffect(t *testing.T) {
	t.Parallel()

	testACLChangeNotEffect(t)
}
func TestBucketAclGrantNonExistUser(t *testing.T) {
	t.Parallel()

	testGrantNonexistentUser(t)
}
func TestBucketAclNoGrants(t *testing.T) {
	t.Parallel()

	testBucketNoGrants(t)
}
func TestBucketAclMultiGrants(t *testing.T) {
	t.Parallel()

	testMultiGrants(t, false)
}
func TestObjectAclMultiGrants(t *testing.T) {
	t.Parallel()

	testMultiGrants(t, true)
}
func TestBucketAclRevokeAll(t *testing.T) {
	t.Parallel()

	testRevokeOwner(t, false)
}
func TestObjectAclRevokeAll(t *testing.T) {
	t.Parallel()

	testRevokeOwner(t, true)
}
func TestBucketAclRevokeAllId(t *testing.T) {
	t.Parallel()

	testRevokeGranteeID(t)
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
	s := newSuite(t)
	bucket, key := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter), "revoke-owner"
	put(t, s, bucket, key, key, nil)
	emptyOwner := &types.Owner{}
	if object {
		out, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			AccessControlPolicy: &types.AccessControlPolicy{Owner: emptyOwner, Grants: out.Grants},
		})
		assertS3Error(t, err, 400, "MalformedACLError")
		return
	}
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
