package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const replicationRole = "arn:aws:iam::635518764071:role/replication"

func TestReplication(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 Replication 설정이 되는지 확인(put/get/delete)
		{"test_replication_set", testReplicationSet},
		// 원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
		{"test_replication_invalid_source_bucket_name", testReplicationInvalidSource},
		// 원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
		{"test_replication_invalid_source_bucket_versioning", testReplicationInvalidSourceVersioning},
		// 대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
		{"test_replication_invalid_target_bucket_name", testReplicationInvalidTarget},
		// 대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
		{"test_replication_invalid_target_bucket_versioning", testReplicationInvalidTargetVersioning},
		// 버킷에 복제 설정을 하고 버저닝 설정을 중단 했을 때 실패하는지 확인
		{"test_replication_bucket_versioning_suspend", testReplicationSuspendVersioning},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func replicationConfiguration(target, prefix string) *types.ReplicationConfiguration {
	rule := types.ReplicationRule{Status: types.ReplicationRuleStatusEnabled, Destination: &types.Destination{Bucket: aws.String("arn:aws:s3:::" + target)}}
	if prefix != "" {
		rule.Priority = aws.Int32(1)
		rule.Filter = &types.ReplicationRuleFilter{Prefix: aws.String(prefix)}
		rule.DeleteMarkerReplication = &types.DeleteMarkerReplication{Status: types.DeleteMarkerReplicationStatusDisabled}
	}
	return &types.ReplicationConfiguration{Role: aws.String(replicationRole), Rules: []types.ReplicationRule{rule}}
}

func putReplication(t *testing.T, s *suite, source string, configuration *types.ReplicationConfiguration) {
	t.Helper()
	if _, err := putReplicationError(s, source, configuration); err != nil {
		t.Fatal(err)
	}
}

func testReplicationSet(t *testing.T) {
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	enableVersioning(t, s, source)
	enableVersioning(t, s, target)
	want := replicationConfiguration(target, "test/")
	putReplication(t, s, source, want)
	out, err := s.client.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String(source)})
	if err != nil || out.ReplicationConfiguration == nil {
		t.Fatalf("configuration=%#v err=%v", out.ReplicationConfiguration, err)
	}
	assertReplicationConfiguration(t, out.ReplicationConfiguration, want)
	if _, err := s.client.DeleteBucketReplication(context.Background(), &s3.DeleteBucketReplicationInput{Bucket: aws.String(source)}); err != nil {
		t.Fatal(err)
	}
	_, err = s.client.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String(source)})
	assertHTTPError(t, err, 404)
}

func testReplicationInvalidSource(t *testing.T) {
	s := newSuite(t)
	source, target := "missing-source-"+uniqueBucketSuffix(t), "missing-target-"+uniqueBucketSuffix(t)
	_, err := putReplicationError(s, source, replicationConfiguration(target, ""))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

func testReplicationInvalidSourceVersioning(t *testing.T) {
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	_, err := putReplicationError(s, source, replicationConfiguration(target, ""))
	assertS3Error(t, err, 400, "InvalidRequest")
}

func testReplicationInvalidTarget(t *testing.T) {
	s := newSuite(t)
	source := s.bucket(t)
	enableVersioning(t, s, source)
	target := "missing-target-" + uniqueBucketSuffix(t)
	_, err := putReplicationError(s, source, replicationConfiguration(target, "test/"))
	assertS3Error(t, err, 400, "InvalidRequest")
}

func testReplicationInvalidTargetVersioning(t *testing.T) {
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	enableVersioning(t, s, source)
	_, err := putReplicationError(s, source, replicationConfiguration(target, "test/"))
	assertS3Error(t, err, 400, "InvalidRequest")
}

func testReplicationSuspendVersioning(t *testing.T) {
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	enableVersioning(t, s, source)
	enableVersioning(t, s, target)
	putReplication(t, s, source, replicationConfiguration(target, "test/"))
	_, err := s.client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(source), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended}})
	assertS3Error(t, err, 409, "InvalidBucketState")
}

func assertReplicationConfiguration(t *testing.T, got, want *types.ReplicationConfiguration) {
	t.Helper()
	if aws.ToString(got.Role) != aws.ToString(want.Role) || len(got.Rules) != 1 || len(want.Rules) != 1 {
		t.Fatalf("configuration=%#v want=%#v", got, want)
	}
	g, w := got.Rules[0], want.Rules[0]
	if g.Status != w.Status || g.Destination == nil || aws.ToString(g.Destination.Bucket) != aws.ToString(w.Destination.Bucket) || aws.ToInt32(g.Priority) != aws.ToInt32(w.Priority) || g.Filter == nil || aws.ToString(g.Filter.Prefix) != aws.ToString(w.Filter.Prefix) || g.DeleteMarkerReplication == nil || g.DeleteMarkerReplication.Status != w.DeleteMarkerReplication.Status {
		t.Fatalf("rule=%#v want=%#v", g, w)
	}
}

func putReplicationError(s *suite, source string, configuration *types.ReplicationConfiguration) (*s3.PutBucketReplicationOutput, error) {
	return s.client.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{Bucket: aws.String(source), ReplicationConfiguration: configuration})
}
