package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 복제 역할 ARN. 계정 아이디는 설정의 AccountId(없으면 UserID)를 쓴다.
func replicationRole(s *suite) string {
	return "arn:aws:iam::" + s.cfg.Main.ExpectedOwnerID() + ":role/replication"
}

// 버킷의 Replication 설정이 되는지 확인(put/get/delete)
func TestReplicationSet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 1), s.bucket(t, 1)
	enableVersioning(t, s, source)
	enableVersioning(t, s, target)
	want := replicationConfiguration(s, target, "test/")
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

// 원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
func TestReplicationInvalidSourceBucketName(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := "missing-source-"+uniqueBucketSuffix(t), "missing-target-"+uniqueBucketSuffix(t)
	_, err := putReplicationError(s, source, replicationConfiguration(s, target, ""))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
func TestReplicationInvalidSourceBucketVersioning(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 3), s.bucket(t, 3)
	_, err := putReplicationError(s, source, replicationConfiguration(s, target, ""))
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인
func TestReplicationInvalidTargetBucketName(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source := s.bucket(t, 4)
	enableVersioning(t, s, source)
	target := "missing-target-" + uniqueBucketSuffix(t)
	_, err := putReplicationError(s, source, replicationConfiguration(s, target, "test/"))
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인
func TestReplicationInvalidTargetBucketVersioning(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 5), s.bucket(t, 5)
	enableVersioning(t, s, source)
	_, err := putReplicationError(s, source, replicationConfiguration(s, target, "test/"))
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 버킷에 복제 설정을 하고 버저닝 설정을 중단 했을 때 실패하는지 확인
func TestReplicationBucketVersioningSuspend(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 6), s.bucket(t, 6)
	enableVersioning(t, s, source)
	enableVersioning(t, s, target)
	putReplication(t, s, source, replicationConfiguration(s, target, "test/"))
	_, err := s.client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(source), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended}})
	assertS3Error(t, err, 409, "InvalidBucketState")
}

// 복제 설정이 없는 버킷의 조회는 실패하고 삭제는 성공하는지 확인
func TestReplicationGetNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 7)
	_, err := s.client.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "ReplicationConfigurationNotFoundError")
	// 설정이 없어도 삭제는 성공한다
	if _, err := s.client.DeleteBucketReplication(context.Background(), &s3.DeleteBucketReplicationInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
}

// 태그 필터로 복제 설정이 되는지 확인
func TestReplicationTagFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 8)
	rule := replicationRule(targetArn)
	rule.Filter = &types.ReplicationRuleFilter{Tag: &types.Tag{Key: aws.String("key1"), Value: aws.String("value1")}}
	putReplicationRules(t, s, source, rule)
	got := getReplicationRules(t, s, source)[0].Filter
	if got == nil || got.Prefix != nil || got.And != nil || got.Tag == nil || aws.ToString(got.Tag.Key) != "key1" || aws.ToString(got.Tag.Value) != "value1" {
		t.Fatalf("filter=%#v", got)
	}
}

// 접두어와 태그를 묶은 And 필터로 복제 설정이 되는지 확인
func TestReplicationAndFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 9)
	rule := replicationRule(targetArn)
	rule.Filter = &types.ReplicationRuleFilter{And: &types.ReplicationRuleAndOperator{
		Prefix: aws.String("test/"),
		Tags:   []types.Tag{{Key: aws.String("key1"), Value: aws.String("value1")}, {Key: aws.String("key2"), Value: aws.String("value2")}},
	}}
	putReplicationRules(t, s, source, rule)
	got := getReplicationRules(t, s, source)[0].Filter
	if got == nil || got.Prefix != nil || got.Tag != nil || got.And == nil || aws.ToString(got.And.Prefix) != "test/" || len(got.And.Tags) != 2 ||
		aws.ToString(got.And.Tags[0].Key) != "key1" || aws.ToString(got.And.Tags[0].Value) != "value1" ||
		aws.ToString(got.And.Tags[1].Key) != "key2" || aws.ToString(got.And.Tags[1].Value) != "value2" {
		t.Fatalf("filter=%#v", got)
	}
}

// 빈 접두어 필터로 복제 설정이 되는지 확인
func TestReplicationEmptyFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 10)
	// 빈 접두어는 버킷 전체를 대상으로 한다
	rule := replicationRule(targetArn)
	rule.Filter = &types.ReplicationRuleFilter{Prefix: aws.String("")}
	putReplicationRules(t, s, source, rule)
	got := getReplicationRules(t, s, source)[0].Filter
	if got == nil || aws.ToString(got.Prefix) != "" || got.Tag != nil || got.And != nil {
		t.Fatalf("filter=%#v", got)
	}
}

// 구버전(V1) 스키마로 복제 설정이 되는지 확인
func TestReplicationV1Schema(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 11)
	// 구버전(V1) 스키마: Filter/Priority/DeleteMarkerReplication 없이 규칙 직속 Prefix 를 쓴다
	putReplicationRules(t, s, source, types.ReplicationRule{
		ID: aws.String("v1-rule"), Status: types.ReplicationRuleStatusEnabled, Prefix: aws.String("v1/"),
		Destination: &types.Destination{Bucket: aws.String(targetArn)},
	})
	got := getReplicationRules(t, s, source)[0]
	if aws.ToString(got.ID) != "v1-rule" || got.Prefix == nil || aws.ToString(got.Prefix) != "v1/" || got.Status != types.ReplicationRuleStatusEnabled ||
		got.Destination == nil || aws.ToString(got.Destination.Bucket) != targetArn || got.Filter != nil {
		t.Fatalf("rule=%#v", got)
	}
}

// 여러 규칙으로 복제 설정이 되는지 확인
func TestReplicationMultipleRules(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 12)
	putReplicationRules(t, s, source,
		replicationRuleWith(targetArn, "rule-1", 1, "a/"),
		replicationRuleWith(targetArn, "rule-2", 2, "b/"),
	)
	got := getReplicationRules(t, s, source)
	if len(got) != 2 {
		t.Fatalf("rule count=%d, want 2", len(got))
	}
	for index, want := range []struct {
		id       string
		priority int32
		prefix   string
	}{{"rule-1", 1, "a/"}, {"rule-2", 2, "b/"}} {
		rule := got[index]
		if aws.ToString(rule.ID) != want.id || rule.Priority == nil || aws.ToInt32(rule.Priority) != want.priority || rule.Filter == nil || aws.ToString(rule.Filter.Prefix) != want.prefix {
			t.Fatalf("rule[%d]=%#v, want %v", index, rule, want)
		}
	}
}

// 규칙의 우선순위가 중복되면 실패하는지 확인
func TestReplicationDuplicatePriority(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 13)
	err := putReplicationRulesError(s.client, s, source,
		replicationRuleWith(targetArn, "rule-1", 1, "a/"),
		replicationRuleWith(targetArn, "rule-2", 1, "b/"),
	)
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 규칙의 아이디가 중복되면 실패하는지 확인
func TestReplicationDuplicateId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 14)
	err := putReplicationRulesError(s.client, s, source,
		replicationRuleWith(targetArn, "rule-1", 1, "a/"),
		replicationRuleWith(targetArn, "rule-1", 2, "b/"),
	)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// Filter 를 쓰는 규칙에 Priority 또는 DeleteMarkerReplication 이 없으면 실패하는지 확인
// KSAN 알려진 실패(일부러 다르게 동작, 당분간 유지): Filter 가 있는데 Priority 나 DeleteMarkerReplication 이 없어도 받는다.
// 이전부터 받아 온 형태라 검증을 넣으면 기존 설정이 거부될 수 있어 일단 넣지 않는다. AWS 는 400 InvalidRequest 다.
func TestReplicationFilterWithoutPriority(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 15)
	// Filter 를 쓰는 V2 스키마에서는 Priority 와 DeleteMarkerReplication 이 필수다
	rule := replicationRule(targetArn)
	rule.Priority = nil
	err := putReplicationRulesError(s.client, s, source, rule)
	assertS3Error(t, err, 400, "InvalidRequest")

	rule = replicationRule(targetArn)
	rule.DeleteMarkerReplication = nil
	err = putReplicationRulesError(s.client, s, source, rule)
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 삭제 마커 복제를 켠 복제 설정이 되는지 확인
func TestReplicationDeleteMarkerEnabled(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 16)
	rule := replicationRule(targetArn)
	rule.DeleteMarkerReplication = &types.DeleteMarkerReplication{Status: types.DeleteMarkerReplicationStatusEnabled}
	putReplicationRules(t, s, source, rule)
	got := getReplicationRules(t, s, source)[0]
	if got.DeleteMarkerReplication == nil || got.DeleteMarkerReplication.Status != types.DeleteMarkerReplicationStatusEnabled {
		t.Fatalf("rule=%#v", got)
	}
}

// 태그 필터와 삭제 마커 복제를 함께 설정하면 실패하는지 확인
func TestReplicationDeleteMarkerWithTagFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 17)
	// 태그 필터가 있으면 삭제 마커 복제를 켤 수 없다
	rule := replicationRule(targetArn)
	rule.Filter = &types.ReplicationRuleFilter{Tag: &types.Tag{Key: aws.String("key1"), Value: aws.String("value1")}}
	rule.DeleteMarkerReplication = &types.DeleteMarkerReplication{Status: types.DeleteMarkerReplicationStatusEnabled}
	err := putReplicationRulesError(s.client, s, source, rule)
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 비활성화된 규칙으로 복제 설정이 되는지 확인
func TestReplicationDisabledRule(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, targetArn := replicationBuckets(t, s, 18)
	rule := replicationRule(targetArn)
	rule.Status = types.ReplicationRuleStatusDisabled
	putReplicationRules(t, s, source, rule)
	if got := getReplicationRules(t, s, source)[0]; got.Status != types.ReplicationRuleStatusDisabled {
		t.Fatalf("rule=%#v", got)
	}
}

// 원본 버킷을 대상으로 지정하면 복제 설정이 실패하는지 확인
func TestReplicationSameBucket(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source := s.bucket(t, 20)
	enableVersioning(t, s, source)
	err := putReplicationRulesError(s.client, s, source, replicationRule("arn:aws:s3:::"+source))
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 대상 버킷이 ARN 형식이 아니면 복제 설정이 실패하는지 확인
func TestReplicationInvalidDestinationArn(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 21), s.bucket(t, 21)
	enableVersioning(t, s, source)
	enableVersioning(t, s, target)
	// 대상 버킷은 ARN 형식이어야 한다
	err := putReplicationRulesError(s.client, s, source, replicationRule(target))
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 존재하지 않는 버킷의 복제 설정 조회/삭제 시 NoSuchBucket 에러가 발생하는지 확인
func TestReplicationBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 22)
	_, err := s.client.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketReplication(context.Background(), &s3.DeleteBucketReplicationInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버킷 소유자가 아닌 사용자의 복제 설정/조회/삭제가 실패하는지 확인
func TestReplicationAltUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	altClient := s3Client(s.cfg, s.cfg.Alt)
	source, targetArn := replicationBuckets(t, s, 23)
	rule := replicationRule(targetArn)
	putReplicationRules(t, s, source, rule)
	_, err := altClient.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String(source)})
	assertS3Error(t, err, 403, "AccessDenied")
	err = putReplicationRulesError(altClient, s, source, rule)
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = altClient.DeleteBucketReplication(context.Background(), &s3.DeleteBucketReplicationInput{Bucket: aws.String(source)})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 버저닝이 켜진 원본·대상 버킷을 만들고 (원본 버킷명, 대상 버킷 ARN)을 돌려준다.
func replicationBuckets(t *testing.T, s *suite, id int) (string, string) {
	t.Helper()
	source, target := s.bucket(t, id), s.bucket(t, id)
	enableVersioning(t, s, source)
	enableVersioning(t, s, target)
	return source, "arn:aws:s3:::" + target
}

// 기본 규칙(Enabled, Priority 1, 접두어 test/, 삭제 마커 복제 Disabled)을 만든다. 호출부에서 필드를 덮어쓴다.
func replicationRule(targetArn string) types.ReplicationRule {
	return types.ReplicationRule{
		Status:                  types.ReplicationRuleStatusEnabled,
		Priority:                aws.Int32(1),
		Filter:                  &types.ReplicationRuleFilter{Prefix: aws.String("test/")},
		Destination:             &types.Destination{Bucket: aws.String(targetArn)},
		DeleteMarkerReplication: &types.DeleteMarkerReplication{Status: types.DeleteMarkerReplicationStatusDisabled},
	}
}

func replicationRuleWith(targetArn, id string, priority int32, prefix string) types.ReplicationRule {
	rule := replicationRule(targetArn)
	rule.ID = aws.String(id)
	rule.Priority = aws.Int32(priority)
	rule.Filter = &types.ReplicationRuleFilter{Prefix: aws.String(prefix)}
	return rule
}

func putReplicationRules(t *testing.T, s *suite, source string, rules ...types.ReplicationRule) {
	t.Helper()
	if err := putReplicationRulesError(s.client, s, source, rules...); err != nil {
		t.Fatal(err)
	}
}

func putReplicationRulesError(client *s3.Client, s *suite, source string, rules ...types.ReplicationRule) error {
	_, err := client.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{
		Bucket:                   aws.String(source),
		ReplicationConfiguration: &types.ReplicationConfiguration{Role: aws.String(replicationRole(s)), Rules: rules},
	})
	return err
}

func getReplicationRules(t *testing.T, s *suite, source string) []types.ReplicationRule {
	t.Helper()
	out, err := s.client.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String(source)})
	if err != nil {
		t.Fatalf("GetBucketReplication: %v", err)
	}
	if out.ReplicationConfiguration == nil || len(out.ReplicationConfiguration.Rules) == 0 {
		t.Fatalf("configuration=%#v, want rules", out.ReplicationConfiguration)
	}
	return out.ReplicationConfiguration.Rules
}

func replicationConfiguration(s *suite, target, prefix string) *types.ReplicationConfiguration {
	rule := types.ReplicationRule{Status: types.ReplicationRuleStatusEnabled, Destination: &types.Destination{Bucket: aws.String("arn:aws:s3:::" + target)}}
	if prefix != "" {
		rule.Priority = aws.Int32(1)
		rule.Filter = &types.ReplicationRuleFilter{Prefix: aws.String(prefix)}
		rule.DeleteMarkerReplication = &types.DeleteMarkerReplication{Status: types.DeleteMarkerReplicationStatusDisabled}
	}
	return &types.ReplicationConfiguration{Role: aws.String(replicationRole(s)), Rules: []types.ReplicationRule{rule}}
}

func putReplication(t *testing.T, s *suite, source string, configuration *types.ReplicationConfiguration) {
	t.Helper()
	if _, err := putReplicationError(s, source, configuration); err != nil {
		t.Fatal(err)
	}
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
