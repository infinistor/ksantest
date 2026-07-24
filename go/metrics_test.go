package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestMetrics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷에 Metrics를 설정하지 않은 상태에서 조회가 가능한지 확인
		{"test_metrics", testMetricsList},
		// 버킷에 Metrics를 설정할 수 있는지 확인
		{"test_put_metrics", testMetricsPut},
		// 버킷에 Metrics 설정이 되었는지 확인
		{"test_check_metrics", testMetricsCheck},
		// 버킷에 설정된 Metrics를 조회할 수 있는지 확인
		{"test_get_metrics", testMetricsGet},
		// 버킷에 설정된 Metrics를 삭제할 수 있는지 확인
		{"test_delete_metrics", testMetricsDelete},
		// 존재하지 않은 Metrics를 가져오려고 할 경우 실패하는지 확인
		{"test_get_metrics_not_exist", testMetricsGetMissing},
		// 존재하지 않은 Metrics를 삭제하려고 할 경우 실패하는지 확인
		{"test_delete_metrics_not_exist", testMetricsDeleteMissing},
		// 존재하지 않은 Metrics를 설정하려고 할 경우 실패하는지 확인
		{"test_put_metrics_not_exist", testMetricsPutMissingBucket},
		// Metrics 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
		{"test_put_metrics_empty_id", testMetricsEmptyID},
		// Metrics 아이디를 설정하지 않고 설정하려고 할 경우 실패하는지 확인
		{"test_put_metrics_no_id", testMetricsNoID},
		// Metrics 아이디를 중복으로 설정하려고 할 경우 덮어쓰기 확인
		{"test_put_metrics_duplicate_id", testMetricsDuplicateID},
		// 접두어를 포함한 Metrics 설정이 올바르게 적용되는지 확인
		{"test_metrics_prefix", testMetricsPrefix},
		// Metrics 설정에 태그를 적용할 수 있는지 확인
		{"test_metrics_tag", testMetricsTag},
		// Metrics 설정에 필터를 적용할 수 있는지 확인
		{"test_metrics_filter", testMetricsAndFilter},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func metricsConfiguration(id string, filter types.MetricsFilter) *types.MetricsConfiguration {
	return &types.MetricsConfiguration{Id: aws.String(id), Filter: filter}
}

func putMetrics(t *testing.T, s *suite, bucket, id string, configuration *types.MetricsConfiguration) {
	t.Helper()
	if _, err := putMetricsError(s, bucket, id, configuration); err != nil {
		t.Fatal(err)
	}
}

func testMetricsList(t *testing.T) {
	s := newSuite(t)
	out, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(s.bucket(t))})
	if err != nil || len(out.MetricsConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(out.MetricsConfigurationList), err)
	}
}

func testMetricsPut(t *testing.T) {
	s := newSuite(t)
	putMetrics(t, s, s.bucket(t), "metrics-id", metricsConfiguration("metrics-id", nil))
}

func testMetricsCheck(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putMetrics(t, s, bucket, "metrics-id", metricsConfiguration("metrics-id", nil))
	out, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.MetricsConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(out.MetricsConfigurationList), err)
	}
}

func testMetricsGet(t *testing.T) {
	s := newSuite(t)
	bucket, id := s.bucket(t), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, nil))
	out := getMetrics(t, s, bucket, id)
	if aws.ToString(out.Id) != id {
		t.Fatalf("ID=%q", aws.ToString(out.Id))
	}
}

func testMetricsDelete(t *testing.T) {
	s := newSuite(t)
	bucket, id := s.bucket(t), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, nil))
	if _, err := s.client.DeleteBucketMetricsConfiguration(context.Background(), &s3.DeleteBucketMetricsConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	listed, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil || len(listed.MetricsConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(listed.MetricsConfigurationList), err)
	}
}

func testMetricsGetMissing(t *testing.T) {
	s := newSuite(t)
	_, err := s.client.GetBucketMetricsConfiguration(context.Background(), &s3.GetBucketMetricsConfigurationInput{Bucket: aws.String(s.bucket(t)), Id: aws.String("metrics-id")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

func testMetricsDeleteMissing(t *testing.T) {
	s := newSuite(t)
	_, err := s.client.DeleteBucketMetricsConfiguration(context.Background(), &s3.DeleteBucketMetricsConfigurationInput{Bucket: aws.String(s.bucket(t)), Id: aws.String("metrics-id")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

func testMetricsPutMissingBucket(t *testing.T) {
	s := newSuite(t)
	_, err := putMetricsError(s, "missing-"+uniqueBucketSuffix(t), "metrics-id", metricsConfiguration("metrics-id", nil))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

func testMetricsEmptyID(t *testing.T) {
	s := newSuite(t)
	_, err := putMetricsError(s, s.bucket(t), "", metricsConfiguration("", nil))
	assertS3Error(t, err, 400, "InvalidConfigurationId")
}

func testMetricsNoID(t *testing.T) {
	s := newSuite(t)
	_, err := s.client.PutBucketMetricsConfiguration(context.Background(), &s3.PutBucketMetricsConfigurationInput{Bucket: aws.String(s.bucket(t))})
	if err == nil {
		t.Fatal("PutBucketMetricsConfiguration without Id unexpectedly succeeded")
	}
}

func testMetricsDuplicateID(t *testing.T) {
	s := newSuite(t)
	bucket, id := s.bucket(t), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberPrefix{Value: "test1"}))
	assertMetricsPrefix(t, getMetrics(t, s, bucket, id), "test1")
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberPrefix{Value: "test2"}))
	assertMetricsPrefix(t, getMetrics(t, s, bucket, id), "test2")
	listed, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil || len(listed.MetricsConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(listed.MetricsConfigurationList), err)
	}
}

func testMetricsPrefix(t *testing.T) {
	s := newSuite(t)
	bucket, id := s.bucket(t), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberPrefix{Value: "test"}))
	assertMetricsPrefix(t, getMetrics(t, s, bucket, id), "test")
}

func testMetricsTag(t *testing.T) {
	s := newSuite(t)
	bucket, id := s.bucket(t), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberTag{Value: types.Tag{Key: aws.String("key"), Value: aws.String("value")}}))
	out := getMetrics(t, s, bucket, id)
	tag, ok := out.Filter.(*types.MetricsFilterMemberTag)
	if !ok || aws.ToString(tag.Value.Key) != "key" || aws.ToString(tag.Value.Value) != "value" {
		t.Fatalf("filter=%#v", out.Filter)
	}
}

func testMetricsAndFilter(t *testing.T) {
	s := newSuite(t)
	bucket, id := s.bucket(t), "metrics-id"
	filter := &types.MetricsFilterMemberAnd{Value: types.MetricsAndOperator{Prefix: aws.String("test"), Tags: []types.Tag{{Key: aws.String("key"), Value: aws.String("value")}}}}
	putMetrics(t, s, bucket, id, metricsConfiguration(id, filter))
	out := getMetrics(t, s, bucket, id)
	and, ok := out.Filter.(*types.MetricsFilterMemberAnd)
	if !ok || aws.ToString(and.Value.Prefix) != "test" || len(and.Value.Tags) != 1 || aws.ToString(and.Value.Tags[0].Key) != "key" || aws.ToString(and.Value.Tags[0].Value) != "value" {
		t.Fatalf("filter=%#v", out.Filter)
	}
}

func getMetrics(t *testing.T, s *suite, bucket, id string) *types.MetricsConfiguration {
	t.Helper()
	out, err := s.client.GetBucketMetricsConfiguration(context.Background(), &s3.GetBucketMetricsConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id)})
	if err != nil || out.MetricsConfiguration == nil {
		t.Fatalf("configuration=%#v err=%v", out.MetricsConfiguration, err)
	}
	return out.MetricsConfiguration
}

func assertMetricsPrefix(t *testing.T, configuration *types.MetricsConfiguration, want string) {
	t.Helper()
	prefix, ok := configuration.Filter.(*types.MetricsFilterMemberPrefix)
	if !ok || prefix.Value != want {
		t.Fatalf("filter=%#v want prefix=%q", configuration.Filter, want)
	}
}

func putMetricsError(s *suite, bucket, id string, configuration *types.MetricsConfiguration) (*s3.PutBucketMetricsConfigurationOutput, error) {
	return s.client.PutBucketMetricsConfiguration(context.Background(), &s3.PutBucketMetricsConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id), MetricsConfiguration: configuration})
}
