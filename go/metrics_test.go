package s3tests

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 Metrics를 설정하지 않은 상태에서 조회가 가능한지 확인
func TestMetrics(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	out, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(s.bucket(t, 1))})
	if err != nil || len(out.MetricsConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(out.MetricsConfigurationList), err)
	}
}

// 버킷에 Metrics를 설정할 수 있는지 확인
func TestPutMetrics(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	putMetrics(t, s, s.bucket(t, 2), "metrics-id", metricsConfiguration("metrics-id", nil))
}

// 버킷에 Metrics 설정이 되었는지 확인
func TestCheckMetrics(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 3)
	putMetrics(t, s, bucket, "metrics-id", metricsConfiguration("metrics-id", nil))
	out, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.MetricsConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(out.MetricsConfigurationList), err)
	}
}

// 버킷에 설정된 Metrics를 조회할 수 있는지 확인
func TestGetMetrics(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, id := s.bucket(t, 4), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, nil))
	out := getMetrics(t, s, bucket, id)
	if aws.ToString(out.Id) != id {
		t.Fatalf("ID=%q", aws.ToString(out.Id))
	}
}

// 버킷에 설정된 Metrics를 삭제할 수 있는지 확인
func TestDeleteMetrics(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, id := s.bucket(t, 5), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, nil))
	if _, err := s.client.DeleteBucketMetricsConfiguration(context.Background(), &s3.DeleteBucketMetricsConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	listed, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil || len(listed.MetricsConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(listed.MetricsConfigurationList), err)
	}
}

// 존재하지 않은 Metrics를 가져오려고 할 경우 실패하는지 확인
func TestGetMetricsNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.GetBucketMetricsConfiguration(context.Background(), &s3.GetBucketMetricsConfigurationInput{Bucket: aws.String(s.bucket(t, 6)), Id: aws.String("metrics-id")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

// 존재하지 않은 Metrics를 삭제하려고 할 경우 실패하는지 확인
func TestDeleteMetricsNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.DeleteBucketMetricsConfiguration(context.Background(), &s3.DeleteBucketMetricsConfigurationInput{Bucket: aws.String(s.bucket(t, 7)), Id: aws.String("metrics-id")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

// 존재하지 않은 Metrics를 설정하려고 할 경우 실패하는지 확인
func TestPutMetricsNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := putMetricsError(s, "missing-"+uniqueBucketSuffix(t), "metrics-id", metricsConfiguration("metrics-id", nil))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// Metrics 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
func TestPutMetricsEmptyId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := putMetricsError(s, s.bucket(t, 9), "", metricsConfiguration("", nil))
	assertS3Error(t, err, 400, "InvalidConfigurationId")
}

// Metrics 아이디를 설정하지 않고 설정하려고 할 경우 실패하는지 확인
func TestPutMetricsNoId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.PutBucketMetricsConfiguration(context.Background(), &s3.PutBucketMetricsConfigurationInput{Bucket: aws.String(s.bucket(t, 10))})
	if err == nil {
		t.Fatal("PutBucketMetricsConfiguration without Id unexpectedly succeeded")
	}
}

// Metrics 아이디를 중복으로 설정하려고 할 경우 덮어쓰기 확인
func TestPutMetricsDuplicateId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, id := s.bucket(t, 11), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberPrefix{Value: "test1"}))
	assertMetricsPrefix(t, getMetrics(t, s, bucket, id), "test1")
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberPrefix{Value: "test2"}))
	assertMetricsPrefix(t, getMetrics(t, s, bucket, id), "test2")
	listed, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil || len(listed.MetricsConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(listed.MetricsConfigurationList), err)
	}
}

// 접두어를 포함한 Metrics 설정이 올바르게 적용되는지 확인
func TestMetricsPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, id := s.bucket(t, 12), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberPrefix{Value: "test"}))
	assertMetricsPrefix(t, getMetrics(t, s, bucket, id), "test")
}

// Metrics 설정에 태그를 적용할 수 있는지 확인
func TestMetricsTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, id := s.bucket(t, 13), "metrics-id"
	putMetrics(t, s, bucket, id, metricsConfiguration(id, &types.MetricsFilterMemberTag{Value: types.Tag{Key: aws.String("key"), Value: aws.String("value")}}))
	out := getMetrics(t, s, bucket, id)
	tag, ok := out.Filter.(*types.MetricsFilterMemberTag)
	if !ok || aws.ToString(tag.Value.Key) != "key" || aws.ToString(tag.Value.Value) != "value" {
		t.Fatalf("filter=%#v", out.Filter)
	}
}

// Metrics 설정에 필터를 적용할 수 있는지 확인
func TestMetricsFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, id := s.bucket(t, 14), "metrics-id"
	filter := &types.MetricsFilterMemberAnd{Value: types.MetricsAndOperator{Prefix: aws.String("test"), Tags: []types.Tag{{Key: aws.String("key"), Value: aws.String("value")}}}}
	putMetrics(t, s, bucket, id, metricsConfiguration(id, filter))
	out := getMetrics(t, s, bucket, id)
	and, ok := out.Filter.(*types.MetricsFilterMemberAnd)
	if !ok || aws.ToString(and.Value.Prefix) != "test" || len(and.Value.Tags) != 1 || aws.ToString(and.Value.Tags[0].Key) != "key" || aws.ToString(and.Value.Tags[0].Value) != "value" {
		t.Fatalf("filter=%#v", out.Filter)
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

func listMetricsCount(t *testing.T, s *suite, bucket string) int {
	t.Helper()
	out, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	return len(out.MetricsConfigurationList)
}

// ID 는 64자까지 허용한다
func TestPutMetricsIdLength(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, id := s.bucket(t, 15), strings.Repeat("a", 64)
	putMetrics(t, s, bucket, id, metricsConfiguration(id, nil))
	if got := aws.ToString(getMetrics(t, s, bucket, id).Id); got != id {
		t.Fatalf("ID=%q want=%q", got, id)
	}

	tooLong := strings.Repeat("a", 65)
	_, err := putMetricsError(s, bucket, tooLong, metricsConfiguration(tooLong, nil))
	assertS3Error(t, err, 400, "MalformedXML")
}

// ID 에는 영숫자와 - _ . 만 쓸 수 있다
func TestPutMetricsInvalidId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 16)
	_, err := putMetricsError(s, bucket, "a b/c", metricsConfiguration("a b/c", nil))
	assertS3Error(t, err, 400, "MalformedXML")
	if got := listMetricsCount(t, s, bucket); got != 0 {
		t.Fatalf("configurations=%d want=0", got)
	}
}

// 쿼리의 id 와 본문의 Id 가 다르면 거부된다
func TestPutMetricsIdMismatch(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 17)
	_, err := putMetricsError(s, bucket, "metrics-id", metricsConfiguration("other-id", nil))
	assertS3Error(t, err, 400, "MalformedXML")
	if got := listMetricsCount(t, s, bucket); got != 0 {
		t.Fatalf("configurations=%d want=0", got)
	}
}

// And 안에 조건이 하나뿐이면 거부된다
// (And 없이 Prefix 와 Tag 를 함께 주는 경우는 SDK 의 MetricsFilter 가 union 이라 만들 수 없어 확인하지 않는다)
func TestPutMetricsInvalidFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	filter := &types.MetricsFilterMemberAnd{Value: types.MetricsAndOperator{Prefix: aws.String("test")}}
	_, err := putMetricsError(s, s.bucket(t, 18), "metrics-id", metricsConfiguration("metrics-id", filter))
	assertS3Error(t, err, 400, "MalformedXML")
}

// 목록은 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
func TestListMetricsPagination(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 19)
	ids := make([]string, 101)
	for index := range ids {
		ids[index] = fmt.Sprintf("metrics-%03d", index)
		putMetrics(t, s, bucket, ids[index], metricsConfiguration(ids[index], nil))
	}

	out, err := s.client.ListBucketMetricsConfigurations(ctx, &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.MetricsConfigurationList) != 100 || !aws.ToBool(out.IsTruncated) {
		t.Fatalf("configurations=%d truncated=%v", len(out.MetricsConfigurationList), aws.ToBool(out.IsTruncated))
	}
	listed := make([]string, 0, len(ids))
	for _, configuration := range out.MetricsConfigurationList {
		listed = append(listed, aws.ToString(configuration.Id))
	}

	out, err = s.client.ListBucketMetricsConfigurations(ctx, &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket), ContinuationToken: out.NextContinuationToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.MetricsConfigurationList) != 1 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("configurations=%d truncated=%v", len(out.MetricsConfigurationList), aws.ToBool(out.IsTruncated))
	}
	for _, configuration := range out.MetricsConfigurationList {
		listed = append(listed, aws.ToString(configuration.Id))
	}
	slices.Sort(listed)
	if !slices.Equal(listed, ids) {
		t.Fatalf("listed=%v want=%v", listed, ids)
	}
}

// 올바르지 않은 토큰으로 목록을 요청하면 거부된다
func TestListMetricsInvalidToken(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListBucketMetricsConfigurations(context.Background(), &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(s.bucket(t, 20)), ContinuationToken: aws.String("invalid")})
	assertS3Error(t, err, 400, "MalformedContinuationToken")
}

// 없는 버킷에 대한 Metrics 요청은 실패한다
func TestMetricsBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucketNameOnly(21)
	_, err := s.client.ListBucketMetricsConfigurations(ctx, &s3.ListBucketMetricsConfigurationsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.GetBucketMetricsConfiguration(ctx, &s3.GetBucketMetricsConfigurationInput{Bucket: aws.String(bucket), Id: aws.String("metrics-id")})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketMetricsConfiguration(ctx, &s3.DeleteBucketMetricsConfigurationInput{Bucket: aws.String(bucket), Id: aws.String("metrics-id")})
	assertS3Error(t, err, 404, "NoSuchBucket")
}
