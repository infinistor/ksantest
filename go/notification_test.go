package s3tests

import (
	"context"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 알람 설정이 없는지 확인
func TestNotificationGetEmpty(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 1)
	out, err := s.client.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration: %v", err)
	}
	if len(out.LambdaFunctionConfigurations) != 0 || len(out.QueueConfigurations) != 0 || len(out.TopicConfigurations) != 0 {
		t.Fatalf("empty notification configuration = %#v", out)
	}
}

// 버킷에 알람 설정이 가능한지 확인
func TestNotificationPut(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipNotificationOnAWS(t, s)
	bucket := s.bucket(t, 2)
	putNotification(t, s, bucket, notificationConfiguration(s))
}

// 버킷에 알람 설정이 되어있는지 확인
func TestNotificationGet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipNotificationOnAWS(t, s)
	bucket := s.bucket(t, 3)
	want := notificationConfiguration(s)
	putNotification(t, s, bucket, want)
	out := getNotification(t, s, bucket)
	assertNotification(t, out, want.LambdaFunctionConfigurations[0])
}

// 버킷에 알람 설정이 삭제되는지 확인
func TestNotificationDelete(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipNotificationOnAWS(t, s)
	bucket := s.bucket(t, 4)
	want := notificationConfiguration(s)
	putNotification(t, s, bucket, want)
	assertNotification(t, getNotification(t, s, bucket), want.LambdaFunctionConfigurations[0])
	putNotification(t, s, bucket, &types.NotificationConfiguration{})
	if got := getNotification(t, s, bucket); len(got.LambdaFunctionConfigurations) != 0 {
		t.Fatalf("Lambda configurations after delete = %#v", got.LambdaFunctionConfigurations)
	}
}

// 형식이 잘못된 Lambda ARN으로 알람 설정 시 실패하는지 확인
func TestNotificationPutInvalidArn(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 5)
	// arn: 접두어와 리전이 없는 잘못된 형식
	configuration := &types.NotificationConfiguration{LambdaFunctionConfigurations: []types.LambdaFunctionConfiguration{{
		Id: aws.String("my-lambda"), LambdaFunctionArn: aws.String("aws:lambda::" + s.cfg.Main.ID + ":function:my-function"),
		Events: []types.Event{types.Event("s3:ObjectCreated:*")},
	}}}
	_, err := s.client.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String(bucket), NotificationConfiguration: configuration})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷에 EventBridge 알람 설정, 조회, 삭제가 가능한지 확인
func TestNotificationEventBridge(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 6)
	// EventBridge 는 대상 리소스가 없어 AWS 에서도 그대로 설정된다
	putNotification(t, s, bucket, &types.NotificationConfiguration{EventBridgeConfiguration: &types.EventBridgeConfiguration{}})
	out := getNotification(t, s, bucket)
	if out.EventBridgeConfiguration == nil || len(out.LambdaFunctionConfigurations) != 0 {
		t.Fatalf("notification configuration = %#v, want EventBridge only", out)
	}
	putNotification(t, s, bucket, &types.NotificationConfiguration{})
	if got := getNotification(t, s, bucket); got.EventBridgeConfiguration != nil {
		t.Fatalf("EventBridge configuration after delete = %#v", got.EventBridgeConfiguration)
	}
}

// Lambda 함수 ARN을 정식 형식(arn:aws:lambda:리전:계정:function:이름)으로 만든다.
// AWS는 형식이 틀린 ARN을 400 InvalidArgument 로 거부한다.
func lambdaFunctionArn(s *suite) string {
	region := s.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	return "arn:aws:lambda:" + region + ":" + s.cfg.Main.ExpectedOwnerID() + ":function:my-function"
}

func notificationConfiguration(s *suite) *types.NotificationConfiguration {
	events := []types.Event{types.Event("s3:ObjectCreated:*"), types.Event("s3:ObjectRemoved:*")}
	return &types.NotificationConfiguration{LambdaFunctionConfigurations: []types.LambdaFunctionConfiguration{{
		Id: aws.String("my-lambda"), LambdaFunctionArn: aws.String(lambdaFunctionArn(s)), Events: events,
	}}}
}

func putNotification(t *testing.T, s *suite, bucket string, configuration *types.NotificationConfiguration) {
	t.Helper()
	_, err := s.client.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String(bucket), NotificationConfiguration: configuration})
	if err != nil {
		t.Fatalf("PutBucketNotificationConfiguration: %v", err)
	}
}

func getNotification(t *testing.T, s *suite, bucket string) *s3.GetBucketNotificationConfigurationOutput {
	t.Helper()
	out, err := s.client.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration: %v", err)
	}
	return out
}

func assertNotification(t *testing.T, out *s3.GetBucketNotificationConfigurationOutput, want types.LambdaFunctionConfiguration) {
	t.Helper()
	if len(out.LambdaFunctionConfigurations) != 1 {
		t.Fatalf("Lambda configuration count = %d, want 1", len(out.LambdaFunctionConfigurations))
	}
	got := out.LambdaFunctionConfigurations[0]
	if aws.ToString(got.Id) != aws.ToString(want.Id) || aws.ToString(got.LambdaFunctionArn) != aws.ToString(want.LambdaFunctionArn) || !slices.Equal(got.Events, want.Events) {
		t.Fatalf("Lambda configuration = %#v, want %#v", got, want)
	}
}

func skipNotificationOnAWS(t *testing.T, s *suite) {
	t.Helper()
	if s.cfg.Endpoint() == "" {
		t.Skip("AWS validates that the configured Lambda ARN exists")
	}
}
