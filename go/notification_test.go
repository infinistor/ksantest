package s3tests

import (
	"context"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestNotification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷에 알람 설정이 없는지 확인
		{"test_notification_get_empty", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			out, err := s.client.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("GetBucketNotificationConfiguration: %v", err)
			}
			if len(out.LambdaFunctionConfigurations) != 0 || len(out.QueueConfigurations) != 0 || len(out.TopicConfigurations) != 0 {
				t.Fatalf("empty notification configuration = %#v", out)
			}
		}},
		// 버킷에 알람 설정이 가능한지 확인
		{"test_notification_put", func(t *testing.T) {
			s := newSuite(t)
			skipNotificationOnAWS(t, s)
			bucket := s.bucket(t)
			putNotification(t, s, bucket, notificationConfiguration(s))
		}},
		// 버킷에 알람 설정이 되어있는지 확인
		{"test_notification_get", func(t *testing.T) {
			s := newSuite(t)
			skipNotificationOnAWS(t, s)
			bucket := s.bucket(t)
			want := notificationConfiguration(s)
			putNotification(t, s, bucket, want)
			out := getNotification(t, s, bucket)
			assertNotification(t, out, want.LambdaFunctionConfigurations[0])
		}},
		// 버킷에 알람 설정이 삭제되는지 확인
		{"test_notification_delete", func(t *testing.T) {
			s := newSuite(t)
			skipNotificationOnAWS(t, s)
			bucket := s.bucket(t)
			want := notificationConfiguration(s)
			putNotification(t, s, bucket, want)
			assertNotification(t, getNotification(t, s, bucket), want.LambdaFunctionConfigurations[0])
			putNotification(t, s, bucket, &types.NotificationConfiguration{})
			if got := getNotification(t, s, bucket); len(got.LambdaFunctionConfigurations) != 0 {
				t.Fatalf("Lambda configurations after delete = %#v", got.LambdaFunctionConfigurations)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func notificationConfiguration(s *suite) *types.NotificationConfiguration {
	events := []types.Event{types.Event("s3:ObjectCreated:*"), types.Event("s3:ObjectRemoved:*")}
	return &types.NotificationConfiguration{LambdaFunctionConfigurations: []types.LambdaFunctionConfiguration{{
		Id: aws.String("my-lambda"), LambdaFunctionArn: aws.String("aws:lambda::" + s.cfg.Main.ID + ":function:my-function"), Events: events,
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
