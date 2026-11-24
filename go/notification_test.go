package s3tests

import (
	"context"
	"slices"
	"strings"
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
	bucket := s.bucket(t, 2)
	putNotification(t, s, bucket, notificationConfiguration(s))
}

// 버킷에 알람 설정이 되어있는지 확인
func TestNotificationGet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
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
	err := putNotificationError(s.client, bucket, configuration, true)
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

// 버킷에 SQS 큐 알람 설정, 조회, 삭제가 가능한지 확인
func TestNotificationQueue(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 7)
	queueArn := notificationArn(s, "sqs", "my-queue")
	events := []types.Event{types.Event("s3:ObjectCreated:*"), types.Event("s3:ObjectRemoved:*")}
	putNotification(t, s, bucket, &types.NotificationConfiguration{QueueConfigurations: []types.QueueConfiguration{{
		Id: aws.String("my-queue"), QueueArn: aws.String(queueArn), Events: events,
	}}})
	out := getNotification(t, s, bucket)
	if len(out.QueueConfigurations) != 1 {
		t.Fatalf("Queue configuration count = %d, want 1", len(out.QueueConfigurations))
	}
	got := out.QueueConfigurations[0]
	if aws.ToString(got.Id) != "my-queue" || aws.ToString(got.QueueArn) != queueArn || !slices.Equal(got.Events, events) {
		t.Fatalf("Queue configuration = %#v", got)
	}

	putNotification(t, s, bucket, &types.NotificationConfiguration{})
	if got := getNotification(t, s, bucket); len(got.QueueConfigurations) != 0 {
		t.Fatalf("Queue configurations after delete = %#v", got.QueueConfigurations)
	}
}

// 버킷에 SNS 토픽 알람 설정, 조회, 삭제가 가능한지 확인
func TestNotificationTopic(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 8)
	topicArn := notificationArn(s, "sns", "my-topic")
	events := []types.Event{types.Event("s3:ObjectCreated:*"), types.Event("s3:ObjectRemoved:*")}
	putNotification(t, s, bucket, &types.NotificationConfiguration{TopicConfigurations: []types.TopicConfiguration{{
		Id: aws.String("my-topic"), TopicArn: aws.String(topicArn), Events: events,
	}}})
	out := getNotification(t, s, bucket)
	if len(out.TopicConfigurations) != 1 {
		t.Fatalf("Topic configuration count = %d, want 1", len(out.TopicConfigurations))
	}
	got := out.TopicConfigurations[0]
	if aws.ToString(got.Id) != "my-topic" || aws.ToString(got.TopicArn) != topicArn || !slices.Equal(got.Events, events) {
		t.Fatalf("Topic configuration = %#v", got)
	}

	putNotification(t, s, bucket, &types.NotificationConfiguration{})
	if got := getNotification(t, s, bucket); len(got.TopicConfigurations) != 0 {
		t.Fatalf("Topic configurations after delete = %#v", got.TopicConfigurations)
	}
}

// 알람 설정에 접두어/접미어 필터를 설정하고 조회할 수 있는지 확인
func TestNotificationFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 9)
	rules := []types.FilterRule{
		{Name: types.FilterRuleNamePrefix, Value: aws.String("logs/")},
		{Name: types.FilterRuleNameSuffix, Value: aws.String(".txt")},
	}
	putNotification(t, s, bucket, lambdaNotification(lambdaConfig(s, "my-lambda", nil, rules)))
	out := getNotification(t, s, bucket)
	if len(out.LambdaFunctionConfigurations) != 1 || out.LambdaFunctionConfigurations[0].Filter == nil || out.LambdaFunctionConfigurations[0].Filter.Key == nil {
		t.Fatalf("Lambda configurations = %#v", out.LambdaFunctionConfigurations)
	}
	// AWS 는 필터 이름을 Prefix/Suffix 로 정규화해 돌려주므로 대소문자를 무시하고 비교한다
	var got []string
	for _, rule := range out.LambdaFunctionConfigurations[0].Filter.Key.FilterRules {
		got = append(got, strings.ToLower(string(rule.Name))+"="+aws.ToString(rule.Value))
	}
	slices.Sort(got)
	if want := []string{"prefix=logs/", "suffix=.txt"}; !slices.Equal(got, want) {
		t.Fatalf("filter rules = %v, want %v", got, want)
	}
}

// 지원하지 않는 이벤트로 알람 설정 시 실패하는지 확인
func TestNotificationInvalidEvent(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 11)
	configuration := lambdaNotification(lambdaConfig(s, "my-lambda", []types.Event{types.Event("s3:Foo:*")}, nil))
	err := putNotificationError(s.client, bucket, configuration, true)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 잘못된 필터 이름으로 알람 설정 시 실패하는지 확인
func TestNotificationInvalidFilterName(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 12)
	rules := []types.FilterRule{{Name: types.FilterRuleName("foo"), Value: aws.String("a")}}
	err := putNotificationError(s.client, bucket, lambdaNotification(lambdaConfig(s, "my-lambda", nil, rules)), true)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 같은 이벤트를 공유하는 알람 설정이 겹치면 실패하고, 접두어가 겹치지 않으면 성공하는지 확인
func TestNotificationOverlappingEvent(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 13)
	// 필터 없이 같은 이벤트를 두 설정이 공유하면 거부된다
	err := putNotificationError(s.client, bucket, lambdaNotification(lambdaConfig(s, "c1", nil, nil), lambdaConfig(s, "c2", nil, nil)), true)
	assertS3Error(t, err, 400, "InvalidArgument")
	// 접두어가 겹치지 않으면 같은 이벤트라도 허용된다
	putNotification(t, s, bucket, lambdaNotification(
		lambdaConfig(s, "c1", nil, prefixFilterRule("a/")),
		lambdaConfig(s, "c2", nil, prefixFilterRule("b/")),
	))
	var ids []string
	for _, configuration := range getNotification(t, s, bucket).LambdaFunctionConfigurations {
		ids = append(ids, aws.ToString(configuration.Id))
	}
	slices.Sort(ids)
	if want := []string{"c1", "c2"}; !slices.Equal(ids, want) {
		t.Fatalf("Lambda configuration ids = %v, want %v", ids, want)
	}
}

// 알람 설정의 아이디가 중복되면 실패하는지 확인
func TestNotificationDuplicateId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 14)
	err := putNotificationError(s.client, bucket, lambdaNotification(
		lambdaConfig(s, "c1", nil, prefixFilterRule("a/")),
		lambdaConfig(s, "c1", nil, prefixFilterRule("b/")),
	), true)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 존재하지 않는 버킷에 알람 설정/조회 시 NoSuchBucket 에러가 발생하는지 확인
func TestNotificationBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 15)
	_, err := s.client.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	err = putNotificationError(s.client, bucket, &types.NotificationConfiguration{}, true)
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버킷 소유자가 아닌 사용자가 알람 설정/조회 시 실패하는지 확인
func TestNotificationAltUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	altClient := s3Client(s.cfg, s.cfg.Alt)
	bucket := s.bucket(t, 16)
	_, err := altClient.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	err = putNotificationError(altClient, bucket, &types.NotificationConfiguration{}, true)
	assertS3Error(t, err, 403, "AccessDenied")
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

// 서비스(sqs, sns 등)와 리소스 이름으로 ARN을 만든다.
func notificationArn(s *suite, service, resource string) string {
	region := s.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	return "arn:aws:" + service + ":" + region + ":" + s.cfg.Main.ExpectedOwnerID() + ":" + resource
}

// Lambda 알람 설정을 만든다. events 가 nil 이면 s3:ObjectCreated:* 를 쓰고, rules 가 nil 이면 필터를 넣지 않는다.
func lambdaConfig(s *suite, id string, events []types.Event, rules []types.FilterRule) types.LambdaFunctionConfiguration {
	if events == nil {
		events = []types.Event{types.Event("s3:ObjectCreated:*")}
	}
	configuration := types.LambdaFunctionConfiguration{Id: aws.String(id), LambdaFunctionArn: aws.String(lambdaFunctionArn(s)), Events: events}
	if rules != nil {
		configuration.Filter = &types.NotificationConfigurationFilter{Key: &types.S3KeyFilter{FilterRules: rules}}
	}
	return configuration
}

func lambdaNotification(configurations ...types.LambdaFunctionConfiguration) *types.NotificationConfiguration {
	return &types.NotificationConfiguration{LambdaFunctionConfigurations: configurations}
}

func prefixFilterRule(prefix string) []types.FilterRule {
	return []types.FilterRule{{Name: types.FilterRuleNamePrefix, Value: aws.String(prefix)}}
}

// 대상 리소스 검증을 건너뛰고 알림 설정을 저장한다 (x-amz-skip-destination-validation).
func putNotification(t *testing.T, s *suite, bucket string, configuration *types.NotificationConfiguration) {
	t.Helper()
	if err := putNotificationError(s.client, bucket, configuration, true); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration: %v", err)
	}
}

// skipValidation 이 false 면 x-amz-skip-destination-validation 헤더를 보내지 않는다.
func putNotificationError(client *s3.Client, bucket string, configuration *types.NotificationConfiguration, skipValidation bool) error {
	input := &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String(bucket), NotificationConfiguration: configuration}
	if skipValidation {
		input.SkipDestinationValidation = aws.Bool(true)
	}
	_, err := client.PutBucketNotificationConfiguration(context.Background(), input)
	return err
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
