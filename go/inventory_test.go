package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 인벤토리를 설정하지 않은 상태에서 조회가 가능한지 확인
func TestListBucketInventory(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(s.bucket(t, 1))})
	if err != nil || len(out.InventoryConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

// 버킷에 인벤토리를 설정할 수 있는지 확인
func TestPutBucketInventory(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 2), s.bucket(t, 2)
	putInventory(t, s, source, "my-inventory-v2", standardInventory("my-inventory-v2", target))
}

// 버킷에 인벤토리 설정이 되었는지 확인
func TestCheckBucketInventory(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 3), s.bucket(t, 3)
	putInventory(t, s, source, "my-inventory", standardInventory("my-inventory", target))
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil || len(out.InventoryConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

// 버킷에 설정된 인벤토리를 조회할 수 있는지 확인
func TestGetBucketInventory(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 4), s.bucket(t, 4), "my-inventory"
	putInventory(t, s, source, id, inventoryConfiguration(id, target, "a/", nil, types.InventoryIncludedObjectVersionsCurrent, types.InventoryFrequencyDaily, types.InventoryFormatCsv))
	out, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)})
	if err != nil || out.InventoryConfiguration == nil || aws.ToString(out.InventoryConfiguration.Id) != id {
		t.Fatalf("configuration=%#v err=%v", out.InventoryConfiguration, err)
	}
}

// 버킷에 설정된 인벤토리를 삭제할 수 있는지 확인
func TestDeleteBucketInventory(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 5), s.bucket(t, 5), "my-inventory"
	putInventory(t, s, source, id, standardInventory(id, target))
	if _, err := s.client.DeleteBucketInventoryConfiguration(context.Background(), &s3.DeleteBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil || len(out.InventoryConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

// 존재하지 않은 인벤토리를 가져오려고 할 경우 실패하는지 확인
func TestGetBucketInventoryNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(s.bucket(t, 6)), Id: aws.String("my-inventory")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

// 존재하지 않은 인벤토리를 삭제하려고 할 경우 실패하는지 확인
func TestDeleteBucketInventoryNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.DeleteBucketInventoryConfiguration(context.Background(), &s3.DeleteBucketInventoryConfigurationInput{Bucket: aws.String(s.bucket(t, 7)), Id: aws.String("my-inventory")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

// 존재하지 않은 버킷에 인벤토리를 설정하려고 할 경우 실패하는지 확인
func TestPutBucketInventoryNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	target := s.bucket(t, 8)
	bucket := "missing-" + uniqueBucketSuffix(t)
	_, err := putInventoryError(s, bucket, "my-inventory", standardInventory("my-inventory", target))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 인벤토리 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
func TestPutBucketInventoryIdNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 9), s.bucket(t, 9)
	_, err := putInventoryError(s, source, "", standardInventory("", target))
	assertS3Error(t, err, 400, "MalformedXML")
}

// 인벤토리 아이디가 중복되는 경우 덮어쓰기 되는지 확인
func TestPutBucketInventoryIdDuplicate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 10), s.bucket(t, 10), "my-inventory"
	configuration := standardInventory(id, target)
	putInventory(t, s, source, id, configuration)
	putInventory(t, s, source, id, configuration)
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil || len(out.InventoryConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

// 타깃 버킷이 존재하지 않을 경우 실패하는지 확인
func TestPutBucketInventoryTargetNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)

	if s.cfg.Endpoint() == "" {
		t.Skip("AWS does not validate inventory destination bucket existence")
	}
	source := s.bucket(t, 11)
	target := "missing-" + uniqueBucketSuffix(t)
	_, err := putInventoryError(s, source, "my-inventory", standardInventory("my-inventory", target))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 지원하지 않는 파일 형식의 인벤토리를 설정하려고 할 경우 실패하는지 확인
func TestPutBucketInventoryInvalidFormat(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 12), s.bucket(t, 12), "my-inventory"
	configuration := standardInventory(id, target)
	configuration.Destination.S3BucketDestination.Format = types.InventoryFormat("JSON")
	_, err := putInventoryError(s, source, id, configuration)
	assertS3Error(t, err, 400, "MalformedXML")
}

// 올바르지 않은 주기의 인벤토리를 설정하려고 할 경우 실패하는지 확인
func TestPutBucketInventoryInvalidFrequency(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 13), s.bucket(t, 13), "my-inventory"
	configuration := standardInventory(id, target)
	configuration.Schedule.Frequency = types.InventoryFrequency("Hourly")
	_, err := putInventoryError(s, source, id, configuration)
	assertS3Error(t, err, 400, "MalformedXML")
}

// 대소문자를 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인
func TestPutBucketInventoryInvalidCase(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 14), s.bucket(t, 14), "my-inventory"
	configuration := standardInventory(id, target)
	configuration.IncludedObjectVersions = types.InventoryIncludedObjectVersions("CUrrENT")
	_, err := putInventoryError(s, source, id, configuration)
	assertS3Error(t, err, 400, "MalformedXML")
}

// 접두어를 포함한 인벤토리 설정이 올바르게 적용되는지 확인
func TestPutBucketInventoryPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 15), s.bucket(t, 15), "my-inventory"
	putInventory(t, s, source, id, inventoryConfiguration(id, target, "a/", nil, types.InventoryIncludedObjectVersionsCurrent, types.InventoryFrequencyDaily, types.InventoryFormatCsv))
	out, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)})
	if err != nil || out.InventoryConfiguration == nil || aws.ToString(out.InventoryConfiguration.Destination.S3BucketDestination.Prefix) != "a/" {
		t.Fatalf("configuration=%#v err=%v", out.InventoryConfiguration, err)
	}
}

// 옵션을 포함한 인벤토리 설정이 올바르게 적용되는지 확인
func TestPutBucketInventoryOptional(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 16), s.bucket(t, 16), "my-inventory"
	want := []types.InventoryOptionalField{types.InventoryOptionalFieldSize, types.InventoryOptionalFieldLastModifiedDate}
	putInventory(t, s, source, id, inventoryConfiguration(id, target, "a/", want, types.InventoryIncludedObjectVersionsCurrent, types.InventoryFrequencyDaily, types.InventoryFormatCsv))
	out, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)})
	if err != nil || out.InventoryConfiguration == nil || aws.ToString(out.InventoryConfiguration.Destination.S3BucketDestination.Prefix) != "a/" || len(out.InventoryConfiguration.OptionalFields) != len(want) {
		t.Fatalf("configuration=%#v err=%v", out.InventoryConfiguration, err)
	}
	for index := range want {
		if out.InventoryConfiguration.OptionalFields[index] != want[index] {
			t.Fatalf("optional fields=%v want=%v", out.InventoryConfiguration.OptionalFields, want)
		}
	}
}

// 올바르지 않은 옵션을 포함한 인벤토리를 설정하려고 할 경우 실패하는지 확인
func TestPutBucketInventoryInvalidOptional(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 17), s.bucket(t, 17), "my-inventory"
	configuration := standardInventory(id, target)
	configuration.OptionalFields = []types.InventoryOptionalField{"SIZE", "--"}
	configuration.Destination.S3BucketDestination.Prefix = aws.String("a/")
	_, err := putInventoryError(s, source, id, configuration)
	assertS3Error(t, err, 400, "MalformedXML")
}

func inventoryConfiguration(id, target, prefix string, optional []types.InventoryOptionalField, versions types.InventoryIncludedObjectVersions, frequency types.InventoryFrequency, format types.InventoryFormat) *types.InventoryConfiguration {
	destination := &types.InventoryS3BucketDestination{Bucket: aws.String("arn:aws:s3:::" + target), Format: format}
	if prefix != "" {
		destination.Prefix = aws.String(prefix)
	}
	return &types.InventoryConfiguration{Id: aws.String(id), Destination: &types.InventoryDestination{S3BucketDestination: destination}, IsEnabled: aws.Bool(true), IncludedObjectVersions: versions, Schedule: &types.InventorySchedule{Frequency: frequency}, OptionalFields: optional}
}

func standardInventory(id, target string) *types.InventoryConfiguration {
	return inventoryConfiguration(id, target, "", nil, types.InventoryIncludedObjectVersionsCurrent, types.InventoryFrequencyDaily, types.InventoryFormatCsv)
}

func putInventory(t *testing.T, s *suite, bucket, id string, configuration *types.InventoryConfiguration) {
	t.Helper()
	_, err := s.client.PutBucketInventoryConfiguration(context.Background(), &s3.PutBucketInventoryConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id), InventoryConfiguration: configuration})
	if err != nil {
		t.Fatal(err)
	}
}

func putInventoryError(s *suite, bucket, id string, configuration *types.InventoryConfiguration) (*s3.PutBucketInventoryConfigurationOutput, error) {
	return s.client.PutBucketInventoryConfiguration(context.Background(), &s3.PutBucketInventoryConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id), InventoryConfiguration: configuration})
}
