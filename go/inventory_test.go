package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestInventory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷에 인벤토리를 설정하지 않은 상태에서 조회가 가능한지 확인
		{"test_list_bucket_inventory", testInventoryList},
		// 버킷에 인벤토리를 설정할 수 있는지 확인
		{"test_put_bucket_inventory", testInventoryPut},
		// 버킷에 인벤토리 설정이 되었는지 확인
		{"test_check_bucket_inventory", testInventoryCheck},
		// 버킷에 설정된 인벤토리를 조회할 수 있는지 확인
		{"test_get_bucket_inventory", testInventoryGet},
		// 버킷에 설정된 인벤토리를 삭제할 수 있는지 확인
		{"test_delete_bucket_inventory", testInventoryDelete},
		// 존재하지 않은 인벤토리를 가져오려고 할 경우 실패하는지 확인
		{"test_get_bucket_inventory_not_exist", testInventoryGetMissing},
		// 존재하지 않은 인벤토리를 삭제하려고 할 경우 실패하는지 확인
		{"test_delete_bucket_inventory_not_exist", testInventoryDeleteMissing},
		// 존재하지 않은 버킷에 인벤토리를 설정하려고 할 경우 실패하는지 확인
		{"test_put_bucket_inventory_not_exist", testInventoryPutMissingBucket},
		// 인벤토리 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
		{"test_put_bucket_inventory_id_not_exist", testInventoryMissingID},
		// 인벤토리 아이디가 중복되는 경우 덮어쓰기 되는지 확인
		{"test_put_bucket_inventory_id_duplicate", testInventoryDuplicateID},
		// 타깃 버킷이 존재하지 않을 경우 실패하는지 확인
		{"test_put_bucket_inventory_target_not_exist", testInventoryMissingTarget},
		// 지원하지 않는 파일 형식의 인벤토리를 설정하려고 할 경우 실패하는지 확인
		{"test_put_bucket_inventory_invalid_format", testInventoryInvalidFormat},
		// 올바르지 않은 주기의 인벤토리를 설정하려고 할 경우 실패하는지 확인
		{"test_put_bucket_inventory_invalid_frequency", testInventoryInvalidFrequency},
		// 대소문자를 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인
		{"test_put_bucket_inventory_invalid_case", testInventoryInvalidCase},
		// 접두어를 포함한 인벤토리 설정이 올바르게 적용되는지 확인
		{"test_put_bucket_inventory_prefix", testInventoryPrefix},
		// 옵션을 포함한 인벤토리 설정이 올바르게 적용되는지 확인
		{"test_put_bucket_inventory_optional", testInventoryOptional},
		// 올바르지 않은 옵션을 포함한 인벤토리를 설정하려고 할 경우 실패하는지 확인
		{"test_put_bucket_inventory_invalid_optional", testInventoryInvalidOptional},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
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

func testInventoryList(t *testing.T) {
	s := newSuite(t)
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(s.bucket(t))})
	if err != nil || len(out.InventoryConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

func testInventoryPut(t *testing.T) {
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	putInventory(t, s, source, "my-inventory-v2", standardInventory("my-inventory-v2", target))
}

func testInventoryCheck(t *testing.T) {
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	putInventory(t, s, source, "my-inventory", standardInventory("my-inventory", target))
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil || len(out.InventoryConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

func testInventoryGet(t *testing.T) {
	s := newSuite(t)
	source, target, id := s.bucket(t), s.bucket(t), "my-inventory"
	putInventory(t, s, source, id, inventoryConfiguration(id, target, "a/", nil, types.InventoryIncludedObjectVersionsCurrent, types.InventoryFrequencyDaily, types.InventoryFormatCsv))
	out, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)})
	if err != nil || out.InventoryConfiguration == nil || aws.ToString(out.InventoryConfiguration.Id) != id {
		t.Fatalf("configuration=%#v err=%v", out.InventoryConfiguration, err)
	}
}

func testInventoryDelete(t *testing.T) {
	s := newSuite(t)
	source, target, id := s.bucket(t), s.bucket(t), "my-inventory"
	putInventory(t, s, source, id, standardInventory(id, target))
	if _, err := s.client.DeleteBucketInventoryConfiguration(context.Background(), &s3.DeleteBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil || len(out.InventoryConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

func testInventoryGetMissing(t *testing.T) {
	s := newSuite(t)
	_, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(s.bucket(t)), Id: aws.String("my-inventory")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

func testInventoryDeleteMissing(t *testing.T) {
	s := newSuite(t)
	_, err := s.client.DeleteBucketInventoryConfiguration(context.Background(), &s3.DeleteBucketInventoryConfigurationInput{Bucket: aws.String(s.bucket(t)), Id: aws.String("my-inventory")})
	assertS3Error(t, err, 404, "NoSuchConfiguration")
}

func testInventoryPutMissingBucket(t *testing.T) {
	s := newSuite(t)
	target := s.bucket(t)
	bucket := "missing-" + uniqueBucketSuffix(t)
	_, err := putInventoryError(s, bucket, "my-inventory", standardInventory("my-inventory", target))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

func testInventoryMissingID(t *testing.T) {
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	_, err := putInventoryError(s, source, "", standardInventory("", target))
	assertS3Error(t, err, 400, "MalformedXML")
}

func testInventoryDuplicateID(t *testing.T) {
	s := newSuite(t)
	source, target, id := s.bucket(t), s.bucket(t), "my-inventory"
	configuration := standardInventory(id, target)
	putInventory(t, s, source, id, configuration)
	putInventory(t, s, source, id, configuration)
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil || len(out.InventoryConfigurationList) != 1 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

func testInventoryMissingTarget(t *testing.T) {
	s := newSuite(t)
	source := s.bucket(t)
	target := "missing-" + uniqueBucketSuffix(t)
	_, err := putInventoryError(s, source, "my-inventory", standardInventory("my-inventory", target))
	assertS3Error(t, err, 404, "NoSuchBucket")
}

func testInventoryInvalidFormat(t *testing.T) {
	testInvalidInventory(t, func(configuration *types.InventoryConfiguration) {
		configuration.Destination.S3BucketDestination.Format = types.InventoryFormat("JSON")
	})
}

func testInventoryInvalidFrequency(t *testing.T) {
	testInvalidInventory(t, func(configuration *types.InventoryConfiguration) {
		configuration.Schedule.Frequency = types.InventoryFrequency("Hourly")
	})
}

func testInventoryInvalidCase(t *testing.T) {
	testInvalidInventory(t, func(configuration *types.InventoryConfiguration) {
		configuration.IncludedObjectVersions = types.InventoryIncludedObjectVersions("CUrrENT")
	})
}

func testInventoryPrefix(t *testing.T) {
	s := newSuite(t)
	source, target, id := s.bucket(t), s.bucket(t), "my-inventory"
	putInventory(t, s, source, id, inventoryConfiguration(id, target, "a/", nil, types.InventoryIncludedObjectVersionsCurrent, types.InventoryFrequencyDaily, types.InventoryFormatCsv))
	out, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)})
	if err != nil || out.InventoryConfiguration == nil || aws.ToString(out.InventoryConfiguration.Destination.S3BucketDestination.Prefix) != "a/" {
		t.Fatalf("configuration=%#v err=%v", out.InventoryConfiguration, err)
	}
}

func testInventoryOptional(t *testing.T) {
	s := newSuite(t)
	source, target, id := s.bucket(t), s.bucket(t), "my-inventory"
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

func testInventoryInvalidOptional(t *testing.T) {
	testInvalidInventory(t, func(configuration *types.InventoryConfiguration) {
		configuration.OptionalFields = []types.InventoryOptionalField{"SIZE", "--"}
		configuration.Destination.S3BucketDestination.Prefix = aws.String("a/")
	})
}

func testInvalidInventory(t *testing.T, mutate func(*types.InventoryConfiguration)) {
	t.Helper()
	s := newSuite(t)
	source, target, id := s.bucket(t), s.bucket(t), "my-inventory"
	configuration := standardInventory(id, target)
	mutate(configuration)
	_, err := putInventoryError(s, source, id, configuration)
	assertS3Error(t, err, 400, "MalformedXML")
}

func putInventoryError(s *suite, bucket, id string, configuration *types.InventoryConfiguration) (*s3.PutBucketInventoryConfigurationOutput, error) {
	return s.client.PutBucketInventoryConfiguration(context.Background(), &s3.PutBucketInventoryConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id), InventoryConfiguration: configuration})
}
