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

// 타깃 버킷이 존재하지 않아도 설정이 저장되는지 확인 (AWS 는 타깃 버킷 존재 여부를 확인하지 않음)
func TestPutBucketInventoryTargetNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, id := s.bucket(t, 11), "my-inventory"
	target := "missing-" + uniqueBucketSuffix(t)
	putInventory(t, s, source, id, standardInventory(id, target))
	out, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(source), Id: aws.String(id)})
	if err != nil || out.InventoryConfiguration == nil || aws.ToString(out.InventoryConfiguration.Id) != id {
		t.Fatalf("configuration=%#v err=%v", out.InventoryConfiguration, err)
	}
	if got := aws.ToString(out.InventoryConfiguration.Destination.S3BucketDestination.Bucket); got != "arn:aws:s3:::"+target {
		t.Fatalf("destination bucket = %q, want %q", got, "arn:aws:s3:::"+target)
	}
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

func getInventory(t *testing.T, s *suite, bucket, id string) *types.InventoryConfiguration {
	t.Helper()
	out, err := s.client.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id)})
	if err != nil {
		t.Fatal(err)
	}
	if out.InventoryConfiguration == nil || out.InventoryConfiguration.Destination == nil || out.InventoryConfiguration.Destination.S3BucketDestination == nil || out.InventoryConfiguration.Schedule == nil {
		t.Fatalf("configuration=%#v", out.InventoryConfiguration)
	}
	return out.InventoryConfiguration
}

// ID 는 64자까지 허용한다
func TestPutBucketInventoryIdLength(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 18), s.bucket(t, 18), strings.Repeat("a", 64)
	putInventory(t, s, source, id, standardInventory(id, target))
	if got := aws.ToString(getInventory(t, s, source, id).Id); got != id {
		t.Fatalf("ID=%q want=%q", got, id)
	}

	tooLong := strings.Repeat("a", 65)
	_, err := putInventoryError(s, source, tooLong, standardInventory(tooLong, target))
	assertS3Error(t, err, 400, "MalformedXML")
}

// ID 에는 영숫자와 - _ . 만 쓸 수 있다
func TestPutBucketInventoryInvalidId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 19), s.bucket(t, 19), "a b/c"
	_, err := putInventoryError(s, source, id, standardInventory(id, target))
	assertS3Error(t, err, 400, "MalformedXML")
}

// 쿼리의 id 와 본문의 Id 가 다르면 거부된다
func TestPutBucketInventoryIdMismatch(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 20), s.bucket(t, 20)
	_, err := putInventoryError(s, source, "my-inventory", standardInventory("other-inventory", target))
	assertS3Error(t, err, 400, "IdMismatch")
	out, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil || len(out.InventoryConfigurationList) != 0 {
		t.Fatalf("configurations=%d err=%v", len(out.InventoryConfigurationList), err)
	}
}

// 비활성(IsEnabled false) 인벤토리가 그대로 저장된다
func TestPutBucketInventoryDisabled(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 21), s.bucket(t, 21), "my-inventory"
	configuration := standardInventory(id, target)
	configuration.IsEnabled = aws.Bool(false)
	putInventory(t, s, source, id, configuration)
	if got := getInventory(t, s, source, id).IsEnabled; got == nil || *got {
		t.Fatalf("IsEnabled=%v want=false", got)
	}
}

// 올바르지 않은 대상 버전(IncludedObjectVersions)은 거부된다
func TestPutBucketInventoryInvalidVersions(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 22), s.bucket(t, 22), "my-inventory"
	configuration := standardInventory(id, target)
	configuration.IncludedObjectVersions = types.InventoryIncludedObjectVersions("Invalid")
	_, err := putInventoryError(s, source, id, configuration)
	assertS3Error(t, err, 400, "MalformedXML")
}

// 형식(CSV/ORC/Parquet), 주기(Daily/Weekly), 대상 버전(All/Current)이 그대로 저장된다
func TestPutBucketInventoryFormats(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target := s.bucket(t, 23), s.bucket(t, 23)
	cases := []struct {
		id        string
		format    types.InventoryFormat
		frequency types.InventoryFrequency
		versions  types.InventoryIncludedObjectVersions
	}{
		{"inventory-csv", types.InventoryFormatCsv, types.InventoryFrequencyDaily, types.InventoryIncludedObjectVersionsAll},
		{"inventory-orc", types.InventoryFormatOrc, types.InventoryFrequencyWeekly, types.InventoryIncludedObjectVersionsCurrent},
		{"inventory-parquet", types.InventoryFormatParquet, types.InventoryFrequencyWeekly, types.InventoryIncludedObjectVersionsAll},
	}
	for _, tc := range cases {
		putInventory(t, s, source, tc.id, inventoryConfiguration(tc.id, target, "", nil, tc.versions, tc.frequency, tc.format))
	}
	for _, tc := range cases {
		got := getInventory(t, s, source, tc.id)
		if got.Destination.S3BucketDestination.Format != tc.format || got.Schedule.Frequency != tc.frequency || got.IncludedObjectVersions != tc.versions {
			t.Fatalf("%s: format=%s frequency=%s versions=%s want=%s/%s/%s", tc.id, got.Destination.S3BucketDestination.Format, got.Schedule.Frequency, got.IncludedObjectVersions, tc.format, tc.frequency, tc.versions)
		}
	}
}

// 대상의 접두어와 SSE-S3 암호화 설정이 그대로 저장된다
func TestPutBucketInventoryDestination(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 24), s.bucket(t, 24), "my-inventory"
	configuration := inventoryConfiguration(id, target, "inventory/", nil, types.InventoryIncludedObjectVersionsCurrent, types.InventoryFrequencyDaily, types.InventoryFormatCsv)
	configuration.Destination.S3BucketDestination.Encryption = &types.InventoryEncryption{SSES3: &types.SSES3{}}
	putInventory(t, s, source, id, configuration)

	destination := getInventory(t, s, source, id).Destination.S3BucketDestination
	if got := aws.ToString(destination.Bucket); got != "arn:aws:s3:::"+target {
		t.Fatalf("destination bucket = %q, want %q", got, "arn:aws:s3:::"+target)
	}
	if got := aws.ToString(destination.Prefix); got != "inventory/" {
		t.Fatalf("destination prefix = %q, want %q", got, "inventory/")
	}
	if destination.Encryption == nil || destination.Encryption.SSES3 == nil {
		t.Fatalf("destination encryption = %#v, want SSES3", destination.Encryption)
	}
}

// 대상 버킷은 ARN 형식이어야 한다
func TestPutBucketInventoryInvalidDestinationArn(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	source, target, id := s.bucket(t, 25), s.bucket(t, 25), "my-inventory"
	configuration := standardInventory(id, target)
	configuration.Destination.S3BucketDestination.Bucket = aws.String(target)
	_, err := putInventoryError(s, source, id, configuration)
	assertS3Error(t, err, 400, "InvalidS3DestinationBucket")
}

// 목록은 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
func TestListBucketInventoryPagination(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	source, target := s.bucket(t, 26), s.bucket(t, 26)
	ids := make([]string, 101)
	for index := range ids {
		ids[index] = fmt.Sprintf("inventory-%03d", index)
		putInventory(t, s, source, ids[index], standardInventory(ids[index], target))
	}

	out, err := s.client.ListBucketInventoryConfigurations(ctx, &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.InventoryConfigurationList) != 100 || !aws.ToBool(out.IsTruncated) {
		t.Fatalf("configurations=%d truncated=%v", len(out.InventoryConfigurationList), aws.ToBool(out.IsTruncated))
	}
	listed := make([]string, 0, len(ids))
	for _, configuration := range out.InventoryConfigurationList {
		listed = append(listed, aws.ToString(configuration.Id))
	}

	out, err = s.client.ListBucketInventoryConfigurations(ctx, &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(source), ContinuationToken: out.NextContinuationToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.InventoryConfigurationList) != 1 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("configurations=%d truncated=%v", len(out.InventoryConfigurationList), aws.ToBool(out.IsTruncated))
	}
	for _, configuration := range out.InventoryConfigurationList {
		listed = append(listed, aws.ToString(configuration.Id))
	}
	slices.Sort(listed)
	if !slices.Equal(listed, ids) {
		t.Fatalf("listed=%v want=%v", listed, ids)
	}
}

// 올바르지 않은 토큰으로 목록을 요청하면 거부된다
func TestListBucketInventoryInvalidToken(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListBucketInventoryConfigurations(context.Background(), &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(s.bucket(t, 27)), ContinuationToken: aws.String("invalid")})
	assertS3Error(t, err, 400, "MalformedContinuationToken")
}

// 없는 버킷에 대한 인벤토리 요청은 실패한다
func TestBucketInventoryBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucketNameOnly(28)
	_, err := s.client.ListBucketInventoryConfigurations(ctx, &s3.ListBucketInventoryConfigurationsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.GetBucketInventoryConfiguration(ctx, &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String(bucket), Id: aws.String("my-inventory")})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketInventoryConfiguration(ctx, &s3.DeleteBucketInventoryConfigurationInput{Bucket: aws.String(bucket), Id: aws.String("my-inventory")})
	assertS3Error(t, err, 404, "NoSuchBucket")
}
