package s3tests

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"ksantest/go-s3tests/internal/testconfig"
)

func TestPutBucket(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 생성한 버킷이 비어있는지 확인
		{"test_bucket_list_empty", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("ListObjectsV2: %v", err)
			}
			if got := len(out.Contents); got != 0 {
				t.Fatalf("contents length = %d, want 0", got)
			}
		}},
		// 생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_bad_starts_non_alpha", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "_go-bucket"))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_bad_short_one", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "a"))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_bad_short_two", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "aa"))
			assertAPIError(t, err)
		}},
		// [생성규칙] bucket create naming good long 64 확인
		{"test_bucket_create_naming_good_long_64", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, strings.Repeat("a", 64)))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_bad_ip", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "192.168.11.123"))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_dns_underscore", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo_bar"))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_dns_dash_at_end", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo-"))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_dns_dot_dot", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo..bar"))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_dns_dot_dash", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo.-bar"))
			assertAPIError(t, err)
		}},
		// 생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인
		{"test_bucket_create_naming_dns_dash_dot", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo-.bar"))
			assertAPIError(t, err)
		}},
		// [생성규칙] bucket create naming good long 60 확인
		{"test_bucket_create_naming_good_long_60", func(t *testing.T) {
			s := newSuite(t)
			bucket := bucketNameOfLength(uniqueBucketSuffix(t), 60)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// [생성규칙] bucket create naming good long 61 확인
		{"test_bucket_create_naming_good_long_61", func(t *testing.T) {
			s := newSuite(t)
			bucket := bucketNameOfLength(uniqueBucketSuffix(t), 61)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// [생성규칙] bucket create naming good long 62 확인
		{"test_bucket_create_naming_good_long_62", func(t *testing.T) {
			s := newSuite(t)
			bucket := bucketNameOfLength(uniqueBucketSuffix(t), 62)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// [생성규칙] bucket create naming good long 63 확인
		{"test_bucket_create_naming_good_long_63", func(t *testing.T) {
			s := newSuite(t)
			bucket := bucketNameOfLength(uniqueBucketSuffix(t), 63)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// 생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인
		{"test_bucket_create_naming_dns_long", func(t *testing.T) {
			s := newSuite(t)
			bucket := bucketNameOfLength(uniqueBucketSuffix(t), 63)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// 생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인
		{"test_bucket_create_naming_good_starts_alpha", func(t *testing.T) {
			s := newSuite(t)
			bucket := "a" + uniqueBucketSuffix(t)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// 생성할 버킷의 이름이 숫자로 시작할 경우 생성되는지 확인
		{"test_bucket_create_naming_good_starts_digit", func(t *testing.T) {
			s := newSuite(t)
			bucket := "0" + uniqueBucketSuffix(t)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// 생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인
		{"test_bucket_create_naming_good_contains_period", func(t *testing.T) {
			s := newSuite(t)
			bucket := "aaa.111-" + uniqueBucketSuffix(t)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// 생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인
		{"test_bucket_create_naming_good_contains_hyphen", func(t *testing.T) {
			s := newSuite(t)
			bucket := "aaa-111-" + uniqueBucketSuffix(t)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			createAndCleanupBucket(t, s, bucket)
		}},
		// 버킷 중복 생성시 실패 확인
		{"test_bucket_create_exists", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket))
			assertAPIErrorCode(t, err, "BucketAlreadyOwnedByYou")
		}},
		// [다른 2명의 사용자가 버킷 생성하려고 할 경우] 메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인
		{"test_bucket_create_exists_nonowner", func(t *testing.T) {
			s := newSuite(t)
			if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
				t.Skip("configure Alt User credentials in config.ini")
			}
			bucket := s.bucket(t)
			altClient := s3Client(s.cfg, s.cfg.Alt)
			_, err := altClient.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket))
			assertAPIErrorCode(t, err, "BucketAlreadyExists")
		}},
		// 버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인 (버킷은 중복 생성 할 수 없음을 확인)
		{"test_bucket_recreate_not_overriding", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			want := []string{"my_key1", "my_key2"}
			for _, key := range want {
				put(t, s, bucket, key, key, nil)
			}
			_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket))
			assertAPIErrorCode(t, err, "BucketAlreadyOwnedByYou")

			out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("ListObjectsV2: %v", err)
			}
			got := make([]string, 0, len(out.Contents))
			for _, object := range out.Contents {
				got = append(got, aws.ToString(object.Key))
			}
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("objects after recreate = %v, want %v", got, want)
			}
		}},
		// 버킷의 location 정보 조회
		{"test_get_bucket_location", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			if _, err := s.client.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: aws.String(bucket)}); err != nil {
				t.Fatalf("GetBucketLocation: %v", err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func uniqueBucketSuffix(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if len(name) > 24 {
		name = name[len(name)-24:]
	}
	return fmt.Sprintf("%s-%x", name, len(t.Name()))
}

func bucketNameOfLength(seed string, length int) string {
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(seed)))
	return strings.Repeat(digest, (length/len(digest))+1)[:length]
}

func createAndCleanupBucket(t *testing.T, s *suite, bucket string) {
	t.Helper()
	if _, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket)); err != nil {
		t.Fatalf("CreateBucket(%q): %v", bucket, err)
	}
	t.Cleanup(func() {
		if s.cfg.NotDelete {
			return
		}
		_, _ = s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
}

func assertAPIError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded, want S3 API error")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T, want smithy.APIError: %v", err, err)
	}
}

func assertAPIErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	assertAPIError(t, err)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T, want smithy.APIError: %v", err, err)
	}
	if got := apiErr.ErrorCode(); got != want {
		t.Fatalf("error code = %q, want %q", got, want)
	}
}

func s3Client(cfg testconfig.Config, user testconfig.User) *s3.Client {
	options := s3.Options{
		Region:       cfg.Region,
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(user.AccessKey, user.SecretKey, "")),
		UsePathStyle: cfg.Endpoint() != "",
	}
	if endpoint := cfg.Endpoint(); endpoint != "" {
		options.BaseEndpoint = aws.String(endpoint)
	}
	applyCompatibleS3Options(&options)
	return s3.New(options)
}
