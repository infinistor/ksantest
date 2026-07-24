package s3tests

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const selectObjectContentSkipReason = "SelectObjectContent migration policy: implementation retained but execution is intentionally disabled"

func TestSelectObjectContent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// CSV 오브젝트에 대해 SelectObjectContent 기본 동작 확인 (select * from s3object)
		{"test_select_object_content_csv_basic", func(t *testing.T) {
			runSkippedSelect(t, func(t *testing.T) {
				result := selectCSVFixture(t, "name,age,city\nAlice,30,Seoul\nBob,25,Busan", "SELECT * FROM s3object", types.FileHeaderInfoUse)
				assertSelectContains(t, result, "Alice", "Bob", "Seoul", "Busan")
			})
		}},
		// CSV 오브젝트에 WHERE 조건 적용
		{"test_select_object_content_csv_with_where", func(t *testing.T) {
			runSkippedSelect(t, func(t *testing.T) {
				result := selectCSVFixture(t, "name,age,city\nAlice,30,Seoul\nBob,25,Busan\nCharlie,30,Incheon", `SELECT * FROM s3object WHERE s3."age" = 30`, types.FileHeaderInfoUse)
				assertSelectContains(t, result, "Alice", "Charlie")
				if strings.Contains(result.records, "Bob") {
					t.Fatalf("WHERE result unexpectedly contains Bob: %q", result.records)
				}
			})
		}},
		// CSV 오브젝트 LIMIT 적용
		{"test_select_object_content_csv_limit", func(t *testing.T) {
			runSkippedSelect(t, func(t *testing.T) {
				result := selectCSVFixture(t, "id,value\n1,a\n2,b\n3,c", "SELECT * FROM s3object LIMIT 2", types.FileHeaderInfoUse)
				assertSelectContains(t, result, "1", "2")
			})
		}},
		// JSON 오브젝트에 대해 SelectObjectContent 기본 동작 확인
		{"test_select_object_content_json_basic", func(t *testing.T) {
			runSkippedSelect(t, func(t *testing.T) {
				s := newSuite(t)
				bucket, key := s.bucket(t), "data.json"
				put(t, s, bucket, key, "{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}", nil)
				input := selectInput(bucket, key, "SELECT * FROM s3object s WHERE s.age > 26", &types.InputSerialization{CompressionType: types.CompressionTypeNone, JSON: &types.JSONInput{Type: types.JSONTypeLines}}, &types.OutputSerialization{JSON: &types.JSONOutput{}})
				result := collectSelect(t, s.client, input)
				assertSelectContains(t, result, "Alice", "30")
				if strings.Contains(result.records, "Bob") {
					t.Fatalf("JSON result unexpectedly contains Bob: %q", result.records)
				}
			})
		}},
		// 존재하지 않는 버킷에 SelectObjectContent 요청 시 실패 확인
		{"test_select_object_content_non_existent_bucket", func(t *testing.T) {
			runSkippedSelect(t, func(t *testing.T) {
				s := newSuite(t)
				input := selectInput("missing-"+uniqueBucketSuffix(t), "data.csv", "SELECT * FROM s3object", csvSelectInput(types.FileHeaderInfoNone), csvSelectOutput())
				_, err := s.client.SelectObjectContent(context.Background(), input)
				assertS3Error(t, err, 404, "NoSuchBucket")
			})
		}},
		// 존재하지 않는 오브젝트에 SelectObjectContent 요청 시 실패 확인
		{"test_select_object_content_non_existent_object", func(t *testing.T) {
			runSkippedSelect(t, func(t *testing.T) {
				s := newSuite(t)
				input := selectInput(s.bucket(t), "non-existent-key.csv", "SELECT * FROM s3object", csvSelectInput(types.FileHeaderInfoNone), csvSelectOutput())
				_, err := s.client.SelectObjectContent(context.Background(), input)
				assertS3Error(t, err, 404, "NoSuchKey")
			})
		}},
		// 빈 CSV 오브젝트에 SelectObjectContent (헤더만 있는 경우)
		{"test_select_object_content_csv_empty_rows", func(t *testing.T) {
			runSkippedSelect(t, func(t *testing.T) {
				result := selectCSVFixture(t, "col1,col2\n", "SELECT * FROM s3object", types.FileHeaderInfoUse)
				if !result.ended || !result.sawStats {
					t.Fatalf("empty result stream ended=%v stats=%v", result.ended, result.sawStats)
				}
			})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func runSkippedSelect(t *testing.T, implementation func(*testing.T)) {
	t.Helper()
	t.Skip(selectObjectContentSkipReason)
	implementation(t)
}

type selectResult struct {
	records  string
	sawStats bool
	ended    bool
}

func selectCSVFixture(t *testing.T, content, expression string, header types.FileHeaderInfo) selectResult {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), "data.csv"
	put(t, s, bucket, key, content, nil)
	return collectSelect(t, s.client, selectInput(bucket, key, expression, csvSelectInput(header), csvSelectOutput()))
}

func selectInput(bucket, key, expression string, input *types.InputSerialization, output *types.OutputSerialization) *s3.SelectObjectContentInput {
	return &s3.SelectObjectContentInput{Bucket: aws.String(bucket), Key: aws.String(key), Expression: aws.String(expression), ExpressionType: types.ExpressionTypeSql, InputSerialization: input, OutputSerialization: output, RequestProgress: &types.RequestProgress{Enabled: aws.Bool(true)}}
}

func csvSelectInput(header types.FileHeaderInfo) *types.InputSerialization {
	return &types.InputSerialization{CompressionType: types.CompressionTypeNone, CSV: &types.CSVInput{FileHeaderInfo: header}}
}

func csvSelectOutput() *types.OutputSerialization {
	return &types.OutputSerialization{CSV: &types.CSVOutput{}}
}

func collectSelect(t *testing.T, client *s3.Client, input *s3.SelectObjectContentInput) selectResult {
	t.Helper()
	out, err := client.SelectObjectContent(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	stream := out.GetStream()
	defer stream.Close()
	var records bytes.Buffer
	result := selectResult{}
	for event := range stream.Events() {
		switch value := event.(type) {
		case *types.SelectObjectContentEventStreamMemberRecords:
			if _, err := records.Write(value.Value.Payload); err != nil {
				t.Fatal(err)
			}
		case *types.SelectObjectContentEventStreamMemberStats:
			if value.Value.Details == nil {
				t.Fatal("Stats event has no details")
			}
			result.sawStats = true
		case *types.SelectObjectContentEventStreamMemberEnd:
			result.ended = true
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	result.records = records.String()
	if !result.ended || !result.sawStats {
		t.Fatalf("event stream ended=%v stats=%v records=%q", result.ended, result.sawStats, result.records)
	}
	return result
}

func assertSelectContains(t *testing.T, result selectResult, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(result.records, value) {
			t.Fatalf("select result %q does not contain %q", result.records, value)
		}
	}
}
