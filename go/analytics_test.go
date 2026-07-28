package s3tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	"github.com/aws/aws-sdk-go-v2/service/s3/types"
// )

// func TestPutBucketAnalytics(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	source, target := s.bucket(t), s.bucket(t)
// 	putAnalytics(t, s, source, "test", analyticsConfig("test", target, types.StorageClassAnalysisSchemaVersionV1, types.AnalyticsS3ExportFileFormatCsv))
// }
// func TestGetBucketAnalytics(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	source, target := s.bucket(t), s.bucket(t)
// 	putAnalytics(t, s, source, "test", analyticsConfig("test", target, types.StorageClassAnalysisSchemaVersionV1, types.AnalyticsS3ExportFileFormatCsv))
// 	out, err := s.client.GetBucketAnalyticsConfiguration(context.Background(), &s3.GetBucketAnalyticsConfigurationInput{Bucket: aws.String(source), Id: aws.String("test")})
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	assertAnalytics(t, out.AnalyticsConfiguration, "test", target)
// }
// func TestAddBucketAnalytics(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	source, target := s.bucket(t), s.bucket(t)
// 	putAnalytics(t, s, source, "test", analyticsConfig("test", target, types.StorageClassAnalysisSchemaVersionV1, types.AnalyticsS3ExportFileFormatCsv))
// 	putAnalytics(t, s, source, "test2", analyticsConfig("test2", target, types.StorageClassAnalysisSchemaVersionV1, types.AnalyticsS3ExportFileFormatCsv))
// 	out, err := s.client.ListBucketAnalyticsConfigurations(context.Background(), &s3.ListBucketAnalyticsConfigurationsInput{Bucket: aws.String(source)})
// 	if err != nil || len(out.AnalyticsConfigurationList) != 2 {
// 		t.Fatalf("analytics list=%v err=%v", out, err)
// 	}
// }
// func TestListBucketAnalytics(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	source, target := s.bucket(t), s.bucket(t)
// 	putAnalytics(t, s, source, "test", analyticsConfig("test", target, types.StorageClassAnalysisSchemaVersionV1, types.AnalyticsS3ExportFileFormatCsv))
// 	out, err := s.client.ListBucketAnalyticsConfigurations(context.Background(), &s3.ListBucketAnalyticsConfigurationsInput{Bucket: aws.String(source)})
// 	if err != nil || len(out.AnalyticsConfigurationList) != 1 {
// 		t.Fatalf("analytics list=%v err=%v", out, err)
// 	}
// 	assertAnalytics(t, &out.AnalyticsConfigurationList[0], "test", target)
// }
// func TestDeleteBucketAnalytics(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	source, target := s.bucket(t), s.bucket(t)
// 	putAnalytics(t, s, source, "test", analyticsConfig("test", target, types.StorageClassAnalysisSchemaVersionV1, types.AnalyticsS3ExportFileFormatCsv))
// 	if _, err := s.client.DeleteBucketAnalyticsConfiguration(context.Background(), &s3.DeleteBucketAnalyticsConfigurationInput{Bucket: aws.String(source), Id: aws.String("test")}); err != nil {
// 		t.Fatal(err)
// 	}
// 	_, err := s.client.GetBucketAnalyticsConfiguration(context.Background(), &s3.GetBucketAnalyticsConfigurationInput{Bucket: aws.String(source), Id: aws.String("test")})
// 	assertS3Error(t, err, 404, "NoSuchConfiguration")
// }
// func TestPutBucketAnalyticsInvalid(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	source, target := s.bucket(t), s.bucket(t)
// 	_, err := s.client.PutBucketAnalyticsConfiguration(context.Background(), &s3.PutBucketAnalyticsConfigurationInput{Bucket: aws.String(source), Id: aws.String("test"), AnalyticsConfiguration: analyticsConfig("test", target, types.StorageClassAnalysisSchemaVersion("V_2"), types.AnalyticsS3ExportFileFormatCsv)})
// 	assertS3Error(t, err, 400, "MalformedXML")
// }

// func analyticsConfig(id, bucket string, schema types.StorageClassAnalysisSchemaVersion, format types.AnalyticsS3ExportFileFormat) *types.AnalyticsConfiguration {
// 	return &types.AnalyticsConfiguration{Id: aws.String(id), StorageClassAnalysis: &types.StorageClassAnalysis{DataExport: &types.StorageClassAnalysisDataExport{OutputSchemaVersion: schema, Destination: &types.AnalyticsExportDestination{S3BucketDestination: &types.AnalyticsS3BucketDestination{Bucket: aws.String("arn:aws:s3:::" + bucket), Format: format}}}}}
// }
// func putAnalytics(t *testing.T, s *suite, bucket, id string, cfg *types.AnalyticsConfiguration) {
// 	t.Helper()
// 	if _, err := s.client.PutBucketAnalyticsConfiguration(context.Background(), &s3.PutBucketAnalyticsConfigurationInput{Bucket: aws.String(bucket), Id: aws.String(id), AnalyticsConfiguration: cfg}); err != nil {
// 		t.Fatal(err)
// 	}
// }
// func assertAnalytics(t *testing.T, cfg *types.AnalyticsConfiguration, id, bucket string) {
// 	t.Helper()
// 	if cfg == nil || aws.ToString(cfg.Id) != id || cfg.StorageClassAnalysis == nil || cfg.StorageClassAnalysis.DataExport == nil || cfg.StorageClassAnalysis.DataExport.Destination == nil || cfg.StorageClassAnalysis.DataExport.Destination.S3BucketDestination == nil {
// 		t.Fatalf("invalid analytics configuration: %#v", cfg)
// 	}
// 	dst := cfg.StorageClassAnalysis.DataExport.Destination.S3BucketDestination
// 	if aws.ToString(dst.Bucket) != "arn:aws:s3:::"+bucket || dst.Format != types.AnalyticsS3ExportFileFormatCsv {
// 		t.Fatalf("invalid destination: %#v", dst)
// 	}
// }
