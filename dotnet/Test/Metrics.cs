/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using Amazon.Runtime;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Metrics : TestBase
	{
		public Metrics(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "버킷 메트릭스 설정 목록 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMetrics()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var response = client.ListBucketMetricsConfigurations(bucketName);
			Assert.Empty(response.MetricsConfigurationList);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷 메트릭스 설정")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutMetrics()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketMetricsConfiguration(bucketName, "metrics-id",
				new MetricsConfiguration { MetricsId = "metrics-id" });
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷 메트릭스 설정 후 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCheckMetrics()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketMetricsConfiguration(bucketName, "metrics-id",
				new MetricsConfiguration { MetricsId = "metrics-id" });
			var response = client.ListBucketMetricsConfigurations(bucketName);
			Assert.Single(response.MetricsConfigurationList);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷 메트릭스 설정 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetMetrics()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var metricId = "metrics-id";

			client.PutBucketMetricsConfiguration(bucketName, metricId,
				new MetricsConfiguration { MetricsId = metricId });
			var response = client.GetBucketMetricsConfiguration(bucketName, metricId);
			Assert.Equal(metricId, response.MetricsConfiguration.MetricsId);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷 메트릭스 설정 삭제")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteMetrics()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var metricId = "metrics-id";

			client.PutBucketMetricsConfiguration(bucketName, metricId,
				new MetricsConfiguration { MetricsId = metricId });
			client.DeleteBucketMetricsConfiguration(bucketName, metricId);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 메트릭스 설정 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetMetricsNotExist()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckErrorResponse(HttpStatusCode.NotFound,
				() => client.GetBucketMetricsConfiguration(bucketName, "metrics-id"), MainData.NO_SUCH_CONFIGURATION);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 메트릭스 설정 삭제 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestDeleteMetricsNotExist()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() =>
				client.DeleteBucketMetricsConfiguration(bucketName, "metrics-id"));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_CONFIGURATION, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷에 메트릭스 설정 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutMetricsNotExist()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			var e = Assert.Throws<AggregateException>(() =>
				client.PutBucketMetricsConfiguration(bucketName, "metrics-id",
					new MetricsConfiguration { MetricsId = "metrics-id" }));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "빈 ID로 메트릭스 설정 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutMetricsEmptyId()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// AWS 대상에서는 SDK v4가 빈 MetricsId를 클라이언트 측에서 먼저 거부해 요청이 나가지 않는다.
			CheckRejected(HttpStatusCode.BadRequest,
				() => client.PutBucketMetricsConfiguration(bucketName, "", new MetricsConfiguration { MetricsId = "" }),
				MainData.INVALID_CONFIGURATION_ID);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "ID 없이 메트릭스 설정 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutMetricsNoId()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.ThrowsAny<Exception>(() =>
			{
				var request = new PutBucketMetricsConfigurationRequest { BucketName = bucketName };
				client.Client.PutBucketMetricsConfigurationAsync(request).GetAwaiter().GetResult();
			});
			Assert.NotNull(e);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "동일 ID 메트릭스 설정 덮어쓰기")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutMetricsDuplicateId()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var metricId = "metrics-id";

			client.PutBucketMetricsConfiguration(bucketName, metricId,
				new MetricsConfiguration
				{
					MetricsId = metricId,
					MetricsFilter = new MetricsFilter
					{
						MetricsFilterPredicate = new MetricsPrefixPredicate("test1")
					}
				});
			var response = client.GetBucketMetricsConfiguration(bucketName, metricId);
			Assert.Equal("test1", ((MetricsPrefixPredicate)response.MetricsConfiguration.MetricsFilter.MetricsFilterPredicate).Prefix);

			client.PutBucketMetricsConfiguration(bucketName, metricId,
				new MetricsConfiguration
				{
					MetricsId = metricId,
					MetricsFilter = new MetricsFilter
					{
						MetricsFilterPredicate = new MetricsPrefixPredicate("test2")
					}
				});
			response = client.GetBucketMetricsConfiguration(bucketName, metricId);
			Assert.Equal("test2", ((MetricsPrefixPredicate)response.MetricsConfiguration.MetricsFilter.MetricsFilterPredicate).Prefix);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Filtering")]
		[Trait(MainData.Explanation, "Prefix 필터 메트릭스 설정")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMetricsPrefix()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var metricId = "metrics-id";
			var prefix = "test";

			client.PutBucketMetricsConfiguration(bucketName, metricId,
				new MetricsConfiguration
				{
					MetricsId = metricId,
					MetricsFilter = new MetricsFilter
					{
						MetricsFilterPredicate = new MetricsPrefixPredicate(prefix)
					}
				});
			var response = client.GetBucketMetricsConfiguration(bucketName, metricId);
			Assert.Equal(prefix, ((MetricsPrefixPredicate)response.MetricsConfiguration.MetricsFilter.MetricsFilterPredicate).Prefix);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Filtering")]
		[Trait(MainData.Explanation, "Tag 필터 메트릭스 설정")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMetricsTag()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var metricId = "metrics-id";
			var tag = new Tag { Key = "key", Value = "value" };

			client.PutBucketMetricsConfiguration(bucketName, metricId,
				new MetricsConfiguration
				{
					MetricsId = metricId,
					MetricsFilter = new MetricsFilter
					{
						MetricsFilterPredicate = new MetricsTagPredicate(tag)
					}
				});
			var response = client.GetBucketMetricsConfiguration(bucketName, metricId);
			var tagPred = (MetricsTagPredicate)response.MetricsConfiguration.MetricsFilter.MetricsFilterPredicate;
			Assert.Equal(tag.Key, tagPred.Tag.Key);
			Assert.Equal(tag.Value, tagPred.Tag.Value);
		}

		[Fact]
		[Trait(MainData.Major, "Metrics")]
		[Trait(MainData.Minor, "Filtering")]
		[Trait(MainData.Explanation, "And 필터 메트릭스 설정")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMetricsFilter()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var metricId = "metrics-id";
			var prefix = "test";
			var tag = new Tag { Key = "key", Value = "value" };

			client.PutBucketMetricsConfiguration(bucketName, metricId,
				new MetricsConfiguration
				{
					MetricsId = metricId,
					MetricsFilter = new MetricsFilter
					{
						MetricsFilterPredicate = new MetricsAndOperator(
						[
							new MetricsPrefixPredicate(prefix),
							new MetricsTagPredicate(tag)
						])
					}
				});
			var response = client.GetBucketMetricsConfiguration(bucketName, metricId);
			var andOp = (MetricsAndOperator)response.MetricsConfiguration.MetricsFilter.MetricsFilterPredicate;
			Assert.Equal(prefix, ((MetricsPrefixPredicate)andOp.Operands[0]).Prefix);
			Assert.Equal(tag.Key, ((MetricsTagPredicate)andOp.Operands[1]).Tag.Key);
			Assert.Equal(tag.Value, ((MetricsTagPredicate)andOp.Operands[1]).Tag.Value);
		}
	}
}
