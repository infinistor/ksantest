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
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Logging : TestBase
	{
		public Logging(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Put/Get")]
		[Trait(MainData.Explanation, "버킷에 로깅 설정 조회 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingGet()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Response = client.GetBucketLogging(bucketName);
			Assert.Null(Response.BucketLoggingConfig.TargetBucketName);
			Assert.Null(Response.BucketLoggingConfig.TargetPrefix);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Put/Get")]
		[Trait(MainData.Explanation, "버킷에 로깅 설정 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingSet()
		{
			TestId = 2;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName };
			client.PutBucketLogging(SourceBucketName, Config);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Put/Get")]
		[Trait(MainData.Explanation, "버킷에 설정한 로깅 정보 조회가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingSetGet()
		{
			TestId = 3;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = "" };
			client.PutBucketLogging(SourceBucketName, Config);

			var Response = client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Put/Get")]
		[Trait(MainData.Explanation, "버킷의 로깅에 Prefix가 설정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingPrefix()
		{
			TestId = 4;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Prefix = "logs/";
			var client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			client.PutBucketLogging(SourceBucketName, Config);

			var Response = client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "버저닝 설정된 버킷의 로깅이 설정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingVersioning()
		{
			TestId = 5;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Prefix = "logs/";
			var client = GetClient();

			CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Enabled);

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			client.PutBucketLogging(SourceBucketName, Config);

			var Response = client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Encryption")]
		[Trait(MainData.Explanation, "SSE-s3설정된 버킷의 로깅이 설정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingEncryption()
		{
			TestId = 6;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Prefix = "logs/";
			var client = GetClient();
			var SSEConfig = new ServerSideEncryptionConfiguration()
			{
				ServerSideEncryptionRules =
				[
					new()
					{
						ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
						{
							ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
						}
					}
				]
			};

			client.PutBucketEncryption(SourceBucketName, SSEConfig);

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			client.PutBucketLogging(SourceBucketName, Config);

			var Response = client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷에 로깅 설정 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLoggingBucketNotFound()
		{
			TestId = 7;
			var SourceBucketName = GetNewBucketName(false);
			var TargetBucketName = GetNewBucketName(false);
			var Prefix = "logs/";
			var client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			var e = Assert.Throws<AggregateException>(() => client.PutBucketLogging(SourceBucketName, Config));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLoggingTargetBucketNotFound()
		{
			TestId = 8;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName(false);
			var Prefix = "logs/";
			var client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			var e = Assert.Throws<AggregateException>(() => client.PutBucketLogging(SourceBucketName, Config));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Put/Get")]
		[Trait(MainData.Explanation, "빈 설정을 넣으면 버킷 로깅이 해제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingDisable()
		{
			TestId = 9;
			var client = GetClient();
			var sourceBucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);

			client.PutBucketLogging(sourceBucketName,
				new S3BucketLoggingConfig() { TargetBucketName = targetBucketName, TargetPrefix = "logs/" });
			Assert.NotNull(client.GetBucketLogging(sourceBucketName).BucketLoggingConfig?.TargetBucketName);

			// 빈 설정을 넣으면 로깅이 해제된다
			client.PutBucketLogging(sourceBucketName, new S3BucketLoggingConfig());
			Assert.Null(client.GetBucketLogging(sourceBucketName).BucketLoggingConfig?.TargetBucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Put/Get")]
		[Trait(MainData.Explanation, "버킷 로깅 설정을 다시 저장하면 새 설정으로 교체되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingOverwrite()
		{
			TestId = 10;
			var client = GetClient();
			var sourceBucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var newTargetBucketName = GetNewBucket(client);

			client.PutBucketLogging(sourceBucketName,
				new S3BucketLoggingConfig() { TargetBucketName = targetBucketName, TargetPrefix = "logs/" });
			client.PutBucketLogging(sourceBucketName,
				new S3BucketLoggingConfig() { TargetBucketName = newTargetBucketName, TargetPrefix = "other/" });

			var response = client.GetBucketLogging(sourceBucketName);
			Assert.Equal(newTargetBucketName, response.BucketLoggingConfig.TargetBucketName);
			Assert.Equal("other/", response.BucketLoggingConfig.TargetPrefix);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "KeyFormat")]
		[Trait(MainData.Explanation, "로그 오브젝트 키 형식을 SimplePrefix 로 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingSimplePrefix()
		{
			TestId = 11;
			var client = GetClient();
			var sourceBucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);

			client.PutBucketLogging(sourceBucketName, new S3BucketLoggingConfig()
			{
				TargetBucketName = targetBucketName,
				TargetPrefix = "logs/",
				TargetObjectKeyFormat = new TargetObjectKeyFormat() { SimplePrefix = new SimplePrefix() },
			});

			var response = client.GetBucketLogging(sourceBucketName);
			var keyFormat = response.BucketLoggingConfig.TargetObjectKeyFormat;
			Assert.NotNull(keyFormat.SimplePrefix);
			Assert.Null(keyFormat.PartitionedPrefix);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "KeyFormat")]
		[Trait(MainData.Explanation, "로그 오브젝트 키 형식을 PartitionedPrefix(EventTime/DeliveryTime)로 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingPartitionedPrefix()
		{
			TestId = 12;
			var client = GetClient();
			var sourceBucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);

			foreach (var dateSource in new[] { PartitionDateSource.EventTime, PartitionDateSource.DeliveryTime })
			{
				client.PutBucketLogging(sourceBucketName, new S3BucketLoggingConfig()
				{
					TargetBucketName = targetBucketName,
					TargetPrefix = "logs/",
					TargetObjectKeyFormat = new TargetObjectKeyFormat()
					{
						PartitionedPrefix = new PartitionedPrefix() { PartitionDateSource = dateSource },
					},
				});

				var response = client.GetBucketLogging(sourceBucketName);
				var keyFormat = response.BucketLoggingConfig.TargetObjectKeyFormat;
				Assert.Equal(dateSource, keyFormat.PartitionedPrefix.PartitionDateSource);
				Assert.Null(keyFormat.SimplePrefix);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Put/Get")]
		[Trait(MainData.Explanation, "소스 버킷 자신을 로깅 타깃으로 지정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLoggingSameBucket()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// 소스 버킷 자신을 타깃으로 지정할 수 있다
			client.PutBucketLogging(bucketName,
				new S3BucketLoggingConfig() { TargetBucketName = bucketName, TargetPrefix = "self/" });

			var response = client.GetBucketLogging(bucketName);
			Assert.Equal(bucketName, response.BucketLoggingConfig.TargetBucketName);
			Assert.Equal("self/", response.BucketLoggingConfig.TargetPrefix);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "소스와 타깃 버킷의 소유자가 다르면 로깅 설정이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLoggingTargetBucketOtherOwner()
		{
			TestId = 14;
			var client = GetClient();
			var altClient = GetAltClient();
			var sourceBucketName = GetNewBucket(client);
			// 정리(BucketClear)는 메인 사용자로 수행되므로 다른 사용자의 버킷은 테스트에서 직접 지운다
			var targetBucketName = GetNewBucketName(false);
			altClient.PutBucket(targetBucketName);
			try
			{
				// 소스와 타깃 버킷의 소유자가 다르면 거부된다
				var e = Assert.Throws<AggregateException>(() => client.PutBucketLogging(sourceBucketName,
					new S3BucketLoggingConfig() { TargetBucketName = targetBucketName, TargetPrefix = "logs/" }));
				Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
				Assert.Equal(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, GetErrorCode(e));
			}
			finally
			{
				altClient.DeleteBucket(targetBucketName);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 로깅 설정 조회가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLoggingGetBucketNotExist()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			// SDK v4는 이 오퍼레이션의 404를 예외로 던지지 않고 HttpStatusCode만 채워 돌려주기도 한다(이때 에러 코드는 확인 불가)
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketLogging(bucketName), MainData.NO_SUCH_BUCKET);
		}

		[Fact]
		[Trait(MainData.Major, "Logging")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "권한이 없는 사용자의 로깅 설정 조회, 저장이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLoggingAltUser()
		{
			TestId = 16;
			var client = GetClient();
			var altClient = GetAltClient();
			var sourceBucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => altClient.GetBucketLogging(sourceBucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => altClient.PutBucketLogging(sourceBucketName,
				new S3BucketLoggingConfig() { TargetBucketName = targetBucketName, TargetPrefix = "logs/" }));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}
	}
}