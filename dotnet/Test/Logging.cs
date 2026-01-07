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
		public void test_logging_prefix()
		{
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
		public void test_logging_versioning()
		{
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
		public void test_logging_encryption()
		{
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
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName(false);
			var Prefix = "logs/";
			var client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			var e = Assert.Throws<AggregateException>(() => client.PutBucketLogging(SourceBucketName, Config));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_TARGET_BUCKET_FOR_LOGGING, GetErrorCode(e));
		}
	}
}