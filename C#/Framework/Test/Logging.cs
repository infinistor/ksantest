/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Amazon.S3;
using System.Net;

namespace s3tests2
{
	[TestClass]
	public class Logging : TestBase
	{
		[TestMethod("test_logging_get")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Put/Get")]
		[TestProperty(MainData.Explanation, "버킷에 로깅 설정 조회 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_logging_get()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Response = Client.GetBucketLogging(BucketName);
			Assert.IsNull(Response.BucketLoggingConfig.TargetBucketName);
			Assert.IsNull(Response.BucketLoggingConfig.TargetPrefix);
		}

		[TestMethod("test_logging_set")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Put/Get")]
		[TestProperty(MainData.Explanation, "버킷에 로깅 설정 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_logging_set()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName };
			Client.PutBucketLogging(SourceBucketName, Config);
		}

		[TestMethod("test_logging_set_get")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Put/Get")]
		[TestProperty(MainData.Explanation, "버킷에 설정한 로깅 정보 조회가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_logging_set_get()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = "" };
			Client.PutBucketLogging(SourceBucketName, Config);

			var Response = Client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[TestMethod("test_logging_prefix")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Put/Get")]
		[TestProperty(MainData.Explanation, "버킷의 로깅에 Prefix가 설정되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_logging_prefix()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Prefix = "logs/";
			var Client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			Client.PutBucketLogging(SourceBucketName, Config);

			var Response = Client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[TestMethod("test_logging_versioning")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Versioning")]
		[TestProperty(MainData.Explanation, "버저닝 설정된 버킷의 로깅이 설정되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_logging_versioning()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Prefix = "logs/";
			var Client = GetClient();

			CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Enabled);

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			Client.PutBucketLogging(SourceBucketName, Config);

			var Response = Client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[TestMethod("test_logging_encryption")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Encryption")]
		[TestProperty(MainData.Explanation, "SSE-s3설정된 버킷의 로깅이 설정되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_logging_encryption()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Prefix = "logs/";
			var Client = GetClient();
			var SSEConfig = new ServerSideEncryptionConfiguration()
			{
				ServerSideEncryptionRules = new List<ServerSideEncryptionRule>()
				 {
					 new ServerSideEncryptionRule()
					 {
						  ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
						  {
							   ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
						  }
					 }
				 }
			};

			Client.PutBucketEncryption(SourceBucketName, SSEConfig);

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			Client.PutBucketLogging(SourceBucketName, Config);

			var Response = Client.GetBucketLogging(SourceBucketName);
			LoggingConfigCompare(Config, Response.BucketLoggingConfig);
		}

		[TestMethod("test_logging_bucket_not_found")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "존재하지 않는 버킷에 로깅 설정 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_logging_bucket_not_found()
		{
			var SourceBucketName = GetNewBucketName(false);
			var TargetBucketName = GetNewBucketName(false);
			var Prefix = "logs/";
			var Client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutBucketLogging(SourceBucketName, Config));

			Assert.AreEqual(HttpStatusCode.NotFound, e.StatusCode);
			Assert.AreEqual(MainData.NoSuchBucket, e.ErrorCode);
		}

		[TestMethod("test_logging_target_bucket_not_found")]
		[TestProperty(MainData.Major, "Logging")]
		[TestProperty(MainData.Minor, "Error")]
		[TestProperty(MainData.Explanation, "타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_logging_target_bucket_not_found()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName(false);
			var Prefix = "logs/";
			var Client = GetClient();

			var Config = new S3BucketLoggingConfig() { TargetBucketName = TargetBucketName, TargetPrefix = Prefix };
			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutBucketLogging(SourceBucketName, Config));

			Assert.AreEqual(HttpStatusCode.BadRequest, e.StatusCode);
			Assert.AreEqual(MainData.InvalidTargetBucketForLogging, e.ErrorCode);
		}
	}
}