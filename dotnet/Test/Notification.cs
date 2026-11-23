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
	public class Notification : TestBase
	{
		public Notification(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		private static readonly List<EventType> S3Events =
		[
			EventType.ObjectCreatedAll,
			EventType.ObjectRemovedAll
		];

		private static List<LambdaFunctionConfiguration> BuildLambdaConfigs(string roleId, string functionArn)
		{
			return
			[
				new LambdaFunctionConfiguration
				{
					Id = roleId,
					FunctionArn = functionArn,
					Events = S3Events
				}
			];
		}

		/// <summary>
		/// Lambda 함수 ARN을 정식 형식(arn:aws:lambda:리전:계정:function:이름)으로 만든다.
		/// AWS는 형식이 틀린 ARN을 400 InvalidArgument 로 거부한다.
		/// </summary>
		private string LambdaFunctionArn()
		{
			var region = string.IsNullOrWhiteSpace(Config.S3.RegionName) ? "us-east-1" : Config.S3.RegionName;
			return "arn:aws:lambda:" + region + ":" + Config.MainUser.ExpectedOwnerId + ":function:my-function";
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "빈 버킷 알림 설정 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationGetEmpty()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var result = client.GetBucketNotificationConfiguration(bucketName);

			Assert.Empty(result.LambdaFunctionConfigurations ?? []);
			Assert.Empty(result.QueueConfigurations ?? []);
			Assert.Empty(result.TopicConfigurations ?? []);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷 알림 설정")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationPut()
		{
			TestId = 2;
			SkipIfAws();

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = LambdaFunctionArn();

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: BuildLambdaConfigs(roleId, functionArn));
		}

		[SkippableFact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷 알림 설정 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationGet()
		{
			TestId = 3;
			SkipIfAws();

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = LambdaFunctionArn();

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: BuildLambdaConfigs(roleId, functionArn));

			var result = client.GetBucketNotificationConfiguration(bucketName);
			var resultLambda = result.LambdaFunctionConfigurations[0];
			S3EventCompare(S3Events, resultLambda.Events);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷 알림 설정 삭제")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationDelete()
		{
			TestId = 4;
			SkipIfAws();

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = LambdaFunctionArn();

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: BuildLambdaConfigs(roleId, functionArn));

			var result = client.GetBucketNotificationConfiguration(bucketName);
			S3EventCompare(S3Events, result.LambdaFunctionConfigurations[0].Events);

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: []);

			var deleteResult = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Empty(deleteResult.LambdaFunctionConfigurations ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "형식이 잘못된 Lambda ARN으로 알림 설정 시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationPutInvalidArn()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// arn: 접두어와 리전이 없는 잘못된 형식
			var functionArn = "aws:lambda::" + Config.MainUser.UserId + ":function:my-function";

			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketNotificationConfiguration(bucketName,
					lambdaFunctionConfigurations: BuildLambdaConfigs("my-lambda", functionArn)));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "EventBridge")]
		[Trait(MainData.Explanation, "버킷에 EventBridge 알림 설정, 조회, 삭제가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationEventBridge()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// EventBridge 는 대상 리소스가 없어 AWS 에서도 그대로 설정된다
			client.PutBucketNotificationConfiguration(bucketName,
				eventBridgeConfiguration: new EventBridgeConfiguration());

			var result = client.GetBucketNotificationConfiguration(bucketName);
			Assert.NotNull(result.EventBridgeConfiguration);
			Assert.Empty(result.LambdaFunctionConfigurations ?? []);

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: []);

			var deleteResult = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Null(deleteResult.EventBridgeConfiguration);
		}
	}
}
