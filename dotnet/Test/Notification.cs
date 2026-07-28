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
using System.Collections.Generic;
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

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "빈 버킷 알림 설정 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationGetEmpty()
		{
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
			SkipIfAws();

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = "aws:lambda::" + Config.MainUser.UserId + ":function:my-function";

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
			SkipIfAws();

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = "aws:lambda::" + Config.MainUser.UserId + ":function:my-function";

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
			SkipIfAws();

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = "aws:lambda::" + Config.MainUser.UserId + ":function:my-function";

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: BuildLambdaConfigs(roleId, functionArn));

			var result = client.GetBucketNotificationConfiguration(bucketName);
			S3EventCompare(S3Events, result.LambdaFunctionConfigurations[0].Events);

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: []);

			var deleteResult = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Empty(deleteResult.LambdaFunctionConfigurations ?? []);
		}
	}
}
