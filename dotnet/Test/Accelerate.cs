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
using System.Net;
using Xunit;

namespace s3tests
{
	public class Accelerate : TestBase
	{
		public Accelerate(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;


		[Fact]
		[Trait(MainData.Major, "Accelerate")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷의 Accelerate 설정 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetBucketAccelerate()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var response = client.GetBucketAccelerateConfiguration(bucketName);
			Assert.Equal(BucketAccelerateStatus.Suspended, response.Status);
		}

		[Fact]
		[Trait(MainData.Major, "Accelerate")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷의 Accelerate 설정 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketAccelerate()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketAccelerateConfiguration(bucketName, BucketAccelerateStatus.Enabled);

			var response = client.GetBucketAccelerateConfiguration(bucketName);
			Assert.Equal(BucketAccelerateStatus.Enabled, response.Status);
		}

		[Fact]
		[Trait(MainData.Major, "Accelerate")]
		[Trait(MainData.Minor, "Change")]
		[Trait(MainData.Explanation, "버킷의 Accelerate 설정 변경 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestChangeBucketAccelerate()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketAccelerateConfiguration(bucketName, BucketAccelerateStatus.Enabled);

			var response = client.GetBucketAccelerateConfiguration(bucketName);
			Assert.Equal(BucketAccelerateStatus.Enabled, response.Status);

			client.PutBucketAccelerateConfiguration(bucketName, BucketAccelerateStatus.Suspended);

			response = client.GetBucketAccelerateConfiguration(bucketName);
			Assert.Equal(BucketAccelerateStatus.Suspended, response.Status);
		}

		[Fact]
		[Trait(MainData.Major, "Accelerate")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "잘못된 Accelerate 설정시 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketAccelerateInvalid()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var request = new PutBucketAccelerateConfigurationRequest
			{
				BucketName = bucketName,
				AccelerateConfiguration = new AccelerateConfiguration
				{
					Status = "Invalid"
				}
			};

			var e = Assert.Throws<AggregateException>(() => client.PutBucketAccelerateConfiguration(request));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal("MalformedXML", GetErrorCode(e));
		}
	}
}
