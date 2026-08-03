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
	public class Inventory : TestBase
	{
		public Inventory(Xunit.Abstractions.ITestOutputHelper output) => Output = output;

		/// <summary>인벤토리 설정 객체를 생성한다. 오류 케이스를 위해 잘못된 값을 문자열로 주입할 수 있다.</summary>
		private static InventoryConfiguration GetInventoryConfig(string inventoryId, string targetBucketName,
			InventoryFormat format = null, string prefix = null,
			InventoryIncludedObjectVersions includedVersions = null, InventoryFrequency frequency = null,
			List<InventoryOptionalField> optionalFields = null)
		{
			var destination = new InventoryS3BucketDestination
			{
				BucketName = $"arn:aws:s3:::{targetBucketName}",
				InventoryFormat = format ?? InventoryFormat.CSV,
			};
			if (prefix != null) destination.Prefix = prefix;

			return new InventoryConfiguration
			{
				Destination = new InventoryDestination { S3BucketDestination = destination },
				InventoryId = inventoryId,
				IncludedObjectVersions = includedVersions ?? InventoryIncludedObjectVersions.Current,
				IsEnabled = true,
				Schedule = new InventorySchedule { Frequency = frequency ?? InventoryFrequency.Daily },
				InventoryOptionalFields = optionalFields,
			};
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "버킷에 인벤토리를 설정하지 않은 상태에서 조회가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketInventory()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var response = client.ListBucketInventoryConfigurations(bucketName);
			Assert.Empty(response.InventoryConfigurationList);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷에 인벤토리를 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventory()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷에 인벤토리가 설정되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCheckBucketInventory()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));

			var response = client.ListBucketInventoryConfigurations(bucketName);
			Assert.Single(response.InventoryConfigurationList);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷에 설정된 인벤토리 설정을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetBucketInventory()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName, prefix: "a/"));

			var response = client.GetBucketInventoryConfiguration(bucketName, inventoryId);
			Assert.Equal(inventoryId, response.InventoryConfiguration.InventoryId);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷에 설정된 인벤토리 설정을 삭제할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteBucketInventory()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));

			client.DeleteBucketInventoryConfiguration(bucketName, inventoryId);

			var response = client.ListBucketInventoryConfigurations(bucketName);
			Assert.Empty(response.InventoryConfigurationList);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 인벤토리를 가져오려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetBucketInventoryNotExist()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			CheckErrorResponse(HttpStatusCode.NotFound,
				() => client.GetBucketInventoryConfiguration(bucketName, inventoryId), MainData.NO_SUCH_CONFIGURATION);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 인벤토리를 삭제하려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestDeleteBucketInventoryNotExist()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			var e = Assert.Throws<AggregateException>(() => client.DeleteBucketInventoryConfiguration(bucketName, inventoryId));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_CONFIGURATION, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷에 인벤토리 설정을 추가하려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryNotExist()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucketName();
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName)));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "인벤토리 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryIdNotExist()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);

			// AWS 대상에서는 SDK v4가 빈 InventoryId를 클라이언트 측에서 먼저 거부해 요청이 나가지 않는다.
			CheckRejected(HttpStatusCode.BadRequest,
				() => client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig("", targetBucketName)), MainData.MALFORMED_XML);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "동일한 아이디로 인벤토리를 중복 설정할 경우 하나로 유지되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryIdDuplicate()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));
			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));

			var response = client.ListBucketInventoryConfigurations(bucketName);
			Assert.Single(response.InventoryConfigurationList);
		}

		[Fact(Skip = "aws에서 타깃 버킷이 존재하는지 확인하지 않음")]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "타깃 버킷이 존재하지 않을 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryTargetNotExist()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucketName();
			var inventoryId = "my-inventory";

			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName)));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "지원하지 않는 파일 형식의 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryInvalidFormat()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName, format: "JSON")));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "올바르지 않은 주기의 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryInvalidFrequency()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName, frequency: "Hourly")));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		// .NET SDK v4는 IncludedObjectVersions를 ConstantClass로 다루며 마셜링 시 대소문자를 정규화한다
		// ("CUrrENT" -> "Current"). 잘못된 대소문자가 서버에 전달되지 않아 이 검증을 수행할 수 없다.
		// (java SDK v2는 문자열을 그대로 보내 AWS가 400 MalformedXML을 반환한다)
		[Fact(Skip = ".NET SDK v4 marshaller normalizes IncludedObjectVersions casing; invalid value never reaches S3")]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "대소문자를 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryInvalidCase()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";

			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName, includedVersions: "CUrrENT")));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "접두어를 포함한 인벤토리 설정이 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryPrefix()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";
			var inventoryPrefix = "a/";

			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName, prefix: inventoryPrefix));

			var response = client.GetBucketInventoryConfiguration(bucketName, inventoryId);
			Assert.Equal(inventoryId, response.InventoryConfiguration.InventoryId);
			Assert.Equal(inventoryPrefix, response.InventoryConfiguration.Destination.S3BucketDestination.Prefix);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "옵션을 포함한 인벤토리 설정이 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryOptional()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";
			var inventoryPrefix = "a/";
			var inventoryOptionalFields = new List<InventoryOptionalField> { InventoryOptionalField.Size, InventoryOptionalField.LastModifiedDate };

			client.PutBucketInventoryConfiguration(bucketName,
				GetInventoryConfig(inventoryId, targetBucketName, prefix: inventoryPrefix, optionalFields: inventoryOptionalFields));

			var response = client.GetBucketInventoryConfiguration(bucketName, inventoryId);
			Assert.Equal(inventoryId, response.InventoryConfiguration.InventoryId);
			Assert.Equal(inventoryPrefix, response.InventoryConfiguration.Destination.S3BucketDestination.Prefix);
			Assert.Equal(inventoryOptionalFields, response.InventoryConfiguration.InventoryOptionalFields);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "옵션을 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryInvalidOptional()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";
			var inventoryPrefix = "a/";
			var inventoryOptionalFields = new List<InventoryOptionalField> { "SIZE", "--" };

			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketInventoryConfiguration(bucketName,
					GetInventoryConfig(inventoryId, targetBucketName, prefix: inventoryPrefix, optionalFields: inventoryOptionalFields)));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}
	}
}
