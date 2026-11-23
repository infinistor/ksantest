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

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "타깃 버킷이 존재하지 않아도 설정이 저장되는지 확인 (AWS 는 타깃 버킷 존재 여부를 확인하지 않음)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryTargetNotExist()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucketName();
			var inventoryId = "my-inventory";

			// AWS 는 타깃 버킷의 존재 여부를 확인하지 않고 설정을 저장한다
			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));

			var response = client.GetBucketInventoryConfiguration(bucketName, inventoryId);
			Assert.Equal(inventoryId, response.InventoryConfiguration.InventoryId);
			Assert.Equal("arn:aws:s3:::" + targetBucketName,
				response.InventoryConfiguration.Destination.S3BucketDestination.BucketName);
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

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "인벤토리 ID 는 64자까지만 허용하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryIdLength()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			// ID 는 64자까지 허용한다
			var inventoryId = new string('a', 64);
			client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));
			var response = client.GetBucketInventoryConfiguration(bucketName, inventoryId);
			Assert.Equal(inventoryId, response.InventoryConfiguration.InventoryId);

			var tooLong = new string('a', 65);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(tooLong, targetBucketName)));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "인벤토리 ID 에 허용하지 않는 문자가 있으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryInvalidId()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			// ID 에는 영숫자와 - _ . 만 쓸 수 있다
			var inventoryId = "a b/c";
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName)));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "쿼리의 id 와 본문의 Id 가 다르면 인벤토리 설정이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryIdMismatch()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			// 쿼리의 id 와 본문의 Id 가 다르면 거부된다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.ID_MISMATCH,
				() => client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig("other-inventory", targetBucketName), id: "my-inventory"));
			Assert.Empty(client.ListBucketInventoryConfigurations(bucketName).InventoryConfigurationList ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "비활성(IsEnabled=false) 인벤토리 설정이 그대로 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryDisabled()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";
			var config = GetInventoryConfig(inventoryId, targetBucketName);
			config.IsEnabled = false;
			client.PutBucketInventoryConfiguration(bucketName, config);
			var response = client.GetBucketInventoryConfiguration(bucketName, inventoryId);
			Assert.False(response.InventoryConfiguration.IsEnabled);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "대상 버전(IncludedObjectVersions)이 잘못된 인벤토리 설정은 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryInvalidVersions()
		{
			TestId = 22;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName, includedVersions: "Invalid")));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "인벤토리의 형식·주기·대상 버전이 그대로 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryFormats()
		{
			TestId = 23;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			// 형식(CSV/ORC/Parquet), 주기(Daily/Weekly), 대상 버전(All/Current)이 그대로 저장된다
			var cases = new List<(string Id, InventoryFormat Format, InventoryFrequency Frequency, InventoryIncludedObjectVersions Versions)>()
			{
				("inventory-csv", InventoryFormat.CSV, InventoryFrequency.Daily, InventoryIncludedObjectVersions.All),
				("inventory-orc", InventoryFormat.ORC, InventoryFrequency.Weekly, InventoryIncludedObjectVersions.Current),
				("inventory-parquet", InventoryFormat.Parquet, InventoryFrequency.Weekly, InventoryIncludedObjectVersions.All),
			};
			foreach (var item in cases)
				client.PutBucketInventoryConfiguration(bucketName,
					GetInventoryConfig(item.Id, targetBucketName, format: item.Format, includedVersions: item.Versions, frequency: item.Frequency));
			foreach (var item in cases)
			{
				var config = client.GetBucketInventoryConfiguration(bucketName, item.Id).InventoryConfiguration;
				Assert.Equal(item.Format, config.Destination.S3BucketDestination.InventoryFormat);
				Assert.Equal(item.Frequency, config.Schedule.Frequency);
				Assert.Equal(item.Versions, config.IncludedObjectVersions);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "인벤토리 대상의 접두어와 SSE-S3 암호화 설정이 그대로 저장되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutBucketInventoryDestination()
		{
			TestId = 24;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";
			// 대상의 접두어와 SSE-S3 암호화 설정이 그대로 저장된다
			var config = GetInventoryConfig(inventoryId, targetBucketName, prefix: "inventory/");
			config.Destination.S3BucketDestination.InventoryEncryption = new InventoryEncryption { SSES3 = new SSES3() };
			client.PutBucketInventoryConfiguration(bucketName, config);

			var destination = client.GetBucketInventoryConfiguration(bucketName, inventoryId).InventoryConfiguration.Destination.S3BucketDestination;
			Assert.Equal($"arn:aws:s3:::{targetBucketName}", destination.BucketName);
			Assert.Equal("inventory/", destination.Prefix);
			Assert.NotNull(destination.InventoryEncryption?.SSES3);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "인벤토리 대상 버킷이 ARN 형식이 아니면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutBucketInventoryInvalidDestinationArn()
		{
			TestId = 25;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var inventoryId = "my-inventory";
			// 대상 버킷은 ARN 형식이어야 한다
			var config = GetInventoryConfig(inventoryId, targetBucketName);
			config.Destination.S3BucketDestination.BucketName = targetBucketName;
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_S3_DESTINATION_BUCKET,
				() => client.PutBucketInventoryConfiguration(bucketName, config));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "인벤토리 설정 목록이 100개씩 나뉘어 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestListBucketInventoryPagination()
		{
			TestId = 26;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			var ids = new List<string>();
			for (int i = 0; i < 101; i++) ids.Add($"inventory-{i:D3}");
			foreach (var inventoryId in ids)
				client.PutBucketInventoryConfiguration(bucketName, GetInventoryConfig(inventoryId, targetBucketName));

			// 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
			var response = client.ListBucketInventoryConfigurations(bucketName);
			Assert.Equal(100, response.InventoryConfigurationList.Count);
			Assert.True(response.IsTruncated);
			var listed = new List<string>();
			foreach (var config in response.InventoryConfigurationList) listed.Add(config.InventoryId);

			response = client.ListBucketInventoryConfigurations(bucketName, continuationToken: response.NextToken);
			Assert.Single(response.InventoryConfigurationList);
			Assert.False(response.IsTruncated);
			foreach (var config in response.InventoryConfigurationList) listed.Add(config.InventoryId);
			listed.Sort(StringComparer.Ordinal);
			Assert.Equal(ids, listed);
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "List")]
		[Trait(MainData.Explanation, "잘못된 토큰으로 인벤토리 설정 목록을 조회하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestListBucketInventoryInvalidToken()
		{
			TestId = 27;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_CONTINUATION_TOKEN,
				() => client.ListBucketInventoryConfigurations(bucketName, continuationToken: "invalid"));
		}

		[Fact]
		[Trait(MainData.Major, "Inventory")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "없는 버킷에 인벤토리 목록/조회/삭제를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketInventoryBucketNotExist()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.ListBucketInventoryConfigurations(bucketName));
			// SDK v4 는 단건 조회의 404 를 예외로 던지지 않고 상태 코드만 채운 응답을 돌려준다
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketInventoryConfiguration(bucketName, "my-inventory"), MainData.NO_SUCH_BUCKET);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.DeleteBucketInventoryConfiguration(bucketName, "my-inventory"));
		}
	}
}
