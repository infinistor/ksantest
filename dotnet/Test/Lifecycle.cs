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
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Lifecycle : TestBase
	{
		public Lifecycle(Xunit.Abstractions.ITestOutputHelper output) => Output = output;

		private static LifecycleFilter PrefixFilter(string prefix) =>
			new() { LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = prefix } };

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle 규칙을 추가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSet()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 1 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
				new() { Id = "rule2", Expiration = new LifecycleRuleExpiration(){ Days = 2 }, Filter = PrefixFilter("test2/"), Status = LifecycleRuleStatus.Disabled },
			};

			var response = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷에 설정한 Lifecycle 규칙을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleGet()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 31 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
				new() { Id = "rule2", Expiration = new LifecycleRuleExpiration(){ Days = 120 }, Filter = PrefixFilter("test2/"), Status = LifecycleRuleStatus.Enabled },
			};

			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			var response = client.GetLifecycleConfiguration(bucketName);
			PrefixLifecycleConfigurationCheck(rules, response.Configuration.Rules);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "Id 없이 버킷에 Lifecycle 규칙을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleGetNoId()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Expiration = new LifecycleRuleExpiration(){ Days = 31 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
				new() { Expiration = new LifecycleRuleExpiration(){ Days = 120 }, Filter = PrefixFilter("test2/"), Status = LifecycleRuleStatus.Enabled },
			};

			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			var response = client.GetLifecycleConfiguration(bucketName);
			var getRules = response.Configuration.Rules;

			for (int i = 0; i < rules.Count; i++)
			{
				Assert.NotEmpty(getRules[i].Id);
				Assert.Equal(rules[i].Expiration.Date, getRules[i].Expiration.Date);
				Assert.Equal(rules[i].Expiration.Days, getRules[i].Expiration.Days);
				Assert.Equal((rules[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix,
							 (getRules[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix);
				Assert.Equal(rules[i].Status, getRules[i].Status);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버킷에 버저닝 설정이 되어있는 상태에서 Lifecycle 규칙을 추가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleExpirationVersioningEnabled()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "test1/a";
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			SetupMultipleVersion(client, bucketName, key, 1);
			client.DeleteObject(bucketName, key);

			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 1 }, Filter = PrefixFilter("expire1/"), Status = LifecycleRuleStatus.Enabled },
			};
			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });

			var response = client.ListVersions(bucketName);
			Assert.Single(GetVersions(response.Versions));
			Assert.Single(GetDeleteMarkers(response.Versions));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷에 Lifecycle 규칙을 설정할때 Id의 길이가 너무 길면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleIdTooLong()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = S3Utils.RandomTextToLong(256), Expiration = new LifecycleRuleExpiration(){ Days = 2 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
			};

			var e = Assert.Throws<AggregateException>(() => client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules }));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Duplicate")]
		[Trait(MainData.Explanation, "버킷에 Lifecycle 규칙을 설정할때 같은 Id로 규칙을 여러개 설정할경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleSameId()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 1 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 2 }, Filter = PrefixFilter("test2/"), Status = LifecycleRuleStatus.Disabled },
			};

			var e = Assert.Throws<AggregateException>(() => client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules }));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷에 Lifecycle 규칙중 status를 잘못 설정할때 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleInvalidStatus()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 2 }, Filter = PrefixFilter("test1/"), Status = new LifecycleRuleStatus("invalid") },
			};

			var e = Assert.Throws<AggregateException>(() => client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules }));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Date")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle규칙에 날짜를 입력가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSetDate()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Date = DateTime.Parse("2099-10-10 00:00:00 GMT") }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
			};

			var response = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle규칙에 날짜를 올바르지 않은 형식(자정이 아닌 시간)으로 입력했을때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleSetInvalidDate()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Date = DateTime.Parse("2099-10-10 15:00:00 GMT") }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
			};

			var e = Assert.Throws<AggregateException>(() => client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules }));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버킷의 버저닝설정이 없는 환경에서 버전관리용 Lifecycle이 올바르게 설정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSetNoncurrent()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = SetupObjects(["past/foo", "future/bar"]);

			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 2 }, Filter = PrefixFilter("past/"), Status = LifecycleRuleStatus.Enabled },
				new() { Id = "rule2", NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 3 }, Filter = PrefixFilter("future/"), Status = LifecycleRuleStatus.Enabled },
			};

			var response = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버킷의 버저닝설정이 되어있는 환경에서 Lifecycle 이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleNoncurrentExpiration()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			SetupMultipleVersion(client, bucketName, "test1/a", 3);
			SetupMultipleVersion(client, bucketName, "test2/abc", 3, false);

			var response = client.ListVersions(bucketName);
			var versions = GetVersions(response.Versions);

			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 2 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
			};

			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(6, versions.Count);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "DeleteMarker")]
		[Trait(MainData.Explanation, "DeleteMarker에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSetDeleteMarker()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ ExpiredObjectDeleteMarker = true }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
			};

			var response = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "Lifecycle 규칙에 필터링값을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSetFilter()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ ExpiredObjectDeleteMarker = true }, Filter = PrefixFilter("foo"), Status = LifecycleRuleStatus.Enabled },
			};

			var response = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "Lifecycle 규칙에 필터링에 비어있는 값을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSetEmptyFilter()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ ExpiredObjectDeleteMarker = true }, Filter = PrefixFilter(""), Status = LifecycleRuleStatus.Enabled },
			};

			var response = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "DeleteMarker")]
		[Trait(MainData.Explanation, "DeleteMarker에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleDeleteMarkerExpiration()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			SetupMultipleVersion(client, bucketName, "test1/a", 1);
			SetupMultipleVersion(client, bucketName, "test2/abc", 1, false);
			client.DeleteObject(bucketName, "test1/a");
			client.DeleteObject(bucketName, "test2/abc");

			var response = client.ListVersions(bucketName);
			Assert.Equal(2, GetVersions(response.Versions).Count);
			Assert.Equal(2, GetDeleteMarkers(response.Versions).Count);

			var rules = new List<LifecycleRule>()
			{
				new()
				{
					Id = "rule1",
					NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 1 },
					Expiration = new LifecycleRuleExpiration() { ExpiredObjectDeleteMarker = true },
					Filter = PrefixFilter("test1/"),
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var putResponse = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, putResponse.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSetMultipart()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled, AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload(){ DaysAfterInitiation = 2 } },
				new() { Id = "rule2", Filter = PrefixFilter("test2/"), Status = LifecycleRuleStatus.Enabled, AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload(){ DaysAfterInitiation = 3 } },
			};

			var response = client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleMultipartExpiration()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var keyNames = new List<string>() { "test1/a", "test2/b" };
			var uploadIds = new List<string>();

			foreach (var key in keyNames)
			{
				var response = client.InitiateMultipartUpload(bucketName, key);
				uploadIds.Add(response.UploadId);
			}

			var listResponse = client.ListMultipartUploads(bucketName);
			var uploads = listResponse.MultipartUploads;

			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled, AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload(){ DaysAfterInitiation = 2 } },
			};

			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			Assert.Equal(2, uploads.Count);

			foreach (var upload in uploads)
				client.AbortMultipartUpload(bucketName, upload.Key, upload.UploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle 규칙을 삭제 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleDelete()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 1 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
				new() { Id = "rule2", Expiration = new LifecycleRuleExpiration(){ Days = 2 }, Filter = PrefixFilter("test2/"), Status = LifecycleRuleStatus.Disabled },
			};

			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });
			client.DeleteLifecycleConfiguration(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle규칙에 만료일을 0일로 설정할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleSetExpirationZero()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 0 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
			};

			var e = Assert.Throws<AggregateException>(() => client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules }));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact(Skip = "SDK 버전에 따라 expires 헤더 검증 동작이 달라 비활성화 (testV2 @Disabled와 동일)")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "Lifecycle 만료 규칙 설정 시 오브젝트의 expires 헤더가 올바르게 반영되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleSetExpiration()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>()
			{
				new() { Id = "rule1", Expiration = new LifecycleRuleExpiration(){ Days = 1 }, Filter = PrefixFilter("test1/"), Status = LifecycleRuleStatus.Enabled },
			};

			client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules });

			var key = "test1/a";
			client.PutObject(bucketName, key, body: key);

			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}
	}
}
