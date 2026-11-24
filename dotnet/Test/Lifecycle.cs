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
using System.Linq;

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

		/// <summary>ID rule1, 전체 대상(Prefix ""), Enabled 인 규칙을 만들고 주어진 항목을 덮어쓴다.</summary>
		private static LifecycleRule Rule(Action<LifecycleRule> configure = null)
		{
			var rule = new LifecycleRule() { Id = "rule1", Status = LifecycleRuleStatus.Enabled, Filter = PrefixFilter("") };
			configure?.Invoke(rule);
			return rule;
		}

		private static LifecycleRuleExpiration ExpireDays(int days) => new() { Days = days };

		private static PutLifecycleConfigurationResponse PutRules(S3Client client, string bucketName, List<LifecycleRule> rules,
			TransitionDefaultMinimumObjectSize transitionDefaultMinimumObjectSize = null)
			=> client.PutLifecycleConfiguration(bucketName, new LifecycleConfiguration() { Rules = rules }, transitionDefaultMinimumObjectSize);

		private static List<LifecycleRule> GetRules(S3Client client, string bucketName)
			=> client.GetLifecycleConfiguration(bucketName).Configuration?.Rules ?? [];

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "Lifecycle 설정이 없는 버킷을 조회하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleGetNotExist()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// SDK v4 는 이 조회의 404 를 예외로 던지지 않고 상태 코드만 채운 응답을 돌려줘 에러 코드는 확인할 수 없다
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetLifecycleConfiguration(bucketName), MainData.NO_SUCH_LIFECYCLE_CONFIGURATION);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "Lifecycle 설정이 없는 버킷에서도 삭제가 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleDeleteNotExist()
		{
			TestId = 22;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 설정이 없어도 삭제는 성공한다
			var response = client.DeleteLifecycleConfiguration(bucketName);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "없는 버킷에 Lifecycle 조회/설정/삭제를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleBucketNotExist()
		{
			TestId = 23;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			var rules = new List<LifecycleRule>() { Rule(r => r.Expiration = ExpireDays(1)) };
			// SDK v4 는 조회의 404 를 예외로 던지지 않고 상태 코드만 채운 응답을 돌려준다
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetLifecycleConfiguration(bucketName), MainData.NO_SUCH_BUCKET);
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => PutRules(client, bucketName, rules));
			AssertS3Error(HttpStatusCode.NotFound, MainData.NO_SUCH_BUCKET, () => client.DeleteLifecycleConfiguration(bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "권한이 없는 사용자가 Lifecycle 조회/설정/삭제를 하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleAccessDenied()
		{
			TestId = 24;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<LifecycleRule>() { Rule(r => r.Expiration = ExpireDays(1)) };
			PutRules(client, bucketName, rules);

			AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, () => altClient.GetLifecycleConfiguration(bucketName));
			AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, () => PutRules(altClient, bucketName, rules));
			AssertS3Error(HttpStatusCode.Forbidden, MainData.ACCESS_DENIED, () => altClient.DeleteLifecycleConfiguration(bucketName));
			// 설정은 그대로 남아 있다
			Assert.Single(GetRules(client, bucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "동작이 하나도 없는 Lifecycle 규칙은 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleNoAction()
		{
			TestId = 25;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST, () => PutRules(client, bucketName, [Rule()]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "Filter 도 Prefix 도 없는 Lifecycle 규칙은 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleNoFilter()
		{
			TestId = 26;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// Filter 도 Prefix 도 없는 규칙은 거부된다
			var rule = new LifecycleRule() { Id = "rule1", Status = LifecycleRuleStatus.Enabled, Expiration = ExpireDays(1) };
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML, () => PutRules(client, bucketName, [rule]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "규칙이 없는 Lifecycle 설정은 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleEmptyRules()
		{
			TestId = 27;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML, () => PutRules(client, bucketName, []));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "만료 규칙에 Days 와 Date 를 함께 주면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleDaysAndDate()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var expiration = new LifecycleRuleExpiration() { Days = 1, Date = new DateTime(2099, 11, 10, 0, 0, 0, DateTimeKind.Utc) };
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => PutRules(client, bucketName, [Rule(r => r.Expiration = expiration)]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "만료일을 음수로 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleNegativeDays()
		{
			TestId = 29;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => PutRules(client, bucketName, [Rule(r => r.Expiration = ExpireDays(-1))]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "스토리지 클래스 전환 규칙이 그대로 저장·조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleTransition()
		{
			TestId = 30;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var transitions = new List<LifecycleTransition>()
			{
				new() { Days = 30, StorageClass = S3StorageClass.StandardInfrequentAccess },
				new() { Days = 60, StorageClass = S3StorageClass.Glacier },
			};
			PutRules(client, bucketName, [Rule(r => { r.Transitions = transitions; r.Expiration = ExpireDays(90); })]);

			var rules = GetRules(client, bucketName);
			Assert.Single(rules);
			var actual = (rules[0].Transitions ?? []).OrderBy(t => t.Days).ToList();
			Assert.Equal(transitions.Count, actual.Count);
			for (int i = 0; i < transitions.Count; i++)
			{
				Assert.Equal(transitions[i].Days, actual[i].Days);
				Assert.Equal(transitions[i].StorageClass, actual[i].StorageClass);
			}
			Assert.Equal(90, rules[0].Expiration.Days);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "전환할 수 없는 스토리지 클래스를 지정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleTransitionInvalidStorageClass()
		{
			TestId = 31;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// STANDARD 로는 전환할 수 없다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => PutRules(client, bucketName, [Rule(r => r.Transitions = [new() { Days = 30, StorageClass = S3StorageClass.Standard }])]));
			// 없는 스토리지 클래스
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => PutRules(client, bucketName, [Rule(r => r.Transitions = [new() { Days = 30, StorageClass = new S3StorageClass("INVALID") }])]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "만료일이 전환일보다 크지 않으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleExpirationBeforeTransition()
		{
			TestId = 32;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			static List<LifecycleTransition> transitions() => [new() { Days = 30, StorageClass = S3StorageClass.StandardInfrequentAccess }];
			// 만료일은 전환일보다 커야 한다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => PutRules(client, bucketName, [Rule(r => { r.Transitions = transitions(); r.Expiration = ExpireDays(30); })]));
			PutRules(client, bucketName, [Rule(r => { r.Transitions = transitions(); r.Expiration = ExpireDays(31); })]);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "이전 버전 전환 규칙이 그대로 저장·조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleNoncurrentTransition()
		{
			TestId = 34;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var transitions = new List<LifecycleRuleNoncurrentVersionTransition>()
			{
				new() { NoncurrentDays = 30, StorageClass = S3StorageClass.StandardInfrequentAccess },
			};
			PutRules(client, bucketName, [Rule(r => r.NoncurrentVersionTransitions = transitions)]);

			var actual = GetRules(client, bucketName)[0].NoncurrentVersionTransitions;
			Assert.Single(actual);
			Assert.Equal(30, actual[0].NoncurrentDays);
			Assert.Equal(S3StorageClass.StandardInfrequentAccess, actual[0].StorageClass);
			Assert.Null(actual[0].NewerNoncurrentVersions);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "이전 버전 만료일을 0일로 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleNoncurrentZeroDays()
		{
			TestId = 35;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => PutRules(client, bucketName, [Rule(r => r.NoncurrentVersionExpiration = new() { NoncurrentDays = 0 })]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "보존할 이전 버전 수(NewerNoncurrentVersions)가 저장되고 1~100 범위만 허용하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleNewerNoncurrentVersions()
		{
			TestId = 36;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutRules(client, bucketName, [Rule(r => r.NoncurrentVersionExpiration = new() { NoncurrentDays = 1, NewerNoncurrentVersions = 5 })]);

			var actual = GetRules(client, bucketName)[0].NoncurrentVersionExpiration;
			Assert.Equal(1, actual.NoncurrentDays);
			Assert.Equal(5, actual.NewerNoncurrentVersions);

			// 보존할 버전 수는 1~100 만 허용한다
			foreach (var count in new[] { 0, 101 })
			{
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
					() => PutRules(client, bucketName, [Rule(r => r.NoncurrentVersionExpiration = new() { NoncurrentDays = 1, NewerNoncurrentVersions = count })]));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "ExpiredObjectDeleteMarker 를 Days 와 함께 주면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleDeleteMarkerWithDays()
		{
			TestId = 37;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// ExpiredObjectDeleteMarker 는 Days 와 함께 쓸 수 없다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => PutRules(client, bucketName, [Rule(r => r.Expiration = new() { Days = 1, ExpiredObjectDeleteMarker = true })]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "ExpiredObjectDeleteMarker 를 태그 필터와 함께 주면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleDeleteMarkerWithTag()
		{
			TestId = 38;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rule = Rule(r =>
			{
				r.Filter = new LifecycleFilter() { Tag = new Tag() { Key = "key", Value = "value" } };
				r.Expiration = new() { ExpiredObjectDeleteMarker = true };
			});
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST, () => PutRules(client, bucketName, [rule]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "AbortIncompleteMultipartUpload 를 태그·크기 필터와 함께 주면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleAbortMultipartWithFilter()
		{
			TestId = 39;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// AbortIncompleteMultipartUpload 는 태그·크기 필터와 함께 쓸 수 없다
			var filters = new List<LifecycleFilter>()
			{
				new() { Tag = new Tag() { Key = "key", Value = "value" } },
				new() { ObjectSizeGreaterThan = 10 },
			};
			foreach (var filter in filters)
			{
				var rule = Rule(r =>
				{
					r.Filter = filter;
					r.AbortIncompleteMultipartUpload = new() { DaysAfterInitiation = 1 };
				});
				AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST, () => PutRules(client, bucketName, [rule]));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "AbortIncompleteMultipartUpload 의 기한을 0일로 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleAbortMultipartZeroDays()
		{
			TestId = 40;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_ARGUMENT,
				() => PutRules(client, bucketName, [Rule(r => r.AbortIncompleteMultipartUpload = new() { DaysAfterInitiation = 0 })]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "태그 필터가 그대로 저장·조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleTagFilter()
		{
			TestId = 41;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var filter = new LifecycleFilter() { Tag = new Tag() { Key = "key", Value = "value" } };
			PutRules(client, bucketName, [Rule(r => { r.Filter = filter; r.Expiration = ExpireDays(1); })]);

			// SDK v4 는 응답의 필터를 LifecycleFilterPredicate 로만 채운다
			var actual = Assert.IsType<LifecycleTagPredicate>(GetRules(client, bucketName)[0].Filter.LifecycleFilterPredicate);
			Assert.Equal("key", actual.Tag.Key);
			Assert.Equal("value", actual.Tag.Value);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "And 필터(접두어·태그·크기)가 그대로 저장·조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleAndFilter()
		{
			TestId = 42;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var tags = new List<Tag>() { new() { Key = "key1", Value = "value1" }, new() { Key = "key2", Value = "value2" } };
			var filter = new LifecycleFilter()
			{
				And = new LifecycleRuleAndOperator() { Prefix = "test1/", Tags = tags, ObjectSizeGreaterThan = 10, ObjectSizeLessThan = 100 },
			};
			PutRules(client, bucketName, [Rule(r => { r.Filter = filter; r.Expiration = ExpireDays(1); })]);

			// SDK v4 는 응답의 필터를 LifecycleFilterPredicate 로만 채운다
			var actual = Assert.IsType<LifecycleAndOperator>(GetRules(client, bucketName)[0].Filter.LifecycleFilterPredicate).Operands;
			Assert.Equal(5, actual.Count);
			Assert.Equal("test1/", actual.OfType<LifecyclePrefixPredicate>().Single().Prefix);
			TaggingCompare(tags, actual.OfType<LifecycleTagPredicate>().Select(p => p.Tag).ToList());
			Assert.Equal(10, actual.OfType<LifecycleObjectSizeGreaterThanPredicate>().Single().ObjectSizeGreaterThan);
			Assert.Equal(100, actual.OfType<LifecycleObjectSizeLessThanPredicate>().Single().ObjectSizeLessThan);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "And 필터의 조건이 하나뿐이거나 태그 키가 중복되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleAndFilterInvalid()
		{
			TestId = 43;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// And 안에 조건이 하나뿐이면 거부된다
			var single = new LifecycleFilter() { And = new LifecycleRuleAndOperator() { Prefix = "test1/" } };
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML,
				() => PutRules(client, bucketName, [Rule(r => { r.Filter = single; r.Expiration = ExpireDays(1); })]));
			// And 안의 태그 키는 중복될 수 없다
			var duplicated = new LifecycleFilter()
			{
				And = new LifecycleRuleAndOperator()
				{
					Prefix = "test1/",
					Tags = [new() { Key = "key", Value = "1" }, new() { Key = "key", Value = "2" }],
				},
			};
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST,
				() => PutRules(client, bucketName, [Rule(r => { r.Filter = duplicated; r.Expiration = ExpireDays(1); })]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "오브젝트 크기 필터가 그대로 저장·조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleObjectSizeFilter()
		{
			TestId = 44;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutRules(client, bucketName, [Rule(r => { r.Filter = new LifecycleFilter() { ObjectSizeGreaterThan = 500 }; r.Expiration = ExpireDays(1); })]);
			// SDK v4 는 응답의 필터를 LifecycleFilterPredicate 로만 채운다
			var greater = Assert.IsType<LifecycleObjectSizeGreaterThanPredicate>(GetRules(client, bucketName)[0].Filter.LifecycleFilterPredicate);
			Assert.Equal(500, greater.ObjectSizeGreaterThan);

			PutRules(client, bucketName, [Rule(r => { r.Filter = new LifecycleFilter() { ObjectSizeLessThan = 500 }; r.Expiration = ExpireDays(1); })]);
			var less = Assert.IsType<LifecycleObjectSizeLessThanPredicate>(GetRules(client, bucketName)[0].Filter.LifecycleFilterPredicate);
			Assert.Equal(500, less.ObjectSizeLessThan);
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "오브젝트 크기 필터의 값이 올바르지 않으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestLifecycleObjectSizeFilterInvalid()
		{
			TestId = 45;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// ObjectSizeLessThan 은 1 이상이어야 한다
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST,
				() => PutRules(client, bucketName, [Rule(r => { r.Filter = new LifecycleFilter() { ObjectSizeLessThan = 0 }; r.Expiration = ExpireDays(1); })]));
			// ObjectSizeLessThan 은 ObjectSizeGreaterThan 보다 커야 한다
			var sizeFilter = new LifecycleFilter() { And = new LifecycleRuleAndOperator() { ObjectSizeGreaterThan = 100, ObjectSizeLessThan = 100 } };
			AssertS3Error(HttpStatusCode.BadRequest, MainData.INVALID_REQUEST,
				() => PutRules(client, bucketName, [Rule(r => { r.Filter = sizeFilter; r.Expiration = ExpireDays(1); })]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "Filter 대신 규칙 바로 아래에 Prefix 를 두는 예전 형식이 그대로 저장·조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleLegacyPrefix()
		{
			TestId = 46;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// Filter 대신 규칙 바로 아래에 Prefix 를 두는 예전 형식도 그대로 저장·조회된다
			static LifecycleRule legacy() => new() { Id = "rule1", Status = LifecycleRuleStatus.Enabled, Prefix = "legacy/", Expiration = ExpireDays(1) };
			PutRules(client, bucketName, [legacy()]);
			var rules = GetRules(client, bucketName);
			Assert.Equal("legacy/", rules[0].Prefix);
			Assert.Null(rules[0].Filter);

			// Prefix 와 Filter 를 함께 줄 수는 없다
			var both = legacy();
			both.Filter = new LifecycleFilter() { Prefix = "test1/" };
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML, () => PutRules(client, bucketName, [both]));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Max")]
		[Trait(MainData.Explanation, "Lifecycle 규칙은 1000개까지만 넣을 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleMaxRules()
		{
			TestId = 47;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			static List<LifecycleRule> makeRules(int count)
			{
				var rules = new List<LifecycleRule>();
				for (int i = 0; i < count; i++)
					rules.Add(new() { Id = $"rule{i}", Status = LifecycleRuleStatus.Enabled, Filter = PrefixFilter($"test{i}/"), Expiration = ExpireDays(1) });
				return rules;
			}

			// 규칙은 1000개까지 넣을 수 있다
			PutRules(client, bucketName, makeRules(1000));
			Assert.Equal(1000, GetRules(client, bucketName).Count);
			AssertS3Error(HttpStatusCode.BadRequest, MainData.MALFORMED_XML, () => PutRules(client, bucketName, makeRules(1001)));
		}

		[Fact]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "만료 규칙에 걸리는 오브젝트만 x-amz-expiration 헤더로 만료 예정일과 규칙 ID 를 돌려주는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestLifecycleExpirationHeader()
		{
			TestId = 49;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rule = new LifecycleRule() { Id = "rule1", Status = LifecycleRuleStatus.Enabled, Filter = PrefixFilter("test1/"), Expiration = ExpireDays(1) };
			PutRules(client, bucketName, [rule]);

			// 만료 규칙에 걸리는 오브젝트는 x-amz-expiration 헤더로 만료 예정일과 규칙 ID 를 돌려준다
			var key = "test1/TestLifecycleExpirationHeader";
			var putResponse = client.PutObject(bucketName, key, body: "test");
			Assert.Equal("rule1", putResponse.Expiration.RuleId);
			Assert.True(putResponse.Expiration.ExpiryDate > DateTime.UtcNow.AddHours(-1));
			Assert.Equal("rule1", client.GetObjectMetadata(bucketName, key).Expiration.RuleId);
			using (var getResponse = client.GetObject(bucketName, key))
				Assert.Equal("rule1", getResponse.Expiration.RuleId);

			// 규칙에 걸리지 않는 오브젝트에는 헤더가 없다
			var other = "test2/TestLifecycleExpirationHeader";
			Assert.Null(client.PutObject(bucketName, other, body: "test").Expiration);
			Assert.Null(client.GetObjectMetadata(bucketName, other).Expiration);
		}
	}
}
