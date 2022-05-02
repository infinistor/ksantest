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
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
	public class Lifecycle : TestBase
	{
		public Lifecycle(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_lifecycle_set")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle 규칙을 추가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ Days = 1 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
				new LifecycleRule()
				{
					Id = "rule2",
					Expiration = new LifecycleRuleExpiration(){ Days = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test2/" } },
					Status = LifecycleRuleStatus.Disabled,
				},
			};
			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };

			var Response = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_get")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷에 설정한 Lifecycle 규칙을 가져올 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_get()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "test1/",
					Expiration = new LifecycleRuleExpiration(){ Days = 31 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
				new LifecycleRule()
				{
					Id = "test2/",
					Expiration = new LifecycleRuleExpiration(){ Days = 120 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test2/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};
			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };

			Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			var Response = Client.GetLifecycleConfiguration(BucketName);
			PrefixLifecycleConfigurationCheck(Rules, Response.Configuration.Rules);
		}

		[Fact(DisplayName = "test_lifecycle_get_no_id")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "Id 없이 버킷에 Lifecycle 규칙을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_get_no_id()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Expiration = new LifecycleRuleExpiration(){ Days = 31 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
				new LifecycleRule()
				{
					Expiration = new LifecycleRuleExpiration(){ Days = 120 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test2/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};
			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };

			Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			var Response = Client.GetLifecycleConfiguration(BucketName);
			var CurrentLifeCycle = Response.Configuration.Rules;

			for (int i = 0; i < Rules.Count; i++)
			{
				Assert.NotEmpty(CurrentLifeCycle[i].Id);
				Assert.Equal(Rules[i].Expiration.DateUtc, CurrentLifeCycle[i].Expiration.DateUtc);
				Assert.Equal(Rules[i].Expiration.Days, CurrentLifeCycle[i].Expiration.Days);
				Assert.Equal((Rules[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix,
							 (CurrentLifeCycle[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix);
				Assert.Equal(Rules[i].Status, CurrentLifeCycle[i].Status);
			}
		}

		[Fact(DisplayName = "test_lifecycle_expiration_versioning_enabled")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버킷에 버저닝 설정이 되어있는 상태에서 Lifecycle 규칙을 추가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_expiration_versioning_enabled()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "test1/a";
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			SetupMultipleVersion(Client, BucketName, Key, 1);
			Client.DeleteObject(BucketName, Key);

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ Days = 1 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "expire1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};
			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			Client.PutLifecycleConfiguration(BucketName, LifeCycle);

			var Response = Client.ListVersions(BucketName);
			var Versions = GetVersions(Response.Versions);
			var DeleteMarkers = GetDeleteMarkers(Response.Versions);
			Assert.Single(Versions);
			Assert.Single(DeleteMarkers);
		}

		[Fact(DisplayName = "test_lifecycle_id_too_long")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷에 Lifecycle 규칙을 설정할때 Id의 길이가 너무 길면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_lifecycle_id_too_long()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = RandomTextToLong(256),
					Expiration = new LifecycleRuleExpiration(){ Days = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "tset1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var e = Assert.Throws<AggregateException>(() => Client.PutLifecycleConfiguration(BucketName, LifeCycle));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.InvalidArgument, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_lifecycle_same_id")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Duplicate")]
		[Trait(MainData.Explanation, "버킷에 Lifecycle 규칙을 설정할때 같은 Id로 규칙을 여러개 설정할경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_lifecycle_same_id()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ Days = 1 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "tset1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ Days = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "tset2/" } },
					Status = LifecycleRuleStatus.Disabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var e = Assert.Throws<AggregateException>(() => Client.PutLifecycleConfiguration(BucketName, LifeCycle));
			var ErrorCode = GetErrorCode(e);
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			if (Config.S3.IsAWS) Assert.Equal(MainData.InvalidArgument, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_lifecycle_invalid_status")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷에 Lifecycle 규칙중 status를 잘못 설정할때 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_lifecycle_invalid_status()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ Days = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "tset1/" } },
					Status = new LifecycleRuleStatus("invalid"),
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var e = Assert.Throws<AggregateException>(() => Client.PutLifecycleConfiguration(BucketName, LifeCycle));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MalformedXML, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_lifecycle_set_date")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Date")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle규칙에 날짜를 입력가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set_date()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ DateUtc = DateTime.Parse("2099-10-10 00:00:00 GMT") },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "tset1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var Response = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_set_invalid_date")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle규칙에 날짜를 올바르지 않은 형식으로 입력했을때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_lifecycle_set_invalid_date()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ DateUtc = DateTime.Parse("2017-09-27") },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "tset1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var e = Assert.Throws<AggregateException>(() => Client.PutLifecycleConfiguration(BucketName, LifeCycle));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}

		[Fact(DisplayName = "test_lifecycle_set_noncurrent")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버킷의 버저닝설정이 없는 환경에서 버전관리용 Lifecycle이 올바르게 설정되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set_noncurrent()
		{
			var BucketName = SetupObjects(new List<string>() { "past/foo", "future/bar" });
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "past/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
				new LifecycleRule()
				{
					Id = "rule2",
					NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 3 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "futrue/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var Response = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_noncur_expiration")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버킷의 버저닝설정이 되어있는 환경에서 Lifecycle 이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_noncur_expiration()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			SetupMultipleVersion(Client, BucketName, "test1/a", 3);
			SetupMultipleVersion(Client, BucketName, "test2/abc", 3, false);

			var Response = Client.ListVersions(BucketName);
			var InitVersions = Response.Versions;

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var PutResponse = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);
			Assert.Equal(6, InitVersions.Count);

			//Thread.Sleep(50000);
			//
			//Response = Client.ListVersions(BucketName);
			//var ExpireVersions = Response.Versions;
			//Assert.Equal(4, ExpireVersions.Count);
		}

		[Fact(DisplayName = "test_lifecycle_set_deletemarker")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Delete Marker")]
		[Trait(MainData.Explanation, "DeleteMarker에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set_deletemarker()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ ExpiredObjectDeleteMarker = true },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var PutResponse = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_set_filter")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "Lifecycle 규칙에 필터링값을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set_filter()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ ExpiredObjectDeleteMarker = true },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "foo" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var PutResponse = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_set_empty_filter")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "Lifecycle 규칙에 필터링에 비어있는 값을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set_empty_filter()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ ExpiredObjectDeleteMarker = true },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var PutResponse = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_deletemarker_expiration")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Delete Marker")]
		[Trait(MainData.Explanation, "DeleteMarker에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_deletemarker_expiration()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			SetupMultipleVersion(Client, BucketName, "test1/a", 1);
			SetupMultipleVersion(Client, BucketName, "test2/abc", 1, false);
			Client.DeleteObject(BucketName, "test1/a");
			Client.DeleteObject(BucketName, "test2/abc");

			var Response = Client.ListVersions(BucketName);
			var TotalVersions = Response.Versions;

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 1 },
					Expiration = new LifecycleRuleExpiration() { ExpiredObjectDeleteMarker = true },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var PutResponse = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);
			Assert.Equal(4, TotalVersions.Count);

			//Thread.Sleep(50000);
			//
			//Response = Client.ListVersions(BucketName);
			//var TotalExpireVersions = Response.Versions;
			//Assert.Equal(2, TotalExpireVersions.Count);
		}

		[Fact(DisplayName = "test_lifecycle_set_multipart")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set_multipart()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
					AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload(){ DaysAfterInitiation = 2},
				},
				new LifecycleRule()
				{
					Id = "rule2",
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test2/" } },
					Status = LifecycleRuleStatus.Disabled,
					AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload(){ DaysAfterInitiation = 3},
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			var PutResponse = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, PutResponse.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_multipart_expiration")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_multipart_expiration()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var KeyNames = new List<string>() { "test1/a", "test2/b" };
			var UploadIds = new List<string>();

			foreach (var Key in KeyNames)
			{
				var Response = Client.InitiateMultipartUpload(BucketName, Key);
				UploadIds.Add(Response.UploadId);
			}

			var ListResponse = Client.ListMultipartUploads(BucketName);
			var InitUploads = ListResponse.MultipartUploads;

			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
					AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload(){ DaysAfterInitiation = 2},
				},
			};

			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };
			Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(2, InitUploads.Count);
		}

		[Fact(DisplayName = "test_lifecycle_delete")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷의 Lifecycle 규칙을 삭제 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_delete()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){ Days = 1 },
					NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration() { NoncurrentDays = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } },
					Status = LifecycleRuleStatus.Enabled,
				},
				new LifecycleRule()
				{
					Id = "rule2",
					Expiration = new LifecycleRuleExpiration(){ Days = 2 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test2/" } },
					Status = LifecycleRuleStatus.Disabled,
				},
			};
			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };

			var Response = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var DelResponse = Client.DeleteLifecycleConfiguration(BucketName);
			Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);
		}

		[Fact(DisplayName = "test_lifecycle_set_and", Skip = "테스트 규격이 확정되지 않음")]
		[Trait(MainData.Major, "Lifecycle")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷에 다양한 Lifecycle 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_lifecycle_set_and()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Rules = new List<LifecycleRule>()
			{
				new LifecycleRule()
				{
					Id = "rule1",
					Expiration = new LifecycleRuleExpiration(){
						Days = 31, //31뒤에 삭제
                        ExpiredObjectDeleteMarker = true}, // Object의 모든 버전이 삭제되고 DeleteMarker만 남았을 경우 삭제
                    AbortIncompleteMultipartUpload = new LifecycleRuleAbortIncompleteMultipartUpload(){ DaysAfterInitiation = 2},// Multipart의 유예시간을 2일로 설정
                    Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test1/" } }, // Object명이 test1/ 으로 시작할 경우에만 동작
                    Status = LifecycleRuleStatus.Enabled,
				},
				new LifecycleRule()
				{
					Id = "rule2",
					Expiration = new LifecycleRuleExpiration(){ Days = 31 },
					Filter = new LifecycleFilter(){ LifecycleFilterPredicate = new LifecyclePrefixPredicate() { Prefix = "test2/" } },
					Status = LifecycleRuleStatus.Disabled,
				},
			};
			var LifeCycle = new LifecycleConfiguration() { Rules = Rules };

			var Response = Client.PutLifecycleConfiguration(BucketName, LifeCycle);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
		}
	}
}
