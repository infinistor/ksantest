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
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Amazon.S3;
using Amazon.S3.Model;
using Xunit;

namespace s3tests.Test
{
	public class Replication : TestBase
	{
		public Replication(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Replication 설정이 되는지 확인(put/get/delete)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationSet()
		{
			TestId = 1;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(TargetBucketName, VersionStatus.Enabled);

			//룰 생성
			// Priority/DeleteMarkerReplication을 쓰면 V2 스키마라 Filter가 필수다. null이면 AWS가 MalformedXML로 거부한다.
			var Rule1 = new ReplicationRule
			{
				Id = "Rule1",
				Status = ReplicationRuleStatus.Enabled,
				Priority = 1,
				Filter = new ReplicationRuleFilter() { Prefix = "test/" },
				DeleteMarkerReplication = new DeleteMarkerReplication() { Status = DeleteMarkerReplicationStatus.Disabled },
				Destination = new ReplicationDestination()
				{
					BucketArn = "arn:aws:s3:::" + TargetBucketName,
				},
			};

			ReplicationConfiguration Replication = new()
			{
				Role = ReplicationRole(),
			};
			Replication.Rules.Add(Rule1);
			Client.PutBucketReplication(SourceBucketName, Replication);

			var Response = Client.GetBucketReplication(SourceBucketName);
			Assert.Equal(Replication.ToString(), Response.Configuration.ToString());

			Client.DeleteBucketReplication(SourceBucketName);

			CheckErrorResponse(HttpStatusCode.NotFound, () => Client.GetBucketReplication(SourceBucketName));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationInvalidSourceBucketName()
		{
			TestId = 2;
			var SourceBucketName = GetNewBucketName(false);
			var TargetBucketName = GetNewBucketName(false);
			var Client = GetClient();

			//룰 생성
			var Rule1 = new ReplicationRule
			{
				Id = "Rule1",
				Priority = 1,
				Status = ReplicationRuleStatus.Enabled,
				Destination = new ReplicationDestination()
				{
					BucketArn = "arn:aws:s3:::" + TargetBucketName,
				},
			};

			ReplicationConfiguration Replication = new()
			{
				Role = ReplicationRole()
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
		}


		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationInvalidSourceBucketVersioning()
		{
			TestId = 3;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName(false);
			var Client = GetClient();

			//룰 생성
			var Rule1 = new ReplicationRule
			{
				Id = "Rule1",
				Priority = 1,
				Status = ReplicationRuleStatus.Enabled,
				Destination = new ReplicationDestination()
				{
					BucketArn = "arn:aws:s3:::" + TargetBucketName,
				},
			};

			ReplicationConfiguration Replication = new()
			{
				Role = ReplicationRole()
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}


		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationInvalidTargetBucketName()
		{
			TestId = 4;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName(false);
			var Client = GetClient();

			//룰 생성
			var Rule1 = new ReplicationRule
			{
				Id = "Rule1",
				Priority = 1,
				Status = ReplicationRuleStatus.Enabled,
				Destination = new ReplicationDestination()
				{
					BucketArn = "arn:aws:s3:::" + TargetBucketName,
				},
			};

			ReplicationConfiguration Replication = new()
			{
				Role = ReplicationRole()
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}


		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationInvalidTargetBucketVersioning()
		{
			TestId = 5;
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Client = GetClient();

			//룰 생성
			var Rule1 = new ReplicationRule
			{
				Id = "Rule1",
				Priority = 1,
				Status = ReplicationRuleStatus.Enabled,
				Destination = new ReplicationDestination()
				{
					BucketArn = "arn:aws:s3:::" + TargetBucketName,
				},
			};

			ReplicationConfiguration Replication = new()
			{
				Role = ReplicationRole()
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "복제 설정된 원본 버킷의 버저닝을 중단할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationBucketVersioningSuspend()
		{
			TestId = 6;
			var Prefix = "test/";
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Client = GetClient();

			//원본, 대상 버킷 버저닝 설정
			CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(TargetBucketName, VersionStatus.Enabled);

			//룰 생성
			var Rule1 = new ReplicationRule
			{
				Id = "Rule1",
				Status = ReplicationRuleStatus.Enabled,
				Priority = 1,
				Filter = new ReplicationRuleFilter() { Prefix = Prefix },
				DeleteMarkerReplication = new DeleteMarkerReplication() { Status = DeleteMarkerReplicationStatus.Disabled },
				Destination = new ReplicationDestination()
				{
					BucketArn = "arn:aws:s3:::" + TargetBucketName,
				},
			};

			ReplicationConfiguration Replication = new()
			{
				Role = ReplicationRole()
			};
			Replication.Rules.Add(Rule1);

			//원본 버킷 복제 설정
			Client.PutBucketReplication(SourceBucketName, Replication);

			//원본 버킷 버저닝 중단 실패 확인
			var e = Assert.Throws<AggregateException>(() => CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Suspended));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
			Assert.Equal(MainData.INVALID_BUCKET_STATE, GetErrorCode(e));
		}

		/// <summary>복제 역할 ARN. 계정 아이디는 설정의 AccountId(없으면 UserID)를 쓴다.</summary>
		private string ReplicationRole() => "arn:aws:iam::" + Config.MainUser.ExpectedOwnerId + ":role/replication";

		/// <summary>버저닝이 켜진 원본·대상 버킷을 만들고 (원본 버킷명, 대상 버킷 ARN)을 돌려준다.</summary>
		private (string SourceBucketName, string TargetBucketArn) SetupReplicationBuckets(S3Client client)
		{
			var sourceBucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(sourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(targetBucketName, VersionStatus.Enabled);
			return (sourceBucketName, "arn:aws:s3:::" + targetBucketName);
		}

		/// <summary>V2 스키마 기본 규칙(Priority 1, Prefix 필터 test/, 삭제 마커 복제 Disabled). 테스트에서 필요한 속성만 바꿔 쓴다.</summary>
		private static ReplicationRule NewReplicationRule(string targetBucketArn, string id = null, int priority = 1, string prefix = "test/")
		{
			var rule = new ReplicationRule
			{
				Status = ReplicationRuleStatus.Enabled,
				Priority = priority,
				Filter = new ReplicationRuleFilter() { Prefix = prefix },
				Destination = new ReplicationDestination() { BucketArn = targetBucketArn },
				DeleteMarkerReplication = new DeleteMarkerReplication() { Status = DeleteMarkerReplicationStatus.Disabled },
			};
			if (id != null) rule.Id = id;
			return rule;
		}

		private void PutReplication(S3Client client, string sourceBucketName, List<ReplicationRule> rules)
			=> client.PutBucketReplication(sourceBucketName, new ReplicationConfiguration() { Role = ReplicationRole(), Rules = rules });

		private static List<ReplicationRule> GetReplicationRules(S3Client client, string sourceBucketName)
			=> client.GetBucketReplication(sourceBucketName).Configuration.Rules;

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "복제 설정이 없는 버킷의 조회는 실패하고 삭제는 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationGetNotExist()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// SDK v4는 이 오퍼레이션의 404를 예외로 던지지 않고 HttpStatusCode만 채워 돌려주기도 한다(이때 에러 코드는 확인 불가)
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketReplication(bucketName),
				MainData.REPLICATION_CONFIGURATION_NOT_FOUND_ERROR);

			// 설정이 없어도 삭제는 성공한다
			client.DeleteBucketReplication(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "태그 필터를 가진 복제 규칙이 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationTagFilter()
		{
			TestId = 8;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			var rule = NewReplicationRule(targetBucketArn);
			rule.Filter = new ReplicationRuleFilter() { Tag = new Tag() { Key = "key1", Value = "value1" } };
			PutReplication(client, sourceBucketName, [rule]);

			var rules = GetReplicationRules(client, sourceBucketName);
			Assert.Equal("key1", rules[0].Filter.Tag.Key);
			Assert.Equal("value1", rules[0].Filter.Tag.Value);
			Assert.Null(rules[0].Filter.Prefix);
			Assert.Null(rules[0].Filter.And);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "접두어와 태그를 함께 쓰는 And 필터 복제 규칙이 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationAndFilter()
		{
			TestId = 9;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			var rule = NewReplicationRule(targetBucketArn);
			rule.Filter = new ReplicationRuleFilter()
			{
				And = new ReplicationRuleAndOperator()
				{
					Prefix = "test/",
					Tags =
					[
						new Tag() { Key = "key1", Value = "value1" },
						new Tag() { Key = "key2", Value = "value2" },
					],
				},
			};
			PutReplication(client, sourceBucketName, [rule]);

			var rules = GetReplicationRules(client, sourceBucketName);
			var and = rules[0].Filter.And;
			Assert.Equal("test/", and.Prefix);
			Assert.Equal(2, and.Tags.Count);
			Assert.Equal("key1", and.Tags[0].Key);
			Assert.Equal("value1", and.Tags[0].Value);
			Assert.Equal("key2", and.Tags[1].Key);
			Assert.Equal("value2", and.Tags[1].Value);
			Assert.Null(rules[0].Filter.Prefix);
			Assert.Null(rules[0].Filter.Tag);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "빈 접두어 필터(버킷 전체 대상)를 가진 복제 규칙이 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationEmptyFilter()
		{
			TestId = 10;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			// 빈 접두어는 버킷 전체를 대상으로 한다
			PutReplication(client, sourceBucketName, [NewReplicationRule(targetBucketArn, prefix: "")]);

			var rules = GetReplicationRules(client, sourceBucketName);
			Assert.NotNull(rules[0].Filter);
			Assert.Equal("", rules[0].Filter.Prefix ?? "");
			Assert.Null(rules[0].Filter.Tag);
			Assert.Null(rules[0].Filter.And);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "구버전(V1) 스키마(규칙 직속 Prefix)의 복제 규칙이 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationV1Schema()
		{
			TestId = 11;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			// 구버전(V1) 스키마: Filter/Priority/DeleteMarkerReplication 없이 규칙 직속 Prefix 를 쓴다
			var rule = new ReplicationRule
			{
				Id = "v1-rule",
				Status = ReplicationRuleStatus.Enabled,
				Prefix = "v1/",
				Destination = new ReplicationDestination() { BucketArn = targetBucketArn },
			};
			PutReplication(client, sourceBucketName, [rule]);

			var rules = GetReplicationRules(client, sourceBucketName);
			Assert.Equal("v1-rule", rules[0].Id);
			Assert.Equal("v1/", rules[0].Prefix);
			Assert.Equal(ReplicationRuleStatus.Enabled, rules[0].Status);
			Assert.Equal(targetBucketArn, rules[0].Destination.BucketArn);
			Assert.Null(rules[0].Filter);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "여러 복제 규칙이 순서대로 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationMultipleRules()
		{
			TestId = 12;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			PutReplication(client, sourceBucketName,
			[
				NewReplicationRule(targetBucketArn, id: "rule-1", priority: 1, prefix: "a/"),
				NewReplicationRule(targetBucketArn, id: "rule-2", priority: 2, prefix: "b/"),
			]);

			var rules = GetReplicationRules(client, sourceBucketName);
			Assert.Equal(2, rules.Count);
			Assert.Equal("rule-1", rules[0].Id);
			Assert.Equal(1, rules[0].Priority);
			Assert.Equal("a/", rules[0].Filter.Prefix);
			Assert.Equal("rule-2", rules[1].Id);
			Assert.Equal(2, rules[1].Priority);
			Assert.Equal("b/", rules[1].Filter.Prefix);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "복제 규칙의 Priority 가 중복되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationDuplicatePriority()
		{
			TestId = 13;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			var e = Assert.Throws<AggregateException>(() => PutReplication(client, sourceBucketName,
			[
				NewReplicationRule(targetBucketArn, id: "rule-1", priority: 1, prefix: "a/"),
				NewReplicationRule(targetBucketArn, id: "rule-2", priority: 1, prefix: "b/"),
			]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "복제 규칙의 ID 가 중복되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationDuplicateId()
		{
			TestId = 14;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			var e = Assert.Throws<AggregateException>(() => PutReplication(client, sourceBucketName,
			[
				NewReplicationRule(targetBucketArn, id: "rule-1", priority: 1, prefix: "a/"),
				NewReplicationRule(targetBucketArn, id: "rule-1", priority: 2, prefix: "b/"),
			]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "Filter 를 쓰는 복제 규칙에 Priority 나 DeleteMarkerReplication 이 없으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 당분간 유지): Filter 가 있는데 Priority 나 DeleteMarkerReplication 이 없어도 받는다.
		// 이전부터 받아 온 형태라 검증을 넣으면 기존 설정이 거부될 수 있어 일단 넣지 않는다. AWS 는 400 InvalidRequest 다.
		public void TestReplicationFilterWithoutPriority()
		{
			TestId = 15;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			// Filter 를 쓰는 V2 스키마에서는 Priority 와 DeleteMarkerReplication 이 필수다
			var noPriority = NewReplicationRule(targetBucketArn);
			noPriority.Priority = null;
			var e = Assert.Throws<AggregateException>(() => PutReplication(client, sourceBucketName, [noPriority]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));

			var noDeleteMarker = NewReplicationRule(targetBucketArn);
			noDeleteMarker.DeleteMarkerReplication = null;
			e = Assert.Throws<AggregateException>(() => PutReplication(client, sourceBucketName, [noDeleteMarker]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "DeleteMarker")]
		[Trait(MainData.Explanation, "삭제 마커 복제를 켠 복제 규칙이 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationDeleteMarkerEnabled()
		{
			TestId = 16;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			var rule = NewReplicationRule(targetBucketArn);
			rule.DeleteMarkerReplication = new DeleteMarkerReplication() { Status = DeleteMarkerReplicationStatus.Enabled };
			PutReplication(client, sourceBucketName, [rule]);

			var rules = GetReplicationRules(client, sourceBucketName);
			Assert.Equal(DeleteMarkerReplicationStatus.Enabled, rules[0].DeleteMarkerReplication.Status);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "태그 필터가 있는 복제 규칙에서 삭제 마커 복제를 켜면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationDeleteMarkerWithTagFilter()
		{
			TestId = 17;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			// 태그 필터가 있으면 삭제 마커 복제를 켤 수 없다
			var rule = NewReplicationRule(targetBucketArn);
			rule.Filter = new ReplicationRuleFilter() { Tag = new Tag() { Key = "key1", Value = "value1" } };
			rule.DeleteMarkerReplication = new DeleteMarkerReplication() { Status = DeleteMarkerReplicationStatus.Enabled };

			var e = Assert.Throws<AggregateException>(() => PutReplication(client, sourceBucketName, [rule]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "Disabled 상태의 복제 규칙이 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestReplicationDisabledRule()
		{
			TestId = 18;
			var client = GetClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);

			var rule = NewReplicationRule(targetBucketArn);
			rule.Status = ReplicationRuleStatus.Disabled;
			PutReplication(client, sourceBucketName, [rule]);

			var rules = GetReplicationRules(client, sourceBucketName);
			Assert.Equal(ReplicationRuleStatus.Disabled, rules[0].Status);
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "원본 버킷 자신을 복제 대상으로 지정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationSameBucket()
		{
			TestId = 20;
			var client = GetClient();
			var sourceBucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(sourceBucketName, VersionStatus.Enabled);

			var e = Assert.Throws<AggregateException>(() => PutReplication(client, sourceBucketName,
				[NewReplicationRule("arn:aws:s3:::" + sourceBucketName)]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "복제 대상 버킷을 ARN 형식이 아닌 이름으로 지정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationInvalidDestinationArn()
		{
			TestId = 21;
			var client = GetClient();
			var sourceBucketName = GetNewBucket(client);
			var targetBucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(sourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(targetBucketName, VersionStatus.Enabled);

			// 대상 버킷은 ARN 형식이어야 한다
			var e = Assert.Throws<AggregateException>(() => PutReplication(client, sourceBucketName,
				[NewReplicationRule(targetBucketName)]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 복제 설정 조회, 삭제가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationBucketNotExist()
		{
			TestId = 22;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			// SDK v4는 이 오퍼레이션의 404를 예외로 던지지 않고 HttpStatusCode만 채워 돌려주기도 한다(이때 에러 코드는 확인 불가)
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketReplication(bucketName), MainData.NO_SUCH_BUCKET);

			var e = Assert.Throws<AggregateException>(() => client.DeleteBucketReplication(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "권한이 없는 사용자의 복제 설정 조회, 저장, 삭제가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestReplicationAltUser()
		{
			TestId = 23;
			var client = GetClient();
			var altClient = GetAltClient();
			var (sourceBucketName, targetBucketArn) = SetupReplicationBuckets(client);
			PutReplication(client, sourceBucketName, [NewReplicationRule(targetBucketArn)]);

			var e = Assert.Throws<AggregateException>(() => altClient.GetBucketReplication(sourceBucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => PutReplication(altClient, sourceBucketName, [NewReplicationRule(targetBucketArn)]));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => altClient.DeleteBucketReplication(sourceBucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}
	}
}
