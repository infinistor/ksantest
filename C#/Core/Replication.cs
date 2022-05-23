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
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Amazon.S3;
using Amazon.S3.Model;
using Xunit;

namespace s3tests
{
	public class Replication : TestBase
	{
		public Replication(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_replication_set")]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Replication 설정이 되는지 확인(put/get/delete)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_replication_set()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(TargetBucketName, VersionStatus.Enabled);

			//룰 생성
			var Rule1 = new ReplicationRule
			{
				Id = "Rule1",
				Status = ReplicationRuleStatus.Enabled,
				Priority = 1,
				Filter = null,
				DeleteMarkerReplication = new DeleteMarkerReplication(){ Status = DeleteMarkerReplicationStatus.Disabled},
				Destination = new ReplicationDestination()
				{
					BucketArn = "arn:aws:s3:::" + TargetBucketName,
				},
			};

			ReplicationConfiguration Replication = new ReplicationConfiguration
			{
				Role = "arn:aws:iam::635518764071:role/awsreplicationtest",
			};
			Replication.Rules.Add(Rule1);
			Client.PutBucketReplication(SourceBucketName, Replication);

			var Response = Client.GetBucketReplication(SourceBucketName);
			Assert.Equal(Replication.ToString(), Response.Configuration.ToString());

			Client.DeleteBucketReplication(SourceBucketName);

			Response = Client.GetBucketReplication(SourceBucketName);
			Assert.Empty(Response.Configuration.Rules);
		}

		[Fact(DisplayName = "trest_replication_invalid_source_bucket_name")]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void trest_replication_invalid_source_bucket_name()
		{
			var SourceBucketName = GetNewBucketName();
			var TargetBucketName = GetNewBucketName();
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

			ReplicationConfiguration Replication = new ReplicationConfiguration
			{
				Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
		}


		[Fact(DisplayName = "trest_replication_invalid_source_bucket_versioning")]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void trest_replication_invalid_source_bucket_versioning()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName();
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

			ReplicationConfiguration Replication = new ReplicationConfiguration
			{
				Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}


		[Fact(DisplayName = "trest_replication_invalid_target_bucket_name")]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void trest_replication_invalid_target_bucket_name()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName();
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

			ReplicationConfiguration Replication = new ReplicationConfiguration
			{
				Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}


		[Fact(DisplayName = "trest_replication_invalid_target_bucket_versioning")]
		[Trait(MainData.Major, "Replication")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void trest_replication_invalid_target_bucket_versioning()
		{
			var SourceBucketName = GetNewBucket();
			var TargetBucketName = GetNewBucketName();
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

			ReplicationConfiguration Replication = new ReplicationConfiguration
			{
				Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
			};
			Replication.Rules.Add(Rule1);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketReplication(SourceBucketName, Replication));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}
	}
}
