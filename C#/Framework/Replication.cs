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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace s3tests2
{
    [TestClass]
    public class Replication : TestBase
    {
        [TestMethod("test_replication_set")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "버킷의 Replication 설정이 되는지 확인(put/get/delete)")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
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
            Client.PutBucketReplication(SourceBucketName, Replication);

            var Response = Client.GetBucketReplication(SourceBucketName);
            Assert.AreEqual(Replication.ToString(), Response.Configuration.ToString());

            Client.DeleteBucketReplication(SourceBucketName);

            Response = Client.GetBucketReplication(SourceBucketName);
            Assert.AreEqual(0, Response.Configuration.Rules.Count);
        }


        [TestMethod("test_replication_no_rule")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "복제설정중 role이 없어도 설정되는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_replication_no_rule()
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
                Priority = 1,
                Status = ReplicationRuleStatus.Enabled,
                Destination = new ReplicationDestination()
                {
                    BucketArn = "arn:aws:s3:::" + TargetBucketName,
                },
            };

            ReplicationConfiguration Replication = new ReplicationConfiguration
            {
                Role = ""
            };
            Replication.Rules.Add(Rule1);
            Client.PutBucketReplication(SourceBucketName, Replication);

            var Response = Client.GetBucketReplication(SourceBucketName);
            Assert.AreEqual(Replication.ToString(), Response.Configuration.ToString());
        }


        [TestMethod("test_replication_full_copy")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "버킷의 복제설정이 올바르게 동작하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_replication_full_copy()
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
            Client.PutBucketReplication(SourceBucketName, Replication);

            var Response = Client.GetBucketReplication(SourceBucketName);
            Assert.AreEqual(Replication.ToString(), Response.Configuration.ToString());

            Thread.Sleep(5000);

            // 3개의 오브젝트 업로드
            var SourceKeys = new List<string>();
            for (int i = 0; i < 3; i++)
            {
                var KeyName = string.Format("test{0}", i);

                // 2개의 버전정보 생성
                for (int j = 0; j < 2; j++) Client.PutObject(SourceBucketName, KeyName, RandomTextToLong(100));
                SourceKeys.Add(KeyName);
            }

            Thread.Sleep(5000);

            // 검증
            var TargetResource = Client.ListObjects(TargetBucketName);
            var TargetKeys = GetKeys(TargetResource);
            CollectionAssert.AreEqual(SourceKeys, TargetKeys);

            var SourceVersionsResource = Client.ListVersions(SourceBucketName);
            var TargetVersionsResource = Client.ListVersions(TargetBucketName);
            var SourceVerions = GetVersions(SourceVersionsResource.Versions);
            var TargetVerions = GetVersions(TargetVersionsResource.Versions);

            VersionIdsCompare(SourceVerions, TargetVerions);
        }


        [TestMethod("test_replication_prefix_copy")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "버킷의 복제 설정중 prefix가 올바르게 동작하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_replication_prefix_copy()
        {
            var SourceBucketName = GetNewBucket();
            var TargetBucketName = GetNewBucket();
            var Client = GetClient();
            var Prefix = "test1/";

            CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Enabled);
            CheckConfigureVersioningRetry(TargetBucketName, VersionStatus.Enabled);

            //룰 생성
            var Rule1 = new ReplicationRule
            {
                Id = "Rule1",
                Priority = 1,
                Status = ReplicationRuleStatus.Enabled,
                Filter = new ReplicationRuleFilter()
                {
                    Prefix = Prefix,
                },
                Destination = new ReplicationDestination()
                {
                    BucketArn = "arn:aws:s3:::" + TargetBucketName
                },
            };

            ReplicationConfiguration Replication = new ReplicationConfiguration
            {
                Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
            };
            Replication.Rules.Add(Rule1);
            Client.PutBucketReplication(SourceBucketName, Replication);

            var Response = Client.GetBucketReplication(SourceBucketName);
            Assert.AreEqual(Replication.ToString(), Response.Configuration.ToString());

            Thread.Sleep(5000);

            // 3개의 폴더 생성
            for (int i = 0; i < 3; i++)
            {
                var Dir = string.Format("test{0}", i);

                // 5개의 오브젝트 업로드
                for (int j = 0; j < 3; j++)
                {
                    var KeyName = string.Format("{0}/{1}", Dir, RandomText(10));
                    Client.PutObject(SourceBucketName, KeyName, RandomTextToLong(100));
                }
            }

            Thread.Sleep(5000);

            // 검증
            var SourceResource = Client.ListObjects(SourceBucketName, Prefix: Prefix);
            var TargetResource = Client.ListObjects(TargetBucketName);
            var SourceKeys = GetKeys(SourceResource);
            var TargetKeys = GetKeys(TargetResource);
            CollectionAssert.AreEqual(SourceKeys, TargetKeys);

            var SourceVersionsResource = Client.ListVersions(SourceBucketName, Prefix);
            var TargetVersionsResource = Client.ListVersions(TargetBucketName);
            var SourceVerions = GetVersions(SourceVersionsResource.Versions);
            var TargetVerions = GetVersions(TargetVersionsResource.Versions);

            VersionIdsCompare(SourceVerions, TargetVerions);
        }


        [TestMethod("test_replication_deletemarker_copy")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "Check")]
        [TestProperty(MainData.Explanation, "버킷의 복제 설정중 DeleteMarker가 올바르게 동작하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultSuccess)]
        public void test_replication_deletemarker_copy()
        {
            var SourceBucketName = GetNewBucket();
            var Target1BucketName = GetNewBucket();
            var Target2BucketName = GetNewBucket();
            var Client = GetClient();

            CheckConfigureVersioningRetry(SourceBucketName, VersionStatus.Enabled);
            CheckConfigureVersioningRetry(Target1BucketName, VersionStatus.Enabled);
            CheckConfigureVersioningRetry(Target2BucketName, VersionStatus.Enabled);

            //룰 생성
            var Rule1 = new ReplicationRule
            {
                Id = "Rule1",
                Priority = 1,
                Status = ReplicationRuleStatus.Enabled,
                Destination = new ReplicationDestination()
                {
                    BucketArn = "arn:aws:s3:::" + Target1BucketName
                },
            };
            var Rule2 = new ReplicationRule
            {
                Id = "Rule2",
                Priority = 2,
                Status = ReplicationRuleStatus.Enabled,
                Destination = new ReplicationDestination()
                {
                    BucketArn = "arn:aws:s3:::" + Target2BucketName
                },
                DeleteMarkerReplication = new DeleteMarkerReplication() { Status = DeleteMarkerReplicationStatus.Enabled },
            };

            ReplicationConfiguration Replication = new ReplicationConfiguration
            {
                Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
            };
            Replication.Rules.Add(Rule1);
            Replication.Rules.Add(Rule2);
            Client.PutBucketReplication(SourceBucketName, Replication);

            var Response = Client.GetBucketReplication(SourceBucketName);
            Assert.AreEqual(Replication.ToString(), Response.Configuration.ToString());

            Thread.Sleep(5000);

            var Keys = new List<string>();
            // 3개의 오브젝트 업로드
            for (int i = 0; i < 3; i++)
            {
                var KeyName = string.Format("test{0}", i);

                // 2개의 버전정보 생성
                for (int j = 0; j < 2; j++) Client.PutObject(SourceBucketName, KeyName, RandomTextToLong(100));
                Keys.Add(KeyName);
            }

            Thread.Sleep(5000);

            //원본 삭제
            foreach (string KeyName in Keys)
                Client.DeleteObject(SourceBucketName, KeyName);

            Thread.Sleep(5000);

            // 검증
            var SourceResource = Client.ListObjects(SourceBucketName);
            var Target1Resource = Client.ListObjects(Target1BucketName);
            var Target2Resource = Client.ListObjects(Target2BucketName);
            var SourceKeys = GetKeys(SourceResource);
            var Target1Keys = GetKeys(Target1Resource);
            var Target2Keys = GetKeys(Target2Resource);
            Assert.AreEqual(SourceKeys.Count, Target2Keys.Count);
            Assert.AreEqual(Keys, Target1Keys);

            var SourceVersionsResource = Client.ListVersions(SourceBucketName);
            var Target1VersionsResource = Client.ListVersions(Target1BucketName);
            var Target2VersionsResource = Client.ListVersions(Target2BucketName);
            var SourceVerions = GetVersions(SourceVersionsResource.Versions);
            var SourceDeleteMarkers = GetDeleteMarkers(SourceVersionsResource.Versions);
            var Target1Verions = GetVersions(Target1VersionsResource.Versions);
            var Target2Verions = GetVersions(Target2VersionsResource.Versions);
            var Target2DeleteMarkers = GetDeleteMarkers(Target2VersionsResource.Versions);

            VersionIdsCompare(SourceVerions, Target1Verions);
            VersionIdsCompare(SourceVerions, Target2Verions);
            VersionIdsCompare(SourceVerions, Target2Verions);
            VersionIdsCompare(SourceDeleteMarkers, Target2DeleteMarkers);
        }


        [TestMethod("trest_replication_invalid_source_bucket_name")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "원본 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
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
                    BucketArn = "arn:aws:s3:::" + TargetBucketName
                },
            };

            ReplicationConfiguration Replication = new ReplicationConfiguration
            {
                Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
            };
            Replication.Rules.Add(Rule1);

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutBucketReplication(SourceBucketName, Replication));
            var StatusCode = e.StatusCode;
            Assert.AreEqual(HttpStatusCode.NotFound, StatusCode);
        }


        [TestMethod("trest_replication_invalid_source_bucket_versioning")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "원본 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
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
                    BucketArn = "arn:aws:s3:::" + TargetBucketName
                },
            };

            ReplicationConfiguration Replication = new ReplicationConfiguration
            {
                Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
            };
            Replication.Rules.Add(Rule1);

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutBucketReplication(SourceBucketName, Replication));
            var StatusCode = e.StatusCode;
            Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
        }


        [TestMethod("trest_replication_invalid_target_bucket_name")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "대상 버킷이 존재하지 않을때 버킷 복제 설정이 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
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
                    BucketArn = "arn:aws:s3:::" + TargetBucketName
                },
            };

            ReplicationConfiguration Replication = new ReplicationConfiguration
            {
                Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
            };
            Replication.Rules.Add(Rule1);

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutBucketReplication(SourceBucketName, Replication));
            var StatusCode = e.StatusCode;
            Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
        }


        [TestMethod("trest_replication_invalid_target_bucket_versioning")]
        [TestProperty(MainData.Major, "Replication")]
        [TestProperty(MainData.Minor, "ERROR")]
        [TestProperty(MainData.Explanation, "대상 버킷의 버저닝 설정이 되어있지 않을때 실패하는지 확인")]
        [TestProperty(MainData.Result, MainData.ResultFailure)]
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
                    BucketArn = "arn:aws:s3:::" + TargetBucketName
                },
            };

            ReplicationConfiguration Replication = new ReplicationConfiguration
            {
                Role = "arn:aws:iam::635518764071:role/awsreplicationtest"
            };
            Replication.Rules.Add(Rule1);

            var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutBucketReplication(SourceBucketName, Replication));
            var StatusCode = e.StatusCode;
            Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
        }
    }
}
