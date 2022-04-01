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
using log4net;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;

namespace ReplicationTest
{
    class S3Test
    {
        private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly MainConfig Config;
        private readonly DBManager DB;
        private readonly int BuildId;

        
        // 버킷 구성
        private const string Prefix = "1/";
        private const string MainBucket = "bucket-source";
        private readonly List<BucketData> MainTargetList;
        private readonly List<BucketData> AltTargetList;

        public S3Test(MainConfig Config, DBManager DB, int BuildId)
        {
            this.Config = Config;
            this.BuildId = BuildId;
            this.DB = DB;

            MainTargetList = new List<BucketData>()
            {
                new BucketData(){
                    BucketName = "bucket-copy-target",
                    Prefix = Prefix,
                },
                new BucketData(){
                    BucketName = "bucket-full-copy-target",
                },
                new BucketData(){
                    BucketName = "bucket-copy-and-del-target",
                    Prefix = Prefix,
                    DeleteMarker = true
                },
                new BucketData(){
                    BucketName = "bucket-full-copy-and-del-target",
                    DeleteMarker = true
                },
            };

            AltTargetList = new List<BucketData>()
            {
                new BucketData(){
                    BucketName = "alt-copy-target",
                    Prefix = Prefix,
                },
                new BucketData(){
                    BucketName = "alt-full-copy-target",
                },
                new BucketData(){
                    BucketName = "alt-copy-and-del-target",
                    Prefix = Prefix,
                    DeleteMarker = true
                },
                new BucketData(){
                    BucketName = "alt-full-copy-and-del-target",
                    DeleteMarker = true
                },
            };
        }


        /************************************************ Init **********************************************/
        public void Start()
        {
            // 클라이언트 선언
            AmazonS3Client MainClient = CreateClient(Config.MainUser);
            AmazonS3Client AltClient = CreateClient(Config.AltUser);

            //원본 버킷 생성 및 버저닝 설정
            CreateBucket(MainClient, MainBucket);
            BucketVersioning(MainClient, MainBucket, VersionStatus.Enabled);


            //동일 시스템 타깃 버킷 생성 및 버저닝 설정
            foreach (var Bucket in MainTargetList)
            {
                CreateBucket(MainClient, Bucket.BucketName);
                BucketVersioning(MainClient, Bucket.BucketName, VersionStatus.Enabled);
            }

            //다른 시스템 타깃 버킷 생성 및 버저닝 설정
            foreach (var Bucket in AltTargetList)
            {
                CreateBucket(AltClient, Bucket.BucketName);
                BucketVersioning(AltClient, Bucket.BucketName, VersionStatus.Enabled);
            }
            log.Info("Create Bucket!");

            //룰 생성
            var MainRuleList = CreateRelicationRules(MainTargetList, 1);
            var AltRuleList = CreateRelicationRules(AltTargetList, 5, Config.AltUser);

            ReplicationConfiguration Replication = new ReplicationConfiguration { Role = "" };
            Replication.Rules.AddRange(MainRuleList);
            Replication.Rules.AddRange(AltRuleList);

            // 복제 설정
            if(PutBucketReplication(MainClient, MainBucket, Replication))
            {
                log.Info("Bucket Replication!");

                // 파일 업로드
                var ObjectList = new List<string>() { "aaa", "bbb", "ccc", "1/ddd", "2/eee" };
                foreach (var ObjectName in ObjectList) PutObject(MainClient, MainBucket, ObjectName);

                Thread.Sleep(Config.Delay * 1000);

                // 버저닝 정보를 포함한 비교
                Compare2Target(MainClient, MainBucket, "Init", MainClient, MainTargetList);
                Compare2Target(MainClient, MainBucket, "Init", AltClient, AltTargetList);
                log.Info("Target Check!");
            }
            else
                log.Info("Bucket Replication Failed!");


            // 버킷 삭제
            BucketClear(MainClient, MainBucket);
            foreach (var Bucket in MainTargetList) BucketClear(MainClient, Bucket.BucketName);
            foreach (var Bucket in AltTargetList) BucketClear(AltClient, Bucket.BucketName);
            log.Info("Bucket Clear!");
        }


        /********************************************** Utility *********************************************/

        private AmazonS3Client CreateClient(UserData User)
        {
            var config = new AmazonS3Config
            {
                ServiceURL = $"http://{User.URL}:8080",
                Timeout = TimeSpan.FromSeconds(3600),
                MaxErrorRetry = 2,
                ForcePathStyle = true,
            };

            return new AmazonS3Client(User.AccessKey, User.SecretKey, config);
        }

        static List<ReplicationRule> CreateRelicationRules(List<BucketData> Buckets, int Index, UserData User = null)
        {
            var BucketArnPrefix = User == null ? "arn:aws:s3:::" : $"arn:aws:s3:{User.URL}:{User.AccessKey}-{User.SecretKey}:";
            var Rules = new List<ReplicationRule>();

            foreach (var Bucket in Buckets)
            {
                var Rule = new ReplicationRule
                {
                    Id = $"Rule{Index}",
                    Priority = Index++,
                    Status = ReplicationRuleStatus.Enabled,
                    Destination = new ReplicationDestination() { BucketArn = $"{BucketArnPrefix}{Bucket.BucketName}" },
                    Filter = new ReplicationRuleFilter() { Prefix = Bucket.Prefix, },
                };

                Rules.Add(Rule);
            }

            return Rules;
        }

        static string CompareMain2Alt(List<S3ObjectVersion> Main, List<S3ObjectVersion> Alt)
        {
            if (Main.Count != Alt.Count) return $"{Main.Count} != {Alt.Count}"; 

            for (int i = 0; i < Main.Count; i++)
            {
                if (Main[i].Key != Alt[i].Key)   return $"{Main[i].Key} != {Alt[i].Key}";
                if (Main[i].Size != Alt[i].Size) return $"{Main[i].Key} Size does not match! {Main[i].Size} != {Alt[i].Size}";
                if (Main[i].ETag != Alt[i].ETag) return $"{Main[i].Key} ETag does not match! {Main[i].ETag} != {Alt[i].ETag}";
                if (Main[i].IsDeleteMarker != Alt[i].IsDeleteMarker) return $"{Main[i].Key} DeleteMarker does not match! {Main[i].IsDeleteMarker} != {Alt[i].IsDeleteMarker}";
                //if (Main[i].VersionId != Alt[i].VersionId) { log.Error($"{Main[i].Key} VersionId does not match! {Main[i].VersionId} != {Alt[i].VersionId}"); return false; }
            }
            return "";
        }

        void Compare2Target(AmazonS3Client Main, string MainBucket, string TestCase, AmazonS3Client Alt, List<BucketData> Buckets)
        {
            foreach (var Bucket in Buckets)
            {
                var SubTestCaseName = $"[{TestCase}] {Bucket.BucketName}";

                log.Info($"{Bucket.BucketName} Compare Check!");
                var SourceData = GetListVersions(Main, MainBucket, Bucket.Filtering ? Bucket.Prefix : null);
                var TargetData = GetListVersions(Alt, Bucket.BucketName, Bucket.Filtering ? Bucket.Prefix : null);

                var Result = CompareMain2Alt(SourceData, TargetData);

                if (string.IsNullOrWhiteSpace(Result))
                {
                    log.Info($"{SubTestCaseName} is match!");
                    DB.Insert(BuildId, SubTestCaseName, "Pass", "");
                }
                else
                {
                    log.Error($"{SubTestCaseName} is not match! -> {Result}");
                    DB.Insert(BuildId, SubTestCaseName, "Failed", Result);
                }
            }
        }
        static void BucketClear(AmazonS3Client Client, string BucketName)
        {
            try
            {
                var Response = Client.ListVersionsAsync(BucketName);
                Response.Wait();
                var Versions = Response.Result;

                foreach (var Object in Versions.Versions) Client.DeleteObjectAsync(BucketName, Object.Key, Object.VersionId).Wait();
                Client.DeleteBucketAsync(BucketName).Wait();
            }
            catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
            catch (Exception e) { log.Error(e); }
        }
        /************************************************ S3 **********************************************/
        static void CreateBucket(AmazonS3Client Client, string BucketName)
        {
            try
            {
                var putBucketRequest = new PutBucketRequest { BucketName = BucketName };

                var TaskResponse = Client.PutBucketAsync(putBucketRequest); //Create Bucket request
                TaskResponse.Wait();
                var Response = TaskResponse.Result;

                if (Response.HttpStatusCode == System.Net.HttpStatusCode.OK) log.Info($"CreateBucket({BucketName}) : Create!!");
                else log.Error($"CreateBucket({BucketName}) : Create failed");
            }
            catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}"); }
            catch (Exception e) { log.Error(e); }
        }
        static void BucketVersioning(AmazonS3Client Client, string BucketName, VersionStatus Status)
        {
            try
            {
                S3BucketVersioningConfig versioningConfig = new S3BucketVersioningConfig() { Status = Status, };

                var Request = new PutBucketVersioningRequest()
                {
                    BucketName = BucketName,
                    VersioningConfig = versioningConfig,
                };

                var TaskResponse = Client.PutBucketVersioningAsync(Request);
                TaskResponse.Wait();
            }

            catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
            catch (Exception e) { log.Error(e); }
        }

        static bool PutBucketReplication(AmazonS3Client Client, string BucketName, ReplicationConfiguration Config)
        {
            if (Client == null) return false;

            try
            {
                var Request = new PutBucketReplicationRequest()
                {
                    BucketName = BucketName,
                    Configuration = Config
                };

                var Response = Client.PutBucketReplicationAsync(Request);
                Response.Wait();
                if (Response.Result.HttpStatusCode == System.Net.HttpStatusCode.OK) return true;
            }
            catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
            catch (Exception e) { log.Error(e); }
            return false;
        }
        static void PutObject(AmazonS3Client Client, string BucketName, string ObjectName)
        {
            try
            {
                var Request = new PutObjectRequest
                {
                    BucketName = BucketName,
                    Key = ObjectName,
                    ContentBody = Utility.RandomTextToLong(100)
                };

                var TaskResponse = Client.PutObjectAsync(Request);
                TaskResponse.Wait();
                var Response = TaskResponse.Result;
            }
            catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
            catch (Exception e) { log.Error(e); }
        }
        static List<S3ObjectVersion> GetListVersions(AmazonS3Client Client, string BucketName, string Prefix = null)
        {
            try
            {
                var Request = new ListVersionsRequest { BucketName = BucketName, };
                if (Prefix != null) Request.Prefix = Prefix;
                var TaskResponse = Client.ListVersionsAsync(Request);
                TaskResponse.Wait();

                return TaskResponse.Result.Versions;
            }
            catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
            catch (Exception e) { log.Error(e); }
            return null;
        }
    }
}
