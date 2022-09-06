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
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;

namespace ReplicationTest
{
	class ReplicationTest
	{
		private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
		private const int TEST_OPTION_LOCAL_ONLY = 1;
		private const int TEST_OPTION_ANOTHER_ONLY = 2;
		private const int TEST_OPTION_HTTP_ONLY = 1;
		private const int TEST_OPTION_HTTPS_ONLY = 2;

		private readonly MainConfig Config;
		private readonly DBManager DB;

		// 버킷 구성
		private const string Prefix = "1/";

		public ReplicationTest(MainConfig Config, DBManager DB)
		{
			this.Config = Config;
			this.DB = DB;
		}

		public void Test()
		{
			// http 테스트
			if (Config.SSL != TEST_OPTION_HTTPS_ONLY)
			{
				Start(Config.Normal, false);
				Start(Config.Encryption, false);
			}
			// https 테스트
			if (Config.SSL != TEST_OPTION_HTTP_ONLY)
			{
				Start(Config.Normal, true);
				Start(Config.Encryption, true);
			}
		}

		public void Start(BucketData MainBucket, bool SSL)
		{
			// 클라이언트 선언
			AmazonS3Client MainClient = SSL ? CreateClientHttps(Config.MainUser) : CreateClient(Config.MainUser);
			AmazonS3Client AltClient = SSL ? CreateClientHttps(Config.AltUser) : CreateClient(Config.AltUser);

			// 타겟 버킷 설정
			var MainTargetList = CreateTargetBucketList(Config.TargetBucketPrefix);
			var AltTargetList = CreateTargetBucketList($"{Config.TargetBucketPrefix}alt-");

			//원본 버킷 생성 및 버저닝 설정
			if (!SetBucket(MainClient, MainBucket)) return;
			//로컬 시스템 타깃 버킷 생성 및 버저닝 설정
			var Create = true;
			if (Config.TestOption != TEST_OPTION_ANOTHER_ONLY)
			{
				foreach (var Bucket in MainTargetList)
				{
					if (!SetBucket(MainClient, Bucket))
					{
						Create = false;
						break;
					}
					else Bucket.Create = true;
				}
			}

			//다른 시스템 타깃 버킷 생성 및 버저닝 설정
			if (Config.TestOption != TEST_OPTION_LOCAL_ONLY)
			{
				foreach (var Bucket in AltTargetList)
				{
					if (!SetBucket(AltClient, Bucket))
					{
						Create = false;
						break;
					}
					else Bucket.Create = true;
				}
			}

			if (Create)
			{
				log.Info("Create Bucket!");

				//룰 생성
				var MainRuleList = CreateRelicationRules(MainTargetList);
				var AltRuleList = CreateRelicationRules(AltTargetList, MainRuleList.Count + 1, Config.AltUser);
				ReplicationConfiguration Replication = new ReplicationConfiguration { Role = "" };
				if (Config.TestOption != TEST_OPTION_ANOTHER_ONLY) Replication.Rules.AddRange(MainRuleList);
				if (Config.TestOption != TEST_OPTION_LOCAL_ONLY) Replication.Rules.AddRange(AltRuleList);

				// 복제 설정
				if (PutBucketReplication(MainClient, MainBucket.BucketName, Replication))
				{
					log.Info("Bucket Replication!");

					// 파일 업로드
					var ObjectList = new List<string>() { "aaa", "bbb", "ccc", "1/ddd", "2/eee" };
					foreach (var ObjectName in ObjectList) PutObject(MainClient, MainBucket.BucketName, ObjectName);
					MultipartUpload(MainClient, MainBucket.BucketName, "multi");

					Thread.Sleep(Config.Delay * 1000);

					// 버저닝 정보를 포함한 비교
					if (Config.TestOption != TEST_OPTION_ANOTHER_ONLY) Compare2Target(MainClient, MainBucket, MainClient, MainTargetList);
					if (Config.TestOption != TEST_OPTION_LOCAL_ONLY) Compare2Target(MainClient, MainBucket, AltClient, AltTargetList);
					log.Info("Replication Check End!");
				}
				else
					log.Info("Bucket Replication Failed!");
			}
			else
				log.Error("Bucket Create Failed");

			// 버킷 삭제
			BucketClear(MainClient, MainBucket.BucketName);
			if (Config.TestOption != TEST_OPTION_ANOTHER_ONLY) foreach (var Bucket in MainTargetList) if (Bucket.Create) BucketClear(MainClient, Bucket.BucketName);
			if (Config.TestOption != TEST_OPTION_LOCAL_ONLY) foreach (var Bucket in AltTargetList) if (Bucket.Create) BucketClear(AltClient, Bucket.BucketName);
			log.Info("Bucket Delete End");
		}

		#region Utility
		static List<ReplicationRule> CreateRelicationRules(List<BucketData> Buckets, int Index = 1, UserData User = null)
		{
			var BucketArnPrefix = "";
			if (User == null) BucketArnPrefix = "arn:aws:s3:::";
			else BucketArnPrefix = User.IsRegion ? $"arn:aws:s3:{User.RegionName}::" : $"arn:aws:s3:{User.URL}:{User.AccessKey}-{User.SecretKey}:";
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
		static List<ReplicationRule> CreateRelicationRules(List<BucketData> Buckets, int Index, string RegionName)
		{
			var Rules = new List<ReplicationRule>();

			foreach (var Bucket in Buckets)
			{
				var Rule = new ReplicationRule
				{
					Id = $"Rule{Index}",
					Priority = Index++,
					Status = ReplicationRuleStatus.Enabled,
					Destination = new ReplicationDestination() { BucketArn = $"arn:aws:s3:{RegionName}::{Bucket.BucketName}" },
					Filter = new ReplicationRuleFilter() { Prefix = Bucket.Prefix, },
				};

				Rules.Add(Rule);
			}

			return Rules;
		}

		void Compare2Target(AmazonS3Client Main, BucketData MainBucket, AmazonS3Client Alt, List<BucketData> Buckets)
		{
			foreach (var Bucket in Buckets)
			{
				log.Info($"[{Bucket.BucketName}] Compare Check!");
				var SourceData = GetListVersions(Main, MainBucket.BucketName, Bucket.Filtering ? Bucket.Prefix : null);
				var TargetData = GetListVersions(Alt, Bucket.BucketName, Bucket.Filtering ? Bucket.Prefix : null);

				var Message = CompareMain2Alt(SourceData, TargetData);
				var Result = "";
				if (string.IsNullOrWhiteSpace(Message))
				{
					log.Info($"[{Bucket.BucketName}] is match!");
					Result = "Pass";
				}
				else
				{
					log.Error($"[{Bucket.BucketName}] is not match! -> {Message}");
					Result = "Failed";
				}
				if (DB != null) DB.Insert(MainBucket, Bucket, Main.Config.ServiceURL, Config.AltUser, Result, Message);
			}
		}

		string CompareMain2Alt(List<S3ObjectVersion> Main, List<S3ObjectVersion> Alt)
		{
			if (Main.Count != Alt.Count) return $"{Main.Count} != {Alt.Count}";

			for (int i = 0; i < Main.Count; i++)
			{
				if (Main[i].Key != Alt[i].Key) return $"{Main[i].Key} != {Alt[i].Key}";
				if (Main[i].Size != Alt[i].Size) return $"{Main[i].Key} Size does not match! {Main[i].Size} != {Alt[i].Size}";
				if (Config.CheckEtag && Main[i].ETag != Alt[i].ETag) return $"{Main[i].Key} ETag does not match! {Main[i].ETag} != {Alt[i].ETag}";
				if (Main[i].IsDeleteMarker != Alt[i].IsDeleteMarker) return $"{Main[i].Key} DeleteMarker does not match! {Main[i].IsDeleteMarker} != {Alt[i].IsDeleteMarker}";
				if (Config.CheckVersionId && Main[i].VersionId != Alt[i].VersionId) return $"{Main[i].Key} VersionId does not match! {Main[i].VersionId} != {Alt[i].VersionId}";
			}
			return "";
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
				log.Info($"[{BucketName}] is Delete.");
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
		}
		#endregion
		#region S3 API
		private AmazonS3Client CreateClient(UserData User)
		{
			var Config = new AmazonS3Config
			{
				ServiceURL = $"http://{User.URL}:{User.Port}",
				Timeout = TimeSpan.FromSeconds(3600),
				MaxErrorRetry = 2,
				ForcePathStyle = true,
				UseHttp = true,
			};

			return new AmazonS3Client(User.AccessKey, User.SecretKey, Config);
		}
		private AmazonS3Client CreateClientHttps(UserData User)
		{
			var Config = new AmazonS3Config
			{
				ServiceURL = $"https://{User.URL}:{User.SSLPort}",
				Timeout = TimeSpan.FromSeconds(3600),
				MaxErrorRetry = 2,
				ForcePathStyle = true,
				UseHttp = true,
			};

			return new AmazonS3Client(User.AccessKey, User.SecretKey, Config);
		}

		static bool SetBucket(AmazonS3Client Client, BucketData Bucket)
		{
			CreateBucket(Client, Bucket.BucketName);
			// 암호화 설정
			if (Bucket.Encryption)
			{
				if (!BucketEncryption(Client, Bucket.BucketName))
				{
					log.Error($"[{Bucket.BucketName}] is not enabled Encryption");
					return false;
				}
			}
			// 버저닝 설정
			if (!BucketVersioning(Client, Bucket.BucketName, VersionStatus.Enabled))
			{
				log.Error($"[{Bucket.BucketName}] is not enabled Versioning");
				return false;
			}
			else
				log.Info($"[{Bucket.BucketName}] Set End!");
			return true;
		}

		static bool CreateBucket(AmazonS3Client Client, string BucketName)
		{
			try
			{
				var putBucketRequest = new PutBucketRequest { BucketName = BucketName };

				var TaskResponse = Client.PutBucketAsync(putBucketRequest); //Create Bucket request
				TaskResponse.Wait();
				var Response = TaskResponse.Result;

				if (Response.HttpStatusCode != System.Net.HttpStatusCode.OK)
				{
					log.Info($"CreateBucket({BucketName}) : Create!!");
					return true;
				}
			}
			catch (AggregateException e) { log.Error($"{BucketName} StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}"); }
			catch (Exception e) { log.Error(e); }
			return false;
		}
		static bool BucketVersioning(AmazonS3Client Client, string BucketName, VersionStatus Status)
		{
			try
			{
				var Request = new PutBucketVersioningRequest()
				{
					BucketName = BucketName,
					VersioningConfig = new S3BucketVersioningConfig() { Status = Status, },
				};

				var TaskResponse = Client.PutBucketVersioningAsync(Request);
				TaskResponse.Wait();
				return true;
			}
			catch (AggregateException e) { log.Error($"{BucketName} StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
			return false;
		}
		static bool BucketEncryption(AmazonS3Client Client, string BucketName)
		{
			try
			{
				var Request = new PutBucketEncryptionRequest()
				{
					BucketName = BucketName,
					ServerSideEncryptionConfiguration = new ServerSideEncryptionConfiguration()
					{
						ServerSideEncryptionRules = new List<ServerSideEncryptionRule>()
						{
							new ServerSideEncryptionRule()
							{
								ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
								{
									ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
								}
							}
						}
					}
				};

				var Response = Client.PutBucketEncryptionAsync(Request);
				Response.Wait();
				return true;
			}
			catch (AggregateException e) { log.Error($"{BucketName} StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
			return false;
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
			catch (AggregateException e) { log.Error($"{BucketName} StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
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
		static bool MultipartUpload(AmazonS3Client Client, string BucketName, string ObjectName)
		{
			try
			{
				var InitRequest = new InitiateMultipartUploadRequest() { BucketName = BucketName, Key = ObjectName };
				var InitResponse = Client.InitiateMultipartUploadAsync(InitRequest);
				InitResponse.Wait();
				var UploadId = InitResponse.Result.UploadId;

				var PartRequest = new UploadPartRequest()
				{
					BucketName = BucketName,
					Key = ObjectName,
					UploadId = UploadId,
					InputStream = new MemoryStream(Encoding.ASCII.GetBytes(Utility.RandomTextToLong(1000))),
					PartNumber = 1,
				};
				var PartResponse = Client.UploadPartAsync(PartRequest);
				PartResponse.Wait();
				var PartList = new List<PartETag>() { new PartETag(PartResponse.Result.PartNumber, PartResponse.Result.ETag) };

				var CompRequest = new CompleteMultipartUploadRequest()
				{
					BucketName = BucketName,
					Key = ObjectName,
					UploadId = UploadId,
					PartETags = PartList,
				};
				var CompResponse = Client.CompleteMultipartUploadAsync(CompRequest);
				CompResponse.Wait();

				return true;
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
			return false;
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
		#endregion

		#region Util
		public static List<BucketData> CreateTargetBucketList(string BucketPrefix)
		=> new List<BucketData>()
			{
				new BucketData(){
					BucketName = $"{BucketPrefix}target-normal",
					Prefix = Prefix,
				},
				new BucketData(){
					BucketName = $"{BucketPrefix}target-normal-full",
				},
				new BucketData(){
					BucketName = $"{BucketPrefix}target-normal-del",
					Prefix = Prefix,
					DeleteMarker = true,
				},
				new BucketData(){
					BucketName = $"{BucketPrefix}target-normal-full-and-del",
					DeleteMarker = true,
				},
				new BucketData(){
					BucketName = $"{BucketPrefix}target-encryption",
					Prefix = Prefix,
					Encryption = true,
				},
				new BucketData(){
					BucketName = $"{BucketPrefix}target-encryption-full",
					Encryption = true,
				},
				new BucketData(){
					BucketName = $"{BucketPrefix}target-encryption-del",
					Prefix = Prefix,
					DeleteMarker = true,
					Encryption = true,
				},
				new BucketData(){
					BucketName = $"{BucketPrefix}target-encryption-full-and-del",
					DeleteMarker = true,
					Encryption = true,
				},
			};

		#endregion
	}
}
