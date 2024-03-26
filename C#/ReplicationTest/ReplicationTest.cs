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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace ReplicationTest
{
	class ReplicationTest
	{
		static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
		const int TEST_OPTION_LOCAL_ONLY = 1;
		const int TEST_OPTION_ANOTHER_ONLY = 2;
		const int TEST_OPTION_HTTP_ONLY = 1;
		const int TEST_OPTION_HTTPS_ONLY = 2;

		readonly MainConfig Config;
		readonly DBManager DB;

		// 버킷 구성
		const string Prefix = "1/";
		readonly Tag MyTag = new() { Key = "Replication", Value = "True" };

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
				var MainRuleList = CreateReplicationRules(MainTargetList);
				var AltRuleList = CreateReplicationRules(AltTargetList, MainRuleList.Count + 1, Config.AltUser);
				var Replication = new ReplicationConfiguration { Role = "" };
				if (Config.TestOption != TEST_OPTION_ANOTHER_ONLY) Replication.Rules.AddRange(MainRuleList);
				if (Config.TestOption != TEST_OPTION_LOCAL_ONLY) Replication.Rules.AddRange(AltRuleList);

				// 복제 설정
				if (PutBucketReplication(MainClient, MainBucket.BucketName, Replication))
				{
					log.Info("Bucket Replication!");

					// 파일 업로드
					var ObjectList = new List<string>() { "aaa", "bbb", "ccc", "1/ddd", "1/eee", "2/fff", "3/ggg" };

					// Put
					foreach (var ObjectName in ObjectList) PutObject(MainClient, MainBucket.BucketName, ObjectName);

					// Delete
					DeleteObject(MainClient, MainBucket.BucketName, "bbb");
					DeleteObjects(MainClient, MainBucket.BucketName, new List<string>() { "1/ddd", "1/eee" });

					// Copy
					CopyObject(MainClient, MainBucket.BucketName, ObjectList[0], MainBucket.BucketName, "copy");

					// another copy
					var AntherBucket = MainBucket.BucketName + "-anther";
					var CopyKey = "source-copy";
					CreateBucket(MainClient, AntherBucket);
					PutObject(MainClient, AntherBucket, CopyKey);
					CopyObject(MainClient, AntherBucket, CopyKey, MainBucket.BucketName, "another-copy");

					// multipart
					MultipartUpload(MainClient, MainBucket.BucketName, "multi");
					log.Info("Upload End!");

					Thread.Sleep(Config.Delay * 1000);

					// 버저닝 정보를 포함한 비교
					if (Config.TestOption != TEST_OPTION_ANOTHER_ONLY) MainCompare(MainClient, MainBucket, MainClient, MainTargetList);
					if (Config.TestOption != TEST_OPTION_LOCAL_ONLY) MainCompare(MainClient, MainBucket, AltClient, AltTargetList);
					log.Info("Replication Check End!");
					BucketClear(MainClient, AntherBucket);
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
		List<ReplicationRule> CreateReplicationRules(List<BucketData> Buckets, int Index = 1, UserData User = null)
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
					Filter = new ReplicationRuleFilter()
					{
						Prefix = Bucket.Prefix ? Prefix : null,
						Tag = Bucket.Tag ? MyTag : null,
					},
					Destination = new ReplicationDestination() { BucketArn = $"{BucketArnPrefix}{Bucket.BucketName}" },
					DeleteMarkerReplication = new DeleteMarkerReplication() { Status = Bucket.DeleteMarker ? DeleteMarkerReplicationStatus.Enabled : DeleteMarkerReplicationStatus.Disabled }
				};


				Rules.Add(Rule);
			}

			return Rules;
		}

		void MainCompare(AmazonS3Client MainClient, BucketData MainBucket, AmazonS3Client AltClient, List<BucketData> Buckets)
		{
			foreach (var Bucket in Buckets)
			{
				log.Info($"[{Bucket.BucketName}] Compare Check!");
				var SourceData = GetListVersions(MainClient, MainBucket.BucketName, Bucket.Prefix ? Prefix : null).OrderBy(x => x.Key).ThenByDescending(x => x.VersionId).ToList();
				var TargetData = GetListVersions(AltClient, Bucket.BucketName, Bucket.Prefix ? Prefix : null).OrderBy(x => x.Key).ThenByDescending(x => x.VersionId).ToList();

				if (!Bucket.DeleteMarker) SourceData.RemoveAll(i => i.IsDeleteMarker);

				var Result = "Pass";
				// 메타데이터 비교
				if (!SubCompare(SourceData, TargetData, MainClient, AltClient, Bucket.Tag, out string Message))
				{
					Result = "Failed";
					log.Error($"[{Bucket.BucketName}] is not match! -> {Message}");
				}
				else
					log.Info($"[{Bucket.BucketName}] is match!");

				DB?.Insert(MainBucket, Bucket, MainClient.Config.ServiceURL, Config.AltUser, Result, Message);
			}
		}

		bool SubCompare(List<S3ObjectVersion> Source, List<S3ObjectVersion> Target, AmazonS3Client SourceClient, AmazonS3Client TargetClient, bool TagChecking, out string Message)
		{
			Message = "";
			if (Source.Count != Target.Count) { Message = $"{Source.Count} != {Target.Count}"; return false; }
			for (int Index = 0; Index < Source.Count; Index++)
			{
				// 오브젝트 명 비교
				if (Source[Index].Key != Target[Index].Key) { Message = $"{Source[Index].Key} != {Target[Index].Key}"; return false; }
				// 오브젝트 사이즈 비교
				if (Source[Index].Size != Target[Index].Size) { Message = $"{Source[Index].Key} Size does not match! {Source[Index].Size} != {Target[Index].Size}"; return false; }
				// 오브젝트 ETag 비교
				if (Config.CheckEtag && Source[Index].ETag != Target[Index].ETag) { Message = $"{Source[Index].Key} ETag does not match! {Source[Index].ETag} != {Target[Index].ETag}"; return false; }
				// 오브젝트 DeleteMarker 비교
				if (Source[Index].IsDeleteMarker != Target[Index].IsDeleteMarker) { Message = $"{Source[Index].Key} DeleteMarker does not match! {Source[Index].IsDeleteMarker} != {Target[Index].IsDeleteMarker}"; return false; }
				// 오브젝트 VersionId 비교
				if (Config.CheckVersionId && Source[Index].VersionId != Target[Index].VersionId) { Message = $"{Source[Index].Key} VersionId does not match! {Source[Index].VersionId} != {Target[Index].VersionId}"; return false; }

				// 메타데이터 비교
				if (!CompareMetadata(SourceClient, TargetClient, Source[Index].BucketName, Source[Index].Key, Source[Index].VersionId, out Message)) return false;
				// 태그 비교
				if (!CompareTagSet(SourceClient, TargetClient, Source[Index].BucketName, Source[Index].Key, Source[Index].VersionId, out Message)) ; return false;
			}
			return true;
		}

		static bool CompareMetadata(AmazonS3Client SourceClient, AmazonS3Client TargetClient, string BucketName, string Key, string VersionId, out string Message)
		{
			Message = "";
			var MainResponse = GetObjectMetadata(SourceClient, BucketName, Key, VersionId);
			var AltResponse = GetObjectMetadata(TargetClient, BucketName, Key, VersionId);

			if (MainResponse == null) { Message = $"MainResponse is null!"; return false; }
			if (AltResponse == null) { Message = $"AltResponse is null!"; return false; }

			if (MainResponse.Metadata.Count != AltResponse.Metadata.Count) { Message = $"Metadata is not match! {MainResponse.Metadata.Count} != {AltResponse.Metadata.Count}"; return false; }
			foreach (var MetaKey in MainResponse.Metadata.Keys)
			{
				if (!AltResponse.Metadata.Keys.Contains(MetaKey)) { Message = $"Alt does not Metadata Key : {MetaKey}"; return false; }
				if (MainResponse.Metadata[MetaKey] != AltResponse.Metadata[MetaKey]) { Message = $"Metadata does not match! {MainResponse.Metadata[MetaKey]} != {AltResponse.Metadata[MetaKey]}"; return false; }
			}

			return true;
		}

		static bool CompareTagSet(AmazonS3Client Main, AmazonS3Client Alt, string BucketName, string Key, string VersionId, out string Message)
		{
			Message = "";
			var MainResponse = GetObjectTagging(Main, BucketName, Key, VersionId);
			var AltResponse = GetObjectTagging(Alt, BucketName, Key, VersionId);

			if (MainResponse == null) { Message = $"MainResponse is null!"; return false; }
			if (AltResponse == null) { Message = $"AltResponse is null!"; return false; }

			if (MainResponse.Tagging.Count != AltResponse.Tagging.Count) { Message = $"Tagging is not match! {MainResponse.Tagging.Count} != {AltResponse.Tagging.Count}"; return false; }

			for (int Index = 0; Index < MainResponse.Tagging.Count; Index++)
			{
				if (MainResponse.Tagging[Index].Key != AltResponse.Tagging[Index].Key) { Message = $"Tagging Key is not match! {MainResponse.Tagging[Index].Key} != {AltResponse.Tagging[Index].Key}"; return false; }
				if (MainResponse.Tagging[Index].Value != AltResponse.Tagging[Index].Value) { Message = $"Tagging Value is not match! {MainResponse.Tagging[Index].Value} != {AltResponse.Tagging[Index].Value}"; return false; }
			}

			return true;
		}

		static void ObjectClear(AmazonS3Client Client, string BucketName)
		{
			try
			{
				while (true)
				{
					var Response = Client.ListVersionsAsync(BucketName);
					Response.Wait();
					var Versions = Response.Result;

					foreach (var Object in Versions.Versions) Client.DeleteObjectAsync(BucketName, Object.Key, Object.VersionId).Wait();
				}
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
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
		AmazonS3Client CreateClient(UserData User)
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
		AmazonS3Client CreateClientHttps(UserData User)
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
			// if (!CreateBucket(Client, Bucket.BucketName)) ObjectClear(Client, Bucket.BucketName);
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
					ContentBody = Utility.RandomTextToLong(100),
					TagSet = new List<Tag> { new Tag { Key = BucketName, Value = ObjectName } },
				};

				Request.Metadata["x-amz-meta-Test"] = ObjectName;

				var TaskResponse = Client.PutObjectAsync(Request);
				TaskResponse.Wait();
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
		}
		static void DeleteObject(AmazonS3Client Client, string BucketName, string ObjectName)
		{
			try
			{
				var Request = new DeleteObjectRequest
				{
					BucketName = BucketName,
					Key = ObjectName,
				};

				var TaskResponse = Client.DeleteObjectAsync(Request);
				TaskResponse.Wait();
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
		}
		static void DeleteObjects(AmazonS3Client Client, string BucketName, List<string> Keys)
		{
			try
			{
				var Objects = new List<KeyVersion>();
				foreach (var Key in Keys) Objects.Add(new KeyVersion { Key = Key });
				var Request = new DeleteObjectsRequest
				{
					BucketName = BucketName,
					Objects = Objects,
				};

				var TaskResponse = Client.DeleteObjectsAsync(Request);
				TaskResponse.Wait();
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
		}
		static void CopyObject(AmazonS3Client Client, string SourceBucket, string SourceKey, string DestinationBucket, string DestinationKey)
		{
			try
			{
				var Request = new CopyObjectRequest
				{
					SourceBucket = SourceBucket,
					SourceKey = SourceKey,
					DestinationBucket = DestinationBucket,
					DestinationKey = DestinationKey,
				};

				var TaskResponse = Client.CopyObjectAsync(Request);
				TaskResponse.Wait();
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
		}
		static void MultipartUpload(AmazonS3Client Client, string BucketName, string ObjectName)
		{
			try
			{
				var InitRequest = new InitiateMultipartUploadRequest()
				{
					BucketName = BucketName,
					Key = ObjectName,
					TagSet = new List<Tag> { new() { Key = BucketName, Value = ObjectName } },
				};
				InitRequest.Metadata["x-amz-meta-Test"] = ObjectName;
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

		static GetObjectMetadataResponse GetObjectMetadata(AmazonS3Client Client, string BucketName, string ObjectName, string VersionId)
		{
			try
			{
				var Request = new GetObjectMetadataRequest { BucketName = BucketName, Key = ObjectName, VersionId = VersionId, };
				var TaskResponse = Client.GetObjectMetadataAsync(Request);
				TaskResponse.Wait();

				return TaskResponse.Result;
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
			return null;
		}

		static GetObjectTaggingResponse GetObjectTagging(AmazonS3Client Client, string BucketName, string ObjectName, string VersionId)
		{
			try
			{
				var Request = new GetObjectTaggingRequest { BucketName = BucketName, Key = ObjectName, VersionId = VersionId, };
				var TaskResponse = Client.GetObjectTaggingAsync(Request);
				TaskResponse.Wait();

				return TaskResponse.Result;
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
			return null;
		}
		#endregion

		#region Util
		public static List<BucketData> CreateTargetBucketList(string BucketPrefix)
		=> new()
			{
				new(){
					BucketName = $"{BucketPrefix}target-prefix",
					Prefix = true,
				},
				new(){
					BucketName = $"{BucketPrefix}target-tag",
					Tag=true,
				},
				new(){
					BucketName = $"{BucketPrefix}target-prefix-tag",
					Prefix = true,
					Tag=true,
				},
				new(){
					BucketName = $"{BucketPrefix}target-del-all",
					DeleteMarker = true,
				},
				new(){
					BucketName = $"{BucketPrefix}target-e-prefix",
					Encryption = true,
					Prefix = true,
				},
				new(){
					BucketName = $"{BucketPrefix}target-e-tag",
					Encryption = true,
					Tag=true,
				},
				new(){
					BucketName = $"{BucketPrefix}target-e-prefix-tag",
					Encryption = true,
					Prefix = true,
					Tag=true,
				},
				new(){
					BucketName = $"{BucketPrefix}target-e-del-all",
					Encryption = true,
					DeleteMarker = true,
				},
			};

		#endregion
	}
}
