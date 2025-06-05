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

		readonly MainConfig _config;
		readonly DBManager _db;

		public ReplicationTest(MainConfig config, DBManager db)
		{
			_config = config;
			_db = db;
		}

		// 버킷 구성
		const string PREFIX = "2/";
		static readonly Tag _defaultTag = new() { Key = "Replication", Value = "True" };

		public void Test()
		{
			// http 테스트
			if (_config.SSL != TEST_OPTION_HTTPS_ONLY)
			{
				Start(_config.Normal, false);
				Start(_config.Encryption, false);
			}
			// https 테스트
			if (_config.SSL != TEST_OPTION_HTTP_ONLY)
			{
				Start(_config.Normal, true);
				Start(_config.Encryption, true);
			}
		}

		public void Start(BucketData mainBucket, bool ssl)
		{
			// 클라이언트 선언
			var mainClient = ssl ? CreateClientHttps(_config.MainUser) : CreateClient(_config.MainUser);
			var altClient = ssl ? CreateClientHttps(_config.AltUser) : CreateClient(_config.AltUser);

			// 타겟 버킷 설정
			var mainTargetList = CreateTargetBucketList(_config.TargetBucketPrefix);
			var altTargetList = CreateTargetBucketList($"{_config.TargetBucketPrefix}alt-");

			//원본 버킷 생성 및 버저닝 설정
			if (!SetBucket(mainClient, mainBucket)) return;
			//로컬 시스템 타깃 버킷 생성 및 버저닝 설정
			var create = true;
			if (_config.TestOption != TEST_OPTION_ANOTHER_ONLY)
			{
				foreach (var bucket in mainTargetList)
				{
					if (!SetBucket(mainClient, bucket))
					{
						create = false;
						break;
					}
					else bucket.Create = true;
				}
			}

			//다른 시스템 타깃 버킷 생성 및 버저닝 설정
			if (_config.TestOption != TEST_OPTION_LOCAL_ONLY)
			{
				foreach (var bucket in altTargetList)
				{
					if (!SetBucket(altClient, bucket))
					{
						create = false;
						break;
					}
					else bucket.Create = true;
				}
			}

			if (create)
			{
				log.Info("Create Bucket!");

				//룰 생성
				var mainRuleList = CreateReplicationRules(mainTargetList);
				var altRuleList = CreateReplicationRules(altTargetList, mainRuleList.Count + 1, _config.AltUser);
				var replication = new ReplicationConfiguration { Role = "" };
				if (_config.TestOption != TEST_OPTION_ANOTHER_ONLY) replication.Rules.AddRange(mainRuleList);
				if (_config.TestOption != TEST_OPTION_LOCAL_ONLY) replication.Rules.AddRange(altRuleList);

				// 복제 설정
				if (PutBucketReplication(mainClient, mainBucket.BucketName, replication))
				{
					log.Info("Bucket Replication!");

					// 파일 업로드
					var keys = new List<string>() { "normal", "1/normal", "2/normal", "2/3/normal" };

					// 삭제할 파일 목록 추가
					var deleteKey = "delete";
					keys.Add(deleteKey);

					// 삭제할 파일 목록 추가
					var deleteKeys = new List<string>() { "1/delete", "2/delete", "2/3/delete" };
					keys.AddRange(deleteKeys);

					// Put
					foreach (var key in keys) PutObject(mainClient, mainBucket.BucketName, key);

					// checksum 파일 목록 추가
					var checksumCrc32 = "crc32";
					var checksumCrc32c = "crc32c";
					var checksumCrc64nvme = "crc64nvme";
					var checksumSha1 = "sha1";
					var checksumSha256 = "sha256";
					keys.AddRange([checksumCrc32, checksumCrc32c, checksumCrc64nvme, checksumSha1, checksumSha256]);

					// Put Checksum
					PutObject(mainClient, mainBucket.BucketName, checksumCrc32, ChecksumAlgorithm.CRC32);
					PutObject(mainClient, mainBucket.BucketName, checksumCrc32c, ChecksumAlgorithm.CRC32C);
					PutObject(mainClient, mainBucket.BucketName, checksumCrc64nvme, ChecksumAlgorithm.CRC64NVME);
					PutObject(mainClient, mainBucket.BucketName, checksumSha1, ChecksumAlgorithm.SHA1);
					PutObject(mainClient, mainBucket.BucketName, checksumSha256, ChecksumAlgorithm.SHA256);

					// Delete
					DeleteObject(mainClient, mainBucket.BucketName, deleteKey);
					DeleteObjects(mainClient, mainBucket.BucketName, deleteKeys);

					// Copy
					CopyObject(mainClient, mainBucket.BucketName, keys[0], mainBucket.BucketName, keys[0] + "-copy");

					// another copy
					var antherBucket = mainBucket.BucketName + "-anther";
					var copyKey = "source";
					CreateBucket(mainClient, antherBucket);
					PutObject(mainClient, antherBucket, copyKey);
					CopyObject(mainClient, antherBucket, copyKey, mainBucket.BucketName, "another-copy");

					// multipart
					MultipartUpload(mainClient, mainBucket.BucketName, "multipart");
					log.Info("Upload End!");

					Thread.Sleep(_config.Delay * 1000);

					// 버저닝 정보를 포함한 비교
					if (_config.TestOption != TEST_OPTION_ANOTHER_ONLY) Compare(mainClient, mainBucket, mainClient, mainTargetList);
					if (_config.TestOption != TEST_OPTION_LOCAL_ONLY) Compare(mainClient, mainBucket, altClient, altTargetList);
					log.Info("Replication Check End!");
					BucketClear(mainClient, antherBucket);
				}
				else
					log.Info("Bucket Replication Failed!");
			}
			else
				log.Error("Bucket Create Failed");

			// 버킷 삭제
			BucketClear(mainClient, mainBucket.BucketName);
			if (_config.TestOption != TEST_OPTION_ANOTHER_ONLY) foreach (var Bucket in mainTargetList) if (Bucket.Create) BucketClear(mainClient, Bucket.BucketName);
			if (_config.TestOption != TEST_OPTION_LOCAL_ONLY) foreach (var Bucket in altTargetList) if (Bucket.Create) BucketClear(altClient, Bucket.BucketName);
			log.Info("Bucket Delete End");
		}

		#region Utility
		static List<ReplicationRule> CreateReplicationRules(List<BucketData> Buckets, int Index = 1, UserData User = null)
		{
			string BucketArnPrefix;
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
						Prefix = Bucket.Prefix ? PREFIX : null,
						Tag = Bucket.Tag ? _defaultTag : null,
					},
					Destination = new ReplicationDestination() { BucketArn = $"{BucketArnPrefix}{Bucket.BucketName}" },
					DeleteMarkerReplication = new DeleteMarkerReplication() { Status = Bucket.DeleteMarker ? DeleteMarkerReplicationStatus.Enabled : DeleteMarkerReplicationStatus.Disabled }
				};

				Rules.Add(Rule);
			}

			return Rules;
		}

		void Compare(AmazonS3Client mainClient, BucketData mainBucket, AmazonS3Client altClient, List<BucketData> buckets)
		{
			foreach (var bucket in buckets)
			{
				log.Info($"[{bucket.BucketName}] Compare Check!");
				var SourceData = GetListVersions(mainClient, mainBucket.BucketName, bucket.Prefix ? PREFIX : null).OrderBy(x => x.Key).ThenByDescending(x => x.VersionId).ToList();
				var TargetData = GetListVersions(altClient, bucket.BucketName, bucket.Prefix ? PREFIX : null).OrderBy(x => x.Key).ThenByDescending(x => x.VersionId).ToList();

				if (!bucket.DeleteMarker) SourceData.RemoveAll(i => i.IsDeleteMarker);

				var result = "Pass";
				// 메타데이터 비교
				if (!CompareObject(SourceData, TargetData, mainClient, altClient, out string Message))
				{
					result = "Failed";
					log.Error($"[{bucket.BucketName}] is not match! -> {Message}");
				}
				else
					log.Info($"[{bucket.BucketName}] is match!");
				
				_db?.Insert(mainBucket, bucket, mainClient.Config.ServiceURL, altClient.Config.ServiceURL, result, Message);
			}
		}

		bool CompareObject(List<S3ObjectVersion> Source, List<S3ObjectVersion> Target, AmazonS3Client SourceClient, AmazonS3Client TargetClient, out string Message)
		{
			Message = "";
			if (Source.Count != Target.Count) { Message = $"{Source.Count} != {Target.Count}"; return false; }

			for (int Index = 0; Index < Source.Count; Index++)
			{
				var sourceObj = Source[Index];
				var targetObj = Target[Index];

				// 둘다 삭제 마커인 경우 패스
				if (sourceObj.IsDeleteMarker && targetObj.IsDeleteMarker) continue;

				if (!CompareObjectProperties(sourceObj, targetObj, _config, out Message)) return false;
				if (!CompareMetadata(SourceClient, TargetClient, sourceObj.BucketName, sourceObj.Key, sourceObj.VersionId, out Message)) return false;
				if (!CompareTagSet(SourceClient, TargetClient, sourceObj.BucketName, sourceObj.Key, sourceObj.VersionId, out Message)) return false;
			}
			return true;
		}

		static bool CompareObjectProperties(S3ObjectVersion source, S3ObjectVersion target, MainConfig config, out string message)
		{
			message = "";

			var comparisons = new List<(string property, bool condition, string sourceValue, string targetValue)>
			{
				("Key", source.Key != target.Key, source.Key, target.Key),
				("Size", source.Size != target.Size, source.Size.ToString(), target.Size.ToString()),
				("ETag", config.CheckEtag && source.ETag != target.ETag, source.ETag, target.ETag),
				("DeleteMarker", source.IsDeleteMarker != target.IsDeleteMarker, source.IsDeleteMarker.ToString(), target.IsDeleteMarker.ToString()),
				("VersionId", config.CheckVersionId && source.VersionId != target.VersionId, source.VersionId, target.VersionId)
			};

			foreach (var (property, condition, sourceValue, targetValue) in comparisons)
			{
				if (condition)
				{
					message = $"{property} does not match for object '{source.Key}'! {sourceValue} != {targetValue}";
					return false;
				}
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
		static AmazonS3Client CreateClient(UserData User)
		{
			var config = new AmazonS3Config
			{
				ServiceURL = $"http://{User.URL}:{User.Port}",
				Timeout = TimeSpan.FromSeconds(3600),
				MaxErrorRetry = 2,
				ForcePathStyle = true,
				UseHttp = true,
			};

			return new AmazonS3Client(User.AccessKey, User.SecretKey, config);
		}
		static AmazonS3Client CreateClientHttps(UserData User)
		{
			var config = new AmazonS3Config
			{
				ServiceURL = $"https://{User.URL}:{User.SSLPort}",
				Timeout = TimeSpan.FromSeconds(3600),
				MaxErrorRetry = 2,
				ForcePathStyle = true,
				UseHttp = true,
			};

			return new AmazonS3Client(User.AccessKey, User.SecretKey, config);
		}

		static bool SetBucket(AmazonS3Client Client, BucketData Bucket)
		{
			CreateBucket(Client, Bucket.BucketName);

			// 암호화 설정
			if (Bucket.Encryption && !BucketEncryption(Client, Bucket.BucketName))
			{
				log.Error($"[{Bucket.BucketName}] is not enabled Encryption");
				return false;
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

		static bool CreateBucket(AmazonS3Client client, string bucketName)
		{
			try
			{
				PutBucketRequest request = new() { BucketName = bucketName };
				var response = client.PutBucketAsync(request).GetAwaiter().GetResult();

				if (response.HttpStatusCode == System.Net.HttpStatusCode.OK) return true;
			}
			catch (AggregateException e) { log.Error($"{bucketName} StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}"); }
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
						ServerSideEncryptionRules = new()
						{
							new()
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
		static bool PutObject(AmazonS3Client Client, string BucketName, string ObjectName, ChecksumAlgorithm checksumAlgorithm = null)
		{
			try
			{
				var Request = new PutObjectRequest
				{
					BucketName = BucketName,
					Key = ObjectName,
					ContentBody = Utility.RandomTextToLong(100),
					TagSet = new() { new Tag { Key = BucketName, Value = ObjectName }, _defaultTag },
				};

				Request.Metadata["x-amz-meta-Test"] = ObjectName;
				if (checksumAlgorithm != null) Request.ChecksumAlgorithm = checksumAlgorithm;

				var TaskResponse = Client.PutObjectAsync(Request).GetAwaiter().GetResult();
				if (TaskResponse.HttpStatusCode == System.Net.HttpStatusCode.OK) return true;
				log.Error($"[s3://{BucketName}/{ObjectName}] PutObject Failed! {TaskResponse.HttpStatusCode}");
				return false;
			}
			catch (AggregateException e) { log.Error($"StatusCode : {Utility.GetStatus(e)}, ErrorCode : {Utility.GetErrorCode(e)}", e); }
			catch (Exception e) { log.Error(e); }
			return false;
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
					MetadataDirective = S3MetadataDirective.COPY,
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
					TagSet = new() { new() { Key = BucketName, Value = ObjectName }, _defaultTag },
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
				var Request = new GetObjectMetadataRequest { BucketName = BucketName, Key = ObjectName, VersionId = VersionId, ChecksumMode = ChecksumMode.ENABLED };
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
		public static List<BucketData> CreateTargetBucketList(string prefix)
		=>
			new(){
				new(){ BucketName = $"{prefix}target-prefix"      , Prefix=true, },
				new(){ BucketName = $"{prefix}target-tag"         , Tag=true, },
				new(){ BucketName = $"{prefix}target-prefix-tag"  , Prefix=true, Tag=true, },
				new(){ BucketName = $"{prefix}target-del-all"     , DeleteMarker=true, },
				new(){ BucketName = $"{prefix}e-target-prefix"    , Encryption=true, Prefix=true, },
				new(){ BucketName = $"{prefix}e-target-tag"       , Encryption=true, Tag=true, },
				new(){ BucketName = $"{prefix}e-target-prefix-tag", Encryption=true, Prefix=true, Tag=true, },
				new(){ BucketName = $"{prefix}e-target-del-all"   , Encryption=true, DeleteMarker=true, },
			};

		#endregion
	}
}
