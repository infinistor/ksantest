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
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace s3tests2
{
	public class S3Client
	{
		private const int S3_TIMEOUT = 3600;

		public readonly AmazonS3Client Client = null;
		public S3Client(AmazonS3Client Client) => this.Client = Client;

		public S3Client(S3Config S3, string SignatureVersion, bool IsSecure, UserData User)
		{

			AWSCredentials Credentials;
			if (User == null) Credentials = new AnonymousAWSCredentials();
			else Credentials = new BasicAWSCredentials(User.AccessKey, User.SecretKey);

			AmazonS3Config S3Config;

			if (S3.IsAWS)
			{
				S3Config = new AmazonS3Config()
				{
					Timeout = TimeSpan.FromSeconds(S3_TIMEOUT),
					SignatureVersion = SignatureVersion,
					RegionEndpoint = S3.GetRegion(),
					ForcePathStyle = true,
				};
			}
			else
			{
				string URL;

				if (IsSecure)
				{
					ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => true;
					URL = S3.GetHttpsURL();
				}
				else URL = S3.GetHttpURL();

				S3Config = new AmazonS3Config
				{
					ServiceURL = URL,
					Timeout = TimeSpan.FromSeconds(S3_TIMEOUT),
					SignatureVersion = SignatureVersion,
					ForcePathStyle = true,
				};
			}

			if (!IsSecure) S3Config.UseHttp = true;

			Client = new AmazonS3Client(Credentials, S3Config);
		}

		/**************************************** Bucket ****************************************************/
		public ListBucketsResponse ListBuckets()
		{
			if (Client == null) return null;
			Task<ListBucketsResponse> Response = Client.ListBucketsAsync();
			Response.Wait();
			return Response.Result;
		}

		public PutBucketResponse PutBucket(string BucketName, S3CannedACL ACL = null, string RegionName = null,
			List<S3Grant> Grants = null, List<KeyValuePair<string, string>> HeaderList = null,
			bool? ObjectLockEnabledForBucket = null)
		{
			var Request = new PutBucketRequest() { BucketName = BucketName };
			if (ACL != null) Request.CannedACL = ACL;
			if (RegionName != null) Request.BucketRegionName = RegionName;
			if (HeaderList != null)
			{
				Client.BeforeRequestEvent += delegate (object sender, RequestEventArgs e)
				{

					var requestEvent = e as WebServiceRequestEventArgs;
					foreach (var Header in HeaderList)
						requestEvent.Headers.Add(Header.Key, Header.Value);
				};
			}
			if (Grants != null) Request.Grants = Grants;
			if (ObjectLockEnabledForBucket.HasValue) Request.ObjectLockEnabledForBucket = ObjectLockEnabledForBucket.Value;

			return Client.PutBucket(Request);
		}

		public DeleteBucketResponse DeleteBucket(string BucketName)
		{
			var Request = new DeleteBucketRequest() { BucketName = BucketName };
			return Client.DeleteBucket(Request);
		}

		public GetBucketLocationResponse GetBucketLocation(string BucketName)
		{
			var Request = new GetBucketLocationRequest() { BucketName = BucketName };
			return Client.GetBucketLocation(Request);
		}
		
		public PutBucketLoggingResponse PutBucketLogging(string BucketName, S3BucketLoggingConfig Config)
		{
			var Request = new PutBucketLoggingRequest() { BucketName = BucketName, LoggingConfig = Config };
			return Client.PutBucketLogging(Request);
		}
		public GetBucketLoggingResponse GetBucketLogging(string BucketName)
		{
			var Request = new GetBucketLoggingRequest() { BucketName = BucketName };
			return Client.GetBucketLogging(Request);
		}

		public PutBucketVersioningResponse PutBucketVersioning(string BucketName, bool? EnableMfaDelete = null, VersionStatus Status = null)
		{
			var Request = new PutBucketVersioningRequest() { BucketName = BucketName };

			if (EnableMfaDelete != null) Request.VersioningConfig.EnableMfaDelete = EnableMfaDelete.Value;
			if (Status != null) Request.VersioningConfig.Status = Status;

			return Client.PutBucketVersioning(Request);
		}

		public GetBucketVersioningResponse GetBucketVersioning(string BucketName)
		{
			var Request = new GetBucketVersioningRequest() { BucketName = BucketName };

			return Client.GetBucketVersioning(Request);
		}

		public GetACLResponse GetBucketACL(string BucketName)
		{
			var Request = new GetACLRequest()
			{
				BucketName = BucketName
			};
			return Client.GetACL(Request);
		}

		public PutACLResponse PutBucketACL(string BucketName, S3CannedACL ACL = null, S3AccessControlList AccessControlPolicy = null)
		{
			var Request = new PutACLRequest()
			{
				BucketName = BucketName
			};

			if (ACL != null) Request.CannedACL = ACL;
			if (AccessControlPolicy != null) Request.AccessControlList = AccessControlPolicy;

			return Client.PutACL(Request);
		}

		public PutCORSConfigurationResponse PutCORSConfiguration(string BucketName, CORSConfiguration Configuration)
		{
			var Request = new PutCORSConfigurationRequest()
			{
				BucketName = BucketName,
				Configuration = Configuration,
			};

			return Client.PutCORSConfiguration(Request);
		}

		public GetCORSConfigurationResponse GetCORSConfiguration(string BucketName)
		{
			var Request = new GetCORSConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetCORSConfiguration(Request);
		}

		public DeleteCORSConfigurationResponse DeleteCORSConfiguration(string BucketName)
		{
			var Request = new DeleteCORSConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return Client.DeleteCORSConfiguration(Request);
		}

		public GetBucketTaggingResponse GetBucketTagging(string BucketName)
		{
			var Request = new GetBucketTaggingRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetBucketTagging(Request);
		}

		public PutBucketTaggingResponse PutBucketTagging(string BucketName, List<Tag> TagSet)
		{
			var Request = new PutBucketTaggingRequest()
			{
				BucketName = BucketName,
				TagSet = TagSet,
			};

			return Client.PutBucketTagging(Request);
		}

		public DeleteBucketTaggingResponse DeleteBucketTagging(string BucketName)
		{
			var Request = new DeleteBucketTaggingRequest()
			{
				BucketName = BucketName,
			};

			return Client.DeleteBucketTagging(Request);
		}

		public PutLifecycleConfigurationResponse PutLifecycleConfiguration(string BucketName, LifecycleConfiguration Configuration)
		{
			var Request = new PutLifecycleConfigurationRequest()
			{
				BucketName = BucketName,
				Configuration = Configuration,
			};

			return Client.PutLifecycleConfiguration(Request);
		}
		public GetLifecycleConfigurationResponse GetLifecycleConfiguration(string BucketName)
		{
			var Request = new GetLifecycleConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetLifecycleConfiguration(Request);
		}
		public DeleteLifecycleConfigurationResponse DeleteLifecycleConfiguration(string BucketName)
		{
			var Request = new DeleteLifecycleConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return Client.DeleteLifecycleConfiguration(Request);
		}

		public PutBucketPolicyResponse PutBucketPolicy(string BucketName, string Policy)
		{
			var Request = new PutBucketPolicyRequest()
			{
				BucketName = BucketName,
				Policy = Policy,
			};

			return Client.PutBucketPolicy(Request);
		}
		public GetBucketPolicyResponse GetBucketPolicy(string BucketName)
		{
			var Request = new GetBucketPolicyRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetBucketPolicy(Request);
		}
		public GetBucketPolicyStatusResponse GetBucketPolicyStatus(string BucketName)
		{
			var Request = new GetBucketPolicyStatusRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetBucketPolicyStatus(Request);
		}

		public DeleteBucketPolicyResponse DeleteBucketPolicy(string BucketName)
		{
			var Request = new DeleteBucketPolicyRequest()
			{
				BucketName = BucketName,
			};

			return Client.DeleteBucketPolicy(Request);
		}
		public PutObjectLockConfigurationResponse PutObjectLockConfiguration(string BucketName, ObjectLockConfiguration ObjectLockConfiguration)
		{
			var Request = new PutObjectLockConfigurationRequest()
			{
				BucketName = BucketName,
				ObjectLockConfiguration = ObjectLockConfiguration,
			};

			return Client.PutObjectLockConfiguration(Request);
		}
		public GetObjectLockConfigurationResponse GetObjectLockConfiguration(string BucketName)
		{
			var Request = new GetObjectLockConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetObjectLockConfiguration(Request);
		}

		public PutPublicAccessBlockResponse PutPublicAccessBlock(string BucketName, PublicAccessBlockConfiguration PublicAccessBlockConfiguration)
		{
			var Request = new PutPublicAccessBlockRequest()
			{
				BucketName = BucketName,
				PublicAccessBlockConfiguration = PublicAccessBlockConfiguration,
			};

			return Client.PutPublicAccessBlock(Request);
		}
		public GetPublicAccessBlockResponse GetPublicAccessBlock(string BucketName)
		{
			var Request = new GetPublicAccessBlockRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetPublicAccessBlock(Request);
		}

		private DeletePublicAccessBlockResponse DeletePublicAccessBlock(DeletePublicAccessBlockRequest Request)
		{
			if (Client == null) return null;
			Task<DeletePublicAccessBlockResponse> Response = Client.DeletePublicAccessBlockAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeletePublicAccessBlockResponse DeletePublicAccessBlock(string BucketName)
		{
			var Request = new DeletePublicAccessBlockRequest()
			{
				BucketName = BucketName,
			};

			return DeletePublicAccessBlock(Request);
		}

		public GetBucketEncryptionResponse GetBucketEncryption(string BucketName)
		{
			var Request = new GetBucketEncryptionRequest()
			{
				BucketName = BucketName
			};

			return Client.GetBucketEncryption(Request);
		}
		public PutBucketEncryptionResponse PutBucketEncryption(string BucketName, ServerSideEncryptionConfiguration SSEConfig)
		{
			var Request = new PutBucketEncryptionRequest()
			{
				BucketName = BucketName,
				ServerSideEncryptionConfiguration = SSEConfig,
			};

			return Client.PutBucketEncryption(Request);
		}
		public DeleteBucketEncryptionResponse DeleteBucketEncryption(string BucketName)
		{
			var Request = new DeleteBucketEncryptionRequest()
			{
				BucketName = BucketName
			};

			return Client.DeleteBucketEncryption(Request);
		}

		public GetBucketWebsiteResponse GetBucketWebsite(string BucketName)
		{
			var Request = new GetBucketWebsiteRequest()
			{
				BucketName = BucketName
			};

			return Client.GetBucketWebsite(Request);
		}
		public PutBucketWebsiteResponse PutBucketWebsite(string BucketName, WebsiteConfiguration WebConfig)
		{
			var Request = new PutBucketWebsiteRequest()
			{
				BucketName = BucketName,
				WebsiteConfiguration = WebConfig,
			};

			return Client.PutBucketWebsite(Request);
		}
		public DeleteBucketWebsiteResponse DeleteBucketWebsite(string BucketName)
		{
			var Request = new DeleteBucketWebsiteRequest()
			{
				BucketName = BucketName
			};

			return Client.DeleteBucketWebsite(Request);
		}
		/**************************************** Object ****************************************************/
		public PutObjectResponse PutObject(string BucketName, string Key, string Body = null, byte[] ByteBody = null, string ContentType = null,
			string CacheControl = null, DateTime? Expires = null, string IfMatch = null, string IfNoneMatch = null,
			string MD5Digest = null, SSECustomerKey SSEC = null,
			List<KeyValuePair<string, string>> MetadataList = null, List<KeyValuePair<string, string>> HeaderList = null,
			S3CannedACL ACL = null, List<S3Grant> Grants = null, ServerSideEncryptionMethod SSE_S3_Method = null,
			ObjectLockLegalHoldStatus ObjectLockLegalHoldStatus = null, DateTime? ObjectLockRetainUntilDate = null,
			ObjectLockMode ObjectLockMode = null, bool? UseChunkEncoding = null, bool? DisablePayloadSigning = null)
		{
			var Request = new PutObjectRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			if (Body != null) Request.ContentBody = Body;
			if (ByteBody != null)
			{
				Stream MyStream = new MemoryStream(ByteBody);
				Request.InputStream = MyStream;
			}
			if (ContentType != null) Request.ContentType = ContentType;
			if (IfMatch != null) Request.Headers["If-Match"] = IfMatch;
			if (IfNoneMatch != null) Request.Headers["If-None-Match"] = IfNoneMatch;
			if (CacheControl != null) Request.Metadata[S3Headers.CacheControl] = CacheControl;
			if (Expires != null) Request.Metadata[S3Headers.Expires] = Expires.Value.ToString(S3Headers.TimeFormat, S3Headers.TimeCulture);
			if (MetadataList != null)
			{
				foreach (var MetaData in MetadataList)
					Request.Metadata[MetaData.Key] = MetaData.Value;
			}
			if (HeaderList != null)
			{
				foreach (var Header in HeaderList)
					Request.Headers[Header.Key] = Header.Value;
			}
			if (ACL != null) Request.CannedACL = ACL;
			if (Grants != null) Request.Grants = Grants;
			if (MD5Digest != null) Request.MD5Digest = MD5Digest;

			//Lock
			if (ObjectLockMode != null) Request.ObjectLockMode = ObjectLockMode;
			if (ObjectLockLegalHoldStatus != null) Request.ObjectLockLegalHoldStatus = ObjectLockLegalHoldStatus;
			if (ObjectLockRetainUntilDate.HasValue) Request.ObjectLockRetainUntilDate = ObjectLockRetainUntilDate.Value;

			//SSE-S3
			if (SSE_S3_Method != null) Request.ServerSideEncryptionMethod = SSE_S3_Method;

			//SSE-C
			if (SSEC != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSEC.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSEC.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSEC.MD5;
			}
			//ChunkEncoding Payload
			if (UseChunkEncoding.HasValue) Request.UseChunkEncoding = UseChunkEncoding.Value;

			//Disable Payload Signing
			if (DisablePayloadSigning.HasValue) Request.DisablePayloadSigning = DisablePayloadSigning;

			return Client.PutObject(Request);
		}

		public GetObjectResponse GetObject(
			string BucketName, string Key, string IfMatch = null, string IfNoneMatch = null, ByteRange Range = null, string VersionId = null, SSECustomerKey SSEC = null,
			string IfModifiedSince = null, DateTime? IfModifiedSinceDateTime = null,
			string IfUnmodifiedSince = null, DateTime? IfUnmodifiedSinceDateTime = null,
			string ResponseContentType = null, string ResponseContentLanguage = null, string ResponseExpires = null, string ResponseCacheControl = null,
			string ResponseContentDisposition = null, string ResponseContentEncoding = null)
		{
			var Request = new GetObjectRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			if (VersionId != null) Request.VersionId = VersionId;
			if (IfMatch != null) Request.EtagToMatch = IfMatch;
			if (IfNoneMatch != null) Request.EtagToNotMatch = IfNoneMatch;
			if (Range != null) Request.ByteRange = Range;

			//SSE-C
			if (SSEC != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSEC.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSEC.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSEC.MD5;
			}

			if (IfModifiedSince != null) Request.ModifiedSinceDateUtc = DateTime.Parse(IfModifiedSince);
			if (IfModifiedSinceDateTime != null) Request.ModifiedSinceDateUtc = IfModifiedSinceDateTime.Value;
			if (IfUnmodifiedSince != null) Request.UnmodifiedSinceDateUtc = DateTime.Parse(IfUnmodifiedSince);
			if (IfUnmodifiedSinceDateTime != null) Request.ModifiedSinceDateUtc = IfUnmodifiedSinceDateTime.Value;

			//ResponseHeaderOverrides
			if (ResponseContentType != null) Request.ResponseHeaderOverrides.ContentType = ResponseContentType;
			if (ResponseContentLanguage != null) Request.ResponseHeaderOverrides.ContentLanguage = ResponseContentLanguage;
			if (ResponseExpires != null) Request.ResponseHeaderOverrides.Expires = ResponseExpires;
			if (ResponseCacheControl != null) Request.ResponseHeaderOverrides.CacheControl = ResponseCacheControl;
			if (ResponseContentDisposition != null) Request.ResponseHeaderOverrides.ContentDisposition = ResponseContentDisposition;
			if (ResponseContentEncoding != null) Request.ResponseHeaderOverrides.ContentEncoding = ResponseContentEncoding;

			return Client.GetObject(Request);
		}

		public CopyObjectResponse CopyObject(string SourceBucket, string SourceKey, string BucketName, string Key,
			List<KeyValuePair<string, string>> MetadataList = null, S3MetadataDirective? MetadataDirective = null, ServerSideEncryptionMethod SSE_S3_Method = null,
			string VersionId = null, S3CannedACL ACL = null, string ETagToMatch = null, string ETagToNotMatch = null, string ContentType = null)
		{
			var Request = new CopyObjectRequest()
			{
				SourceBucket = SourceBucket,
				SourceKey = SourceKey,
				DestinationBucket = BucketName,
				DestinationKey = Key,
			};
			if (ACL != null) Request.CannedACL = ACL;
			if (ContentType != null) Request.ContentType = ContentType;
			if (MetadataList != null)
			{
				foreach (var MetaData in MetadataList)
					Request.Metadata[MetaData.Key] = MetaData.Value;
			}
			if (MetadataDirective != null) Request.MetadataDirective = MetadataDirective.Value;
			if (VersionId != null) Request.SourceVersionId = VersionId;
			if (ETagToMatch != null) Request.ETagToMatch = ETagToMatch;
			if (ETagToNotMatch != null) Request.ETagToNotMatch = ETagToNotMatch;

			//SSE-S3
			if (SSE_S3_Method != null) Request.ServerSideEncryptionMethod = SSE_S3_Method;

			return Client.CopyObject(Request);
		}

		public ListObjectsResponse ListObjects(string BucketName, string Delimiter = null, string Marker = null,
											int MaxKeys = -1, string Prefix = null, string EncodingTypeName = null)
		{
			var Request = new ListObjectsRequest() { BucketName = BucketName };

			if (Delimiter != null) Request.Delimiter = Delimiter;
			if (Marker != null) Request.Marker = Marker;
			if (Prefix != null) Request.Prefix = Prefix;
			if (EncodingTypeName != null) Request.Encoding = new EncodingType(EncodingTypeName);

			if (MaxKeys >= 0) Request.MaxKeys = MaxKeys;

			return Client.ListObjects(Request);

		}
		public ListObjectsV2Response ListObjectsV2(string BucketName, string Delimiter = null, string ContinuationToken = null,
					int MaxKeys = -1, string Prefix = null, string StartAfter = null, string EncodingTypeName = null,
					bool? FetchOwner = null)
		{
			var Request = new ListObjectsV2Request() { BucketName = BucketName };

			if (Delimiter != null) Request.Delimiter = Delimiter;
			if (ContinuationToken != null) Request.ContinuationToken = ContinuationToken;
			if (Prefix != null) Request.Prefix = Prefix;
			if (StartAfter != null) Request.StartAfter = StartAfter;
			if (EncodingTypeName != null) Request.Encoding = new EncodingType(EncodingTypeName);
			if (FetchOwner != null) Request.FetchOwner = FetchOwner.Value;

			if (MaxKeys >= 0) Request.MaxKeys = MaxKeys;

			return Client.ListObjectsV2(Request);

		}
		public ListVersionsResponse ListVersions(string BucketName, string Prefix = null)
		{
			var Request = new ListVersionsRequest()
			{
				BucketName = BucketName,
				MaxKeys = 2000
			};

			if (Prefix != null) Request.Prefix = Prefix;

			return Client.ListVersions(Request);
		}

		public DeleteObjectResponse DeleteObject(string BucketName, string Key, string VersionId = null,
			bool? BypassGovernanceRetention = null)
		{
			var Request = new DeleteObjectRequest()
			{
				BucketName = BucketName,
				Key = Key
			};

			if (VersionId != null) Request.VersionId = VersionId;
			if (BypassGovernanceRetention.HasValue) Request.BypassGovernanceRetention = BypassGovernanceRetention.Value;

			return Client.DeleteObject(Request);
		}

		public DeleteObjectsResponse DeleteObjects(string BucketName, List<KeyVersion> KeyList, bool? Quiet = null)
		{
			var Request = new DeleteObjectsRequest()
			{
				BucketName = BucketName,
				Objects = KeyList
			};

			if (Quiet.HasValue) Request.Quiet = Quiet.Value;

			return Client.DeleteObjects(Request);
		}

		public GetObjectMetadataResponse GetObjectMetadata(string BucketName, string Key, string VersionId = null, SSECustomerKey SSEC = null)
		{
			var Request = new GetObjectMetadataRequest()
			{
				BucketName = BucketName,
				Key = Key
			};

			//Version
			if (VersionId != null) Request.VersionId = VersionId;

			//SSE-C
			if (SSEC != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSEC.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSEC.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSEC.MD5;
			}

			return Client.GetObjectMetadata(Request);
		}

		public PutACLResponse PutObjectACL(string BucketName, string Key, S3CannedACL ACL = null, S3AccessControlList AccessControlPolicy = null)
		{
			var Request = new PutACLRequest()
			{
				BucketName = BucketName,
				Key = Key
			};
			if (ACL != null) Request.CannedACL = ACL;
			if (AccessControlPolicy != null) Request.AccessControlList = AccessControlPolicy;

			return Client.PutACL(Request);
		}
		public GetACLResponse GetObjectACL(string BucketName, string Key, string VersionId = null)
		{
			var Request = new GetACLRequest()
			{
				BucketName = BucketName,
				Key = Key
			};

			if (VersionId != null) Request.VersionId = VersionId;

			return Client.GetACL(Request);
		}

		public GetObjectTaggingResponse GetObjectTagging(string BucketName, string Key)
		{
			var Request = new GetObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return Client.GetObjectTagging(Request);
		}

		public PutObjectTaggingResponse PutObjectTagging(string BucketName, string Key, Tagging Tagging)
		{
			var Request = new PutObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
				Tagging = Tagging,
			};

			return Client.PutObjectTagging(Request);
		}
		public DeleteObjectTaggingResponse DeleteObjectTagging(string BucketName, string Key)
		{
			var Request = new DeleteObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return Client.DeleteObjectTagging(Request);
		}
		public GetObjectRetentionResponse GetObjectRetention(string BucketName, string Key)
		{
			var Request = new GetObjectRetentionRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return Client.GetObjectRetention(Request);
		}
		public PutObjectRetentionResponse PutObjectRetention(string BucketName, string Key, ObjectLockRetention Retention,
			string ContentMD5 = null, string VersionId = null, bool? BypassGovernanceRetention = null)
		{
			var Request = new PutObjectRetentionRequest()
			{
				BucketName = BucketName,
				Key = Key,
				Retention = Retention,
			};
			if (ContentMD5 != null) Request.ContentMD5 = ContentMD5;
			if (VersionId != null) Request.VersionId = VersionId;
			if (BypassGovernanceRetention.HasValue) Request.BypassGovernanceRetention = BypassGovernanceRetention.Value;

			return Client.PutObjectRetention(Request);
		}

		public PutObjectLegalHoldResponse PutObjectLegalHold(string BucketName, string Key, ObjectLockLegalHold LegalHold)
		{
			var Request = new PutObjectLegalHoldRequest()
			{
				BucketName = BucketName,
				Key = Key,
				LegalHold = LegalHold,
			};

			return Client.PutObjectLegalHold(Request);
		}
		public GetObjectLegalHoldResponse GetObjectLegalHold(string BucketName, string Key)
		{
			var Request = new GetObjectLegalHoldRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return Client.GetObjectLegalHold(Request);
		}

		public GetBucketReplicationResponse GetBucketReplication(string BucketName)
		{
			var Request = new GetBucketReplicationRequest()
			{
				BucketName = BucketName,
			};

			return Client.GetBucketReplication(Request);
		}
		public PutBucketReplicationResponse PutBucketReplication(string BucketName, ReplicationConfiguration Configuration,
			string Token = null, string ExpectedBucketOwner = null)
		{
			var Request = new PutBucketReplicationRequest()
			{
				BucketName = BucketName,
				Configuration = Configuration
			};

			if (!string.IsNullOrWhiteSpace(Token)) Request.Token = Token;
			if (!string.IsNullOrWhiteSpace(ExpectedBucketOwner)) Request.ExpectedBucketOwner = ExpectedBucketOwner;

			return Client.PutBucketReplication(Request);
		}
		public DeleteBucketReplicationResponse DeleteBucketReplication(string BucketName)
		{
			var Request = new DeleteBucketReplicationRequest()
			{
				BucketName = BucketName,
			};

			return Client.DeleteBucketReplication(Request);
		}
		/**************************************** MultiPart **************************************************/
		public InitiateMultipartUploadResponse InitiateMultipartUpload(string BucketName, string Key, string ContentType = null,
			List<KeyValuePair<string, string>> MetadataList = null, ServerSideEncryptionMethod SSE_S3 = null, SSECustomerKey SSEC = null)
		{
			var Request = new InitiateMultipartUploadRequest()
			{
				BucketName = BucketName,
				Key = Key
			};
			if (MetadataList != null)
			{
				foreach (var MetaData in MetadataList)
					Request.Metadata[MetaData.Key] = MetaData.Value;
			}
			if (ContentType != null) Request.ContentType = ContentType;

			//SSE-S3
			if (SSE_S3 != null) Request.ServerSideEncryptionMethod = SSE_S3;

			//SSE-C
			if (SSEC != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSEC.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSEC.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSEC.MD5;
			}

			return Client.InitiateMultipartUpload(Request);
		}

		public UploadPartResponse UploadPart(string BucketName, string Key, string UploadId, string Body, int PartNumber, SSECustomerKey SSEC = null)
		{
			var Request = new UploadPartRequest()
			{
				BucketName = BucketName,
				Key = Key,
				PartNumber = PartNumber,
				UploadId = UploadId,
				InputStream = new MemoryStream(Encoding.ASCII.GetBytes(Body))
			};

			//SSE-C
			if (SSEC != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSEC.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSEC.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSEC.MD5;
			}

			return Client.UploadPart(Request);
		}

		public CopyPartResponse CopyPart(string SrcBucketName, string SrcKey, string DestBucketName, string DestKey, string UploadId,
			int PartNumber, int Start, int End, string VersionId = null, SSECustomerKey SrcSSEC = null, SSECustomerKey DestSSEC = null)
		{
			var Request = new CopyPartRequest()
			{
				SourceBucket = SrcBucketName,
				SourceKey = SrcKey,
				DestinationBucket = DestBucketName,
				DestinationKey = DestKey,
				UploadId = UploadId,
				PartNumber = PartNumber,
				FirstByte = Start,
				LastByte = End,
			};

			if (VersionId != null) Request.SourceVersionId = VersionId;

			////SSE-C
			if (SrcSSEC != null)
			{
				Request.CopySourceServerSideEncryptionCustomerMethod = SrcSSEC.Method;
				Request.CopySourceServerSideEncryptionCustomerProvidedKey = SrcSSEC.ProvidedKey;
				Request.CopySourceServerSideEncryptionCustomerProvidedKeyMD5 = SrcSSEC.MD5;
			}
			if (DestSSEC != null)
			{
				Request.ServerSideEncryptionCustomerMethod = DestSSEC.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = DestSSEC.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = DestSSEC.MD5;
			}

			return Client.CopyPart(Request);
		}

		public CompleteMultipartUploadResponse CompleteMultipartUpload(string BucketName, string Key, string UploadId, List<PartETag> Parts)
		{
			var Request = new CompleteMultipartUploadRequest()
			{
				BucketName = BucketName,
				Key = Key,
				UploadId = UploadId,
				PartETags = Parts
			};

			return Client.CompleteMultipartUpload(Request);
		}

		public AbortMultipartUploadResponse AbortMultipartUpload(string BucketNume, string Key, string UploadId)
		{
			var Request = new AbortMultipartUploadRequest()
			{
				BucketName = BucketNume,
				Key = Key,
				UploadId = UploadId,
			};

			return Client.AbortMultipartUpload(Request);
		}

		public ListMultipartUploadsResponse ListMultipartUploads(string BucketNume)
		{
			var Request = new ListMultipartUploadsRequest()
			{
				BucketName = BucketNume
			};

			return Client.ListMultipartUploads(Request);
		}

		public ListPartsResponse ListParts(string BucketNume, string Key, string UploadId)
		{
			var Request = new ListPartsRequest()
			{
				BucketName = BucketNume,
				Key = Key,
				UploadId = UploadId,
			};

			return Client.ListParts(Request);
		}
		/**************************************** URL ****************************************************/
		public string GeneratePresignedURL(string BucketName, string Key, DateTime Expires, HttpVerb Verb)
		{
			var Request = new GetPreSignedUrlRequest()
			{
				BucketName = BucketName,
				Key = Key,
				Expires = Expires,
				Verb = Verb,
				Protocol = Protocol.HTTP
			};

			return Client.GetPreSignedURL(Request);
		}
		/**************************************** Utility ****************************************************/
		private static string CreateURLToHTTP(string Address, int Port)
		{
			string URL;
			if (Address.StartsWith(MainData.HTTPS, StringComparison.OrdinalIgnoreCase)) URL = Address.Replace(MainData.HTTPS, MainData.HTTP);
			else if (Address.StartsWith(MainData.HTTP, StringComparison.OrdinalIgnoreCase)) URL = Address;
			else URL = MainData.HTTP + Address;

			if (URL.EndsWith("/")) URL = URL.Substring(0, URL.Length - 1);

			return string.Format("{0}:{1}", URL, Port);
		}
		private static string CreateURLToHTTPS(string Address, int Port)
		{
			string URL;
			if (Address.StartsWith(MainData.HTTP, StringComparison.OrdinalIgnoreCase)) URL = Address.Replace(MainData.HTTP, MainData.HTTPS);
			else if (Address.StartsWith(MainData.HTTPS, StringComparison.OrdinalIgnoreCase)) URL = Address;
			else URL = MainData.HTTPS + Address;

			if (URL.EndsWith("/")) URL = URL.Substring(0, URL.Length - 1);

			return string.Format("{0}:{1}", URL, Port);
		}
	}
}
