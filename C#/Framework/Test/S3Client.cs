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
				S3Config = new AmazonS3Config
				{
					ServiceURL = IsSecure ? S3.GetHttpsURL() : S3.GetHttpURL(),
					Timeout = TimeSpan.FromSeconds(S3_TIMEOUT),
					SignatureVersion = SignatureVersion,
					ForcePathStyle = true,
				};

				if (IsSecure) ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => true;
			}

			if (!IsSecure) S3Config.UseHttp = true;

			Client = new AmazonS3Client(Credentials, S3Config);
		}

		#region Bucket Function

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

		public DeleteBucketResponse DeleteBucket(string BucketName) => Client.DeleteBucket(BucketName);

		public GetBucketLocationResponse GetBucketLocation(string BucketName) => Client.GetBucketLocation(BucketName);

		public PutBucketLoggingResponse PutBucketLogging(string BucketName, S3BucketLoggingConfig Config) =>
			Client.PutBucketLogging(new PutBucketLoggingRequest()
			{
				BucketName = BucketName,
				LoggingConfig = Config
			});

		public GetBucketLoggingResponse GetBucketLogging(string BucketName) => Client.GetBucketLogging(BucketName);

		public PutBucketVersioningResponse PutBucketVersioning(string BucketName, bool? EnableMfaDelete = null, VersionStatus Status = null)
		{
			var Request = new PutBucketVersioningRequest() { BucketName = BucketName };

			if (EnableMfaDelete != null) Request.VersioningConfig.EnableMfaDelete = EnableMfaDelete.Value;
			if (Status != null) Request.VersioningConfig.Status = Status;

			return Client.PutBucketVersioning(Request);
		}

		public GetBucketVersioningResponse GetBucketVersioning(string BucketName) => Client.GetBucketVersioning(BucketName);

		public GetACLResponse GetBucketACL(string BucketName) => Client.GetACL(BucketName);

		public PutACLResponse PutBucketACL(string BucketName, S3CannedACL ACL = null, S3AccessControlList AccessControlPolicy = null)
		{
			var Request = new PutACLRequest() { BucketName = BucketName };

			if (ACL != null) Request.CannedACL = ACL;
			if (AccessControlPolicy != null) Request.AccessControlList = AccessControlPolicy;

			return Client.PutACL(Request);
		}

		public PutCORSConfigurationResponse PutCORSConfiguration(string BucketName, CORSConfiguration Configuration) => Client.PutCORSConfiguration(BucketName, Configuration);

		public GetCORSConfigurationResponse GetCORSConfiguration(string BucketName) => Client.GetCORSConfiguration(BucketName);

		public DeleteCORSConfigurationResponse DeleteCORSConfiguration(string BucketName) => Client.DeleteCORSConfiguration(BucketName);

		public GetBucketTaggingResponse GetBucketTagging(string BucketName) => Client.GetBucketTagging(new GetBucketTaggingRequest() { BucketName = BucketName });

		public PutBucketTaggingResponse PutBucketTagging(string BucketName, List<Tag> TagSet) => Client.PutBucketTagging(BucketName, TagSet);

		public DeleteBucketTaggingResponse DeleteBucketTagging(string BucketName) => Client.DeleteBucketTagging(BucketName);

		public PutLifecycleConfigurationResponse PutLifecycleConfiguration(string BucketName, LifecycleConfiguration Configuration)
			=> Client.PutLifecycleConfiguration(BucketName, Configuration);
		public GetLifecycleConfigurationResponse GetLifecycleConfiguration(string BucketName) => Client.GetLifecycleConfiguration(BucketName);
		public DeleteLifecycleConfigurationResponse DeleteLifecycleConfiguration(string BucketName) => Client.DeleteLifecycleConfiguration(BucketName);

		public PutBucketPolicyResponse PutBucketPolicy(string BucketName, string Policy) => Client.PutBucketPolicy(BucketName, Policy);
		public GetBucketPolicyResponse GetBucketPolicy(string BucketName) => Client.GetBucketPolicy(BucketName);

		public GetBucketPolicyStatusResponse GetBucketPolicyStatus(string BucketName) => Client.GetBucketPolicyStatus(new GetBucketPolicyStatusRequest() { BucketName = BucketName });

		public DeleteBucketPolicyResponse DeleteBucketPolicy(string BucketName) => Client.DeleteBucketPolicy(BucketName);
		public PutObjectLockConfigurationResponse PutObjectLockConfiguration(string BucketName, ObjectLockConfiguration ObjectLockConfiguration)
			=> Client.PutObjectLockConfiguration(new PutObjectLockConfigurationRequest() { BucketName = BucketName, ObjectLockConfiguration = ObjectLockConfiguration });
		public GetObjectLockConfigurationResponse GetObjectLockConfiguration(string BucketName)
			=> Client.GetObjectLockConfiguration(new GetObjectLockConfigurationRequest() { BucketName = BucketName });

		public PutPublicAccessBlockResponse PutPublicAccessBlock(string BucketName, PublicAccessBlockConfiguration PublicAccessBlockConfiguration)
			=> Client.PutPublicAccessBlock(new PutPublicAccessBlockRequest()
			{
				BucketName = BucketName,
				PublicAccessBlockConfiguration = PublicAccessBlockConfiguration,
			});
		public GetPublicAccessBlockResponse GetPublicAccessBlock(string BucketName)
			=> Client.GetPublicAccessBlock(new GetPublicAccessBlockRequest()
			{
				BucketName = BucketName,
			});

		public DeletePublicAccessBlockResponse DeletePublicAccessBlock(string BucketName) => Client.DeletePublicAccessBlock(new DeletePublicAccessBlockRequest() { BucketName = BucketName });

		public GetBucketEncryptionResponse GetBucketEncryption(string BucketName) => Client.GetBucketEncryption(new GetBucketEncryptionRequest() { BucketName = BucketName });
		public PutBucketEncryptionResponse PutBucketEncryption(string BucketName, ServerSideEncryptionConfiguration SSEConfig)
			=> Client.PutBucketEncryption(new PutBucketEncryptionRequest()
			{
				BucketName = BucketName,
				ServerSideEncryptionConfiguration = SSEConfig,
			});
		public DeleteBucketEncryptionResponse DeleteBucketEncryption(string BucketName) => Client.DeleteBucketEncryption(new DeleteBucketEncryptionRequest() { BucketName = BucketName });

		public GetBucketWebsiteResponse GetBucketWebsite(string BucketName) => Client.GetBucketWebsite(BucketName);
		public PutBucketWebsiteResponse PutBucketWebsite(string BucketName, WebsiteConfiguration WebConfig) => Client.PutBucketWebsite(BucketName, WebConfig);
		public DeleteBucketWebsiteResponse DeleteBucketWebsite(string BucketName) => Client.DeleteBucketWebsite(BucketName);
		#endregion
		#region Object Function
		public PutObjectResponse PutObject(string BucketName, string Key, string Body = null, byte[] ByteBody = null, string ContentType = null,
			string CacheControl = null, DateTime? Expires = null, string IfMatch = null, string IfNoneMatch = null,
			string MD5Digest = null, SSECustomerKey SSE_C = null,
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
			if (SSE_C != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSE_C.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSE_C.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSE_C.MD5;
			}
			//ChunkEncoding Payload
			if (UseChunkEncoding.HasValue) Request.UseChunkEncoding = UseChunkEncoding.Value;

			//Disable Payload Signing
			if (DisablePayloadSigning.HasValue) Request.DisablePayloadSigning = DisablePayloadSigning;

			return Client.PutObject(Request);
		}

		public GetObjectResponse GetObject(
			string BucketName, string Key, string IfMatch = null, string IfNoneMatch = null, ByteRange Range = null, string VersionId = null, SSECustomerKey SSE_C = null,
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
			if (SSE_C != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSE_C.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSE_C.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSE_C.MD5;
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
			List<KeyValuePair<string, string>> MetadataList = null, S3MetadataDirective? MetadataDirective = null,
			ServerSideEncryptionMethod SSE_S3_Method = null, SSECustomerKey SSE_C = null, SSECustomerKey SSE_C_Source = null,
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

			if (SSE_C != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSE_C.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSE_C.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSE_C.MD5;
			}
			if (SSE_C_Source != null)
			{
				Request.CopySourceServerSideEncryptionCustomerMethod = SSE_C_Source.Method;
				Request.CopySourceServerSideEncryptionCustomerProvidedKey = SSE_C_Source.ProvidedKey;
				Request.CopySourceServerSideEncryptionCustomerProvidedKeyMD5 = SSE_C_Source.MD5;
			}

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

		public GetObjectMetadataResponse GetObjectMetadata(string BucketName, string Key, string VersionId = null, SSECustomerKey SSE_C = null)
		{
			var Request = new GetObjectMetadataRequest()
			{
				BucketName = BucketName,
				Key = Key
			};

			//Version
			if (VersionId != null) Request.VersionId = VersionId;

			//SSE-C
			if (SSE_C != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSE_C.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSE_C.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSE_C.MD5;
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
			=> Client.GetObjectTagging(new GetObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
			});

		public PutObjectTaggingResponse PutObjectTagging(string BucketName, string Key, Tagging Tagging)
			=> Client.PutObjectTagging(new PutObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
				Tagging = Tagging,
			});
		public DeleteObjectTaggingResponse DeleteObjectTagging(string BucketName, string Key)
			=> Client.DeleteObjectTagging(new DeleteObjectTaggingRequest() { BucketName = BucketName, Key = Key, });

		public GetObjectRetentionResponse GetObjectRetention(string BucketName, string Key)
			=> Client.GetObjectRetention(new GetObjectRetentionRequest() { BucketName = BucketName, Key = Key, });
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
			=> Client.PutObjectLegalHold(new PutObjectLegalHoldRequest()
			{
				BucketName = BucketName,
				Key = Key,
				LegalHold = LegalHold,
			});
		public GetObjectLegalHoldResponse GetObjectLegalHold(string BucketName, string Key)
			=> Client.GetObjectLegalHold(new GetObjectLegalHoldRequest() { BucketName = BucketName, Key = Key, });

		public GetBucketReplicationResponse GetBucketReplication(string BucketName)
			=> Client.GetBucketReplication(new GetBucketReplicationRequest() { BucketName = BucketName });
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
		public DeleteBucketReplicationResponse DeleteBucketReplication(string BucketName) => Client.DeleteBucketReplication(new DeleteBucketReplicationRequest() { BucketName = BucketName });

		public RestoreObjectResponse RestoreObject(string BucketName, string Key, string VersionId = null)
		{
			var Request = new RestoreObjectRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			if (VersionId != null) Request.VersionId = VersionId;

			return Client.RestoreObject(Request);
		}
		#endregion

		#region Multipart Function
		public InitiateMultipartUploadResponse InitiateMultipartUpload(string BucketName, string Key, string ContentType = null,
			List<KeyValuePair<string, string>> MetadataList = null, ServerSideEncryptionMethod SSE_S3 = null, SSECustomerKey SSE_C = null)
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
			if (SSE_C != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSE_C.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSE_C.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSE_C.MD5;
			}

			return Client.InitiateMultipartUpload(Request);
		}

		public UploadPartResponse UploadPart(string BucketName, string Key, string UploadId, string Body, int PartNumber, SSECustomerKey SSE_C = null)
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
			if (SSE_C != null)
			{
				Request.ServerSideEncryptionCustomerMethod = SSE_C.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = SSE_C.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = SSE_C.MD5;
			}

			return Client.UploadPart(Request);
		}

		public CopyPartResponse CopyPart(string SrcBucketName, string SrcKey, string DestBucketName, string DestKey, string UploadId,
			int PartNumber, int Start, int End, string VersionId = null, SSECustomerKey SrcSSE_C = null, SSECustomerKey DestSSE_C = null)
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
			if (SrcSSE_C != null)
			{
				Request.CopySourceServerSideEncryptionCustomerMethod = SrcSSE_C.Method;
				Request.CopySourceServerSideEncryptionCustomerProvidedKey = SrcSSE_C.ProvidedKey;
				Request.CopySourceServerSideEncryptionCustomerProvidedKeyMD5 = SrcSSE_C.MD5;
			}
			if (DestSSE_C != null)
			{
				Request.ServerSideEncryptionCustomerMethod = DestSSE_C.Method;
				Request.ServerSideEncryptionCustomerProvidedKey = DestSSE_C.ProvidedKey;
				Request.ServerSideEncryptionCustomerProvidedKeyMD5 = DestSSE_C.MD5;
			}

			return Client.CopyPart(Request);
		}

		public CompleteMultipartUploadResponse CompleteMultipartUpload(string BucketName, string Key, string UploadId, List<PartETag> Parts)
			=> Client.CompleteMultipartUpload(new CompleteMultipartUploadRequest()
			{
				BucketName = BucketName,
				Key = Key,
				UploadId = UploadId,
				PartETags = Parts
			});

		public AbortMultipartUploadResponse AbortMultipartUpload(string BucketNume, string Key, string UploadId)
			=> Client.AbortMultipartUpload(new AbortMultipartUploadRequest()
			{
				BucketName = BucketNume,
				Key = Key,
				UploadId = UploadId,
			});

		public ListMultipartUploadsResponse ListMultipartUploads(string BucketNume)
		{
			var Request = new ListMultipartUploadsRequest()
			{
				BucketName = BucketNume
			};

			return Client.ListMultipartUploads(Request);
		}

		public ListPartsResponse ListParts(string BucketNume, string Key, string UploadId, int MaxParts = 1000, int PartNumberMarker = 0)
		{
			var Request = new ListPartsRequest()
			{
				BucketName = BucketNume,
				Key = Key,
				UploadId = UploadId,
				MaxParts = MaxParts,
				PartNumberMarker = PartNumberMarker.ToString(),
			};

			return Client.ListParts(Request);
		}
		#endregion

		#region ETC Function
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
		#endregion
	}
}
