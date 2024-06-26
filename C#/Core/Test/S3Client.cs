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
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace s3tests
{
	public class S3Client
	{
		private const int S3_TIMEOUT = 3600;
		public ITestOutputHelper Output;

		public readonly AmazonS3Client Client = null;
		public S3Client(AmazonS3Client Client) => this.Client = Client;

		public S3Client(S3Config S3, string SignatureVersion, bool IsSecure, UserData User, ITestOutputHelper Output = null)
		{
			if (Output != null) this.Output = Output;

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
			var Response = Client.ListBucketsAsync();
			Response.Wait();
			return Response.Result;
		}

		private PutBucketResponse PutBucket(PutBucketRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketAsync(Request);
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

			return PutBucket(Request);
		}

		private DeleteBucketResponse DeleteBucket(DeleteBucketRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteBucketAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteBucketResponse DeleteBucket(string BucketName)
		{
			var Request = new DeleteBucketRequest() { BucketName = BucketName };
			return DeleteBucket(Request);
		}

		private GetBucketLocationResponse GetBucketLocation(GetBucketLocationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketLocationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketLocationResponse GetBucketLocation(string BucketName)
		{
			var Request = new GetBucketLocationRequest() { BucketName = BucketName };
			return GetBucketLocation(Request);
		}

		private PutBucketLoggingResponse PutBucketLogging(PutBucketLoggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketLoggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutBucketLoggingResponse PutBucketLogging(string BucketName, S3BucketLoggingConfig Config)
		{
			var Request = new PutBucketLoggingRequest() { BucketName = BucketName, LoggingConfig = Config };
			return PutBucketLogging(Request);
		}
		private GetBucketLoggingResponse GetBucketLogging(GetBucketLoggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketLoggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketLoggingResponse GetBucketLogging(string BucketName)
		{
			var Request = new GetBucketLoggingRequest() { BucketName = BucketName };
			return GetBucketLogging(Request);
		}

		private PutBucketVersioningResponse PutBucketVersioning(PutBucketVersioningRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketVersioningAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutBucketVersioningResponse PutBucketVersioning(string BucketName, bool? EnableMfaDelete = null, VersionStatus Status = null)
		{
			var Request = new PutBucketVersioningRequest() { BucketName = BucketName };

			if (EnableMfaDelete != null) Request.VersioningConfig.EnableMfaDelete = EnableMfaDelete.Value;
			if (Status != null) Request.VersioningConfig.Status = Status;

			return PutBucketVersioning(Request);
		}

		private GetBucketVersioningResponse GetBucketVersioning(GetBucketVersioningRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketVersioningAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketVersioningResponse GetBucketVersioning(string BucketName)
		{
			var Request = new GetBucketVersioningRequest() { BucketName = BucketName };

			return GetBucketVersioning(Request);
		}

		private GetACLResponse GetBucketACL(GetACLRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetACLAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetACLResponse GetBucketACL(string BucketName)
		{
			var Request = new GetACLRequest()
			{
				BucketName = BucketName
			};
			return GetBucketACL(Request);
		}

		private PutACLResponse PutBucketACL(PutACLRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutACLAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutACLResponse PutBucketACL(string BucketName, S3CannedACL ACL = null, S3AccessControlList AccessControlPolicy = null)
		{
			var Request = new PutACLRequest()
			{
				BucketName = BucketName
			};

			if (ACL != null) Request.CannedACL = ACL;
			if (AccessControlPolicy != null) Request.AccessControlList = AccessControlPolicy;

			return PutBucketACL(Request);
		}

		private PutCORSConfigurationResponse PutCORSConfiguration(PutCORSConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutCORSConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutCORSConfigurationResponse PutCORSConfiguration(string BucketName, CORSConfiguration Configuration)
		{
			var Request = new PutCORSConfigurationRequest()
			{
				BucketName = BucketName,
				Configuration = Configuration,
			};

			return PutCORSConfiguration(Request);
		}

		private GetCORSConfigurationResponse GetCORSConfiguration(GetCORSConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetCORSConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetCORSConfigurationResponse GetCORSConfiguration(string BucketName)
		{
			var Request = new GetCORSConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return GetCORSConfiguration(Request);
		}

		private DeleteCORSConfigurationResponse DeleteCORSConfiguration(DeleteCORSConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteCORSConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteCORSConfigurationResponse DeleteCORSConfiguration(string BucketName)
		{
			var Request = new DeleteCORSConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return DeleteCORSConfiguration(Request);
		}

		private GetBucketTaggingResponse GetBucketTagging(GetBucketTaggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketTaggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketTaggingResponse GetBucketTagging(string BucketName)
		{
			var Request = new GetBucketTaggingRequest()
			{
				BucketName = BucketName,
			};

			return GetBucketTagging(Request);
		}

		private PutBucketTaggingResponse PutBucketTagging(PutBucketTaggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketTaggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutBucketTaggingResponse PutBucketTagging(string BucketName, List<Tag> TagSet)
		{
			var Request = new PutBucketTaggingRequest()
			{
				BucketName = BucketName,
				TagSet = TagSet,
			};

			return PutBucketTagging(Request);
		}

		private DeleteBucketTaggingResponse DeleteBucketTagging(DeleteBucketTaggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteBucketTaggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteBucketTaggingResponse DeleteBucketTagging(string BucketName)
		{
			var Request = new DeleteBucketTaggingRequest()
			{
				BucketName = BucketName,
			};

			return DeleteBucketTagging(Request);
		}

		private PutLifecycleConfigurationResponse PutLifecycleConfiguration(PutLifecycleConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutLifecycleConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutLifecycleConfigurationResponse PutLifecycleConfiguration(string BucketName, LifecycleConfiguration Configuration)
		{
			var Request = new PutLifecycleConfigurationRequest()
			{
				BucketName = BucketName,
				Configuration = Configuration,
			};

			return PutLifecycleConfiguration(Request);
		}

		private GetLifecycleConfigurationResponse GetLifecycleConfiguration(GetLifecycleConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetLifecycleConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetLifecycleConfigurationResponse GetLifecycleConfiguration(string BucketName)
		{
			var Request = new GetLifecycleConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return GetLifecycleConfiguration(Request);
		}

		private DeleteLifecycleConfigurationResponse DeleteLifecycleConfiguration(DeleteLifecycleConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteLifecycleConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteLifecycleConfigurationResponse DeleteLifecycleConfiguration(string BucketName)
		{
			var Request = new DeleteLifecycleConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return DeleteLifecycleConfiguration(Request);
		}

		private PutBucketPolicyResponse PutBucketPolicy(PutBucketPolicyRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketPolicyAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutBucketPolicyResponse PutBucketPolicy(string BucketName, string Policy)
		{
			var Request = new PutBucketPolicyRequest()
			{
				BucketName = BucketName,
				Policy = Policy,
			};

			return PutBucketPolicy(Request);
		}

		private GetBucketPolicyResponse GetBucketPolicy(GetBucketPolicyRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketPolicyAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketPolicyResponse GetBucketPolicy(string BucketName)
		{
			var Request = new GetBucketPolicyRequest()
			{
				BucketName = BucketName,
			};

			return GetBucketPolicy(Request);
		}

		private DeleteBucketPolicyResponse DeleteBucketPolicy(DeleteBucketPolicyRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteBucketPolicyAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteBucketPolicyResponse DeleteBucketPolicy(string BucketName)
		{
			var Request = new DeleteBucketPolicyRequest()
			{
				BucketName = BucketName,
			};

			return DeleteBucketPolicy(Request);
		}

		private GetBucketPolicyStatusResponse GetBucketPolicyStatus(GetBucketPolicyStatusRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketPolicyStatusAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketPolicyStatusResponse GetBucketPolicyStatus(string BucketName)
		{
			var Request = new GetBucketPolicyStatusRequest()
			{
				BucketName = BucketName,
			};

			return GetBucketPolicyStatus(Request);
		}

		private PutObjectLockConfigurationResponse PutObjectLockConfiguration(PutObjectLockConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutObjectLockConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutObjectLockConfigurationResponse PutObjectLockConfiguration(string BucketName, ObjectLockConfiguration ObjectLockConfiguration)
		{
			var Request = new PutObjectLockConfigurationRequest()
			{
				BucketName = BucketName,
				ObjectLockConfiguration = ObjectLockConfiguration,
			};

			return PutObjectLockConfiguration(Request);
		}

		private GetObjectLockConfigurationResponse GetObjectLockConfiguration(GetObjectLockConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetObjectLockConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetObjectLockConfigurationResponse GetObjectLockConfiguration(string BucketName)
		{
			var Request = new GetObjectLockConfigurationRequest()
			{
				BucketName = BucketName,
			};

			return GetObjectLockConfiguration(Request);
		}

		private PutPublicAccessBlockResponse PutPublicAccessBlock(PutPublicAccessBlockRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutPublicAccessBlockAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutPublicAccessBlockResponse PutPublicAccessBlock(string BucketName, PublicAccessBlockConfiguration PublicAccessBlockConfiguration)
		{
			var Request = new PutPublicAccessBlockRequest()
			{
				BucketName = BucketName,
				PublicAccessBlockConfiguration = PublicAccessBlockConfiguration,
			};

			return PutPublicAccessBlock(Request);
		}

		private GetPublicAccessBlockResponse GetPublicAccessBlock(GetPublicAccessBlockRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetPublicAccessBlockAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetPublicAccessBlockResponse GetPublicAccessBlock(string BucketName)
		{
			var Request = new GetPublicAccessBlockRequest()
			{
				BucketName = BucketName,
			};

			return GetPublicAccessBlock(Request);
		}

		private DeletePublicAccessBlockResponse DeletePublicAccessBlock(DeletePublicAccessBlockRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeletePublicAccessBlockAsync(Request);
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

		private GetBucketEncryptionResponse GetBucketEncryption(GetBucketEncryptionRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketEncryptionAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketEncryptionResponse GetBucketEncryption(string BucketName)
		{
			var Request = new GetBucketEncryptionRequest()
			{
				BucketName = BucketName
			};

			return GetBucketEncryption(Request);
		}

		private PutBucketEncryptionResponse PutBucketEncryption(PutBucketEncryptionRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketEncryptionAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutBucketEncryptionResponse PutBucketEncryption(string BucketName, ServerSideEncryptionConfiguration SSEConfig)
		{
			var Request = new PutBucketEncryptionRequest()
			{
				BucketName = BucketName,
				ServerSideEncryptionConfiguration = SSEConfig,
			};

			return PutBucketEncryption(Request);
		}

		private DeleteBucketEncryptionResponse DeleteBucketEncryption(DeleteBucketEncryptionRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteBucketEncryptionAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteBucketEncryptionResponse DeleteBucketEncryption(string BucketName)
		{
			var Request = new DeleteBucketEncryptionRequest()
			{
				BucketName = BucketName
			};

			return DeleteBucketEncryption(Request);
		}

		private GetBucketWebsiteResponse GetBucketWebsite(GetBucketWebsiteRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketWebsiteAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketWebsiteResponse GetBucketWebsite(string BucketName)
		{
			var Request = new GetBucketWebsiteRequest()
			{
				BucketName = BucketName
			};

			return GetBucketWebsite(Request);
		}

		private PutBucketWebsiteResponse PutBucketWebsite(PutBucketWebsiteRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketWebsiteAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutBucketWebsiteResponse PutBucketWebsite(string BucketName, WebsiteConfiguration WebConfig)
		{
			var Request = new PutBucketWebsiteRequest()
			{
				BucketName = BucketName,
				WebsiteConfiguration = WebConfig,
			};

			return PutBucketWebsite(Request);
		}

		private DeleteBucketWebsiteResponse DeleteBucketWebsite(DeleteBucketWebsiteRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteBucketWebsiteAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteBucketWebsiteResponse DeleteBucketWebsite(string BucketName)
		{
			var Request = new DeleteBucketWebsiteRequest()
			{
				BucketName = BucketName
			};

			return DeleteBucketWebsite(Request);
		}

		private ListBucketInventoryConfigurationsResponse ListBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.ListBucketInventoryConfigurationsAsync(Request);
			Response.Wait();
			return Response.Result;
		}

		public ListBucketInventoryConfigurationsResponse ListBucketInventoryConfigurations(string BucketName)
		{
			var Request = new ListBucketInventoryConfigurationsRequest()
			{
				BucketName = BucketName
			};

			return ListBucketInventoryConfigurations(Request);
		}

		private GetBucketInventoryConfigurationResponse GetBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketInventoryConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}

		public GetBucketInventoryConfigurationResponse GetBucketInventoryConfiguration(string BucketName, string Id)
		{
			var Request = new GetBucketInventoryConfigurationRequest()
			{
				BucketName = BucketName,
				InventoryId = Id
			};

			return GetBucketInventoryConfiguration(Request);
		}

		private PutBucketInventoryConfigurationResponse PutBucketInventoryConfiguration(PutBucketInventoryConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketInventoryConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}

		public PutBucketInventoryConfigurationResponse PutBucketInventoryConfiguration(string BucketName, InventoryConfiguration InventoryConfig)
		{
			var Request = new PutBucketInventoryConfigurationRequest()
			{
				BucketName = BucketName,
				InventoryId = InventoryConfig.InventoryId,
				InventoryConfiguration = InventoryConfig
			};

			return PutBucketInventoryConfiguration(Request);
		}

		private DeleteBucketInventoryConfigurationResponse DeleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteBucketInventoryConfigurationAsync(Request);
			Response.Wait();
			return Response.Result;
		}

		public DeleteBucketInventoryConfigurationResponse DeleteBucketInventoryConfiguration(string BucketName, string Id)
		{
			var Request = new DeleteBucketInventoryConfigurationRequest()
			{
				BucketName = BucketName,
				InventoryId = Id
			};

			return DeleteBucketInventoryConfiguration(Request);
		}

		#endregion

		#region Object Function
		private PutObjectResponse PutObject(PutObjectRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutObjectAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutObjectResponse PutObject(string BucketName, string Key, string Body = null, byte[] ByteBody = null, string ContentType = null,
			string CacheControl = null, DateTime? Expires = null, string IfMatch = null, string IfNoneMatch = null,
			string MD5Digest = null, List<Tag> TagSet = null, SSECustomerKey SSE_C = null,
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

			//Tag
			if (TagSet != null) Request.TagSet = TagSet;

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

			////Disable Payload Signing
			//if (DisablePayloadSigning.HasValue) Request.DisablePayloadSigning = DisablePayloadSigning;

			return PutObject(Request);
		}

		private GetObjectResponse GetObject(GetObjectRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetObjectAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetObjectResponse GetObject(
			string BucketName, string Key, string IfMatch = null, string IfNoneMatch = null, ByteRange Range = null, string VersionId = null, SSECustomerKey SSE_C = null,
			string IfModifiedSince = null, DateTime? IfModifiedSinceDateTime = null, string IfUnmodifiedSince = null, DateTime? IfUnmodifiedSinceDateTime = null,
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

			return GetObject(Request);
		}

		private CopyObjectResponse CopyObject(CopyObjectRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.CopyObjectAsync(Request);
			Response.Wait();
			return Response.Result;
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
			
			return CopyObject(Request);
		}

		private ListObjectsResponse ListObjects(ListObjectsRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.ListObjectsAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return ListObjects(Request);

		}

		private ListObjectsV2Response ListObjectsV2(ListObjectsV2Request Request)
		{
			if (Client == null) return null;
			var Response = Client.ListObjectsV2Async(Request);
			Response.Wait();
			return Response.Result;
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

			return ListObjectsV2(Request);

		}

		private ListVersionsResponse ListVersions(ListVersionsRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.ListVersionsAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public ListVersionsResponse ListVersions(string BucketName, string Prefix = null)
		{
			var Request = new ListVersionsRequest()
			{
				BucketName = BucketName,
				MaxKeys = 2000
			};
			if (Prefix != null) Request.Prefix = Prefix;

			return ListVersions(Request);
		}

		private DeleteObjectResponse DeleteObject(DeleteObjectRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteObjectAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return DeleteObject(Request);
		}

		private DeleteObjectsResponse DeleteObjects(DeleteObjectsRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteObjectsAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteObjectsResponse DeleteObjects(string BucketName, List<KeyVersion> KeyList, bool? Quiet = null)
		{
			var Request = new DeleteObjectsRequest()
			{
				BucketName = BucketName,
				Objects = KeyList
			};

			if (Quiet.HasValue) Request.Quiet = Quiet.Value;

			return DeleteObjects(Request);
		}

		private GetObjectMetadataResponse GetObjectMetadata(GetObjectMetadataRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetObjectMetadataAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return GetObjectMetadata(Request);
		}

		private GetACLResponse GetObjectACL(GetACLRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetACLAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetACLResponse GetObjectACL(string BucketName, string Key, string VersionId = null)
		{
			var Request = new GetACLRequest()
			{
				BucketName = BucketName,
				Key = Key
			};

			if (VersionId != null) Request.VersionId = VersionId;

			return GetObjectACL(Request);
		}

		private PutACLResponse PutObjectACL(PutACLRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutACLAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return PutObjectACL(Request);
		}

		private GetObjectTaggingResponse GetObjectTagging(GetObjectTaggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetObjectTaggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetObjectTaggingResponse GetObjectTagging(string BucketName, string Key)
		{
			var Request = new GetObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return GetObjectTagging(Request);
		}

		private PutObjectTaggingResponse PutObjectTagging(PutObjectTaggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutObjectTaggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutObjectTaggingResponse PutObjectTagging(string BucketName, string Key, Tagging Tagging)
		{
			var Request = new PutObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
				Tagging = Tagging,
			};

			return PutObjectTagging(Request);
		}

		private DeleteObjectTaggingResponse DeleteObjectTagging(DeleteObjectTaggingRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteObjectTaggingAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteObjectTaggingResponse DeleteObjectTagging(string BucketName, string Key)
		{
			var Request = new DeleteObjectTaggingRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return DeleteObjectTagging(Request);
		}

		private GetObjectRetentionResponse GetObjectRetention(GetObjectRetentionRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetObjectRetentionAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetObjectRetentionResponse GetObjectRetention(string BucketName, string Key)
		{
			var Request = new GetObjectRetentionRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return GetObjectRetention(Request);
		}

		private PutObjectRetentionResponse PutObjectRetention(PutObjectRetentionRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutObjectRetentionAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return PutObjectRetention(Request);
		}


		private PutObjectLegalHoldResponse PutObjectLegalHold(PutObjectLegalHoldRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutObjectLegalHoldAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public PutObjectLegalHoldResponse PutObjectLegalHold(string BucketName, string Key, ObjectLockLegalHold LegalHold)
		{
			var Request = new PutObjectLegalHoldRequest()
			{
				BucketName = BucketName,
				Key = Key,
				LegalHold = LegalHold,
			};

			return PutObjectLegalHold(Request);
		}

		private GetObjectLegalHoldResponse GetObjectLegalHold(GetObjectLegalHoldRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetObjectLegalHoldAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetObjectLegalHoldResponse GetObjectLegalHold(string BucketName, string Key)
		{
			var Request = new GetObjectLegalHoldRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};

			return GetObjectLegalHold(Request);
		}


		private GetBucketReplicationResponse GetBucketReplication(GetBucketReplicationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.GetBucketReplicationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public GetBucketReplicationResponse GetBucketReplication(string BucketName)
		{
			var Request = new GetBucketReplicationRequest()
			{
				BucketName = BucketName,
			};

			return GetBucketReplication(Request);
		}

		private PutBucketReplicationResponse PutBucketReplication(PutBucketReplicationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.PutBucketReplicationAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return PutBucketReplication(Request);
		}

		private DeleteBucketReplicationResponse DeleteBucketReplication(DeleteBucketReplicationRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.DeleteBucketReplicationAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public DeleteBucketReplicationResponse DeleteBucketReplication(string BucketName)
		{
			var Request = new DeleteBucketReplicationRequest()
			{
				BucketName = BucketName,
			};

			return DeleteBucketReplication(Request);
		}

		public RestoreObjectResponse RestoreObject(RestoreObjectRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.RestoreObjectAsync(Request);
			Response.Wait();
			return Response.Result;
		}

		public RestoreObjectResponse RestoreObject(string BucketName, string Key)
		{
			var Request = new RestoreObjectRequest()
			{
				BucketName = BucketName,
				Key = Key,
			};
			return RestoreObject(Request);
		}
		#endregion

		#region Multipart Function
		private InitiateMultipartUploadResponse InitiateMultipartUpload(InitiateMultipartUploadRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.InitiateMultipartUploadAsync(Request);
			Response.Wait();
			return Response.Result;
		}
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

			return InitiateMultipartUpload(Request);
		}

		private UploadPartResponse UploadPart(UploadPartRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.UploadPartAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return UploadPart(Request);
		}

		private CopyPartResponse CopyPart(CopyPartRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.CopyPartAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return CopyPart(Request);
		}
		private CompleteMultipartUploadResponse CompleteMultipartUpload(CompleteMultipartUploadRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.CompleteMultipartUploadAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return CompleteMultipartUpload(Request);
		}

		private AbortMultipartUploadResponse AbortMultipartUpload(AbortMultipartUploadRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.AbortMultipartUploadAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public AbortMultipartUploadResponse AbortMultipartUpload(string BucketNume, string Key, string UploadId)
		{
			var Request = new AbortMultipartUploadRequest()
			{
				BucketName = BucketNume,
				Key = Key,
				UploadId = UploadId,
			};

			return AbortMultipartUpload(Request);
		}

		private ListMultipartUploadsResponse ListMultipartUploads(ListMultipartUploadsRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.ListMultipartUploadsAsync(Request);
			Response.Wait();
			return Response.Result;
		}
		public ListMultipartUploadsResponse ListMultipartUploads(string BucketNume)
		{
			var Request = new ListMultipartUploadsRequest()
			{
				BucketName = BucketNume
			};

			return ListMultipartUploads(Request);
		}

		private ListPartsResponse ListParts(ListPartsRequest Request)
		{
			if (Client == null) return null;
			var Response = Client.ListPartsAsync(Request);
			Response.Wait();
			return Response.Result;
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

			return ListParts(Request);
		}
		#endregion

		#region ETC Function
		public string GeneratePresignedURL(string BucketName, string Key, DateTime Expires, HttpVerb Verb,
			ServerSideEncryptionMethod SSE_S3_Method = null, string ContentType = null)
		{
			var Request = new GetPreSignedUrlRequest()
			{
				BucketName = BucketName,
				Key = Key,
				Expires = Expires,
				Verb = Verb,
				Protocol = Protocol.HTTP
			};

			if (SSE_S3_Method != null) Request.ServerSideEncryptionMethod = SSE_S3_Method;
			if (ContentType != null) Request.ContentType = ContentType;

			return Client.GetPreSignedURL(Request);
		}
		#endregion
	}
}
