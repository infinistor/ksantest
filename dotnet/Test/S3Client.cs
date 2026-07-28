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
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using Xunit.Abstractions;

namespace s3tests.Test
{
	public class S3Client
	{
		private const int S3_TIMEOUT = 3600;
		public ITestOutputHelper Output { get; private set; }

		public AmazonS3Client Client { get; private set; }

		/// <summary>모든 요청에 추가할 헤더(백엔드 클라이언트용)</summary>
		private readonly Dictionary<string, string> ClientHeaders = [];
		/// <summary>요청 단위로 추가할 헤더. SDK v4에서 request.BeforeRequestEvent가 제거되어 요청 인스턴스를 키로 보관한다.</summary>
		private readonly ConditionalWeakTable<AmazonWebServiceRequest, Dictionary<string, string>> RequestHeaders = [];

		/// <summary>이 클라이언트의 요청 체크섬 계산 모드.</summary>
		private readonly RequestChecksumCalculation RequestChecksum;

		/// <summary>
		/// SDK v4는 RequestChecksumCalculation이 WHEN_REQUIRED이면 ChecksumAlgorithm을 명시해도
		/// x-amz-sdk-checksum-algorithm 헤더만 붙이고 체크섬 값도 트레일러도 만들지 않아 400을 받는다.
		/// 이 경우에만 값을 미리 계산해 넣는다. (WHEN_SUPPORTED는 SDK가 정상 처리하므로 건드리지 않는다)
		/// </summary>
		private bool NeedsPrecomputedChecksum(ChecksumAlgorithm algorithm, string value)
			=> algorithm != null && value == null && RequestChecksum == RequestChecksumCalculation.WHEN_REQUIRED;

		public S3Client(S3Config s3, bool isSecure, UserData user, ITestOutputHelper output = null,
		RequestChecksumCalculation? requestChecksumCalculation = null,
		ResponseChecksumValidation? responseChecksumValidation = null,
		List<KeyValuePair<string, string>> clientHeaders = null)
		{
			if (output != null) Output = output;

			AWSCredentials credentials = user == null ? new AnonymousAWSCredentials() : new BasicAWSCredentials(user.AccessKey, user.SecretKey);
			AmazonS3Config s3Config = s3.IsAWS ? new() { RegionEndpoint = s3.GetRegion(), } : new() { ServiceURL = s3.GetURL(isSecure) };

			// SSL 인증서 검증 설정 추가
			if (isSecure)
			{
				var httpClientHandler = new HttpClientHandler
				{
					ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => true
				};
				s3Config.HttpClientFactory = new AmazonS3HttpClientFactory(httpClientHandler);
			}

			s3Config.Timeout = TimeSpan.FromSeconds(S3_TIMEOUT);
			s3Config.ForcePathStyle = true;
			if (!isSecure) s3Config.UseHttp = true;

			// SDK v4 기본값은 WHEN_SUPPORTED라 모든 요청에 체크섬(aws-chunked 트레일러)이 붙는다.
			// Java testV2는 표준 클라이언트에 WHEN_REQUIRED를 명시하므로 기본값을 맞춘다.
			s3Config.RequestChecksumCalculation = requestChecksumCalculation ?? RequestChecksumCalculation.WHEN_REQUIRED;
			s3Config.ResponseChecksumValidation = responseChecksumValidation ?? ResponseChecksumValidation.WHEN_REQUIRED;
			RequestChecksum = s3Config.RequestChecksumCalculation;

			Client = new(credentials, s3Config);

			if (clientHeaders != null)
			{
				foreach (var header in clientHeaders)
					ClientHeaders[header.Key] = header.Value;
			}

			// 서명 이전 단계에서 클라이언트 공통 헤더와 요청별 헤더를 주입한다.
			Client.BeforeRequestEvent += (_, e) =>
			{
				if (e is not WebServiceRequestEventArgs args) return;

				foreach (var header in ClientHeaders)
					args.Headers[header.Key] = header.Value;

				if (args.Request == null) return;
				if (!RequestHeaders.TryGetValue(args.Request, out var headers)) return;
				foreach (var header in headers)
					args.Headers[header.Key] = header.Value;
			};
		}

		// HttpClientFactory 클래스 추가
		private class AmazonS3HttpClientFactory : HttpClientFactory
		{
			private readonly HttpClientHandler _clientHandler;

			public AmazonS3HttpClientFactory(HttpClientHandler clientHandler)
			{
				_clientHandler = clientHandler;
			}

			public override HttpClient CreateHttpClient(IClientConfig clientConfig)
			{
				return new HttpClient(_clientHandler, false);
			}
		}

		#region Bucket Function

		public ListBucketsResponse ListBuckets(string prefix = null, int? maxBuckets = null, string continuationToken = null)
		{
			if (Client == null) return null;

			var request = new ListBucketsRequest();
			if (prefix != null) request.Prefix = prefix;
			if (maxBuckets.HasValue) request.MaxBuckets = maxBuckets.Value;
			if (continuationToken != null) request.ContinuationToken = continuationToken;

			var response = Client.ListBucketsAsync(request);
			response.Wait();
			return response.Result;
		}

		private PutBucketResponse PutBucket(PutBucketRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketResponse PutBucket(string bucketName, S3CannedACL acl = null, string regionName = null,
			List<S3Grant> grants = null, List<KeyValuePair<string, string>> headerList = null,
			bool? objectLockEnabledForBucket = null, ObjectOwnership objectOwnership = null)
		{
			var request = new PutBucketRequest() { BucketName = bucketName };
			if (acl != null) request.CannedACL = acl;
			if (regionName != null) request.BucketRegionName = regionName;
			if (objectOwnership != null) request.ObjectOwnership = objectOwnership;
			ApplyRequestHeaders(request, null, headerList);
			if (grants != null) request.Grants = grants;
			if (objectLockEnabledForBucket.HasValue) request.ObjectLockEnabledForBucket = objectLockEnabledForBucket.Value;

			return PutBucket(request);
		}

		private DeleteBucketResponse DeleteBucket(DeleteBucketRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteBucketResponse DeleteBucket(string bucketName)
		{
			var request = new DeleteBucketRequest() { BucketName = bucketName };
			return DeleteBucket(request);
		}

		private GetBucketLocationResponse GetBucketLocation(GetBucketLocationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketLocationAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketLocationResponse GetBucketLocation(string bucketName)
		{
			var request = new GetBucketLocationRequest() { BucketName = bucketName };
			return GetBucketLocation(request);
		}

		private PutBucketLoggingResponse PutBucketLogging(PutBucketLoggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketLoggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketLoggingResponse PutBucketLogging(string bucketName, S3BucketLoggingConfig config)
		{
			var request = new PutBucketLoggingRequest() { BucketName = bucketName, LoggingConfig = config };
			return PutBucketLogging(request);
		}
		private GetBucketLoggingResponse GetBucketLogging(GetBucketLoggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketLoggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketLoggingResponse GetBucketLogging(string bucketName)
		{
			var request = new GetBucketLoggingRequest() { BucketName = bucketName };
			return GetBucketLogging(request);
		}

		private PutBucketVersioningResponse PutBucketVersioning(PutBucketVersioningRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketVersioningAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketVersioningResponse PutBucketVersioning(string bucketName, bool? enableMfaDelete = null, VersionStatus status = null)
		{
			// SDK v4의 VersioningConfig getter는 값을 저장하지 않는 새 인스턴스를 돌려주므로
			// 프로퍼티 경유로 설정하면 유실된다. 반드시 객체를 만들어 대입할 것.
			var config = new S3BucketVersioningConfig();
			if (enableMfaDelete != null) config.EnableMfaDelete = enableMfaDelete.Value;
			if (status != null) config.Status = status;

			var request = new PutBucketVersioningRequest()
			{
				BucketName = bucketName,
				VersioningConfig = config,
			};

			return PutBucketVersioning(request);
		}

		private GetBucketVersioningResponse GetBucketVersioning(GetBucketVersioningRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketVersioningAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketVersioningResponse GetBucketVersioning(string bucketName)
		{
			var request = new GetBucketVersioningRequest() { BucketName = bucketName };

			return GetBucketVersioning(request);
		}

		public GetACLResponse GetBucketACL(string bucketName)
		{
			var request = new GetACLRequest()
			{
				BucketName = bucketName
			};

			return GetACL(request);
		}

		private PutACLResponse PutACL(PutACLRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutACLAsync(request);
			response.Wait();
			return response.Result;
		}

		/// <summary>
		/// 비권장 PutACL 대신 전용 PutBucketAcl을 쓴다.
		/// PutACL 마셜러는 Grants가 비어 있으면 &lt;Owner&gt;까지 통째로 빼고 &lt;AccessControlPolicy/&gt;만 보내
		/// AWS가 MalformedXML로 거부한다. 전용 오퍼레이션은 Owner를 유지한다.
		/// </summary>
		public PutBucketAclResponse PutBucketACL(string bucketName, S3CannedACL acl = null, S3AccessControlList accessControlPolicy = null)
		{
			var request = new PutBucketAclRequest()
			{
				BucketName = bucketName
			};

			if (acl != null) request.ACL = acl;
			if (accessControlPolicy != null) request.AccessControlPolicy = accessControlPolicy;

			if (Client == null) return null;
			var response = Client.PutBucketAclAsync(request);
			response.Wait();
			return response.Result;
		}

		private PutCORSConfigurationResponse PutCORSConfiguration(PutCORSConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutCORSConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutCORSConfigurationResponse PutCORSConfiguration(string bucketName, CORSConfiguration Configuration)
		{
			var request = new PutCORSConfigurationRequest()
			{
				BucketName = bucketName,
				Configuration = Configuration,
			};

			return PutCORSConfiguration(request);
		}

		private GetCORSConfigurationResponse GetCORSConfiguration(GetCORSConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetCORSConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetCORSConfigurationResponse GetCORSConfiguration(string bucketName)
		{
			var request = new GetCORSConfigurationRequest()
			{
				BucketName = bucketName,
			};

			return GetCORSConfiguration(request);
		}

		private DeleteCORSConfigurationResponse DeleteCORSConfiguration(DeleteCORSConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteCORSConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteCORSConfigurationResponse DeleteCORSConfiguration(string bucketName)
		{
			var request = new DeleteCORSConfigurationRequest()
			{
				BucketName = bucketName,
			};

			return DeleteCORSConfiguration(request);
		}

		private GetBucketTaggingResponse GetBucketTagging(GetBucketTaggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketTaggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketTaggingResponse GetBucketTagging(string bucketName)
		{
			var request = new GetBucketTaggingRequest()
			{
				BucketName = bucketName,
			};

			return GetBucketTagging(request);
		}

		private PutBucketTaggingResponse PutBucketTagging(PutBucketTaggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketTaggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketTaggingResponse PutBucketTagging(string bucketName, List<Tag> tagSet)
		{
			var request = new PutBucketTaggingRequest()
			{
				BucketName = bucketName,
				TagSet = tagSet,
			};

			return PutBucketTagging(request);
		}

		private DeleteBucketTaggingResponse DeleteBucketTagging(DeleteBucketTaggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketTaggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteBucketTaggingResponse DeleteBucketTagging(string bucketName)
		{
			var request = new DeleteBucketTaggingRequest()
			{
				BucketName = bucketName,
			};

			return DeleteBucketTagging(request);
		}

		private PutLifecycleConfigurationResponse PutLifecycleConfiguration(PutLifecycleConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutLifecycleConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutLifecycleConfigurationResponse PutLifecycleConfiguration(string bucketName, LifecycleConfiguration configuration)
		{
			var request = new PutLifecycleConfigurationRequest()
			{
				BucketName = bucketName,
				Configuration = configuration,
			};

			return PutLifecycleConfiguration(request);
		}

		private GetLifecycleConfigurationResponse GetLifecycleConfiguration(GetLifecycleConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetLifecycleConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetLifecycleConfigurationResponse GetLifecycleConfiguration(string bucketName)
		{
			var request = new GetLifecycleConfigurationRequest()
			{
				BucketName = bucketName,
			};

			return GetLifecycleConfiguration(request);
		}

		private DeleteLifecycleConfigurationResponse DeleteLifecycleConfiguration(DeleteLifecycleConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteLifecycleConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}

		public DeleteLifecycleConfigurationResponse DeleteLifecycleConfiguration(string bucketName)
		{
			var request = new DeleteLifecycleConfigurationRequest()
			{
				BucketName = bucketName,
			};

			return DeleteLifecycleConfiguration(request);
		}

		private PutBucketPolicyResponse PutBucketPolicy(PutBucketPolicyRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketPolicyAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketPolicyResponse PutBucketPolicy(string bucketName, string Policy)
		{
			var request = new PutBucketPolicyRequest()
			{
				BucketName = bucketName,
				Policy = Policy,
			};

			return PutBucketPolicy(request);
		}

		private GetBucketPolicyResponse GetBucketPolicy(GetBucketPolicyRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketPolicyAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketPolicyResponse GetBucketPolicy(string bucketName)
		{
			var request = new GetBucketPolicyRequest()
			{
				BucketName = bucketName,
			};

			return GetBucketPolicy(request);
		}

		private DeleteBucketPolicyResponse DeleteBucketPolicy(DeleteBucketPolicyRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketPolicyAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteBucketPolicyResponse DeleteBucketPolicy(string bucketName)
		{
			var request = new DeleteBucketPolicyRequest()
			{
				BucketName = bucketName,
			};

			return DeleteBucketPolicy(request);
		}

		private GetBucketPolicyStatusResponse GetBucketPolicyStatus(GetBucketPolicyStatusRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketPolicyStatusAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketPolicyStatusResponse GetBucketPolicyStatus(string bucketName)
		{
			var request = new GetBucketPolicyStatusRequest()
			{
				BucketName = bucketName,
			};

			return GetBucketPolicyStatus(request);
		}

		private PutObjectLockConfigurationResponse PutObjectLockConfiguration(PutObjectLockConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutObjectLockConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutObjectLockConfigurationResponse PutObjectLockConfiguration(string bucketName, ObjectLockConfiguration ObjectLockConfiguration)
		{
			var request = new PutObjectLockConfigurationRequest()
			{
				BucketName = bucketName,
				ObjectLockConfiguration = ObjectLockConfiguration,
			};

			return PutObjectLockConfiguration(request);
		}

		private GetObjectLockConfigurationResponse GetObjectLockConfiguration(GetObjectLockConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetObjectLockConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetObjectLockConfigurationResponse GetObjectLockConfiguration(string bucketName)
		{
			var request = new GetObjectLockConfigurationRequest()
			{
				BucketName = bucketName,
			};

			return GetObjectLockConfiguration(request);
		}

		private PutPublicAccessBlockResponse PutPublicAccessBlock(PutPublicAccessBlockRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutPublicAccessBlockAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutPublicAccessBlockResponse PutPublicAccessBlock(string bucketName, PublicAccessBlockConfiguration PublicAccessBlockConfiguration)
		{
			var request = new PutPublicAccessBlockRequest()
			{
				BucketName = bucketName,
				PublicAccessBlockConfiguration = PublicAccessBlockConfiguration,
			};

			return PutPublicAccessBlock(request);
		}

		private GetPublicAccessBlockResponse GetPublicAccessBlock(GetPublicAccessBlockRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetPublicAccessBlockAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetPublicAccessBlockResponse GetPublicAccessBlock(string bucketName)
		{
			var request = new GetPublicAccessBlockRequest()
			{
				BucketName = bucketName,
			};

			return GetPublicAccessBlock(request);
		}

		private DeletePublicAccessBlockResponse DeletePublicAccessBlock(DeletePublicAccessBlockRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeletePublicAccessBlockAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeletePublicAccessBlockResponse DeletePublicAccessBlock(string bucketName)
		{
			var request = new DeletePublicAccessBlockRequest()
			{
				BucketName = bucketName,
			};

			return DeletePublicAccessBlock(request);
		}

		private GetBucketEncryptionResponse GetBucketEncryption(GetBucketEncryptionRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketEncryptionAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketEncryptionResponse GetBucketEncryption(string bucketName)
		{
			var request = new GetBucketEncryptionRequest()
			{
				BucketName = bucketName
			};

			return GetBucketEncryption(request);
		}

		private PutBucketEncryptionResponse PutBucketEncryption(PutBucketEncryptionRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketEncryptionAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketEncryptionResponse PutBucketEncryption(string bucketName, ServerSideEncryptionConfiguration SSEConfig)
		{
			var request = new PutBucketEncryptionRequest()
			{
				BucketName = bucketName,
				ServerSideEncryptionConfiguration = SSEConfig,
			};

			return PutBucketEncryption(request);
		}

		private DeleteBucketEncryptionResponse DeleteBucketEncryption(DeleteBucketEncryptionRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketEncryptionAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteBucketEncryptionResponse DeleteBucketEncryption(string bucketName)
		{
			var request = new DeleteBucketEncryptionRequest()
			{
				BucketName = bucketName
			};

			return DeleteBucketEncryption(request);
		}

		private GetBucketWebsiteResponse GetBucketWebsite(GetBucketWebsiteRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketWebsiteAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketWebsiteResponse GetBucketWebsite(string bucketName)
		{
			var request = new GetBucketWebsiteRequest()
			{
				BucketName = bucketName
			};

			return GetBucketWebsite(request);
		}

		private PutBucketWebsiteResponse PutBucketWebsite(PutBucketWebsiteRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketWebsiteAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketWebsiteResponse PutBucketWebsite(string bucketName, WebsiteConfiguration webConfig)
		{
			var request = new PutBucketWebsiteRequest()
			{
				BucketName = bucketName,
				WebsiteConfiguration = webConfig,
			};

			return PutBucketWebsite(request);
		}

		private DeleteBucketWebsiteResponse DeleteBucketWebsite(DeleteBucketWebsiteRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketWebsiteAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteBucketWebsiteResponse DeleteBucketWebsite(string bucketName)
		{
			var request = new DeleteBucketWebsiteRequest()
			{
				BucketName = bucketName
			};

			return DeleteBucketWebsite(request);
		}

		private ListBucketInventoryConfigurationsResponse ListBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest request)
		{
			if (Client == null) return null;
			var response = Client.ListBucketInventoryConfigurationsAsync(request);
			response.Wait();
			return response.Result;
		}

		public ListBucketInventoryConfigurationsResponse ListBucketInventoryConfigurations(string bucketName)
		{
			var request = new ListBucketInventoryConfigurationsRequest()
			{
				BucketName = bucketName
			};

			return ListBucketInventoryConfigurations(request);
		}

		private GetBucketInventoryConfigurationResponse GetBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketInventoryConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}

		public GetBucketInventoryConfigurationResponse GetBucketInventoryConfiguration(string bucketName, string id)
		{
			var request = new GetBucketInventoryConfigurationRequest()
			{
				BucketName = bucketName,
				InventoryId = id
			};

			return GetBucketInventoryConfiguration(request);
		}

		private PutBucketInventoryConfigurationResponse PutBucketInventoryConfiguration(PutBucketInventoryConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketInventoryConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}

		public PutBucketInventoryConfigurationResponse PutBucketInventoryConfiguration(string bucketName, InventoryConfiguration inventoryConfig)
		{
			var request = new PutBucketInventoryConfigurationRequest()
			{
				BucketName = bucketName,
				InventoryId = inventoryConfig.InventoryId,
				InventoryConfiguration = inventoryConfig
			};

			return PutBucketInventoryConfiguration(request);
		}

		private DeleteBucketInventoryConfigurationResponse DeleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketInventoryConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}

		public DeleteBucketInventoryConfigurationResponse DeleteBucketInventoryConfiguration(string bucketName, string id)
		{
			var request = new DeleteBucketInventoryConfigurationRequest()
			{
				BucketName = bucketName,
				InventoryId = id
			};

			return DeleteBucketInventoryConfiguration(request);
		}

		public PutBucketAccelerateConfigurationResponse PutBucketAccelerateConfiguration(PutBucketAccelerateConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketAccelerateConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}

		public PutBucketAccelerateConfigurationResponse PutBucketAccelerateConfiguration(string bucketName, BucketAccelerateStatus status)
		{
			var request = new PutBucketAccelerateConfigurationRequest()
			{
				BucketName = bucketName,
				AccelerateConfiguration = new AccelerateConfiguration()
				{
					Status = status
				}
			};

			return PutBucketAccelerateConfiguration(request);
		}

		private GetBucketAccelerateConfigurationResponse GetBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketAccelerateConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}

		public GetBucketAccelerateConfigurationResponse GetBucketAccelerateConfiguration(string bucketName)
		{
			var request = new GetBucketAccelerateConfigurationRequest()
			{
				BucketName = bucketName
			};

			return GetBucketAccelerateConfiguration(request);
		}

		private ListBucketMetricsConfigurationsResponse ListBucketMetricsConfigurations(ListBucketMetricsConfigurationsRequest request)
		{
			if (Client == null) return null;
			var response = Client.ListBucketMetricsConfigurationsAsync(request);
			response.Wait();
			return response.Result;
		}
		public ListBucketMetricsConfigurationsResponse ListBucketMetricsConfigurations(string bucketName)
		{
			var request = new ListBucketMetricsConfigurationsRequest() { BucketName = bucketName };
			return ListBucketMetricsConfigurations(request);
		}

		private PutBucketMetricsConfigurationResponse PutBucketMetricsConfiguration(PutBucketMetricsConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketMetricsConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketMetricsConfigurationResponse PutBucketMetricsConfiguration(string bucketName, string id, MetricsConfiguration metricsConfiguration)
		{
			var request = new PutBucketMetricsConfigurationRequest()
			{
				BucketName = bucketName,
				MetricsConfiguration = metricsConfiguration
			};
			if (id != null) request.MetricsId = id;
			return PutBucketMetricsConfiguration(request);
		}

		private GetBucketMetricsConfigurationResponse GetBucketMetricsConfiguration(GetBucketMetricsConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketMetricsConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketMetricsConfigurationResponse GetBucketMetricsConfiguration(string bucketName, string id)
		{
			var request = new GetBucketMetricsConfigurationRequest()
			{
				BucketName = bucketName,
				MetricsId = id
			};
			return GetBucketMetricsConfiguration(request);
		}

		private DeleteBucketMetricsConfigurationResponse DeleteBucketMetricsConfiguration(DeleteBucketMetricsConfigurationRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketMetricsConfigurationAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteBucketMetricsConfigurationResponse DeleteBucketMetricsConfiguration(string bucketName, string id)
		{
			var request = new DeleteBucketMetricsConfigurationRequest()
			{
				BucketName = bucketName,
				MetricsId = id
			};
			return DeleteBucketMetricsConfiguration(request);
		}

		private GetBucketNotificationResponse GetBucketNotification(GetBucketNotificationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketNotificationAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketNotificationResponse GetBucketNotificationConfiguration(string bucketName)
		{
			var request = new GetBucketNotificationRequest() { BucketName = bucketName };
			return GetBucketNotification(request);
		}

		private PutBucketNotificationResponse PutBucketNotification(PutBucketNotificationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketNotificationAsync(request);
			response.Wait();
			return response.Result;
		}

		public PutBucketNotificationResponse PutBucketNotificationConfiguration(string bucketName,
			List<LambdaFunctionConfiguration> lambdaFunctionConfigurations = null,
			List<TopicConfiguration> topicConfigurations = null,
			List<QueueConfiguration> queueConfigurations = null)
		{
			var request = new PutBucketNotificationRequest()
			{
				BucketName = bucketName,
				LambdaFunctionConfigurations = lambdaFunctionConfigurations,
				TopicConfigurations = topicConfigurations,
				QueueConfigurations = queueConfigurations
			};
			return PutBucketNotification(request);
		}

		private GetBucketOwnershipControlsResponse GetBucketOwnershipControls(GetBucketOwnershipControlsRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketOwnershipControlsAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketOwnershipControlsResponse GetBucketOwnershipControls(string bucketName)
		{
			var request = new GetBucketOwnershipControlsRequest() { BucketName = bucketName };
			return GetBucketOwnershipControls(request);
		}

		private PutBucketOwnershipControlsResponse PutBucketOwnershipControls(PutBucketOwnershipControlsRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketOwnershipControlsAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketOwnershipControlsResponse PutBucketOwnershipControls(string bucketName, OwnershipControls ownershipControls)
		{
			var request = new PutBucketOwnershipControlsRequest()
			{
				BucketName = bucketName,
				OwnershipControls = ownershipControls
			};
			return PutBucketOwnershipControls(request);
		}
		#endregion

		#region Object Function
		private PutObjectResponse PutObject(PutObjectRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutObjectAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutObjectResponse PutObject(string bucketName, string key, string body = null, byte[] byteBody = null, string contentType = null,
			string cacheControl = null, DateTime? expires = null, string ifMatch = null, string ifNoneMatch = null, ChecksumAlgorithm checksumAlgorithm = null,
			string checksumValue = null, string md5Digest = null, List<Tag> tagSet = null, SSECustomerKey sseCustomerKey = null,
			List<KeyValuePair<string, string>> metadataList = null, List<KeyValuePair<string, string>> headerList = null,
			S3CannedACL acl = null, List<S3Grant> grants = null, ServerSideEncryptionMethod sseKey = null,
			ObjectLockLegalHoldStatus objectLockLegalHoldStatus = null, DateTime? objectLockRetainUntilDate = null,
			ObjectLockMode objectLockMode = null, bool? useChunkEncoding = null, bool? disablePayloadSigning = null)
		{
			var request = new PutObjectRequest()
			{
				BucketName = bucketName,
				Key = key,
			};

			if (body != null) request.ContentBody = body;
			if (byteBody != null)
			{
				Stream myStream = new MemoryStream(byteBody);
				request.InputStream = myStream;
			}
			if (contentType != null) request.ContentType = contentType;
			if (ifMatch != null) request.Headers["If-Match"] = ifMatch;
			if (ifNoneMatch != null) request.Headers["If-None-Match"] = ifNoneMatch;
			if (cacheControl != null) request.Metadata[S3Headers.CacheControl] = cacheControl;
			if (expires != null) request.Metadata[S3Headers.Expires] = expires.Value.ToString(S3Headers.TimeFormat, S3Headers.TimeCulture);
			if (metadataList != null)
			{
				foreach (var metaData in metadataList)
					request.Metadata[metaData.Key] = metaData.Value;
			}
			if (headerList != null)
			{
				foreach (var header in headerList)
					request.Headers[header.Key] = header.Value;
			}
			if (acl != null) request.CannedACL = acl;
			if (grants != null) request.Grants = grants;
			if (md5Digest != null) request.MD5Digest = md5Digest;

			//Checksum
			if (checksumValue != null && checksumAlgorithm != null)
				s3tests.Utils.CheckSum.SetChecksum(request, checksumAlgorithm, checksumValue);
			else if (NeedsPrecomputedChecksum(checksumAlgorithm, checksumValue))
			{
				request.ChecksumAlgorithm = checksumAlgorithm;
				var content = byteBody ?? System.Text.Encoding.UTF8.GetBytes(body ?? string.Empty);
				s3tests.Utils.CheckSum.SetChecksum(request, checksumAlgorithm,
					Convert.ToBase64String(s3tests.Utils.CheckSum.CalculateChecksumBytes(checksumAlgorithm, content)));
			}
			else if (checksumAlgorithm != null)
				request.ChecksumAlgorithm = checksumAlgorithm;

			//Tag
			if (tagSet != null) request.TagSet = tagSet;

			//Lock
			if (objectLockMode != null) request.ObjectLockMode = objectLockMode;
			if (objectLockLegalHoldStatus != null) request.ObjectLockLegalHoldStatus = objectLockLegalHoldStatus;
			if (objectLockRetainUntilDate.HasValue) request.ObjectLockRetainUntilDate = objectLockRetainUntilDate.Value;

			//SSE-S3
			if (sseKey != null) request.ServerSideEncryptionMethod = sseKey;

			//SSE-C
			if (sseCustomerKey != null)
			{
				request.ServerSideEncryptionCustomerMethod = sseCustomerKey.Method;
				request.ServerSideEncryptionCustomerProvidedKey = sseCustomerKey.ProvidedKey;
				request.ServerSideEncryptionCustomerProvidedKeyMD5 = sseCustomerKey.MD5;
			}

			//ChunkEncoding Payload
			if (useChunkEncoding.HasValue) request.UseChunkEncoding = useChunkEncoding.Value;

			if (disablePayloadSigning.HasValue) request.DisablePayloadSigning = disablePayloadSigning;

			return PutObject(request);
		}

		private GetObjectResponse GetObject(GetObjectRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetObjectAsync(request);
			response.Wait();
			return response.Result;
		}

		public GetObjectResponse GetObject(
			string bucketName, string key, string ifMatch = null, string ifNoneMatch = null, ByteRange range = null,
			string versionId = null, SSECustomerKey sseCustomerKey = null, string ifModifiedSince = null,
			DateTime? ifModifiedSinceDateTime = null, string ifUnmodifiedSince = null,
			DateTime? ifUnmodifiedSinceDateTime = null, string responseContentType = null,
			string responseContentLanguage = null, string responseExpires = null,
			string responseCacheControl = null, string responseContentDisposition = null,
			string responseContentEncoding = null, int? partNumber = null)
		{
			var request = new GetObjectRequest()
			{
				BucketName = bucketName,
				Key = key,
			};

			if (versionId != null) request.VersionId = versionId;
			if (ifMatch != null) request.EtagToMatch = ifMatch;
			if (ifNoneMatch != null) request.EtagToNotMatch = ifNoneMatch;
			if (range != null) request.ByteRange = range;

			//SSE-C
			if (sseCustomerKey != null)
			{
				request.ServerSideEncryptionCustomerMethod = sseCustomerKey.Method;
				request.ServerSideEncryptionCustomerProvidedKey = sseCustomerKey.ProvidedKey;
				request.ServerSideEncryptionCustomerProvidedKeyMD5 = sseCustomerKey.MD5;
			}

			if (ifModifiedSince != null) request.ModifiedSinceDate = DateTime.Parse(ifModifiedSince);
			if (ifModifiedSinceDateTime != null) request.ModifiedSinceDate = ifModifiedSinceDateTime.Value;
			if (ifUnmodifiedSince != null) request.UnmodifiedSinceDate = DateTime.Parse(ifUnmodifiedSince);
			if (ifUnmodifiedSinceDateTime != null) request.UnmodifiedSinceDate = ifUnmodifiedSinceDateTime.Value;
			if (partNumber != null) request.PartNumber = partNumber.Value;

			//ResponseHeaderOverrides
			if (responseContentType != null) request.ResponseHeaderOverrides.ContentType = responseContentType;
			if (responseContentLanguage != null) request.ResponseHeaderOverrides.ContentLanguage = responseContentLanguage;
			if (responseExpires != null) request.ResponseHeaderOverrides.Expires = responseExpires;
			if (responseCacheControl != null) request.ResponseHeaderOverrides.CacheControl = responseCacheControl;
			if (responseContentDisposition != null) request.ResponseHeaderOverrides.ContentDisposition = responseContentDisposition;
			if (responseContentEncoding != null) request.ResponseHeaderOverrides.ContentEncoding = responseContentEncoding;

			return GetObject(request);
		}

		private CopyObjectResponse CopyObject(CopyObjectRequest request)
		{
			if (Client == null) return null;
			var response = Client.CopyObjectAsync(request);
			response.Wait();
			return response.Result;
		}

		public CopyObjectResponse CopyObject(string sourceBucket, string sourceKey, string bucketName, string key,
			List<KeyValuePair<string, string>> metadataList = null, S3MetadataDirective? metadataDirective = null,
			ServerSideEncryptionMethod sseKey = null, SSECustomerKey srcCustomerKey = null, SSECustomerKey destCustomerKey = null,
			string versionId = null, S3CannedACL acl = null, string eTagToMatch = null, string eTagToNotMatch = null,
			string contentType = null, ChecksumAlgorithm checksumAlgorithm = null,
			List<KeyValuePair<string, string>> headerList = null,
			DateTime? modifiedSince = null, DateTime? unmodifiedSince = null,
			string ifMatch = null, string ifNoneMatch = null)
		{
			var request = new CopyObjectRequest()
			{
				SourceBucket = sourceBucket,
				SourceKey = sourceKey,
				DestinationBucket = bucketName,
				DestinationKey = key,
			};
			if (acl != null) request.CannedACL = acl;
			if (contentType != null) request.ContentType = contentType;
			// CopyObject는 본문이 없어 체크섬을 S3가 계산한다. SDK v4가 x-amz-sdk-checksum-algorithm(본문 체크섬용)을
			// 내보내면 AWS가 대응 값이 없다며 400을 반환하므로, 알고리즘 지정 헤더를 직접 넣는다.
			if (checksumAlgorithm != null) request.Headers["x-amz-checksum-algorithm"] = checksumAlgorithm.Value;
			if (metadataList != null)
			{
				foreach (var metaData in metadataList)
					request.Metadata[metaData.Key] = metaData.Value;
			}
			if (metadataDirective != null) request.MetadataDirective = metadataDirective.Value;
			if (versionId != null) request.SourceVersionId = versionId;

			// SDK v4는 x-amz-copy-source의 '/'까지 %2F로 인코딩한다. 복사 자체는 되지만
			// IAM/버킷 정책의 s3:x-amz-copy-source 조건은 원본 헤더 값과 비교하므로 매칭에 실패한다.
			// java SDK v2와 동일하게 슬래시는 남기고 키만 인코딩한다.
			var copySource = $"{sourceBucket}/{Amazon.Util.AWSSDKUtils.UrlEncode(sourceKey, true)}";
			if (versionId != null) copySource += $"?versionId={versionId}";
			var copySourceHeader = new List<KeyValuePair<string, string>> { new("x-amz-copy-source", copySource) };
			if (headerList != null) copySourceHeader.AddRange(headerList);
			ApplyRequestHeaders(request, null, copySourceHeader);
			// 소스 조건(x-amz-copy-source-if-*)
			if (eTagToMatch != null) request.ETagToMatch = eTagToMatch;
			if (eTagToNotMatch != null) request.ETagToNotMatch = eTagToNotMatch;
			if (modifiedSince != null) request.ModifiedSinceDate = modifiedSince.Value;
			if (unmodifiedSince != null) request.UnmodifiedSinceDate = unmodifiedSince.Value;

			// 대상 조건(If-Match / If-None-Match)
			if (ifMatch != null) request.IfMatch = ifMatch;
			if (ifNoneMatch != null) request.IfNoneMatch = ifNoneMatch;

			//SSE-S3
			if (sseKey != null) request.ServerSideEncryptionMethod = sseKey;

			//SSE-C
			if (srcCustomerKey != null)
			{
				request.CopySourceServerSideEncryptionCustomerMethod = srcCustomerKey.Method;
				request.CopySourceServerSideEncryptionCustomerProvidedKey = srcCustomerKey.ProvidedKey;
				request.CopySourceServerSideEncryptionCustomerProvidedKeyMD5 = srcCustomerKey.MD5;
			}
			if (destCustomerKey != null)
			{
				request.ServerSideEncryptionCustomerMethod = destCustomerKey.Method;
				request.ServerSideEncryptionCustomerProvidedKey = destCustomerKey.ProvidedKey;
				request.ServerSideEncryptionCustomerProvidedKeyMD5 = destCustomerKey.MD5;
			}

			return CopyObject(request);
		}

		private ListObjectsResponse ListObjects(ListObjectsRequest request)
		{
			if (Client == null) return null;
			var response = Client.ListObjectsAsync(request);
			response.Wait();
			return response.Result;
		}
		public ListObjectsResponse ListObjects(string bucketName, string delimiter = null, string marker = null,
											int maxKeys = -1, string prefix = null, string encodingTypeName = null)
		{
			var request = new ListObjectsRequest() { BucketName = bucketName };

			if (delimiter != null) request.Delimiter = delimiter;
			if (marker != null) request.Marker = marker;
			if (prefix != null) request.Prefix = prefix;
			if (encodingTypeName != null) request.Encoding = new EncodingType(encodingTypeName);

			if (maxKeys >= 0) request.MaxKeys = maxKeys;

			return ListObjects(request);
		}

		private ListObjectsV2Response ListObjectsV2(ListObjectsV2Request request)
		{
			if (Client == null) return null;
			var response = Client.ListObjectsV2Async(request);
			response.Wait();
			return response.Result;
		}
		public ListObjectsV2Response ListObjectsV2(string bucketName, string delimiter = null, string continuationToken = null,
					int maxKeys = -1, string prefix = null, string startAfter = null, string encodingTypeName = null,
					bool? fetchOwner = null)
		{
			var request = new ListObjectsV2Request() { BucketName = bucketName };

			if (delimiter != null) request.Delimiter = delimiter;
			if (continuationToken != null) request.ContinuationToken = continuationToken;
			if (prefix != null) request.Prefix = prefix;
			if (startAfter != null) request.StartAfter = startAfter;
			if (encodingTypeName != null) request.Encoding = new EncodingType(encodingTypeName);
			if (fetchOwner != null) request.FetchOwner = fetchOwner.Value;

			if (maxKeys >= 0) request.MaxKeys = maxKeys;

			return ListObjectsV2(request);
		}

		private ListVersionsResponse ListVersions(ListVersionsRequest request)
		{
			if (Client == null) return null;
			var response = Client.ListVersionsAsync(request);
			response.Wait();
			return response.Result;
		}
		public ListVersionsResponse ListVersions(string bucketName, string delimiter = null, string keyMarker = null,
			int maxKeys = -1, string prefix = null, string encodingTypeName = null, bool useLegacyMaxKeys = true)
		{
			var request = new ListVersionsRequest() { BucketName = bucketName };

			if (delimiter != null) request.Delimiter = delimiter;
			if (keyMarker != null) request.KeyMarker = keyMarker;
			if (prefix != null) request.Prefix = prefix;
			if (encodingTypeName != null) request.Encoding = new EncodingType(encodingTypeName);
			if (maxKeys >= 0) request.MaxKeys = maxKeys;
			else if (useLegacyMaxKeys) request.MaxKeys = 2000;

			return ListVersions(request);
		}

		private DeleteObjectResponse DeleteObject(DeleteObjectRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteObjectAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteObjectResponse DeleteObject(string bucketName, string key, string versionId = null,
			bool? bypassGovernanceRetention = null, string ifMatch = null,
			List<KeyValuePair<string, string>> headerList = null)
		{
			var request = new DeleteObjectRequest()
			{
				BucketName = bucketName,
				Key = key
			};

			if (versionId != null) request.VersionId = versionId;
			if (bypassGovernanceRetention.HasValue) request.BypassGovernanceRetention = bypassGovernanceRetention.Value;
			ApplyRequestHeaders(request, ifMatch, headerList);

			return DeleteObject(request);
		}

		private DeleteObjectsResponse DeleteObjects(DeleteObjectsRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteObjectsAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteObjectsResponse DeleteObjects(string bucketName, List<KeyVersion> keyList, bool? quiet = null,
			List<KeyValuePair<string, string>> headerList = null, bool? bypassGovernanceRetention = null)
		{
			var request = new DeleteObjectsRequest()
			{
				BucketName = bucketName,
				Objects = keyList
			};

			if (quiet.HasValue) request.Quiet = quiet.Value;
			if (bypassGovernanceRetention.HasValue) request.BypassGovernanceRetention = bypassGovernanceRetention.Value;
			ApplyRequestHeaders(request, null, headerList);

			return DeleteObjects(request);
		}

		private void ApplyRequestHeaders(AmazonWebServiceRequest request, string ifMatch,
			List<KeyValuePair<string, string>> headerList)
		{
			if (ifMatch == null && (headerList == null || headerList.Count == 0)) return;

			var headers = RequestHeaders.GetOrCreateValue(request);
			if (ifMatch != null) headers["If-Match"] = ifMatch;
			if (headerList == null) return;
			foreach (var header in headerList)
				headers[header.Key] = header.Value;
		}

		private GetObjectMetadataResponse GetObjectMetadata(GetObjectMetadataRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetObjectMetadataAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetObjectMetadataResponse GetObjectMetadata(string bucketName, string key, string versionId = null, SSECustomerKey sseC = null)
			=> HeadObject(bucketName, key, versionId: versionId, sseCustomerKey: sseC);

		public HeadBucketResponse HeadBucket(string bucketName)
		{
			var request = new HeadBucketRequest() { BucketName = bucketName };
			if (Client == null) return null;
			var response = Client.HeadBucketAsync(request);
			response.Wait();
			return response.Result;
		}

		public GetObjectMetadataResponse HeadObject(string bucketName, string key, string ifMatch = null,
			string ifNoneMatch = null, string ifModifiedSince = null, DateTime? ifModifiedSinceDateTime = null,
			string ifUnmodifiedSince = null, DateTime? ifUnmodifiedSinceDateTime = null,
			string versionId = null, SSECustomerKey sseCustomerKey = null)
		{
			var request = new GetObjectMetadataRequest()
			{
				BucketName = bucketName,
				Key = key
			};

			if (versionId != null) request.VersionId = versionId;
			if (ifMatch != null) request.EtagToMatch = ifMatch;
			if (ifNoneMatch != null) request.EtagToNotMatch = ifNoneMatch;
			if (ifModifiedSince != null) request.ModifiedSinceDate = DateTime.Parse(ifModifiedSince);
			if (ifModifiedSinceDateTime != null) request.ModifiedSinceDate = ifModifiedSinceDateTime.Value;
			if (ifUnmodifiedSince != null) request.UnmodifiedSinceDate = DateTime.Parse(ifUnmodifiedSince);
			if (ifUnmodifiedSinceDateTime != null) request.UnmodifiedSinceDate = ifUnmodifiedSinceDateTime.Value;

			if (sseCustomerKey != null)
			{
				request.ServerSideEncryptionCustomerMethod = sseCustomerKey.Method;
				request.ServerSideEncryptionCustomerProvidedKey = sseCustomerKey.ProvidedKey;
				request.ServerSideEncryptionCustomerProvidedKeyMD5 = sseCustomerKey.MD5;
			}

			return GetObjectMetadata(request);
		}

		private GetACLResponse GetACL(GetACLRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetACLAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetACLResponse GetObjectACL(string bucketName, string key, string versionId = null)
		{
			var request = new GetACLRequest()
			{
				BucketName = bucketName,
				Key = key
			};

			if (versionId != null) request.VersionId = versionId;

			return GetACL(request);
		}

		public PutACLResponse PutObjectACL(string bucketName, string key, S3CannedACL acl = null,
			S3AccessControlList accessControlPolicy = null, string versionId = null)
		{
			var request = new PutACLRequest()
			{
				BucketName = bucketName,
				Key = key
			};
			if (acl != null) request.CannedACL = acl;
			if (accessControlPolicy != null) request.AccessControlList = accessControlPolicy;
			if (versionId != null) request.VersionId = versionId;

			return PutACL(request);
		}

		private GetObjectTaggingResponse GetObjectTagging(GetObjectTaggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetObjectTaggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetObjectTaggingResponse GetObjectTagging(string bucketName, string key, string versionId = null)
		{
			var request = new GetObjectTaggingRequest()
			{
				BucketName = bucketName,
				Key = key,
			};
			if (versionId != null) request.VersionId = versionId;

			return GetObjectTagging(request);
		}

		private PutObjectTaggingResponse PutObjectTagging(PutObjectTaggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutObjectTaggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutObjectTaggingResponse PutObjectTagging(string bucketName, string key, Tagging tagging, string versionId = null)
		{
			var request = new PutObjectTaggingRequest()
			{
				BucketName = bucketName,
				Key = key,
				Tagging = tagging,
			};
			if (versionId != null) request.VersionId = versionId;

			return PutObjectTagging(request);
		}

		private DeleteObjectTaggingResponse DeleteObjectTagging(DeleteObjectTaggingRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteObjectTaggingAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteObjectTaggingResponse DeleteObjectTagging(string bucketName, string key, string versionId = null)
		{
			var request = new DeleteObjectTaggingRequest()
			{
				BucketName = bucketName,
				Key = key,
			};
			if (versionId != null) request.VersionId = versionId;

			return DeleteObjectTagging(request);
		}

		private GetObjectRetentionResponse GetObjectRetention(GetObjectRetentionRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetObjectRetentionAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetObjectRetentionResponse GetObjectRetention(string bucketName, string key, string versionId = null)
		{
			var request = new GetObjectRetentionRequest()
			{
				BucketName = bucketName,
				Key = key,
			};
			if (versionId != null) request.VersionId = versionId;

			return GetObjectRetention(request);
		}

		private PutObjectRetentionResponse PutObjectRetention(PutObjectRetentionRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutObjectRetentionAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutObjectRetentionResponse PutObjectRetention(string bucketName, string key, ObjectLockRetention retention,
			string contentMD5 = null, string versionId = null, bool? bypassGovernanceRetention = null)
		{
			var request = new PutObjectRetentionRequest()
			{
				BucketName = bucketName,
				Key = key,
				Retention = retention,
			};
			if (contentMD5 != null) request.ContentMD5 = contentMD5;
			if (versionId != null) request.VersionId = versionId;
			if (bypassGovernanceRetention.HasValue) request.BypassGovernanceRetention = bypassGovernanceRetention.Value;

			return PutObjectRetention(request);
		}

		private PutObjectLegalHoldResponse PutObjectLegalHold(PutObjectLegalHoldRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutObjectLegalHoldAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutObjectLegalHoldResponse PutObjectLegalHold(string bucketName, string key, ObjectLockLegalHold legalHold)
		{
			var request = new PutObjectLegalHoldRequest()
			{
				BucketName = bucketName,
				Key = key,
				LegalHold = legalHold,
			};

			return PutObjectLegalHold(request);
		}

		private GetObjectLegalHoldResponse GetObjectLegalHold(GetObjectLegalHoldRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetObjectLegalHoldAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetObjectLegalHoldResponse GetObjectLegalHold(string bucketName, string key)
		{
			var request = new GetObjectLegalHoldRequest()
			{
				BucketName = bucketName,
				Key = key,
			};

			return GetObjectLegalHold(request);
		}

		private GetBucketReplicationResponse GetBucketReplication(GetBucketReplicationRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetBucketReplicationAsync(request);
			response.Wait();
			return response.Result;
		}
		public GetBucketReplicationResponse GetBucketReplication(string bucketName)
		{
			var request = new GetBucketReplicationRequest()
			{
				BucketName = bucketName,
			};

			return GetBucketReplication(request);
		}

		private PutBucketReplicationResponse PutBucketReplication(PutBucketReplicationRequest request)
		{
			if (Client == null) return null;
			var response = Client.PutBucketReplicationAsync(request);
			response.Wait();
			return response.Result;
		}
		public PutBucketReplicationResponse PutBucketReplication(string bucketName, ReplicationConfiguration configuration,
			string token = null, string expectedBucketOwner = null)
		{
			var request = new PutBucketReplicationRequest()
			{
				BucketName = bucketName,
				Configuration = configuration
			};

			if (!string.IsNullOrWhiteSpace(token)) request.Token = token;

			return PutBucketReplication(request);
		}

		private DeleteBucketReplicationResponse DeleteBucketReplication(DeleteBucketReplicationRequest request)
		{
			if (Client == null) return null;
			var response = Client.DeleteBucketReplicationAsync(request);
			response.Wait();
			return response.Result;
		}
		public DeleteBucketReplicationResponse DeleteBucketReplication(string bucketName)
		{
			var request = new DeleteBucketReplicationRequest()
			{
				BucketName = bucketName,
			};

			return DeleteBucketReplication(request);
		}

		public RestoreObjectResponse RestoreObject(RestoreObjectRequest request)
		{
			if (Client == null) return null;
			var response = Client.RestoreObjectAsync(request);
			response.Wait();
			return response.Result;
		}

		public RestoreObjectResponse RestoreObject(string bucketName, string key)
		{
			var request = new RestoreObjectRequest()
			{
				BucketName = bucketName,
				Key = key,
			};
			return RestoreObject(request);
		}
		#endregion

		#region Multipart Function
		private InitiateMultipartUploadResponse InitiateMultipartUpload(InitiateMultipartUploadRequest request)
		{
			if (Client == null) return null;
			var response = Client.InitiateMultipartUploadAsync(request);
			response.Wait();
			return response.Result;
		}
		public InitiateMultipartUploadResponse InitiateMultipartUpload(string bucketName, string key, string contentType = null,
			List<KeyValuePair<string, string>> metadataList = null, ServerSideEncryptionMethod sseKey = null, SSECustomerKey sseCustomerKey = null,
			ChecksumAlgorithm checksumAlgorithm = null, ChecksumType checksumType = null,
			List<KeyValuePair<string, string>> headerList = null, List<Tag> tagSet = null)
		{
			var request = new InitiateMultipartUploadRequest()
			{
				BucketName = bucketName,
				Key = key
			};
			if (metadataList != null)
			{
				foreach (var metaData in metadataList)
					request.Metadata[metaData.Key] = metaData.Value;
			}
			if (contentType != null) request.ContentType = contentType;
			if (checksumAlgorithm != null) request.ChecksumAlgorithm = checksumAlgorithm;
			if (checksumType != null) request.ChecksumType = checksumType;
			if (tagSet != null) request.TagSet = tagSet;
			ApplyRequestHeaders(request, null, headerList);

			//SSE-S3
			if (sseKey != null) request.ServerSideEncryptionMethod = sseKey;

			//SSE-C
			if (sseCustomerKey != null)
			{
				request.ServerSideEncryptionCustomerMethod = sseCustomerKey.Method;
				request.ServerSideEncryptionCustomerProvidedKey = sseCustomerKey.ProvidedKey;
				request.ServerSideEncryptionCustomerProvidedKeyMD5 = sseCustomerKey.MD5;
			}

			return InitiateMultipartUpload(request);
		}

		private UploadPartResponse UploadPart(UploadPartRequest request)
		{
			if (Client == null) return null;
			var response = Client.UploadPartAsync(request);
			response.Wait();
			return response.Result;
		}
		public UploadPartResponse UploadPart(string bucketName, string key, string uploadId, string body,
			int partNumber, SSECustomerKey sseC = null, ChecksumAlgorithm checksumAlgorithm = null, string checksumValue = null)
		{
			var request = new UploadPartRequest()
			{
				BucketName = bucketName,
				Key = key,
				PartNumber = partNumber,
				UploadId = uploadId,
				InputStream = new MemoryStream(Encoding.UTF8.GetBytes(body))
			};

			if (checksumValue != null && checksumAlgorithm != null)
				s3tests.Utils.CheckSum.SetChecksum(request, checksumAlgorithm, checksumValue);
			else if (NeedsPrecomputedChecksum(checksumAlgorithm, checksumValue))
			{
				request.ChecksumAlgorithm = checksumAlgorithm;
				s3tests.Utils.CheckSum.SetChecksum(request, checksumAlgorithm,
					s3tests.Utils.CheckSum.CalculateChecksum(checksumAlgorithm, body));
			}
			else if (checksumAlgorithm != null)
				request.ChecksumAlgorithm = checksumAlgorithm;

			//SSE-C
			if (sseC != null)
			{
				request.ServerSideEncryptionCustomerMethod = sseC.Method;
				request.ServerSideEncryptionCustomerProvidedKey = sseC.ProvidedKey;
				request.ServerSideEncryptionCustomerProvidedKeyMD5 = sseC.MD5;
			}

			return UploadPart(request);
		}

		private CopyPartResponse CopyPart(CopyPartRequest request)
		{
			if (Client == null) return null;
			var response = Client.CopyPartAsync(request);
			response.Wait();
			return response.Result;
		}
		public CopyPartResponse CopyPart(string srcBucketName, string srcKey, string destBucketName, string destKey,
			string uploadId, int partNumber, int start, int end, string versionId = null,
			SSECustomerKey srcEncryptionKey = null, SSECustomerKey destEncryptionKey = null,
			string eTagToMatch = null, string eTagToNotMatch = null,
			DateTime? modifiedSince = null, DateTime? unmodifiedSince = null,
			string ifMatch = null, string ifNoneMatch = null)
		{
			var request = new CopyPartRequest()
			{
				SourceBucket = srcBucketName,
				SourceKey = srcKey,
				DestinationBucket = destBucketName,
				DestinationKey = destKey,
				UploadId = uploadId,
				PartNumber = partNumber,
				FirstByte = start,
				LastByte = end,
			};

			if (versionId != null) request.SourceVersionId = versionId;
			// 소스 조건(x-amz-copy-source-if-*)
			if (eTagToMatch != null) request.ETagToMatch = [eTagToMatch];
			if (eTagToNotMatch != null) request.ETagsToNotMatch = [eTagToNotMatch];
			if (modifiedSince != null) request.ModifiedSinceDate = modifiedSince.Value;
			if (unmodifiedSince != null) request.UnmodifiedSinceDate = unmodifiedSince.Value;

			// 대상 조건(If-Match / If-None-Match). SDK에 전용 속성이 없어 헤더로 직접 넣는다.
			if (ifMatch != null || ifNoneMatch != null)
			{
				var conditions = new List<KeyValuePair<string, string>>();
				if (ifMatch != null) conditions.Add(new("If-Match", ifMatch));
				if (ifNoneMatch != null) conditions.Add(new("If-None-Match", ifNoneMatch));
				ApplyRequestHeaders(request, null, conditions);
			}

			////SSE-C
			if (srcEncryptionKey != null)
			{
				request.CopySourceServerSideEncryptionCustomerMethod = srcEncryptionKey.Method;
				request.CopySourceServerSideEncryptionCustomerProvidedKey = srcEncryptionKey.ProvidedKey;
				request.CopySourceServerSideEncryptionCustomerProvidedKeyMD5 = srcEncryptionKey.MD5;
			}
			if (destEncryptionKey != null)
			{
				request.ServerSideEncryptionCustomerMethod = destEncryptionKey.Method;
				request.ServerSideEncryptionCustomerProvidedKey = destEncryptionKey.ProvidedKey;
				request.ServerSideEncryptionCustomerProvidedKeyMD5 = destEncryptionKey.MD5;
			}

			return CopyPart(request);
		}
		private CompleteMultipartUploadResponse CompleteMultipartUpload(CompleteMultipartUploadRequest request)
		{
			if (Client == null) return null;
			var response = Client.CompleteMultipartUploadAsync(request);
			response.Wait();
			return response.Result;
		}
		public CompleteMultipartUploadResponse CompleteMultipartUpload(string bucketName, string key,
			string uploadId, List<PartETag> parts, ChecksumType checksumType = null,
			string ifMatch = null, string ifNoneMatch = null,
			List<KeyValuePair<string, string>> headerList = null)
		{
			var request = new CompleteMultipartUploadRequest()
			{
				BucketName = bucketName,
				Key = key,
				UploadId = uploadId,
				PartETags = parts
			};
			if (checksumType != null) request.ChecksumType = checksumType;
			if (ifMatch != null) request.IfMatch = ifMatch;
			if (ifNoneMatch != null) request.IfNoneMatch = ifNoneMatch;
			ApplyRequestHeaders(request, null, headerList);

			return CompleteMultipartUpload(request);
		}

		private GetObjectAttributesResponse GetObjectAttributes(GetObjectAttributesRequest request)
		{
			if (Client == null) return null;
			var response = Client.GetObjectAttributesAsync(request);
			response.Wait();
			return response.Result;
		}

		public GetObjectAttributesResponse GetObjectAttributes(string bucketName, string key,
			List<ObjectAttributes> objectAttributes = null, string versionId = null)
		{
			var request = new GetObjectAttributesRequest()
			{
				BucketName = bucketName,
				Key = key,
			};
			if (objectAttributes != null) request.ObjectAttributes = objectAttributes;
			if (versionId != null) request.VersionId = versionId;
			return GetObjectAttributes(request);
		}

		private AbortMultipartUploadResponse AbortMultipartUpload(AbortMultipartUploadRequest request)
		{
			if (Client == null) return null;
			var response = Client.AbortMultipartUploadAsync(request);
			response.Wait();
			return response.Result;
		}
		public AbortMultipartUploadResponse AbortMultipartUpload(string bucketName, string key, string uploadId)
		{
			var request = new AbortMultipartUploadRequest()
			{
				BucketName = bucketName,
				Key = key,
				UploadId = uploadId,
			};

			return AbortMultipartUpload(request);
		}

		private ListMultipartUploadsResponse ListMultipartUploads(ListMultipartUploadsRequest request)
		{
			if (Client == null) return null;
			var response = Client.ListMultipartUploadsAsync(request);
			response.Wait();
			return response.Result;
		}
		public ListMultipartUploadsResponse ListMultipartUploads(string bucketName)
		{
			var request = new ListMultipartUploadsRequest()
			{
				BucketName = bucketName
			};

			return ListMultipartUploads(request);
		}

		private ListPartsResponse ListParts(ListPartsRequest request)
		{
			if (Client == null) return null;
			var response = Client.ListPartsAsync(request);
			response.Wait();
			return response.Result;
		}
		public ListPartsResponse ListParts(string bucketName, string key, string uploadId,
			int maxParts = 1000, int partNumberMarker = 0)
		{
			var request = new ListPartsRequest()
			{
				BucketName = bucketName,
				Key = key,
				UploadId = uploadId,
				MaxParts = maxParts,
				PartNumberMarker = partNumberMarker.ToString(),
			};

			return ListParts(request);
		}
		#endregion

		#region ETC Function
		public string GeneratePresignedURL(string bucketName, string key, DateTime expires, HttpVerb verb,
			ServerSideEncryptionMethod sseS3Method = null, string contentType = null)
		{
			var request = new GetPreSignedUrlRequest()
			{
				BucketName = bucketName,
				Key = key,
				Expires = expires,
				Verb = verb,
				Protocol = Protocol.HTTP
			};

			if (sseS3Method != null) request.ServerSideEncryptionMethod = sseS3Method;
			if (contentType != null) request.ContentType = contentType;

			return Client.GetPreSignedURL(request);
		}
		#endregion
	}
}
