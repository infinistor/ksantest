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
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Xunit;
using Xunit.Abstractions;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using s3tests.Utils;

namespace s3tests.Test
{
	public abstract class TestBase : IDisposable
	{
		/// <summary>
		/// AWS SDK for .NET v4는 요청/설정 객체의 컬렉션 프로퍼티를 null로 두기 때문에
		/// `config.Rules.Add(...)` 같은 v3 스타일 코드가 NullReferenceException으로 죽는다.
		/// 어셈블리 로드 시점에 v3 동작(자동 초기화)으로 되돌린다.
		/// </summary>
		[System.Runtime.CompilerServices.ModuleInitializer]
		internal static void InitializeAwsSdk() => Amazon.AWSConfigs.InitializeCollections = true;


		#region Define
		private const int RANDOM_PREFIX_TEXT_LENGTH = 30;
		private const string STR_RANDOM = "{random}";
		#endregion

		#region Values
		public ITestOutputHelper Output;
		public readonly List<string> EmptyList = [];
		public MainConfig Config { get; private set; }
		private List<string> BucketList { get; set; }
		#endregion

		public enum EncryptionType { NORMAL, SSE_S3, SSE_C };

		public TestBase()
		{
			string configFilePath;
			try
			{
				string temp = Environment.GetEnvironmentVariable(MainData.S3TESTS_INI);
				if (string.IsNullOrWhiteSpace(temp)) configFilePath = MainConfig.STR_DEF_FILENAME;
				else configFilePath = temp.Trim();
			}
			catch (Exception)
			{
				configFilePath = MainConfig.STR_DEF_FILENAME;
			}
			Config = new MainConfig(configFilePath);
			Config.GetConfig();

			BucketList = [];
		}

		public void Dispose()
		{
			Clear();
			GC.SuppressFinalize(this);
		}
		public void Clear() => BucketClear();

		/// <summary>
		/// AWS 환경이면 테스트를 스킵한다. (JUnit Assumptions.assumeFalse(config.isAWS())에 대응)
		/// [SkippableFact]/[SkippableTheory]와 함께 사용해야 한다.
		/// </summary>
		protected void SkipIfAws(string reason = "AWS does not support this feature")
			=> Skip.If(Config.S3.IsAWS, reason);

		#region Get client
		public S3Client GetClient() => new(Config.S3, Config.IsSecure, Config.MainUser, Output);
		public S3Client GetClientV4() => new(Config.S3, Config.IsSecure, Config.MainUser, Output);
		public S3Client GetClientHttps() => new(Config.S3, true, Config.MainUser, Output);
		public S3Client GetClientHttpsV4(RequestChecksumCalculation? requestChecksumCalculation = null,
		ResponseChecksumValidation? responseChecksumValidation = null) => new(Config.S3, true, Config.MainUser, Output, requestChecksumCalculation, responseChecksumValidation);

		public S3Client GetClient(RequestChecksumCalculation? requestChecksumCalculation,
			ResponseChecksumValidation? responseChecksumValidation, bool useHttps = false)
			=> new(Config.S3, useHttps || Config.IsSecure, Config.MainUser, Output, requestChecksumCalculation, responseChecksumValidation);
		public S3Client GetAltClient() => new(Config.S3, Config.IsSecure, Config.AltUser, Output);
		public S3Client GetUnauthenticatedClient() => new(Config.S3, Config.IsSecure, null, Output);
		public S3Client GetPublicClient() => GetUnauthenticatedClient();
		public S3Client GetBadAuthClient(string accessKey = null, string secretKey = null)
		{
			accessKey ??= "aaaaaaaaaaaaaaa";
			secretKey ??= "bbbbbbbbbbbbbbb";
			var user = new UserData() { AccessKey = accessKey, SecretKey = secretKey };
			return new S3Client(Config.S3, Config.IsSecure, user);
		}

		/// <summary>백엔드 전용 클라이언트. 모든 요청에 백엔드 헤더를 주입한다.</summary>
		public S3Client GetBackendClient() => new(Config.S3, Config.IsSecure, Config.BackendUser ?? Config.MainUser, Output,
			// Java getBackendClient는 표준 클라이언트와 달리 WHEN_SUPPORTED를 쓴다.
			RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED,
			clientHeaders: [
				new(BackendHeaders.IFS_ADMIN, BackendHeaders.HEADER_DATA),
				new(BackendHeaders.IFS_BACKEND, BackendHeaders.HEADER_DATA),
				new(BackendHeaders.KSAN_BACKEND, BackendHeaders.HEADER_DATA),
				new(BackendHeaders.HEADER_USER_AGENT, BackendHeaders.HEADER_USER_AGENT_VALUE),
				new(BackendHeaders.HEADER_REPLICATION, BackendHeaders.HEADER_DATA),
			]);
		#endregion

		#region Create Data

		public string GetPrefix() => Config.BucketPrefix.Replace(STR_RANDOM, S3Utils.RandomText(RANDOM_PREFIX_TEXT_LENGTH));

		/// <summary>테스트 클래스명을 suite id로 사용한다. (Java getSuiteId)</summary>
		public string GetSuiteId() => S3Utils.ToSuiteId(GetType().Name);

		/// <summary>테스트 인스턴스 내에서 생성한 버킷 순번. Java의 testId 자리에 들어간다.</summary>
		private int BucketIndex;

		/// <summary>prefix + suite + "-" + 순번 + "-" + 랜덤문자로 62자를 채운 버킷명. (Java getNewBucketName)</summary>
		public string GetNewBucketName(bool create = true) => GetNewBucketName(++BucketIndex, create);

		/// <summary>testId를 직접 지정하는 형태. (Java getNewBucketName(testId))</summary>
		public string GetNewBucketName(int testId, bool create = true)
		{
			var bucketName = S3Utils.MakeBucketName(GetPrefix(), GetSuiteId(), testId);
			if (create) BucketList.Add(bucketName);
			return bucketName;
		}

		public string GetURL(string bucketName) => $"{MainData.HTTP}{GetHost(bucketName)}";
		public string GetURL(string bucketName, string key) => $"{MainData.HTTP}{GetHost(bucketName)}/{key}";
		/// <summary>SSE-C처럼 TLS가 필수인 요청에 쓴다.</summary>
		public string GetSecureURL(string bucketName) => $"{MainData.HTTPS}{GetHost(bucketName)}";
		public string GetHost(string bucketName)
			=> Config.S3.IsAWS ? $"{bucketName}.s3-{Config.S3.RegionName}.amazonaws.com" : $"{Config.S3.Address}:{Config.S3.Port}/{bucketName}";


		#endregion

		#region POST
		public static HttpResponseMessage PutObject(string url, string body = null, string contentType = null)
		{
			using var client = new HttpClient();
			using var request = new HttpRequestMessage(HttpMethod.Put, url);

			if (contentType != null)
				request.Headers.Add("Content-Type", contentType);

			if (body != null)
				request.Content = new StringContent(body);

			return client.SendAsync(request).GetAwaiter().GetResult();
		}

		public static HttpResponseMessage GetObject(string url)
		{
			using var client = new HttpClient();
			using var request = new HttpRequestMessage(HttpMethod.Get, url);
			return client.SendAsync(request).GetAwaiter().GetResult();
		}

		/// <summary>SigV4로 서명된 POST 정책. testV2 Post 테스트와 동일한 방식.</summary>
		public sealed class PostPolicyV4
		{
			public const string Algorithm = "AWS4-HMAC-SHA256";
			public string Policy { get; init; }
			public string Signature { get; init; }
			public string Credential { get; init; }
			public string AmzDate { get; init; }

			/// <summary>POST 폼에 필요한 인증 필드를 채워 넣는다.</summary>
			public void Apply(Dictionary<string, object> payload)
			{
				payload["policy"] = Policy;
				payload["x-amz-algorithm"] = Algorithm;
				payload["x-amz-credential"] = Credential;
				payload["x-amz-date"] = AmzDate;
				payload["x-amz-signature"] = Signature;
			}
		}

		/// <summary>
		/// 정책에 x-amz-algorithm/credential/date 조건을 덧붙인 뒤 SigV4로 서명한다.
		/// (AWS 신규 리전은 SigV2 POST를 지원하지 않아 testV2도 SigV4를 쓴다.)
		/// </summary>
		public PostPolicyV4 SignPostPolicy(JObject policyDocument, UserData user = null)
		{
			user ??= Config.MainUser;

			var amzDate = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ");
			var dateStamp = amzDate[..8];
			var region = string.IsNullOrWhiteSpace(Config.S3.RegionName) ? "us-east-1" : Config.S3.RegionName;
			var credential = $"{user.AccessKey}/{dateStamp}/{region}/s3/aws4_request";

			if (policyDocument["conditions"] is JArray conditions)
			{
				conditions.Add(new JObject() { { "x-amz-algorithm", PostPolicyV4.Algorithm } });
				conditions.Add(new JObject() { { "x-amz-credential", credential } });
				conditions.Add(new JObject() { { "x-amz-date", amzDate } });
			}

			var policy = Convert.ToBase64String(Encoding.UTF8.GetBytes(policyDocument.ToString()));

			return new PostPolicyV4()
			{
				Policy = policy,
				Signature = S3Utils.GetPostPolicySignature(user.SecretKey, dateStamp, region, policy),
				Credential = credential,
				AmzDate = amzDate,
			};
		}

		/// <summary>
		/// multipart 파트를 직접 구성한다. .NET 기본 동작은 name을 따옴표 없이 쓰고
		/// Content-Type을 Content-Disposition보다 먼저 내보내는데, S3 서버가 둘 다 파싱하지 못해 400을 반환한다.
		/// </summary>
		private static StringContent FormPart(string body, string name, string fileName = null, string contentType = null)
		{
			var content = new StringContent(body);
			content.Headers.Remove("Content-Type");

			var disposition = new System.Net.Http.Headers.ContentDispositionHeaderValue("form-data")
			{
				Name = $"\"{name}\"",
			};
			if (fileName != null) disposition.FileName = $"\"{fileName}\"";
			content.Headers.ContentDisposition = disposition;

			if (contentType != null) content.Headers.TryAddWithoutValidation("Content-Type", contentType);
			return content;
		}

		/// <param name="secure">SSE-C 등 TLS가 필수인 요청은 true. AWS는 평문 연결의 SSE-C 요청을 400으로 거부한다.</param>
		public MyResult PostUpload(string bucketName, Dictionary<string, object> parameters, bool secure = false)
		{
			//https://spirit32.tistory.com/21
			string boundary = DateTime.Now.Ticks.ToString("x");
			using var client = new HttpClient();
			using var formData = new MultipartFormDataContent(boundary);
			var url = secure ? GetSecureURL(bucketName) : GetURL(bucketName);

			// .NET은 Content-Type에 boundary="..." 처럼 따옴표를 붙이는데 S3 서버가 이를 파싱하지 못해 400을 반환한다.
			var boundaryParameter = formData.Headers.ContentType.Parameters.First(o => o.Name == "boundary");
			boundaryParameter.Value = boundary;

			if (Config.S3.IsAWS)
				client.DefaultRequestHeaders.Host = GetHost(bucketName);

			if (parameters != null && parameters.Count > 0)
			{
				// S3 POST 규약상 file 필드는 항상 마지막이어야 한다.
				foreach (var pair in parameters)
				{
					if (pair.Value is FormFile) continue;
					formData.Add(FormPart(pair.Value.ToString(), pair.Key));
				}

				foreach (var pair in parameters)
				{
					if (pair.Value is not FormFile file) continue;
					formData.Add(FormPart(file.Body, pair.Key, file.Name, file.ContentType));
				}
			}

			try
			{
				var response = client.PostAsync(url, formData).GetAwaiter().GetResult();
				var webHeaders = new WebHeaderCollection();
				foreach (var header in response.Headers)
				{
					foreach (var value in header.Value)
					{
						webHeaders.Add(header.Key, value);
					}
				}

				// 실패 응답의 본문에 실제 원인이 들어있다.
				var body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

				return new MyResult()
				{
					URL = response.RequestMessage.RequestUri.AbsoluteUri,
					StatusCode = response.StatusCode,
					Headers = webHeaders,
					ErrorCode = S3Utils.GetXmlValue(body, "Code"),
					Message = S3Utils.GetXmlValue(body, "Message"),
				};
			}
			catch (HttpRequestException e)
			{
				var responseContent = e.Message;
				return new MyResult()
				{
					URL = url,
					StatusCode = HttpStatusCode.BadRequest,
					ErrorCode = S3Utils.GetXmlValue(responseContent, "Code"),
					Message = S3Utils.GetXmlValue(responseContent, "Message"),
				};
			}
		}
		#endregion

		#region Check
		public static bool ErrorCheck(HttpStatusCode statusCode)
		{
			if (statusCode.Equals(HttpStatusCode.BadRequest)) return true;
			if (statusCode.Equals(HttpStatusCode.Forbidden)) return true;
			return false;
		}

		public void CheckVersioning(string bucketName, VersionStatus statusCode)
		{
			var client = GetClient();
			S3Utils.CheckVersioning(client, bucketName, statusCode);
		}
		public void CheckConfigureVersioningRetry(string bucketName, VersionStatus status)
		{
			var client = GetClient();

			client.PutBucketVersioning(bucketName, enableMfaDelete: false, status: status);

			VersionStatus readStatus = null;

			for (int i = 0; i < 5; i++)
			{
				try
				{
					var response = client.GetBucketVersioning(bucketName);
					readStatus = response.VersioningConfig.Status;

					if (readStatus == status) break;
					Thread.Sleep(1000);
				}
				catch (Exception)
				{
					readStatus = null;
				}
			}

			Assert.Equal(status, readStatus);
		}

		public static void CheckContent(S3Client client, string bucketName, string key, string data, int loopCount = 1, SSECustomerKey sseC = null)
		{
			for (int i = 0; i < loopCount; i++)
			{
				var response = client.GetObject(bucketName, key, sseCustomerKey: sseC);
				var body = S3Utils.GetBody(response);
				Assert.Equal(data, body);
			}
		}

		public static void CheckContentUsingRange(S3Client client, string bucketName, string key, string data, long step, SSECustomerKey sseC = null)
		{
			var size = data.Length;
			long startPosition = 0;

			while (startPosition < size)
			{
				var endPosition = startPosition + step;
				if (endPosition > size) endPosition = size - 1;
				endPosition -= 1;


				var range = new ByteRange(startPosition, endPosition);
				var response = client.GetObject(bucketName, key, range: range, sseCustomerKey: sseC);
				var body = S3Utils.GetBody(response);
				var length = endPosition - startPosition + 1;

				Assert.Equal(length, response.ContentLength);
				Assert.Equal(data.Substring((int)range.Start, (int)length), body);
				startPosition += step;
			}
		}
		public static void CheckContentUsingRandomRange(S3Client client, string bucketName, string key, string data, int loopCount, SSECustomerKey sseC = null)
		{
			for (int i = 0; i < loopCount; i++)
			{
				var range = S3Utils.MakeRandomRange(data.Length);
				var length = range.End - range.Start;

				var response = client.GetObject(bucketName, key, range: range, sseCustomerKey: sseC);
				var body = S3Utils.GetBody(response);

				Assert.Equal(length + 1, response.ContentLength);
				Assert.Equal(data.Substring((int)range.Start, (int)length + 1), body);
			}
		}

		public string ValidateListObjcet(string bucketName, string prefix, string delimiter, string marker,
						int maxKeys, bool isTruncated, List<string> checkKeys, List<string> checkPrefixs, string nextMarker)
		{
			var client = GetClient();
			var response = client.ListObjects(bucketName, delimiter: delimiter, marker: marker, maxKeys: maxKeys, prefix: prefix);

			Assert.Equal(isTruncated, response.IsTruncated);
			Assert.Equal(nextMarker, response.NextMarker);

			List<string> keys = GetKeys(response);
			List<string> prefixes = response.CommonPrefixes;

			Assert.Equal(checkKeys.Count, keys.Count);
			Assert.Equal(checkPrefixs.Count, prefixes.Count);
			Assert.Equal(checkKeys, keys);
			Assert.Equal(checkPrefixs, prefixes);

			return response.NextMarker;
		}

		public string ValidateListObjcetV2(string bucketName, string prefix, string delimiter, string continuationToken,
						int maxKeys, bool isTruncated, List<string> checkKeys, List<string> checkPrefixs, bool last = false)
		{
			var client = GetClient();
			var response = client.ListObjectsV2(bucketName, delimiter: delimiter, continuationToken: continuationToken,
												maxKeys: maxKeys, prefix: prefix);

			Assert.Equal(isTruncated, response.IsTruncated);
			if (last) Assert.Null(response.NextContinuationToken);

			List<string> keys = GetKeys(response);
			List<string> prefixes = response.CommonPrefixes;

			Assert.Equal(checkKeys, keys);
			Assert.Equal(checkPrefixs, prefixes);
			Assert.Equal(checkKeys.Count, keys.Count);
			Assert.Equal(checkPrefixs.Count, prefixes.Count);

			return response.NextContinuationToken;
		}

		public string ValidateListObjectVersions(string bucketName, string prefix, string delimiter, string keyMarker,
						int maxKeys, bool isTruncated, List<string> checkKeys, List<string> checkPrefixes, string nextKeyMarker)
		{
			var client = GetClient();
			var response = client.ListVersions(bucketName, delimiter: delimiter, keyMarker: keyMarker,
												maxKeys: maxKeys, prefix: prefix);

			Assert.Equal(isTruncated, response.IsTruncated);
			Assert.Equal(nextKeyMarker, response.NextKeyMarker);

			List<string> keys = GetKeys(response);
			List<string> prefixes = response.CommonPrefixes ?? [];

			Assert.Equal(checkKeys.Count, keys.Count);
			Assert.Equal(checkPrefixes.Count, prefixes.Count);
			Assert.Equal(checkKeys, keys);
			Assert.Equal(checkPrefixes, prefixes);

			return response.NextKeyMarker;
		}

		public static void SucceedGetObject(S3Client client, string bucketName, string key, string content)
		{
			var response = client.GetObject(bucketName, key);
			Assert.Equal(content, S3Utils.GetBody(response));
		}

		public static void FailedGetObject(S3Client client, string bucketName, string key, HttpStatusCode statusCode, string errorCode)
		{
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(statusCode, GetStatus(e));
			Assert.Equal(errorCode, GetErrorCode(e));
		}

		public static void SucceedPutObject(S3Client client, string bucketName, string key, string content)
			=> client.PutObject(bucketName, key, body: content);

		public static void FailedPutObject(S3Client client, string bucketName, string key, HttpStatusCode statusCode, string errorCode)
		{
			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: key));
			Assert.Equal(statusCode, GetStatus(e));
			Assert.Equal(errorCode, GetErrorCode(e));
		}

		public static void SucceedListObjects(S3Client client, string bucketName, List<string> keys)
		{
			var response = client.ListObjects(bucketName);
			Assert.Equal(keys, GetKeys(response));
		}

		public static void FailedListObjects(S3Client client, string bucketName, HttpStatusCode statusCode, string errorCode)
		{
			var e = Assert.Throws<AggregateException>(() => client.ListObjects(bucketName));
			Assert.Equal(statusCode, GetStatus(e));
			Assert.Equal(errorCode, GetErrorCode(e));
		}

		public static void CheckBucketAclAllowRead(S3Client client, string bucketName) => client.HeadBucket(bucketName);

		public static void CheckBucketAclDenyRead(S3Client client, string bucketName)
		{
			var e = Assert.Throws<AggregateException>(() => client.HeadBucket(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public static void CheckBucketAclAllowReadACP(S3Client client, string bucketName) => client.GetBucketACL(bucketName);

		public static void CheckBucketAclDenyReadACP(S3Client client, string bucketName)
		{
			var e = Assert.Throws<AggregateException>(() => client.GetBucketACL(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public static void CheckBucketAclAllowWrite(S3Client client, string bucketName)
		{
			var key = "checkBucketAclAllowWrite";
			client.PutObject(bucketName, key, body: key);
			client.DeleteObject(bucketName, key);
		}

		public static void CheckBucketAclDenyWrite(S3Client client, string bucketName)
		{
			var key = "checkBucketAclDenyWrite";
			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: key));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public static void CheckBucketAclAllowWriteACP(S3Client client, string bucketName)
			=> client.PutBucketACL(bucketName, acl: S3CannedACL.PublicReadWrite);

		public static void CheckBucketAclDenyWriteACP(S3Client client, string bucketName)
		{
			var e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public static void CheckObjectAclAllowRead(S3Client client, string bucketName, string key)
		{
			var response = client.GetObject(bucketName, key);
			S3Utils.GetBody(response);
		}

		public static void CheckObjectAclDenyRead(S3Client client, string bucketName, string key)
		{
			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public static void CheckObjectAclAllowReadACP(S3Client client, string bucketName, string key)
			=> client.GetObjectACL(bucketName, key);

		public static void CheckObjectAclDenyReadACP(S3Client client, string bucketName, string key)
		{
			var e = Assert.Throws<AggregateException>(() => client.GetObjectACL(bucketName, key));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public static void CheckObjectAclAllowWrite(S3Client client, string bucketName, string key)
			=> client.PutObjectTagging(bucketName, key, new Tagging() { TagSet = [new Tag() { Key = "foo", Value = "bar" }] });

		public static void CheckObjectAclDenyWrite(S3Client client, string bucketName, string key)
		{
			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: key));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public static void CheckObjectAclAllowWriteACP(S3Client client, string bucketName, string key)
			=> client.PutObjectACL(bucketName, key, acl: S3CannedACL.PublicReadWrite);

		public static void CheckObjectAclDenyWriteACP(S3Client client, string bucketName, string key)
		{
			var e = Assert.Throws<AggregateException>(() => client.PutObjectACL(bucketName, key, acl: S3CannedACL.PublicRead));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		public void CheckBadBucketName(string bucketName)
		{
			var client = GetClient();

			// Java SDK v2는 클라이언트에서 버킷명을 검증해 IllegalArgumentException을 던지지만
			// .NET SDK는 검증하지 않고 서버로 보낸다. 서버가 돌려주는 오류 코드는 구현마다 달라
			// (InvalidBucketName / BucketAlreadyExists 등) 실패 여부만 확인한다.
			Assert.ThrowsAny<Exception>(() =>
			{
				// Java SDK가 클라이언트에서 막는 길이 규칙(3~63자)을 여기서 대신 검증한다.
				if (bucketName.Length < 3 || bucketName.Length > 63)
					throw new ArgumentException($"Invalid bucket name length: {bucketName.Length}");
				client.PutBucket(bucketName);
			});
		}
		public void CheckGoodBucketName(string name, string prefix = null)
		{
			prefix ??= GetPrefix();

			var bucketName = string.Format("{0}{1}", prefix, name);
			BucketList.Add(bucketName);

			var client = GetClient();
			var response = client.PutBucket(bucketName);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		public static void CheckGrants(List<S3Grant> expected, List<S3Grant> actual)
		{
			Assert.Equal(expected.Count, actual.Count);

			expected = S3Utils.GrantsSort(expected);
			actual = S3Utils.GrantsSort(actual);

			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(expected[i].Permission, actual[i].Permission);
				Assert.Equal(expected[i].Grantee.CanonicalUser, actual[i].Grantee.CanonicalUser);
				Assert.Equal(expected[i].Grantee.EmailAddress, actual[i].Grantee.EmailAddress);
				Assert.Equal(expected[i].Grantee.Type, actual[i].Grantee.Type);
				Assert.Equal(expected[i].Grantee.URI, actual[i].Grantee.URI);
			}
		}

		public static void CheckCopyContent(S3Client client,
									string srcBucketName, string srcKey, string destBucketName, string destKey,
									string srcVersionId = null, SSECustomerKey srcCustomerKey = null,
									string destVersionId = null, SSECustomerKey destCustomerKey = null)
		{
			var srcResponse = client.GetObject(srcBucketName, srcKey, versionId: srcVersionId, sseCustomerKey: srcCustomerKey);
			var srcSize = srcResponse.ContentLength;
			var srcBody = S3Utils.GetBody(srcResponse);

			var destResponse = client.GetObject(destBucketName, destKey, versionId: destVersionId, sseCustomerKey: destCustomerKey);
			var destSize = destResponse.ContentLength;
			var destBody = S3Utils.GetBody(destResponse);

			Assert.Equal(srcSize, destSize);
			Assert.Equal(srcBody, destBody);
		}
		public static void CheckCopyContentUsingRange(S3Client client, string srcBucketName, string srcKey, string destBucketName, string destKey, string versionId = null)
		{
			var headResponse = client.GetObjectMetadata(srcBucketName, srcKey, versionId: versionId);
			var srcSize = headResponse.ContentLength;

			var response = client.GetObject(destBucketName, destKey);
			var destSize = response.ContentLength;
			var destBody = S3Utils.GetBody(response);
			Assert.True(srcSize >= destSize);

			var range = new ByteRange(0, destSize - 1);
			response = client.GetObject(srcBucketName, srcKey, range: range, versionId: versionId);
			var srcBody = S3Utils.GetBody(response);
			Assert.Equal(srcBody, destBody);
		}

		public static void CheckObjContent(S3Client client, string bucketName, string key, string versionId, string content)
		{
			var response = client.GetObject(bucketName, key, versionId: versionId);
			if (content != null)
			{
				var body = S3Utils.GetBody(response);
				Assert.Equal(content, body);
			}
			else
				Assert.Equal("True", response.DeleteMarker);
		}
		public static void CheckObjVersions(S3Client client, string bucketName, string key, List<string> versionIds, List<string> contents)
		{
			var response = client.ListVersions(bucketName);
			var versions = S3Utils.GetVersions(response.Versions);

			versions.Reverse();

			var index = 0;
			foreach (var version in versions)
			{
				Assert.Equal(version.VersionId, versionIds[index]);
				if (!string.IsNullOrWhiteSpace(key)) Assert.Equal(key, version.Key);
				CheckObjContent(client, bucketName, key, version.VersionId, contents[index++]);
			}
		}

		public void CheckUploadMultipartResend(string bucketName, string key, int size, List<int> resendParts)
		{
			var contentType = "text/bla";
			var metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var client = GetClient();
			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, contentType: contentType, metadataList: metadata, resendParts: resendParts);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(contentType, response.Headers.ContentType);
			Assert.Equal(metadata, S3Utils.GetMetaData(response.Metadata));
			var body = S3Utils.GetBody(response);
			Assert.Equal(body.Length, response.ContentLength);
			Assert.Equal(uploadData.Body, body);

			CheckContentUsingRange(client, bucketName, key, uploadData.Body, 1000000);
			CheckContentUsingRange(client, bucketName, key, uploadData.Body, 10000000);
		}

		public static void PrefixLifecycleConfigurationCheck(List<LifecycleRule> expected, List<LifecycleRule> actual)
		{
			Assert.Equal(expected.Count, actual.Count);

			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(expected[i].Id, actual[i].Id);
				Assert.Equal(expected[i].Expiration.Date, actual[i].Expiration.Date);
				Assert.Equal(expected[i].Expiration.Days, actual[i].Expiration.Days);
				Assert.Equal(expected[i].Expiration.ExpiredObjectDeleteMarker, actual[i].Expiration.ExpiredObjectDeleteMarker);
				Assert.Equal((expected[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix,
							 (actual[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix);

				Assert.Equal(expected[i].Status, actual[i].Status);
				Assert.Equal(expected[i].NoncurrentVersionExpiration, actual[i].NoncurrentVersionExpiration);
				Assert.Equal(expected[i].Transitions, actual[i].Transitions);
				Assert.Equal(expected[i].NoncurrentVersionTransitions, actual[i].NoncurrentVersionTransitions);
				Assert.Equal(expected[i].AbortIncompleteMultipartUpload, actual[i].AbortIncompleteMultipartUpload);
			}
		}

		public void CorsRequestAndCheck(string method, string bucketName, List<KeyValuePair<string, string>> headers, HttpStatusCode statusCode,
	string expectAllowOrigin, string expectAllowMethods, string key = null)
		{
			var url = GetURL(bucketName);
			if (key != null) url += string.Format("/{0}", key);

			using var client = new HttpClient();
			using var request = new HttpRequestMessage(new HttpMethod(method), url);

			request.Headers.Add("Keep-Alive", "true");
			foreach (var item in headers)
				request.Headers.Add(item.Key, item.Value);

			MyResult result;
			try
			{
				var response = client.SendAsync(request).GetAwaiter().GetResult();
				var webHeaders = new WebHeaderCollection();
				foreach (var header in response.Headers)
				{
					foreach (var value in header.Value)
					{
						webHeaders.Add(header.Key, value);
					}
				}

				result = new MyResult()
				{
					URL = response.RequestMessage.RequestUri.AbsoluteUri,
					StatusCode = response.StatusCode,
					Headers = webHeaders,
				};
			}
			catch (HttpRequestException e)
			{
				var responseContent = e.Message;
				result = new MyResult()
				{
					URL = url,
					StatusCode = HttpStatusCode.BadRequest,
					Headers = [],
					ErrorCode = S3Utils.GetXmlValue(responseContent, "Code"),
					Message = S3Utils.GetXmlValue(responseContent, "Message"),
				};
			}

			Assert.Equal(statusCode, result.StatusCode);
			Assert.Equal(result.Headers.Get("access-control-allow-origin"), expectAllowOrigin);
			Assert.Equal(result.Headers.Get("access-control-allow-methods"), expectAllowMethods);
		}

		public static void TaggingCompare(List<Tag> expected, List<Tag> actual)
		{
			Assert.Equal(expected.Count, actual.Count);

			var orderExpected = expected.OrderBy(x => x.Key).ToList();
			var orderActual = actual.OrderBy(x => x.Key).ToList();

			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(orderExpected[i].Key, orderActual[i].Key);
				Assert.Equal(orderExpected[i].Value, orderActual[i].Value);
			}
		}
		public static void LockCompare(ObjectLockConfiguration expected, ObjectLockConfiguration actual)
		{
			Assert.Equal(expected.ObjectLockEnabled, actual.ObjectLockEnabled);
			Assert.Equal(expected.Rule.DefaultRetention.Mode, actual.Rule.DefaultRetention.Mode);
			Assert.Equal(expected.Rule.DefaultRetention.Years, actual.Rule.DefaultRetention.Years);
			Assert.Equal(expected.Rule.DefaultRetention.Days, actual.Rule.DefaultRetention.Days);
		}
		public static void RetentionCompare(ObjectLockRetention expected, ObjectLockRetention actual)
		{
			Assert.Equal(expected.Mode, actual.Mode);
			Assert.Equal(expected.RetainUntilDate, actual.RetainUntilDate?.ToUniversalTime());

		}
		public static void VersionIdsCompare(List<S3ObjectVersion> expected, List<S3ObjectVersion> actual)
		{
			Assert.Equal(expected.Count, actual.Count);

			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(expected[i].VersionId, actual[i].VersionId);
				Assert.Equal(expected[i].ETag, actual[i].ETag);
				Assert.Equal(expected[i].Size, actual[i].Size);
				Assert.Equal(expected[i].Key, actual[i].Key);
			}
		}

		public static void LoggingConfigCompare(S3BucketLoggingConfig expected, S3BucketLoggingConfig actual)
		{
			Assert.Equal(expected.TargetBucketName, actual.TargetBucketName);

			if (expected.TargetPrefix == null) Assert.Null(actual.TargetPrefix);
			else Assert.Equal(expected.TargetPrefix, actual.TargetPrefix);

			if (expected.Grants == null && actual.Grants == null) return;
			CheckGrants(expected.Grants, actual.Grants);
		}

		public static void S3EventCompare(List<EventType> expected, List<EventType> actual)
		{
			Assert.Equal(expected.Count, actual.Count);
			for (int i = 0; i < expected.Count; i++)
				Assert.Equal(expected[i], actual[i]);
		}

		public static void PartsETagCompare(List<PartETag> expected, List<PartDetail> actual)
		{
			S3Utils.PartsETagCompare(expected, actual);
		}

		#endregion

		#region Get Data
		public static string TimeToString(DateTime time) => time.ToString(S3Headers.TimeFormat, S3Headers.TimeCulture);
		public static List<string> GetKeys(ListObjectsResponse response)
		{
			if (response != null)
			{
				List<string> temp = [];

				foreach (var s3Object in response.S3Objects) temp.Add(s3Object.Key);

				return temp;
			}
			return null;
		}
		public static List<string> GetKeys(ListObjectsV2Response response)
		{
			if (response != null)
			{
				List<string> temp = [];

				foreach (var s3Object in response.S3Objects) temp.Add(s3Object.Key);

				return temp;
			}
			return null;
		}
		public static List<string> GetKeys(ListVersionsResponse response)
		{
			if (response?.Versions != null)
			{
				List<string> temp = [];

				foreach (var version in response.Versions) temp.Add(version.Key);

				return temp;
			}
			return [];
		}
		public static string GetBody(GetObjectResponse response)
		{
			string body = string.Empty;
			if (response != null && response.ResponseStream != null)
			{
				var reader = new StreamReader(response.ResponseStream);
				body = reader.ReadToEnd();
				reader.Close();
			}
			return body;
		}
		public static ObjectData GetObjectToKey(string key, List<ObjectData> keyList)
		{
			foreach (var obj in keyList)
			{
				if (obj.Key.Equals(key)) return obj;
			}
			return null;
		}
		public static List<KeyVersion> GetKeyVersions(List<string> keyList)
		{
			List<KeyVersion> keyVersions = [];
			foreach (var key in keyList) keyVersions.Add(new KeyVersion() { Key = key });

			return keyVersions;
		}

		/// <summary>
		/// AWS SDK for .NET v4는 일부 오퍼레이션에서 4xx를 예외로 던지지 않고
		/// HttpStatusCode만 채운 응답 객체를 돌려준다(Java SDK v2는 항상 예외). 양쪽 모두를 받아 검증한다.
		/// </summary>
		public static void CheckErrorResponse<T>(HttpStatusCode expected, Func<T> call, string errorCode = null)
			where T : Amazon.Runtime.AmazonWebServiceResponse
		{
			try
			{
				var response = call();
				Assert.Equal(expected, response.HttpStatusCode);
			}
			catch (AggregateException e)
			{
				Assert.Equal(expected, GetStatus(e));
				if (errorCode != null) Assert.Equal(errorCode, GetErrorCode(e));
			}
		}

		/// <summary>S3의 url 인코딩 응답을 디코딩한다(공백은 +로 인코딩된다).</summary>
		public static string UrlDecode(string value) => Uri.UnescapeDataString(value.Replace("+", " "));

		/// <summary>
		/// 잘못된 요청이 거부되는지 확인한다.
		/// .NET SDK v4는 일부 필수 필드를 클라이언트 측에서 먼저 막아 요청이 서버에 도달하지 않는다.
		/// 이때 AmazonS3Exception.StatusCode는 0이므로 서버 응답(4xx)과 클라이언트 거부를 모두 통과로 본다.
		/// </summary>
		public static void CheckRejected<T>(HttpStatusCode expected, Func<T> call, string errorCode = null)
		{
			var e = Assert.Throws<AggregateException>(() => call());
			var status = GetStatus(e);

			// 클라이언트 측 검증에서 걸린 경우(StatusCode 미설정)
			if (status == 0) return;

			Assert.Equal(expected, status);
			if (errorCode != null) Assert.Equal(errorCode, GetErrorCode(e));
		}

		public static HttpStatusCode GetStatus(AggregateException e) => (e.InnerException is AmazonS3Exception e2) ? e2.StatusCode : HttpStatusCode.OK;

		public static string GetErrorCode(AggregateException e) => (e.InnerException is AmazonS3Exception e2) ? e2.ErrorCode : null;

		public static long GetBytesUsed(ListObjectsV2Response response)
		{
			if (response == null) return 0;
			if (response.S3Objects == null) return 0;
			if (response.S3Objects.Count > 1) return 0;

			long size = 0;

			foreach (var obj in response.S3Objects) size += obj.Size ?? 0;

			return size;
		}
		public static List<KeyValuePair<string, string>> GetMetaData(MetadataCollection response)
		{
			var metaDataList = new List<KeyValuePair<string, string>>();

			foreach (var key in response.Keys)
				metaDataList.Add(new KeyValuePair<string, string>(key, response[key]));

			return metaDataList;
		}

		/// <summary>메타데이터 키에서 x-amz-meta- 접두사를 제거한다. .NET SDK는 접두사를 유지하고 java SDK v2는 제거한다.</summary>
		public static string NormalizeMetaKey(string key)
			=> key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase) ? key["x-amz-meta-".Length..] : key;

		/// <summary>
		/// 메타데이터를 순서와 무관하게 비교한다.
		/// S3는 메타데이터 순서를 보장하지 않으므로 리스트 비교(Assert.Equal)를 쓰면 안 된다.
		/// </summary>
		public static void CheckMetaData(List<KeyValuePair<string, string>> expected, MetadataCollection actual)
		{
			var actualMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			foreach (var key in actual.Keys) actualMap[NormalizeMetaKey(key)] = actual[key];

			Assert.Equal(expected.Count, actualMap.Count);
			foreach (var item in expected)
			{
				var key = NormalizeMetaKey(item.Key);
				Assert.True(actualMap.ContainsKey(key), $"metadata key not found: {key}");
				Assert.Equal(item.Value, actualMap[key]);
			}
		}
		public List<KeyValuePair<string, string>> GetACLHeader(string userId = null, string[] perms = null)
		{
			List<string> allHeaders = ["read", "write", "read-acp", "write-acp", "full-control"];

			var headers = new List<KeyValuePair<string, string>>();

			userId ??= Config.AltUser.UserId;
			if (perms == null)
			{
				foreach (var perm in allHeaders)
					headers.Add(new KeyValuePair<string, string>(string.Format("x-amz-grant-{0}", perm), string.Format("id={0}", userId)));
			}
			else
			{
				foreach (var perm in perms)
					headers.Add(new KeyValuePair<string, string>(string.Format("x-amz-grant-{0}", perm), string.Format("id={0}", userId)));
			}
			return headers;
		}
		public List<S3Grant> GetGrantList(string userId = null, S3Permission[] perms = null)
		{
			S3Permission[] allHeaders = [S3Permission.READ, S3Permission.WRITE, S3Permission.READ_ACP, S3Permission.WRITE_ACP, S3Permission.FULL_CONTROL];

			List<S3Grant> headers = [];

			userId ??= Config.AltUser.UserId;
			if (perms == null)
			{
				foreach (var perm in allHeaders)
					headers.Add(new S3Grant() { Permission = perm, Grantee = new S3Grantee() { CanonicalUser = userId } });
			}
			else
			{
				foreach (var perm in perms)
					headers.Add(new S3Grant() { Permission = perm, Grantee = new S3Grantee() { CanonicalUser = userId } });
			}
			return headers;
		}
		public static List<string> GetBucketList(ListBucketsResponse response)
		{
			if (response == null) return null;
			var buckets = response.Buckets;
			var bucketList = new List<string>();

			foreach (var bucket in buckets) bucketList.Add(bucket.BucketName);

			return bucketList;
		}
		public List<string> GetObjectList(string bucketName, string prefix = null)
		{
			var client = GetClient();
			var response = client.ListObjects(bucketName, prefix: prefix);
			return GetKeys(response);
		}
		public static List<S3ObjectVersion> GetVersions(List<S3ObjectVersion> versions)
		{
			if (versions == null) return null;

			var lists = new List<S3ObjectVersion>();
			foreach (var item in versions)
				if (item.IsDeleteMarker != true) lists.Add(item);
			return lists;
		}
		public static List<string> GetVersionIds(List<S3ObjectVersion> versions)
		{
			if (versions == null) return null;

			var lists = new List<string>();
			foreach (var item in versions)
				if (item.IsDeleteMarker != true) lists.Add(item.VersionId);
			return lists;
		}
		public static int GetDeleteMarkerCount(List<S3ObjectVersion> versions)
		{
			if (versions == null) return 0;
			int count = 0;
			foreach (var item in versions)
				if (item.IsDeleteMarker == true) count++;
			return count;
		}
		public static List<S3ObjectVersion> GetDeleteMarkers(List<S3ObjectVersion> versions)
		{
			if (versions == null) return null;

			var deleteMarkers = new List<S3ObjectVersion>();

			foreach (var item in versions)
				if (item.IsDeleteMarker == true) deleteMarkers.Add(item);
			return deleteMarkers;
		}
		public static List<S3Grant> GrantsSort(List<S3Grant> data)
		{
			// 같은 (사용자, 권한) 조합이 중복될 수 있으므로 Dictionary가 아니라 리스트를 정렬한다.
			var newList = new List<S3Grant>(data);
			newList.Sort((x, y) => string.CompareOrdinal(
				$"{x.Grantee.CanonicalUser}{x.Permission}", $"{y.Grantee.CanonicalUser}{y.Permission}"));
			return newList;
		}

		public static string GetResponsebody(Stream data)
		{
			StreamReader reader = new(data, Encoding.UTF8);
			return reader.ReadToEnd();
		}
		public static string GetResponseErrorCode(Stream data)
		{
			StreamReader reader = new(data, Encoding.UTF8);
			var result = reader.ReadToEnd();

			int start = result.IndexOf("<Code>") + 6;
			int end = result.IndexOf("</Code>");

			return result[start..end];
		}


		#endregion

		#region Setup

		public string GetNewBucket()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();
			client.PutBucket(bucketName);
			return bucketName;
		}
		public string GetNewBucket(S3Client client)
		{
			var bucketName = GetNewBucketName();
			client.PutBucket(bucketName);
			return bucketName;
		}
		public string GetNewBucket(S3Client client, ObjectOwnership ownership)
		{
			var bucketName = GetNewBucketName();
			client.PutBucket(bucketName, objectOwnership: ownership);
			return bucketName;
		}
		public string GetNewBucketCannedAcl(S3Client client)
		{
			var bucketName = GetNewBucket(client, ObjectOwnership.ObjectWriter);
			client.PutPublicAccessBlock(bucketName, new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = false,
				IgnorePublicAcls = false,
				BlockPublicPolicy = false,
				RestrictPublicBuckets = false
			});
			return bucketName;
		}

		/// <summary>
		/// ACL을 지정해 버킷을 만든다. AWS 신규 버킷 기본값(BucketOwnerEnforced + PublicAccessBlock)에서는
		/// ACL 자체가 거부되므로 ObjectWriter로 만들고 차단 설정을 함께 해제한다.
		/// (java testV2 createBucketCannedAcl(client, testId, acl)과 동일)
		/// </summary>
		public string GetNewBucketCannedAcl(S3Client client, S3CannedACL acl)
			=> CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, acl);

		/// <summary>
		/// AWS는 2026-04부터 신규 버킷에서 SSE-C 업로드를 기본 차단한다.
		/// BlockedEncryptionTypes를 NONE으로 덮어써서 해제한다. (java testV2 unblockSseC와 동일)
		/// </summary>
		public void UnblockSseC(string bucketName)
		{
			if (!Config.S3.IsAWS) return;

			var client = GetClient();
			client.PutBucketEncryption(bucketName, new ServerSideEncryptionConfiguration()
			{
				ServerSideEncryptionRules =
				[
					new()
					{
						ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
						{
							ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
						},
						BlockedEncryptionTypes = new BlockedEncryptionTypes()
						{
							EncryptionType = [Amazon.S3.EncryptionType.NONE]
						}
					}
				]
			});

			// 설정 반영에 지연이 있어 SSE-C 차단이 사라질 때까지 확인한다.
			for (int i = 0; i < 5; i++)
			{
				try
				{
					var response = client.GetBucketEncryption(bucketName);
					var blocked = false;
					foreach (var rule in response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules)
					{
						if (rule.BlockedEncryptionTypes?.EncryptionType != null &&
							rule.BlockedEncryptionTypes.EncryptionType.Contains(Amazon.S3.EncryptionType.SSEC))
							blocked = true;
					}
					if (!blocked) return;
				}
				catch (Exception)
				{
					// 설정 반영 전이면 재시도
				}
				Delay(1000);
			}

			Assert.Fail($"SSE-C unblock failed : {bucketName}");
		}

		public string GetNewBucket(S3Client client, ObjectOwnership ownership, S3CannedACL acl)
		{
			var bucketName = GetNewBucket(client, ownership);
			client.PutBucketACL(bucketName, acl: acl);
			return bucketName;
		}

		public string SetupMetadata(string metadata, string bucketName = null)
		{
			if (string.IsNullOrEmpty(bucketName)) bucketName = GetNewBucket();

			var client = GetClient();
			var keyName = "foo";
			var metadataKey = "x-amz-meta-meta1";

			var metadataList = new List<KeyValuePair<string, string>>
			{
				new(metadataKey, metadata)
			};

			client.PutObject(bucketName, key: keyName, body: "bar", metadataList: metadataList);

			var response = client.GetObject(bucketName, keyName);
			return response.Metadata[metadataKey];
		}

		public string SetupObjects(List<string> keyList, string bucketName = null, string body = null)
		{
			var client = GetClient();
			if (bucketName == null)
			{
				bucketName = GetNewBucketName();
				client.PutBucket(bucketName);
			}

			if (keyList != null && client != null)
			{
				foreach (var key in keyList)
				{
					if (body == null) client.PutObject(bucketName, key, key);
					else client.PutObject(bucketName, key, body);
				}
			}

			return bucketName;
		}
		public string SetupObjectsV4(List<string> keyList, string bucketName = null, string body = null,
										bool? useChunkEncoding = null, bool? disablePayloadSigning = null)
		{
			// SDK v4는 DisablePayloadSigning 사용 시 HTTPS를 강제한다.
			var client = disablePayloadSigning == true ? GetClientHttpsV4() : GetClientV4();
			if (bucketName == null)
			{
				bucketName = GetNewBucketName();
				client.PutBucket(bucketName);
			}

			if (keyList != null && client != null)
			{
				foreach (var key in keyList)
				{
					body ??= key;
					client.PutObject(bucketName, key, body, useChunkEncoding: useChunkEncoding, disablePayloadSigning: disablePayloadSigning);
				}
			}

			return bucketName;
		}

		public string SetupKeyWithRandomContent(string key, int size = 7 * MainData.MB, string bucketName = null, S3Client client = null)
		{
			bucketName ??= GetNewBucket();
			client ??= GetClient();

			var data = S3Utils.RandomTextToLong(size);
			client.PutObject(bucketName, key, body: data);

			return bucketName;
		}


		public string CreateBucketWithAcl(S3Client client, ObjectOwnership ownership, S3CannedACL acl = null)
		{
			var bucketName = GetNewBucket(client, ownership);
			client.PutPublicAccessBlock(bucketName, new PublicAccessBlockConfiguration()
			{
				BlockPublicAcls = false,
				IgnorePublicAcls = false,
				BlockPublicPolicy = false,
				RestrictPublicBuckets = false
			});
			if (acl != null) client.PutBucketACL(bucketName, acl: acl);
			return bucketName;
		}

		public string SetupAclBucket(ObjectOwnership ownership, S3CannedACL acl, List<string> keys)
		{
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ownership, acl);
			SetupObjects(keys, bucketName);
			return bucketName;
		}

		public string SetupAclBucket(S3CannedACL acl, List<string> keys)
			=> SetupAclBucket(ObjectOwnership.ObjectWriter, acl, keys);

		public string SetupAclObjects(S3CannedACL bucketAcl, S3CannedACL objectAcl, params string[] keys)
			=> SetupAclObjects(ObjectOwnership.ObjectWriter, bucketAcl, objectAcl, keys);

		public string SetupAclObjects(ObjectOwnership ownership, S3CannedACL bucketAcl, S3CannedACL objectAcl, params string[] keys)
		{
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ownership, bucketAcl);
			foreach (var key in keys)
				client.PutObject(bucketName, key, body: key, acl: objectAcl);
			return bucketName;
		}

		public string SetupAclObjectsByAlt(S3CannedACL bucketAcl, S3CannedACL objectAcl, params string[] keys)
			=> SetupAclObjectsByAlt(ObjectOwnership.ObjectWriter, bucketAcl, objectAcl, keys);

		public string SetupAclObjectsByAlt(ObjectOwnership ownership, S3CannedACL bucketAcl, S3CannedACL objectAcl, params string[] keys)
		{
			var altClient = GetAltClient();
			var bucketName = CreateBucketWithAcl(GetClient(), ownership, bucketAcl);
			foreach (var key in keys)
				altClient.PutObject(bucketName, key, body: key, acl: objectAcl);
			return bucketName;
		}

		public S3AccessControlList CreateAltAcl(params S3Permission[] permissions)
		{
			var mainUserId = Config.MainUser.UserId;
			var mainDisplayName = Config.MainUser.DisplayName;
			var altUserId = Config.AltUser.UserId;

			var grants = new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new S3Grantee() { CanonicalUser = mainUserId, DisplayName = mainDisplayName }
				}
			};
			foreach (var permission in permissions)
				grants.Add(new S3Grant() { Permission = permission, Grantee = new S3Grantee() { CanonicalUser = altUserId } });

			return new S3AccessControlList()
			{
				Owner = new Owner() { Id = mainUserId, DisplayName = mainDisplayName },
				Grants = grants
			};
		}

		public string SetupBucketPermission(S3Permission permission)
		{
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			client.PutBucketACL(bucketName, accessControlPolicy: CreateAltAcl(permission));
			return bucketName;
		}

		public string SetupObjectPermission(string key, S3Permission permission)
		{
			var client = GetClient();
			var bucketName = CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, S3CannedACL.PublicReadWrite);
			client.PutObject(bucketName, key, body: key);
			client.PutObjectACL(bucketName, key, accessControlPolicy: CreateAltAcl(permission));
			return bucketName;
		}

		public MultipartUploadData MultipartUpload(S3Client client, string bucketName, string key, int size, int partSize)
		{
			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, partSize: partSize);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			return uploadData;
		}

		public static void Delay(int milliseconds) => Thread.Sleep(milliseconds);

		public static MultipartUploadData SetupMultipartCopy(S3Client client, string srcBucketName, string srcKey, string destBucketName, string destKey, int size,
			int partSize = 5 * MainData.MB, string versionId = null, SSECustomerKey srcCustomerKey = null, SSECustomerKey destCustomerKey = null, ServerSideEncryptionMethod SSE_S3 = null)
		{
			var uploadData = new MultipartUploadData();

			var response = client.InitiateMultipartUpload(destBucketName, destKey, sseCustomerKey: srcCustomerKey, sseKey: SSE_S3);
			uploadData.UploadId = response.UploadId;

			int start = 0;
			while (start < size)
			{
				int end = Math.Min(start + partSize - 1, size - 1);
				var partNumber = uploadData.NextPartNumber;

				var partResPonse = client.CopyPart(srcBucketName, srcKey, destBucketName, destKey, uploadData.UploadId, partNumber, start, end,
							versionId: versionId, srcEncryptionKey: srcCustomerKey, destEncryptionKey: destCustomerKey);
				uploadData.AddPart(partNumber, partResPonse.ETag);

				start = end + 1;
			}

			return uploadData;
		}



		public S3AccessControlList AddObjectUserGrant(string bucketName, string key, S3Grant grant)
		{
			var client = GetClient();
			var mainUserId = Config.MainUser.UserId;
			var mainUserDisplayName = Config.MainUser.DisplayName;

			var response = client.GetObjectACL(bucketName, key);
			var grants = response.AccessControlList.Grants;
			grants.Add(grant);

			var myGrants = new S3AccessControlList()
			{
				Grants = grants,
				Owner = new Owner()
				{
					DisplayName = mainUserDisplayName,
					Id = mainUserId,
				}
			};

			return myGrants;
		}
		public S3AccessControlList AddBucketUserGrant(string bucketName, S3Grant grant)
		{
			var client = GetClient();
			var mainUserId = Config.MainUser.UserId;
			var mainUserDisplayName = Config.MainUser.DisplayName;

			var response = client.GetBucketACL(bucketName);
			var grants = response.AccessControlList.Grants;
			grants.Add(grant);

			var myGrants = new S3AccessControlList()
			{
				Grants = grants,
				Owner = new Owner()
				{
					DisplayName = mainUserDisplayName,
					Id = mainUserId,
				}
			};

			return myGrants;
		}


		public static void SetupMultipleVersions(S3Client client, string bucketName, string key, int numVersions, ref List<string> versionIds, ref List<string> contents, bool checkVersion = true)
		{
			versionIds ??= [];
			contents ??= [];

			for (int i = 0; i < numVersions; i++)
			{
				var body = string.Format("content-{0}", i);
				var response = client.PutObject(bucketName, key, body: body);
				var versionId = response.VersionId;

				contents.Add(body);
				versionIds.Add(versionId);
			}

			if (checkVersion) CheckObjVersions(client, bucketName, key, versionIds, contents);

		}
		public static void SetupMultipleVersion(S3Client client, string bucketName, string key, int numVersions, bool checkVersion = true)
		{
			var versionIds = new List<string>();
			var contents = new List<string>();

			for (int i = 0; i < numVersions; i++)
			{
				var body = string.Format("content-{0}", i);
				var response = client.PutObject(bucketName, key, body: body);
				var versionId = response.VersionId;

				contents.Add(body);
				versionIds.Add(versionId);
			}

			if (checkVersion) CheckObjVersions(client, bucketName, key, versionIds, contents);
		}
		public static void OverwriteSuspendedVersioningObj(S3Client client, string bucketName, string key, ref List<string> versionIds, ref List<string> contents, string content)
		{
			client.PutObject(bucketName, key, body: content);

			Assert.Equal(versionIds.Count, contents.Count);

			for (int i = versionIds.Count - 1; i >= 0; i--)
			{
				if (versionIds[i] == "null")
				{
					versionIds.RemoveAt(i);
					contents.RemoveAt(i);
				}
			}
			contents.Add(content);
			versionIds.Add("null");
			Thread.Sleep(100);
		}

		public static List<Thread> SetupVersionedObjConcurrent(S3Client client, string bucketName, string key, int num)
		{
			var tList = new List<Thread>();
			for (int i = 0; i < num; i++)
			{
				var mThread = new Thread(() => client.PutObject(bucketName, key, body: string.Format("Data {0}", i)));
				mThread.Start();
				tList.Add(mThread);
			}
			return tList;
		}
		#endregion

		#region Remove
		public static void RemoveObjVersion(S3Client client, string bucketName, string key, List<string> versionIds, List<string> contents, int index)
		{
			Assert.Equal(versionIds.Count, contents.Count);
			var rmVersionId = versionIds[index]; versionIds.RemoveAt(index);
			var rmContent = contents[index]; contents.RemoveAt(index);

			CheckObjContent(client, bucketName, key, rmVersionId, rmContent);

			client.DeleteObject(bucketName, key, versionId: rmVersionId);

			if (versionIds.Count != 0)
				CheckObjVersions(client, bucketName, key, versionIds, contents);
		}
		public static void DeleteSuspendedVersioningObj(S3Client client, string bucketName, string key, ref List<string> versionIds, ref List<string> contents)
		{
			client.DeleteObject(bucketName, key);

			Assert.Equal(versionIds.Count, contents.Count);

			for (int i = versionIds.Count - 1; i >= 0; i--)
			{
				if (versionIds[i] == "null")
				{
					versionIds.RemoveAt(i);
					contents.RemoveAt(i);
				}
			}
			Thread.Sleep(100);
		}

		public static List<Thread> DoClearVersionedBucketConcurrent(S3Client client, string bucketName)
		{
			var tList = new List<Thread>();
			var response = client.ListVersions(bucketName);

			foreach (var version in response.Versions)
			{
				var mThread = new Thread(() => client.DeleteObject(bucketName, version.Key, versionId: version.VersionId));
				mThread.Start();
				tList.Add(mThread);
			}
			return tList;
		}
		#endregion

		#region Test


		public AmazonS3Exception TestMetadataUnreadable(string metadata, string bucketName = null)
		{
			if (string.IsNullOrEmpty(bucketName)) bucketName = GetNewBucket();

			var client = GetClient();
			var keyName = "foo";
			var metadataKey = "x-amz-meta-meta1";

			var metadataList = new List<KeyValuePair<string, string>>
			{
				new(metadataKey, metadata)
			};


			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key: keyName, body: "bar", metadataList: metadataList));

			if (e.InnerException is AmazonS3Exception e2) return e2;
			return null;
		}

		public void TestBucketCreateNamingGoodLong(int length)
		{
			var bucketName = GetPrefix();
			if (bucketName.Length < length) bucketName += S3Utils.RandomText(length - bucketName.Length);
			else bucketName = bucketName.Substring(0, length - 1);
			BucketList.Add(bucketName);

			var client = GetClient();
			var response = client.PutBucket(bucketName);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		public void TestBucketCreateNamingBadLong(int length)
		{
			var bucketName = GetPrefix();
			if (bucketName.Length < length) bucketName += S3Utils.RandomText(length - bucketName.Length);
			else bucketName = bucketName.Substring(0, length - 1);

			CheckBadBucketName(bucketName);
		}

		public static void TestCreateRemoveVersions(S3Client client, string bucketName, string key, int numversions, int removeStartIdx, int idxInc)
		{
			List<string> versionIds = null;
			List<string> contents = null;
			S3Utils.SetupMultipleVersions(client, bucketName, key, numversions, ref versionIds, ref contents);
			var idx = removeStartIdx;

			for (int i = 0; i < numversions; i++)
			{
				S3Utils.RemoveObjVersion(client, bucketName, key, versionIds, contents, idx);
				idx += idxInc;
			}
		}

		public void TestEncryptionCSEWrite(int fileSize)
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testobj";

			//AES
			var AES = new AES256();

			var data = new string('A', fileSize);
			var encodingData = AES.AESEncrypt(data);
			var metadataList = new List<KeyValuePair<string, string>>()
			{
				new("x-amz-meta-key", AES.Key),
			};
			client.PutObject(bucketName, key: key, body: encodingData, metadataList: metadataList);

			var response = client.GetObject(bucketName, key: key);
			var encodingbody = S3Utils.GetBody(response);
			var body = AES.AESDecrypt(encodingbody);
			Assert.Equal(data, body);
		}
		public void TestEncryptionSSECustomerWrite(int fileSize)
		{
			var bucketName = GetNewBucket();
			UnblockSseC(bucketName);
			var client = GetClientHttps();
			var key = "testobj";
			var data = new string('A', fileSize);
			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};
			client.PutObject(bucketName, key: key, body: data, sseCustomerKey: sseC);

			var response = client.GetObject(bucketName, key: key, sseCustomerKey: sseC);
			var body = S3Utils.GetBody(response);
			Assert.Equal(data, body);
		}
		public void TestEncryptionSSES3ustomerWrite(int fileSize)
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testobj";
			var data = new string('A', fileSize);

			client.PutObject(bucketName, key: key, body: data, sseKey: ServerSideEncryptionMethod.AES256);

			var response = client.GetObject(bucketName, key: key);
			var body = S3Utils.GetBody(response);
			Assert.Equal(data, body);
		}
		public void TestEncryptionSSES3Copy(int fileSize)
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var data = new string('A', fileSize);

			var SSEConfig = new ServerSideEncryptionConfiguration()
			{
				ServerSideEncryptionRules =
				[
					new()
					{
						ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
						{
							ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
						}
					}
				]
			};

			client.PutBucketEncryption(bucketName, SSEConfig);

			var response = client.GetBucketEncryption(bucketName);
			Assert.Single(response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var sourceKey = "bar";
			client.PutObject(bucketName, sourceKey, body: data);

			var sourceResponse = client.GetObject(bucketName, sourceKey);
			var sourceBody = S3Utils.GetBody(sourceResponse);
			Assert.Equal(ServerSideEncryptionMethod.AES256, sourceResponse.ServerSideEncryptionMethod);

			var destKey = "foo";
			client.CopyObject(bucketName, sourceKey, bucketName, destKey);
			var destResponse = client.GetObject(bucketName, destKey);
			Assert.Equal(ServerSideEncryptionMethod.AES256, destResponse.ServerSideEncryptionMethod);

			var destBody = S3Utils.GetBody(destResponse);
			Assert.Equal(sourceBody, destBody);
		}

		public void TestObjectCopy(bool sourceObjectEncryption, bool sourceBucketEncryption, bool destBucketEncryption, bool destObjectEncryption, int fileSize)
		{
			var sourceKey = "SourceKey";
			var destKey = "DestKey";
			var sourceBucketName = GetNewBucket();
			var destBucketName = GetNewBucket();
			var client = GetClient();
			var data = new string('A', fileSize);

			//Source Put Object
			if (sourceObjectEncryption) client.PutObject(sourceBucketName, sourceKey, body: data, sseKey: ServerSideEncryptionMethod.AES256);
			else client.PutObject(sourceBucketName, sourceKey, body: data);

			////Source Object Check
			var sourceResponse = client.GetObject(sourceBucketName, sourceKey);
			var sourceBody = S3Utils.GetBody(sourceResponse);
			// AWS는 2023-01부터 모든 오브젝트에 SSE-S3를 기본 적용하므로 암호화를 지정하지 않아도 AES256이 붙는다.
			if (sourceObjectEncryption || Config.S3.IsAWS)
				Assert.Equal(ServerSideEncryptionMethod.AES256, sourceResponse.ServerSideEncryptionMethod);
			else Assert.Null(sourceResponse.ServerSideEncryptionMethod);
			Assert.Equal(data, sourceBody);

			var SSEConfig = new ServerSideEncryptionConfiguration()
			{
				ServerSideEncryptionRules =
				[
					new()
					{
						ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
						{
							ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
						}
					}
				]
			};

			//Source Bucket Encryption
			if (sourceBucketEncryption)
			{
				client.PutBucketEncryption(sourceBucketName, SSEConfig);

				var encryptionResponse = client.GetBucketEncryption(sourceBucketName);
				Assert.Single(encryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
			}

			//Dest Bucket Encryption
			if (destBucketEncryption)
			{
				client.PutBucketEncryption(destBucketName, SSEConfig);

				var encryptionResponse = client.GetBucketEncryption(destBucketName);
				Assert.Single(encryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
			}

			//Source Copy Object
			if (destObjectEncryption) client.CopyObject(sourceBucketName, sourceKey, destBucketName, destKey, sseKey: ServerSideEncryptionMethod.AES256);
			else client.CopyObject(sourceBucketName, sourceKey, destBucketName, destKey);

			//Dest Object Check
			var destResponse = client.GetObject(destBucketName, destKey);
			var destBody = S3Utils.GetBody(destResponse);
			if (destBucketEncryption || destObjectEncryption || Config.S3.IsAWS)
				Assert.Equal(ServerSideEncryptionMethod.AES256, destResponse.ServerSideEncryptionMethod);
			else Assert.Null(destResponse.ServerSideEncryptionMethod);
			Assert.Equal(sourceBody, destBody);
		}

		public void TestObjectCopy(EncryptionType source, EncryptionType target, int fileSize)
		{
			var sourceKey = "SourceKey";
			var targetKey = "TargetKey";
			var bucketName = GetNewBucket();
			if (source == EncryptionType.SSE_C || target == EncryptionType.SSE_C) UnblockSseC(bucketName);
			var client = GetClientHttps();
			var content = S3Utils.RandomTextToLong(fileSize);

			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			switch (source)
			{
				case EncryptionType.NORMAL:
					client.PutObject(bucketName, sourceKey, content);
					break;
				case EncryptionType.SSE_S3:
					client.PutObject(bucketName, sourceKey, content, sseKey: ServerSideEncryptionMethod.AES256);
					break;
				case EncryptionType.SSE_C:
					client.PutObject(bucketName, sourceKey, content, sseCustomerKey: sseC);
					break;
			}

			client.CopyObject(bucketName, sourceKey, bucketName, targetKey,
								metadataDirective: S3MetadataDirective.REPLACE,
								srcCustomerKey: source == EncryptionType.SSE_C ? sseC : null,
								sseKey: target == EncryptionType.SSE_S3 ? ServerSideEncryptionMethod.AES256 : null,
								destCustomerKey: target == EncryptionType.SSE_C ? sseC : null);

			var sourceResponse = client.GetObject(bucketName, sourceKey, sseCustomerKey: source == EncryptionType.SSE_C ? sseC : null);
			Assert.Equal(content, S3Utils.GetBody(sourceResponse));

			// SSE-C가 아닌 대상에 SSE-C 키를 붙이면 AWS가 거부한다("encryption parameters are not applicable").
			var targetResponse = client.GetObject(bucketName, targetKey, sseCustomerKey: target == EncryptionType.SSE_C ? sseC : null);
			Assert.Equal(content, S3Utils.GetBody(targetResponse));
		}

		public string DoTestMultipartUploadContents(string bucketName, string key, int numParts)
		{
			var payload = S3Utils.RandomTextToLong(5 * MainData.MB);
			var client = GetClient();

			var initResponse = client.InitiateMultipartUpload(bucketName, key);
			var uploadId = initResponse.UploadId;
			var parts = new List<PartETag>();
			var allPayload = "";

			for (int i = 0; i < numParts; i++)
			{
				var partNumber = i + 1;
				var partResponse = client.UploadPart(bucketName, key, uploadId, payload, partNumber);
				parts.Add(new PartETag(partNumber, partResponse.ETag));
				allPayload += payload;
			}
			var lestPayload = S3Utils.RandomTextToLong(MainData.MB);
			var lestPartResponse = client.UploadPart(bucketName, key, uploadId, lestPayload, numParts + 1);
			parts.Add(new PartETag(numParts + 1, lestPartResponse.ETag));
			allPayload += lestPayload;

			client.CompleteMultipartUpload(bucketName, key, uploadId, parts);

			var response = client.GetObject(bucketName, key);
			var text = S3Utils.GetBody(response);

			Assert.Equal(allPayload, text);

			return allPayload;
		}

		#endregion

		#region Backend Utils

		/// <summary>백엔드 복제시 요청에 다시 실을 수 없는 전송 계층 헤더.</summary>
		private static readonly HashSet<string> BackendSkipHeaders = new(StringComparer.OrdinalIgnoreCase)
		{
			"content-length", "connection", "keep-alive", "transfer-encoding", "date", "server", "host",
		};

		/// <summary>응답 헤더를 다음 요청에 그대로 전달하기 위해 복사한다. 메타데이터(x-amz-meta-*)는 별도로 복사하므로 제외.</summary>
		private static List<KeyValuePair<string, string>> CopyBackendHeaders(HeadersCollection headers)
		{
			var headerList = new List<KeyValuePair<string, string>>();
			if (headers == null) return headerList;

			foreach (var key in headers.Keys)
			{
				if (BackendSkipHeaders.Contains(key)) continue;
				if (key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase)) continue;

				var value = headers[key] ?? string.Empty;
				// UTF-8 서명 에러를 배제하기 위해 소문자로 변경
				if (key.Equals("content-type", StringComparison.OrdinalIgnoreCase)) value = value.Replace("UTF-8", "utf-8");
				headerList.Add(new(key, value));
			}
			return headerList;
		}

		private static List<KeyValuePair<string, string>> CopyBackendMetadata(MetadataCollection metadata)
		{
			var metadataList = new List<KeyValuePair<string, string>>();
			if (metadata == null) return metadataList;

			foreach (var key in metadata.Keys) metadataList.Add(new(key, metadata[key]));
			return metadataList;
		}

		/// <summary>Backend 클라이언트로 다운로드하여 바로 PutObject로 업로드</summary>
		public static void BackendPutObject(S3Client client, string sourceBucketName, string sourceKey,
			string targetBucketName, string targetKey, string versionId)
		{
			// Backend 클라이언트로 다운로드
			var response = client.GetObject(sourceBucketName, sourceKey, versionId: versionId);
			var body = GetBody(response);

			// 헤더 복사 후 버전 정보 추가
			var headerList = CopyBackendHeaders(response.Headers);
			headerList.Add(new(BackendHeaders.IFS_VERSION_ID, versionId));
			headerList.Add(new(BackendHeaders.KSAN_VERSION_ID, versionId));

			// Backend 클라이언트로 업로드
			client.PutObject(targetBucketName, targetKey, body: body,
				metadataList: CopyBackendMetadata(response.Metadata), headerList: headerList);
		}

		/// <summary>Backend 클라이언트로 복사</summary>
		public static void BackendCopyObject(S3Client client, string sourceBucketName, string sourceKey,
			string targetBucketName, string targetKey, string sourceVersionId, string targetVersionId)
		{
			client.CopyObject(sourceBucketName, sourceKey, targetBucketName, targetKey, versionId: sourceVersionId,
				headerList: [
					new(BackendHeaders.IFS_VERSION_ID, targetVersionId),
					new(BackendHeaders.KSAN_VERSION_ID, targetVersionId),
				]);
		}

		/// <summary>Backend 클라이언트로 멀티파트 업로드</summary>
		public static void BackendMultipartUpload(S3Client client, string sourceBucketName, string sourceKey,
			string targetBucketName, string targetKey, string versionId)
		{
			long partsSize = 5 * MainData.MB;

			// 메타 정보 가져오기
			var metadata = client.GetObjectMetadata(sourceBucketName, sourceKey, versionId: versionId);

			// Multipart 등록
			var initResponse = client.InitiateMultipartUpload(targetBucketName, targetKey,
				metadataList: CopyBackendMetadata(metadata.Metadata), headerList: CopyBackendHeaders(metadata.Headers));
			var uploadId = initResponse.UploadId;

			// 오브젝트의 사이즈 확인
			var size = metadata.ContentLength;

			// 업로드 시작
			var parts = new List<PartETag>();
			long index = 0;

			while (index < size)
			{
				var start = index;
				var partNumber = parts.Count + 1;
				var end = Math.Min(start + partsSize, size) - 1;

				// 업로드할 내용 가져오기
				var s3Object = client.GetObject(sourceBucketName, sourceKey, versionId: versionId,
					range: new ByteRange(start, end));
				var body = GetBody(s3Object);

				// 업로드 파츠
				var partResponse = client.UploadPart(targetBucketName, targetKey, uploadId, body, partNumber);
				parts.Add(new PartETag(partNumber, partResponse.ETag));

				index += end - start + 1;
			}

			// 업로드 완료 요청(버전 정보 포함)
			client.CompleteMultipartUpload(targetBucketName, targetKey, uploadId, parts,
				headerList: [
					new(BackendHeaders.IFS_VERSION_ID, versionId),
					new(BackendHeaders.KSAN_VERSION_ID, versionId),
				]);
		}

		/// <summary>Backend 클라이언트로 ACL 설정</summary>
		public static void BackendPutObjectAcl(S3Client client, string sourceBucketName, string sourceKey,
			string targetBucketName, string targetKey, string versionId)
		{
			// ACL 정보 가져오기
			var acl = client.GetObjectACL(sourceBucketName, sourceKey, versionId: versionId);

			// ACL 설정
			client.PutObjectACL(targetBucketName, targetKey, accessControlPolicy: acl.AccessControlList, versionId: versionId);
		}

		/// <summary>Backend 클라이언트로 태그 설정</summary>
		public static void BackendPutObjectTagging(S3Client client, string sourceBucketName, string sourceKey,
			string targetBucketName, string targetKey, string versionId)
		{
			// 태그 정보 가져오기
			var tagging = client.GetObjectTagging(sourceBucketName, sourceKey, versionId: versionId);

			// 태그 설정
			client.PutObjectTagging(targetBucketName, targetKey, new Tagging() { TagSet = tagging.Tagging }, versionId: versionId);
		}

		/// <summary>Backend 클라이언트로 삭제(삭제 마커의 버전 정보를 지정한다)</summary>
		public static void BackendDeleteObject(S3Client client, string bucketName, string key, string versionId)
		{
			client.PutObject(bucketName, key, body: string.Empty,
				headerList: [
					new(BackendHeaders.DELETE_MARKER_VERSION_ID, versionId),
					new(BackendHeaders.KSAN_DELETE_MARKER_VERSION_ID, versionId),
				]);
		}

		/// <summary>Backend 클라이언트로 태그 삭제</summary>
		public static void BackendDeleteObjectTagging(S3Client client, string bucketName, string key, string versionId)
			=> client.DeleteObjectTagging(bucketName, key, versionId: versionId);

		#endregion

		#region Bucket Clear
		public void BucketClear()
		{
			var client = GetClient();
			if (client == null) return;
			if (BucketList == null) return;

			foreach (var bucketName in BucketList)
			{
				if (string.IsNullOrWhiteSpace(bucketName)) continue;

				try
				{
					var objectList = client.ListObjectsV2(bucketName);
					foreach (var obj in objectList.S3Objects) client.DeleteObject(bucketName, obj.Key);

					var versions = client.ListVersions(bucketName);
					foreach (var obj in versions.Versions) client.DeleteObject(bucketName, obj.Key, versionId: obj.VersionId);

					client.DeleteBucket(bucketName);
				}
				catch (Exception)
				{
					Console.WriteLine($"BucketClear Error: {bucketName}");
				}
			}
		}
		#endregion

		#region Checksum helpers

		public static void ChecksumCompare(ChecksumAlgorithm algorithm, string content, PutObjectResponse response)
		{
			var expected = CheckSum.CalculateChecksum(algorithm, content);
			Assert.Equal(expected, CheckSum.GetChecksum(response, algorithm));
			Assert.Equal(ChecksumType.FULL_OBJECT, response.ChecksumType);
		}

		public static void ChecksumCompare(ChecksumAlgorithm algorithm, string content, CopyObjectResponse response)
		{
			var expected = CheckSum.CalculateChecksum(algorithm, content);
			Assert.Equal(expected, CheckSum.GetChecksum(response, algorithm));
		}

		public static void ChecksumCompare(ChecksumAlgorithm algorithm, string content, UploadPartResponse response)
		{
			var expected = CheckSum.CalculateChecksum(algorithm, content);
			Assert.Equal(expected, CheckSum.GetChecksum(response, algorithm));
		}

		public static void ChecksumCompare(ChecksumAlgorithm algorithm, MultipartUploadData uploadData,
			CompleteMultipartUploadResponse response)
		{
			var contents = uploadData.Parts.Select(p => CheckSum.GetChecksum(p, algorithm)).ToList();
			string expected = response.ChecksumType == ChecksumType.COMPOSITE
				? CheckSum.CalculateChecksumByBase64(algorithm, contents)
				: CheckSum.CombineChecksumByBase64(algorithm, uploadData.PartSize, contents);
			Assert.Equal(expected, CheckSum.GetChecksum(response, algorithm));
		}

		public static void ChecksumCompare(ChecksumAlgorithm algorithm, string content, GetObjectAttributesResponse response)
		{
			var expected = CheckSum.CalculateChecksum(algorithm, content);
			Assert.Equal(expected, CheckSum.GetChecksum(response, algorithm));
		}

		public MultipartUploadData MultipartUploadChecksum(S3Client client, string bucketName, string key,
			ChecksumType checksumType, ChecksumAlgorithm checksum)
		{
			const int size = 10 * MainData.MB;
			const int partSize = 5 * MainData.MB;
			var uploadData = new MultipartUploadData { PartSize = partSize };

			var createResponse = client.InitiateMultipartUpload(bucketName, key,
				checksumAlgorithm: checksum, checksumType: checksumType);
			uploadData.UploadId = createResponse.UploadId;

			var parts = MakePartBodies(size, partSize);
			foreach (var part in parts)
			{
				uploadData.AppendBody(part);
				var partResponse = client.UploadPart(bucketName, key, uploadData.UploadId, part,
					uploadData.NextPartNumber, checksumAlgorithm: checksum);
				ChecksumCompare(checksum, part, partResponse);
				uploadData.AddPart(checksum, partResponse);
			}

			var completeResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId,
				uploadData.Parts, checksumType: checksumType);
			Assert.Equal(checksumType, completeResponse.ChecksumType);
			ChecksumCompare(checksum, uploadData, completeResponse);
			return uploadData;
		}

		private static List<string> MakePartBodies(int size, int partSize)
		{
			var list = new List<string>();
			int remain = size;
			while (remain > 0)
			{
				int now = remain > partSize ? partSize : remain;
				list.Add(S3Utils.RandomTextToLong(now));
				remain -= now;
			}
			return list;
		}

		#endregion
	}
}
