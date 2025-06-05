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
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace s3tests
{
	public abstract class TestBase : IDisposable
	{

		#region Define
		private static readonly char[] TEXT = "abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray();
		private static readonly char[] TEXT_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();
		private const int RANDOM_PREFIX_TEXT_LENGTH = 30;
		private const int RANDOM_SUFFIX_TEXT_LENGTH = 5;
		private const int BUCKET_MAX_LENGTH = 63;
		private const string STR_RANDOM = "{random}";
		readonly Random rand = new(Guid.NewGuid().GetHashCode());
		#endregion

		#region Values
		public ITestOutputHelper Output;
		public readonly List<string> EmptyList = new();
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

			BucketList = new List<string>();
		}

		public void Dispose() => Clear();
		public void Clear() => BucketClear();

		#region Get client
		public S3Client GetClient() => new(Config.S3, Config.SignatureVersion, Config.IsSecure, Config.MainUser, Output);
		public S3Client GetClientV4() => new(Config.S3, MainConfig.STR_SIGNATURE_VERSION_4, Config.IsSecure, Config.MainUser, Output);
		public S3Client GetClientHttps() => new(Config.S3, Config.SignatureVersion, true, Config.MainUser, Output);
		public S3Client GetClientHttpsV4(RequestChecksumCalculation? requestChecksumCalculation = null,
		ResponseChecksumValidation? responseChecksumValidation = null) => new(Config.S3, MainConfig.STR_SIGNATURE_VERSION_4, true, Config.MainUser, Output, requestChecksumCalculation, responseChecksumValidation);
		public S3Client GetAltClient() => new(Config.S3, Config.SignatureVersion, Config.IsSecure, Config.AltUser, Output);
		public S3Client GetUnauthenticatedClient() => new(Config.S3, Config.SignatureVersion, Config.IsSecure, null, Output);
		public S3Client GetBadAuthClient(string accessKey = null, string secretKey = null)
		{
			if (accessKey == null) accessKey = "aaaaaaaaaaaaaaa";
			if (secretKey == null) secretKey = "bbbbbbbbbbbbbbb";
			var user = new UserData() { AccessKey = accessKey, SecretKey = secretKey };
			return new S3Client(Config.S3, Config.SignatureVersion, Config.IsSecure, user);
		}
		#endregion

		#region Create Data
		public string RandomText(int length) => new(Enumerable.Range(0, length).Select(x => TEXT[rand.Next(0, TEXT.Length)]).ToArray());
		public string RandomTextToLong(int length) => new(Enumerable.Range(0, length).Select(x => TEXT_STRING[rand.Next(0, TEXT_STRING.Length)]).ToArray());

		public string GetBase64EncodedSHA1Hash(string policy, string secretKey)
		{
			var data = Encoding.UTF8.GetBytes(policy);
			var keyBytes = Encoding.UTF8.GetBytes(secretKey);

			using HMACSHA1 sha1 = new(keyBytes);
			return Convert.ToBase64String(sha1.ComputeHash(data));
		}
		public string GetMD5(string data)
		{
			using var md5 = MD5.Create();
			var byteData = Encoding.UTF8.GetBytes(data);
			var hash = md5.ComputeHash(byteData, 0, byteData.Length);
			return Convert.ToBase64String(hash);
		}

		public string GetPrefix() => Config.BucketPrefix.Replace(STR_RANDOM, RandomText(RANDOM_PREFIX_TEXT_LENGTH));
		public string GetNewBucketName(bool create = true)
		{
			string bucketName = GetPrefix() + RandomText(RANDOM_SUFFIX_TEXT_LENGTH);
			if (bucketName.Length > BUCKET_MAX_LENGTH) bucketName = bucketName.Substring(0, BUCKET_MAX_LENGTH - 1);
			if (create) BucketList.Add(bucketName);
			return bucketName;
		}
		public string GetNewBucketName(int length)
		{
			string bucketName = GetPrefix() + RandomText(BUCKET_MAX_LENGTH);
			bucketName = bucketName.Substring(0, length);
			BucketList.Add(bucketName);
			return bucketName;
		}

		public string GetURL(string bucketName) => $"{MainData.HTTP}{GetHost(bucketName)}";
		public string GetURL(string bucketName, string key) => $"{MainData.HTTP}{GetHost(bucketName)}/{key}";
		public string GetHost(string bucketName)
			=> Config.S3.IsAWS ? $"{bucketName}.s3-{Config.S3.RegionName}.amazonaws.com" : $"{Config.S3.Address}:{Config.S3.Port}/{bucketName}";

		public static string HtmlParser(string data, string flag1, string flag2)
		{
			int index1 = data.IndexOf(flag1) + flag1.Length;
			int index2 = data.IndexOf(flag2);

			return data[index1..index2];
		}

		public List<string> MakePartData(int size, int partSize)
		{
			List<string> stringList = new();

			int remainSize = size;
			while (remainSize > 0)
			{
				int nowPartSize;
				if (remainSize > partSize) nowPartSize = partSize;
				else nowPartSize = remainSize;

				stringList.Add(RandomTextToLong(nowPartSize));

				remainSize -= nowPartSize;
			}

			return stringList;
		}
		public List<string> MakePartData(string data, int partSize)
		{
			List<string> stringList = new();

			int startPoint = 0;
			while (startPoint < data.Length)
			{
				int endPoint;
				if ((startPoint + partSize) < data.Length) endPoint = partSize;
				else endPoint = data.Length - startPoint;

				stringList.Add(data.Substring(startPoint, endPoint));
				startPoint += partSize;
			}

			return stringList;
		}

		public ByteRange MakeRandomRange(int fileSize)
		{
			var offset = rand.Next(fileSize - 1000);
			var length = rand.Next(fileSize - offset) - 1;

			return new ByteRange(offset, offset + length);
		}

		public List<Tag> MakeTagList(int keySize, int valueSize)
		{
			var tagSet = new List<Tag>();
			for (int i = 0; i < 10; i++)
				tagSet.Add(new Tag() { Key = RandomTextToLong(keySize), Value = RandomTextToLong(valueSize) });

			return tagSet;
		}
		public Tagging MakeSimpleTagset(int count)
		{
			var tagList = new Tagging() { TagSet = new List<Tag>() };

			for (int i = 0; i < count; i++)
				tagList.TagSet.Add(new Tag() { Key = i.ToString(), Value = i.ToString() });
			return tagList;
		}
		public string MakeArnResource(string path = "*") => $"arn:aws:s3:::{path}";
		public JObject MakeJsonStatement(string action, string resource, string effect = MainData.PolicyEffectAllow,
										JObject principal = null, JObject conditions = null)
		{
			if (principal == null) principal = new JObject() { { "AWS", "*" } };

			var statement = new JObject()
			{
				{ MainData.PolicyEffect, effect },
				{ MainData.PolicyPrincipal, principal },
				{ MainData.PolicyAction, action },
				{ MainData.PolicyResource, resource },
			};

			if (conditions != null) statement.Add(MainData.PolicyCondition, conditions);

			return statement;
		}
		public JObject MakeJsonPolicy(string action, string resource, JObject principal = null, JObject conditions = null)
			=> new(){
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray(){ MakeJsonStatement(action, resource, principal: principal, conditions: conditions) } },
			};
		public JObject MakeJsonPolicy(JArray statements)
			=> new() { { MainData.PolicyVersion, MainData.PolicyVersionDate }, { MainData.PolicyStatement, statements }, };
		#endregion

		#region POST
		public HttpWebResponse PutObject(string url, string body = null, string contentType = null)
		{
			HttpWebRequest httpRequest = WebRequest.Create(url) as HttpWebRequest;
			httpRequest.Method = "PUT";
			if (contentType != null) httpRequest.ContentType = contentType;

			if (body != null)
			{
				var byteStream = Encoding.ASCII.GetBytes(body);
				using Stream dataStream = httpRequest.GetRequestStream();
				dataStream.Write(byteStream);
			}

			HttpWebResponse response = httpRequest.GetResponse() as HttpWebResponse;
			return response;
		}

		public HttpWebResponse GetObject(string url)
		{
			HttpWebRequest httpRequest = WebRequest.Create(url) as HttpWebRequest;
			httpRequest.Method = "GET";
			HttpWebResponse response = httpRequest.GetResponse() as HttpWebResponse;

			return response;
		}

		public MyResult PostUpload(string bucketName, Dictionary<string, object> parameters)
		{
			//https://spirit32.tistory.com/21
			string boundary = DateTime.Now.Ticks.ToString("x");
			byte[] boundaryBytes = Encoding.ASCII.GetBytes(string.Format("\r\n--{0}\r\n", boundary));

			var url = GetURL(bucketName);
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
			request.ContentType = "multipart/form-data; boundary=" + boundary;
			request.Method = "POST";
			request.KeepAlive = true;
			request.Credentials = CredentialCache.DefaultCredentials;
			if (Config.S3.IsAWS) request.Host = GetHost(bucketName);

			if (parameters != null && parameters.Count > 0)
			{
				using Stream requestStream = request.GetRequestStream();
				foreach (KeyValuePair<string, object> pair in parameters)
				{
					requestStream.Write(boundaryBytes, 0, boundaryBytes.Length);
					if (pair.Value is FormFile)
					{
						FormFile file = pair.Value as FormFile;
						string header = string.Format("Content-Disposition: form-data; name=\"{0}\"; filename=\"{1}\"\r\nContent-Type: {2}\r\n\r\n", pair.Key, file.Name, file.ContentType);
						byte[] bytes = Encoding.UTF8.GetBytes(header);
						requestStream.Write(bytes, 0, bytes.Length);

						byte[] buffer = Encoding.UTF8.GetBytes(file.Body);
						requestStream.Write(buffer, 0, buffer.Length);
					}
					else
					{
						string data = string.Format("Content-Disposition: form-data; name=\"{0}\"\r\n\r\n{1}", pair.Key, pair.Value);
						byte[] bytes = Encoding.UTF8.GetBytes(data);
						requestStream.Write(bytes, 0, bytes.Length);
					}
				}

				byte[] trailer = Encoding.ASCII.GetBytes(string.Format("\r\n--{0}--\r\n", boundary));
				requestStream.Write(trailer, 0, trailer.Length);
				requestStream.Close();
			}

			try
			{
				var response = request.GetResponse() as HttpWebResponse;

				return new MyResult()
				{
					URL = response.ResponseUri.AbsoluteUri,
					StatusCode = response.StatusCode,
				};
			}
			catch (WebException e)
			{
				var stream = e.Response.GetResponseStream();
				StreamReader reader = new(stream);
				string data = reader.ReadToEnd();

				return new MyResult()
				{
					URL = ((HttpWebResponse)e.Response).ResponseUri.AbsoluteUri,
					StatusCode = ((HttpWebResponse)e.Response).StatusCode,
					ErrorCode = HtmlParser(data, "<Code>", "</Code>"),
					Message = HtmlParser(data, "<Message>", "</Message>"),
				};
			}
		}
		#endregion

		#region Check
		public bool ErrorCheck(HttpStatusCode statusCode)
		{
			if (statusCode.Equals(HttpStatusCode.BadRequest)) return true;
			if (statusCode.Equals(HttpStatusCode.Forbidden)) return true;
			return false;
		}

		public void CheckVersioning(string bucketName, VersionStatus statusCode)
		{
			var client = GetClient();

			var response = client.GetBucketVersioning(bucketName);
			Assert.Equal(statusCode, response.VersioningConfig.Status);
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

		public void CheckContent(S3Client client, string bucketName, string key, string data, int loopCount = 1, SSECustomerKey sseC = null)
		{
			for (int i = 0; i < loopCount; i++)
			{
				var response = client.GetObject(bucketName, key, sseCustomerKey: sseC);
				var body = GetBody(response);
				Assert.Equal(data, body);
			}
		}

		public void CheckContentUsingRange(S3Client client, string bucketName, string key, string data, long step, SSECustomerKey sseC = null)
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
				var body = GetBody(response);
				var length = endPosition - startPosition + 1;

				Assert.Equal(length, response.ContentLength);
				Assert.Equal(data.Substring((int)range.Start, (int)length), body);
				startPosition += step;
			}
		}
		public void CheckContentUsingRandomRange(S3Client client, string bucketName, string key, string data, int loopCount, SSECustomerKey sseC = null)
		{
			for (int i = 0; i < loopCount; i++)
			{
				var range = MakeRandomRange(data.Length);
				var length = range.End - range.Start;

				var response = client.GetObject(bucketName, key, range: range, sseCustomerKey: sseC);
				var body = GetBody(response);

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
		public void CheckBucketACLGrantCanRead(string bucketName) => GetAltClient().ListObjects(bucketName);
		public void CheckBucketACLGrantCantRead(string bucketName) => Assert.Throws<AggregateException>(() => GetAltClient().ListObjects(bucketName));
		public void CheckBucketACLGrantCanReadACP(string bucketName) => GetAltClient().GetBucketACL(bucketName);
		public void CheckBucketACLGrantCantReadACP(string bucketName) => Assert.Throws<AggregateException>(() => GetAltClient().GetBucketACL(bucketName));
		public void CheckBucketACLGrantCanWrite(string bucketName) => GetAltClient().PutObject(bucketName, "foo-write", body: "bar");
		public void CheckBucketACLGrantCantWrite(string bucketName) => Assert.Throws<AggregateException>(() => GetAltClient().PutObject(bucketName, "foo-write", body: "bar"));
		public void CheckBucketACLGrantCanWriteACP(string bucketName) => GetAltClient().PutBucketACL(bucketName, acl: S3CannedACL.PublicRead);
		public void CheckBucketACLGrantCantWriteACP(string bucketName) => Assert.Throws<AggregateException>(() => GetAltClient().PutBucketACL(bucketName, acl: S3CannedACL.PublicRead));

		public void CheckBadBucketName(string bucketName)
		{
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.PutBucket(bucketName));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_BUCKET_NAME, GetErrorCode(e));
		}
		public void CheckGoodBucketName(string name, string prefix = null)
		{
			if (prefix == null) prefix = GetPrefix();

			var bucketName = string.Format("{0}{1}", prefix, name);
			BucketList.Add(bucketName);

			var client = GetClient();
			var response = client.PutBucket(bucketName);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		public void CheckGrants(List<S3Grant> expected, List<S3Grant> actual)
		{
			Assert.Equal(expected.Count, actual.Count);

			expected = GrantsSort(expected);
			actual = GrantsSort(actual);

			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(expected[i].Permission, actual[i].Permission);
				Assert.Equal(expected[i].Grantee.CanonicalUser, actual[i].Grantee.CanonicalUser);
				Assert.Equal(expected[i].Grantee.EmailAddress, actual[i].Grantee.EmailAddress);
				Assert.Equal(expected[i].Grantee.Type, actual[i].Grantee.Type);
				Assert.Equal(expected[i].Grantee.URI, actual[i].Grantee.URI);
			}
		}
		public void CheckObjectACL(S3Permission permission)
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";

			client.PutObject(bucketName, key);
			var response = client.GetObjectACL(bucketName, key);

			var policy = response.AccessControlList;
			policy.Grants[0].Permission = permission;

			client.PutObjectACL(bucketName, key, accessControlPolicy: policy);

			response = client.GetObjectACL(bucketName, key);
			var grants = response.AccessControlList.Grants;

			var mainUserId = Config.MainUser.UserId;
			var mainDispalyName = Config.MainUser.DisplayName;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					 Permission = permission,
					 Grantee = new S3Grantee()
					 {
						 CanonicalUser = mainUserId,
						 DisplayName = mainDispalyName,
						 URI = null,
						 EmailAddress = null,
					 }
				}
			},
			grants);
		}

		public void CheckCopyContent(S3Client client,
									string srcBucketName, string srcKey, string destBucketName, string destKey,
									string srcVersionId = null, SSECustomerKey srcCustomerKey = null,
									string destVersionId = null, SSECustomerKey destCustomerKey = null)
		{
			var srcResponse = client.GetObject(srcBucketName, srcKey, versionId: srcVersionId, sseCustomerKey: srcCustomerKey);
			var srcSize = srcResponse.ContentLength;
			var srcBody = GetBody(srcResponse);

			var destResponse = client.GetObject(destBucketName, destKey, versionId: destVersionId, sseCustomerKey: destCustomerKey);
			var destSize = destResponse.ContentLength;
			var destBody = GetBody(destResponse);

			Assert.Equal(srcSize, destSize);
			Assert.Equal(srcBody, destBody);
		}
		public void CheckCopyContentUsingRange(S3Client client, string srcBucketName, string srcKey, string destBucketName, string destKey, string versionId = null)
		{
			var headResponse = client.GetObjectMetadata(srcBucketName, srcKey, versionId: versionId);
			var srcSize = headResponse.ContentLength;

			var response = client.GetObject(destBucketName, destKey);
			var destSize = response.ContentLength;
			var destBody = GetBody(response);
			Assert.True(srcSize >= destSize);

			var range = new ByteRange(0, destSize - 1);
			response = client.GetObject(srcBucketName, srcKey, range: range, versionId: versionId);
			var srcBody = GetBody(response);
			Assert.Equal(srcBody, destBody);
		}

		public void CheckObjContent(S3Client client, string bucketName, string key, string versionId, string content)
		{
			var response = client.GetObject(bucketName, key, versionId: versionId);
			if (content != null)
			{
				var body = GetBody(response);
				Assert.Equal(content, body);
			}
			else
				Assert.Equal("True", response.DeleteMarker);
		}
		public void CheckObjVersions(S3Client client, string bucketName, string key, List<string> versionIds, List<string> contents)
		{
			var response = client.ListVersions(bucketName);
			var versions = GetVersions(response.Versions);

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
			var uploadData = SetupMultipartUpload(client, bucketName, key, size, contentType: contentType, metadataList: metadata, resendParts: resendParts);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);

			var response = client.GetObject(bucketName, key);
			Assert.Equal(contentType, response.Headers.ContentType);
			Assert.Equal(metadata, GetMetaData(response.Metadata));
			var body = GetBody(response);
			Assert.Equal(body.Length, response.ContentLength);
			Assert.Equal(uploadData.Body, body);

			CheckContentUsingRange(client, bucketName, key, uploadData.Body, 1000000);
			CheckContentUsingRange(client, bucketName, key, uploadData.Body, 10000000);
		}

		public void PrefixLifecycleConfigurationCheck(List<LifecycleRule> expected, List<LifecycleRule> actual)
		{
			Assert.Equal(expected.Count, actual.Count);

			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(expected[i].Id, actual[i].Id);
				Assert.Equal(expected[i].Expiration.DateUtc, actual[i].Expiration.DateUtc);
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
			//https://spirit32.tistory.com/21
			var url = GetURL(bucketName);
			if (key != null) url += string.Format("/{0}", key);

			WebRequest request = WebRequest.Create(url);
			request.Method = method;
			request.Credentials = CredentialCache.DefaultCredentials;
			foreach (var item in headers) request.Headers.Add(item.Key, item.Value);

			MyResult result;
			try
			{
				var response = request.GetResponse() as HttpWebResponse;

				result = new MyResult()
				{
					URL = response.ResponseUri.AbsoluteUri,
					StatusCode = response.StatusCode,
					Headers = response.Headers,
				};
			}
			catch (WebException e)
			{
				var stream = e.Response.GetResponseStream();
				StreamReader reader = new(stream);
				string data = reader.ReadToEnd();

				result = new MyResult()
				{
					URL = ((HttpWebResponse)e.Response).ResponseUri.AbsoluteUri,
					StatusCode = ((HttpWebResponse)e.Response).StatusCode,
					Headers = ((HttpWebResponse)e.Response).Headers,
					ErrorCode = HtmlParser(data, "<Code>", "</Code>"),
					Message = HtmlParser(data, "<Message>", "</Message>"),
				};
			}

			Assert.Equal(statusCode, result.StatusCode);
			Assert.Equal(result.Headers.Get("access-control-allow-origin"), expectAllowOrigin);
			Assert.Equal(result.Headers.Get("access-control-allow-methods"), expectAllowMethods);
		}

		public void TaggingCompare(List<Tag> expected, List<Tag> actual)
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
		public void LockCompare(ObjectLockConfiguration expected, ObjectLockConfiguration actual)
		{
			Assert.Equal(expected.ObjectLockEnabled, actual.ObjectLockEnabled);
			Assert.Equal(expected.Rule.DefaultRetention.Mode, actual.Rule.DefaultRetention.Mode);
			Assert.Equal(expected.Rule.DefaultRetention.Years, actual.Rule.DefaultRetention.Years);
			Assert.Equal(expected.Rule.DefaultRetention.Days, actual.Rule.DefaultRetention.Days);
		}
		public void RetentionCompare(ObjectLockRetention expected, ObjectLockRetention actual)
		{
			Assert.Equal(expected.Mode, actual.Mode);
			Assert.Equal(expected.RetainUntilDate, actual.RetainUntilDate.ToUniversalTime());

		}
		public void VersionIdsCompare(List<S3ObjectVersion> expected, List<S3ObjectVersion> actual)
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

		public void LoggingConfigCompare(S3BucketLoggingConfig expected, S3BucketLoggingConfig actual)
		{
			Assert.Equal(expected.TargetBucketName, actual.TargetBucketName);

			if (expected.TargetPrefix == null) Assert.Null(actual.TargetPrefix);
			else Assert.Equal(expected.TargetPrefix, actual.TargetPrefix);

			if (expected.Grants == null && actual.Grants == null) return;
			CheckGrants(expected.Grants, actual.Grants);
		}

		public void PartsETagCompare(List<PartETag> expected, List<PartDetail> actual)
		{
			Assert.Equal(expected.Count, actual.Count);
			for (int i = 0; i < expected.Count; i++)
			{
				Assert.Equal(expected[i].ETag, actual[i].ETag);
				Assert.Equal(expected[i].PartNumber, actual[i].PartNumber);
			}
		}

		#endregion

		#region Get Data
		public string TimeToString(DateTime time) => time.ToString(S3Headers.TimeFormat, S3Headers.TimeCulture);
		public List<string> GetKeys(ListObjectsResponse response)
		{
			if (response != null)
			{
				List<string> temp = new();

				foreach (var s3Object in response.S3Objects) temp.Add(s3Object.Key);

				return temp;
			}
			return null;
		}
		public List<string> GetKeys(ListObjectsV2Response response)
		{
			if (response != null)
			{
				List<string> temp = new();

				foreach (var s3Object in response.S3Objects) temp.Add(s3Object.Key);

				return temp;
			}
			return null;
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
		public List<KeyVersion> GetKeyVersions(List<string> keyList)
		{
			List<KeyVersion> keyVersions = new();
			foreach (var key in keyList) keyVersions.Add(new KeyVersion() { Key = key });

			return keyVersions;
		}

		public static HttpStatusCode GetStatus(AggregateException e) => (e.InnerException is AmazonS3Exception e2) ? e2.StatusCode : HttpStatusCode.OK;

		public static string GetErrorCode(AggregateException e) => (e.InnerException is AmazonS3Exception e2) ? e2.ErrorCode : null;

		public long GetBytesUsed(ListObjectsV2Response response)
		{
			if (response == null) return 0;
			if (response.S3Objects == null) return 0;
			if (response.S3Objects.Count > 1) return 0;

			long size = 0;

			foreach (var obj in response.S3Objects) size += obj.Size;

			return size;
		}
		public List<KeyValuePair<string, string>> GetMetaData(MetadataCollection response)
		{
			var metaDataList = new List<KeyValuePair<string, string>>();

			foreach (var key in response.Keys)
				metaDataList.Add(new KeyValuePair<string, string>(key, response[key]));

			return metaDataList;
		}
		public List<KeyValuePair<string, string>> GetACLHeader(string userId = null, string[] perms = null)
		{
			string[] allHeaders = { "read", "write", "read-acp", "write-acp", "full-control" };

			var headers = new List<KeyValuePair<string, string>>();

			if (userId == null) userId = Config.AltUser.UserId;
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
			S3Permission[] allHeaders = { S3Permission.READ, S3Permission.WRITE, S3Permission.READ_ACP, S3Permission.WRITE_ACP, S3Permission.FULL_CONTROL };

			var headers = new List<S3Grant>();

			if (userId == null) userId = Config.AltUser.UserId;
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
		public List<string> GetBucketList(ListBucketsResponse response)
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
		public List<S3ObjectVersion> GetVersions(List<S3ObjectVersion> versions)
		{
			if (versions == null) return null;

			var lists = new List<S3ObjectVersion>();
			foreach (var item in versions)
				if (!item.IsDeleteMarker) lists.Add(item);
			return lists;
		}
		public List<string> GetVersionIds(List<S3ObjectVersion> versions)
		{
			if (versions == null) return null;

			var lists = new List<string>();
			foreach (var item in versions)
				if (!item.IsDeleteMarker) lists.Add(item.VersionId);
			return lists;
		}
		public int GetDeleteMarkerCount(List<S3ObjectVersion> versions)
		{
			if (versions == null) return 0;
			int count = 0;
			foreach (var item in versions)
				if (item.IsDeleteMarker) count++;
			return count;
		}
		public List<S3ObjectVersion> GetDeleteMarkers(List<S3ObjectVersion> versions)
		{
			if (versions == null) return null;

			var deleteMarkers = new List<S3ObjectVersion>();

			foreach (var item in versions)
				if (item.IsDeleteMarker) deleteMarkers.Add(item);
			return deleteMarkers;
		}
		public List<S3Grant> GrantsSort(List<S3Grant> data)
		{
			var newList = new List<S3Grant>();

			var sortMap = new SortedDictionary<string, S3Grant>();

			foreach (var grant in data) sortMap.Add($"{grant.Grantee.CanonicalUser}{grant.Permission}", grant);

			foreach (var item in sortMap) newList.Add(item.Value);

			return newList;
		}

		public string GetResponsebody(Stream data)
		{
			StreamReader reader = new(data, Encoding.UTF8);
			return reader.ReadToEnd();
		}
		public string GetResponseErrorCode(Stream data)
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
			var client = GetClientV4();
			if (bucketName == null)
			{
				bucketName = GetNewBucketName();
				client.PutBucket(bucketName);
			}

			if (keyList != null && client != null)
			{
				foreach (var key in keyList)
				{
					if (body == null) body = key;
					client.PutObject(bucketName, key, body, useChunkEncoding: useChunkEncoding, disablePayloadSigning: disablePayloadSigning);
				}
			}

			return bucketName;
		}

		public string SetupKeyWithRandomContent(string key, int size = 7 * MainData.MB, string bucketName = null, S3Client client = null)
		{
			if (bucketName == null) bucketName = GetNewBucket();
			if (client == null) client = GetClient();

			var data = RandomTextToLong(size);
			client.PutObject(bucketName, key, body: data);

			return bucketName;
		}

		public string SetupBucketObjectACL(S3CannedACL bucketACL, S3CannedACL objectACL, string key)
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();
			client.PutBucket(bucketName, acl: bucketACL);
			client.PutObject(bucketName, key: key, acl: objectACL);
			return bucketName;
		}
		public string SetupBucketAndObjectsACL(out string key1, out string key2, out string newKey, S3CannedACL bucketACL, S3CannedACL objectACL)
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			key1 = "foo";
			key2 = "bar";
			newKey = "new";

			client.PutBucketACL(bucketName, acl: bucketACL);
			client.PutObject(bucketName, key1, body: key1);
			client.PutObjectACL(bucketName, key1, acl: objectACL);
			client.PutObject(bucketName, key2, body: key2);

			return bucketName;
		}

		public MultipartUploadData SetupMultipartCopy(S3Client client, string srcBucketName, string srcKey, string destBucketName, string destKey, int size,
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

		public MultipartUploadData SetupMultipartUpload(S3Client client, string bucketName, string key, int size, MultipartUploadData uploadData = null,
			int partSize = 5 * MainData.MB, List<KeyValuePair<string, string>> metadataList = null, string contentType = null,
			List<int> resendParts = null, SSECustomerKey sseCustomerKey = null, ServerSideEncryptionMethod sseKey = null)
		{
			if (uploadData == null)
			{
				uploadData = new MultipartUploadData();
				var initResponse = client.InitiateMultipartUpload(bucketName, key, metadataList: metadataList, contentType: contentType, sseCustomerKey: sseCustomerKey, sseKey: sseKey);
				uploadData.UploadId = initResponse.UploadId;
			}

			var parts = MakePartData(size, partSize);

			foreach (var part in parts)
			{
				uploadData.AppendBody(part);
				var partNumber = uploadData.NextPartNumber;
				var partResPonse = client.UploadPart(bucketName, key, uploadData.UploadId, part, partNumber, sseC: sseCustomerKey);
				uploadData.Parts.Add(new PartETag(partResPonse.PartNumber, partResPonse.ETag));

				if (resendParts != null && resendParts.Contains(partNumber)) client.UploadPart(bucketName, key, uploadData.UploadId, part, partNumber);
			}

			return uploadData;
		}

		public MultipartUploadData SetupMultipartUploadData(S3Client client, string bucketName, string key, string data, int partSize = 5 * MainData.MB,
			List<KeyValuePair<string, string>> metadataList = null, string contentType = null)
		{
			var uploadData = new MultipartUploadData();

			var initMultiPartResponse = client.InitiateMultipartUpload(bucketName, key, metadataList: metadataList, contentType: contentType);
			uploadData.UploadId = initMultiPartResponse.UploadId;

			var parts = MakePartData(data, partSize);

			int uploadCount = 1;
			foreach (var part in parts)
			{
				var partResPonse = client.UploadPart(bucketName, key, uploadData.UploadId, part, uploadCount);
				uploadData.Parts.Add(new PartETag(uploadCount++, partResPonse.ETag));
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

		public string SetupBucketACLGrantUserid(S3Permission permission)
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var mainUserId = Config.MainUser.UserId;
			var mainDispalyName = Config.MainUser.DisplayName;

			var altUserId = Config.AltUser.UserId;
			var altDispalyName = Config.AltUser.DisplayName;

			var grant = new S3Grant() { Permission = permission, Grantee = new S3Grantee() { CanonicalUser = altUserId } };

			var grants = AddBucketUserGrant(bucketName, grant);

			client.PutBucketACL(bucketName, accessControlPolicy: grants);

			var response = client.GetBucketACL(bucketName);

			var getGrants = response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					 Permission = S3Permission.FULL_CONTROL,
					 Grantee = new S3Grantee()
					 {
						 CanonicalUser = mainUserId,
						 DisplayName = mainDispalyName,
						 URI = null,
						 EmailAddress = null,
					 }
				},
				new()
				{
					 Permission = permission,
					 Grantee = new S3Grantee()
					 {
						 CanonicalUser = altUserId,
						 DisplayName = altDispalyName,
						 URI = null,
						 EmailAddress = null,
					 }
				},
			},
			getGrants);

			return bucketName;
		}

		public void SetupMultipleVersions(S3Client client, string bucketName, string key, int numVersions, ref List<string> versionIds, ref List<string> contents, bool checkVersion = true)
		{
			if (versionIds == null) versionIds = new List<string>();
			if (contents == null) contents = new List<string>();

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
		public void SetupMultipleVersion(S3Client client, string bucketName, string key, int numVersions, bool checkVersion = true)
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
		public void OverwriteSuspendedVersioningObj(S3Client client, string bucketName, string key, ref List<string> versionIds, ref List<string> contents, string content)
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

		public List<Thread> SetupVersionedObjConcurrent(S3Client client, string bucketName, string key, int num)
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
		public void RemoveObjVersion(S3Client client, string bucketName, string key, List<string> versionIds, List<string> contents, int index)
		{
			Assert.Equal(versionIds.Count, contents.Count);
			var rmVersionId = versionIds[index]; versionIds.RemoveAt(index);
			var rmContent = contents[index]; contents.RemoveAt(index);

			CheckObjContent(client, bucketName, key, rmVersionId, rmContent);

			client.DeleteObject(bucketName, key, versionId: rmVersionId);

			if (versionIds.Count != 0)
				CheckObjVersions(client, bucketName, key, versionIds, contents);
		}
		public void DeleteSuspendedVersioningObj(S3Client client, string bucketName, string key, ref List<string> versionIds, ref List<string> contents)
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

		public List<Thread> DoClearVersionedBucketConcurrent(S3Client client, string bucketName)
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

		public void TestACL(string bucketName, string key, S3Client client, bool pass)
		{
			if (pass)
			{
				var response = client.GetObject(bucketName, key);
				Assert.Equal(key, response.Key);
			}
			else
			{
				var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
				var statusCode = GetStatus(e);
				var errorCode = GetErrorCode(e);

				Assert.Equal(HttpStatusCode.Forbidden, statusCode);
				Assert.Equal(MainData.ACCESS_DENIED, errorCode);
			}
		}

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
			if (bucketName.Length < length) bucketName += RandomText(length - bucketName.Length);
			else bucketName = bucketName.Substring(0, length - 1);
			BucketList.Add(bucketName);

			var client = GetClient();
			var response = client.PutBucket(bucketName);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		public void TestCreateRemoveVersions(S3Client client, string bucketName, string key, int numversions, int removeStartIdx, int idxInc)
		{
			List<string> versionIds = null;
			List<string> contents = null;
			SetupMultipleVersions(client, bucketName, key, numversions, ref versionIds, ref contents);
			var idx = removeStartIdx;

			for (int i = 0; i < numversions; i++)
			{
				RemoveObjVersion(client, bucketName, key, versionIds, contents, idx);
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
			var encodingbody = GetBody(response);
			var body = AES.AESDecrypt(encodingbody);
			Assert.Equal(data, body);
		}
		public void TestEncryptionSSECustomerWrite(int fileSize)
		{
			var bucketName = GetNewBucket();
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
			var body = GetBody(response);
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
			var body = GetBody(response);
			Assert.Equal(data, body);
		}
		public void TestEncryptionSSES3Copy(int fileSize)
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var data = new string('A', fileSize);

			var SSEConfig = new ServerSideEncryptionConfiguration()
			{
				ServerSideEncryptionRules = new List<ServerSideEncryptionRule>()
				{
					new()
					{
						ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
						{
							ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
						}
					}
				}
			};

			client.PutBucketEncryption(bucketName, SSEConfig);

			var response = client.GetBucketEncryption(bucketName);
			Assert.Single(response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var sourceKey = "bar";
			client.PutObject(bucketName, sourceKey, body: data);

			var sourceResponse = client.GetObject(bucketName, sourceKey);
			var sourceBody = GetBody(sourceResponse);
			Assert.Equal(ServerSideEncryptionMethod.AES256, sourceResponse.ServerSideEncryptionMethod);

			var destKey = "foo";
			client.CopyObject(bucketName, sourceKey, bucketName, destKey);
			var destResponse = client.GetObject(bucketName, destKey);
			Assert.Equal(ServerSideEncryptionMethod.AES256, destResponse.ServerSideEncryptionMethod);

			var destBody = GetBody(destResponse);
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
			var sourceBody = GetBody(sourceResponse);
			if (sourceObjectEncryption) Assert.Equal(ServerSideEncryptionMethod.AES256, sourceResponse.ServerSideEncryptionMethod);
			else Assert.Null(sourceResponse.ServerSideEncryptionMethod);
			Assert.Equal(data, sourceBody);

			var SSEConfig = new ServerSideEncryptionConfiguration()
			{
				ServerSideEncryptionRules = new List<ServerSideEncryptionRule>()
				{
					new()
					{
						ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault()
						{
							ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256)
						}
					}
				}
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
			var destBody = GetBody(destResponse);
			if (destBucketEncryption || destObjectEncryption)
				Assert.Equal(ServerSideEncryptionMethod.AES256, destResponse.ServerSideEncryptionMethod);
			else Assert.Null(destResponse.ServerSideEncryptionMethod);
			Assert.Equal(sourceBody, destBody);
		}

		public void TestObjectCopy(EncryptionType source, EncryptionType target, int fileSize)
		{
			var sourceKey = "SourceKey";
			var targetKey = "TargetKey";
			var bucketName = GetNewBucket();
			var client = GetClientHttps();
			var content = RandomTextToLong(fileSize);

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
			Assert.Equal(content, GetBody(sourceResponse));

			var targetResponse = client.GetObject(bucketName, targetKey, sseCustomerKey: sseC);
			Assert.Equal(content, GetBody(targetResponse));
		}

		public string TestMultipartUploadContents(string bucketName, string key, int numParts)
		{
			var payload = RandomTextToLong(5 * MainData.MB);
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
			var lestPayload = RandomTextToLong(MainData.MB);
			var lestPartResponse = client.UploadPart(bucketName, key, uploadId, lestPayload, numParts + 1);
			parts.Add(new PartETag(numParts + 1, lestPartResponse.ETag));
			allPayload += lestPayload;

			client.CompleteMultipartUpload(bucketName, key, uploadId, parts);

			var response = client.GetObject(bucketName, key);
			var text = GetBody(response);

			Assert.Equal(allPayload, text);

			return allPayload;
		}

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
	}
}
