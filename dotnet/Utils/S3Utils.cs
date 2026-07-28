using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using s3tests.Test;
using System.Net;
using Newtonsoft.Json.Linq;
using System.Threading;
using Xunit;

namespace s3tests.Utils
{
	public static class S3Utils
	{
		private static readonly char[] TEXT = "abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray();
		private static readonly char[] TEXT_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

		// Random.Shared: xUnit 병렬 실행에서도 thread-safe
		public static string RandomText(int length)
		{
			if (length <= 0) return string.Empty;
			var chars = new char[length];
			for (int i = 0; i < length; i++) chars[i] = TEXT[Random.Shared.Next(TEXT.Length)];
			return new string(chars);
		}
		public static string RandomTextToLong(int length)
		{
			if (length <= 0) return string.Empty;
			var chars = new char[length];
			for (int i = 0; i < length; i++) chars[i] = TEXT_STRING[Random.Shared.Next(TEXT_STRING.Length)];
			return new string(chars);
		}

		private const int BUCKET_MAX_LENGTH = 63;

		/// <summary>prefix + 랜덤문자로 최대 길이를 채운 버킷명. (Java Utils.randomBucketName)</summary>
		public static string RandomBucketName(string prefix)
		{
			prefix ??= "";
			var maxLength = BUCKET_MAX_LENGTH - 1;
			var bucketName = prefix + RandomText(BUCKET_MAX_LENGTH);
			return bucketName[..Math.Min(maxLength, bucketName.Length)];
		}

		/// <summary>
		/// prefix + suite + "-" + testId + "-" + 랜덤문자 형태로 최대 길이(62자)를 채운 버킷명을 만든다.
		/// (Java Utils.getNewBucketName와 동일)
		/// </summary>
		public static string MakeBucketName(string prefix, string suite, int testId)
		{
			prefix ??= "";
			suite = ToSuiteId(suite);

			var maxLength = BUCKET_MAX_LENGTH - 1;
			const int minRandom = 6;
			var idxPart = testId.ToString();

			// prefix + '-' + testId + '-' + 최소 랜덤길이는 항상 확보한다.
			var reserved = prefix.Length + 1 + idxPart.Length + 1 + minRandom;
			var suiteMax = maxLength - reserved;
			if (suiteMax < 1) return RandomBucketName(prefix);
			if (suite.Length > suiteMax) suite = suite[..suiteMax];

			var head = $"{prefix}{suite}-{idxPart}-";
			var bucketName = head + RandomText(Math.Max(maxLength - head.Length, 0));
			if (bucketName.Length > maxLength) bucketName = bucketName[..maxLength];
			return bucketName;
		}

		/// <summary>클래스명을 버킷명에 쓸 수 있는 suite id로 변환한다. (Java Utils.toSuiteId)</summary>
		public static string ToSuiteId(string className)
		{
			if (string.IsNullOrEmpty(className)) return "x";

			var dot = className.LastIndexOf('.');
			var simple = dot >= 0 ? className[(dot + 1)..] : className;
			simple = new string([.. simple.ToLowerInvariant().Where(char.IsLetterOrDigit)]);
			return simple.Length == 0 ? "x" : simple;
		}

		/// <summary>지정한 길이의 오브젝트 이름을 생성한다. 200자마다 '/'를 넣어 디렉터리 구조를 흉내낸다.</summary>
		public static string RandomObjectName(int length)
		{
			const int MAX_LENGTH = 200;

			var name = new StringBuilder();
			var index = 0;
			while (name.Length <= length)
			{
				if (index + MAX_LENGTH < length)
				{
					name.Append(RandomTextToLong(MAX_LENGTH));
					index += MAX_LENGTH;
				}
				else
				{
					name.Append(RandomTextToLong(length - index));
					index = length;
				}
				name.Append('/');
			}
			return name.ToString()[..length];
		}

		public static ByteRange MakeRandomRange(int fileSize)
		{
			var offset = Random.Shared.Next(fileSize - 1000);
			var length = Random.Shared.Next(fileSize - offset) - 1;

			return new ByteRange(offset, offset + length);
		}

		/// <summary>POST 정책(base64)에 SigV4로 서명한다. (Java AWS4SignerBase.getPostPolicySignature)</summary>
		public static string GetPostPolicySignature(string secretKey, string dateStamp, string regionName, string policyBase64)
		{
			static byte[] Hmac(byte[] key, string data)
			{
				using var hmac = new HMACSHA256(key);
				return hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
			}

			var kSecret = Encoding.UTF8.GetBytes("AWS4" + secretKey);
			var kDate = Hmac(kSecret, dateStamp);
			var kRegion = Hmac(kDate, regionName);
			var kService = Hmac(kRegion, "s3");
			var kSigning = Hmac(kService, "aws4_request");

			return Convert.ToHexString(Hmac(kSigning, policyBase64)).ToLowerInvariant();
		}

		public static string GetBase64EncodedSHA1Hash(string policy, string secretKey)
		{
			var data = Encoding.UTF8.GetBytes(policy);
			var keyBytes = Encoding.UTF8.GetBytes(secretKey);

			using HMACSHA1 sha1 = new(keyBytes);
			return Convert.ToBase64String(sha1.ComputeHash(data));
		}
		/// <summary>ETag 비교용 16진수 MD5. (ETag는 base64가 아니라 hex다)</summary>
		public static string GetMD5Hex(string data)
			=> Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(data))).ToLowerInvariant();

		public static string GetMD5(string data)
		{
			var byteData = Encoding.UTF8.GetBytes(data);
			var hash = MD5.HashData(byteData.AsSpan(0, byteData.Length));
			return Convert.ToBase64String(hash);
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

		static List<string> MakePartData(int size, int partSize)
		{
			List<string> stringList = [];

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
		static List<string> MakePartData(string data, int partSize)
		{
			List<string> stringList = [];

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

		public static MultipartUploadData SetupMultipartUpload(S3Client client, string bucketName, string key, int size, MultipartUploadData uploadData = null,
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
				uploadData.Parts.Add(new PartETag(partNumber, partResPonse.ETag));

				if (resendParts != null && resendParts.Contains(partNumber)) client.UploadPart(bucketName, key, uploadData.UploadId, part, partNumber);
			}

			return uploadData;
		}
		public static MultipartUploadData SetupMultipartUploadData(S3Client client, string bucketName, string key, string data, int partSize = 5 * MainData.MB,
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

		public static string GetXmlValue(string data, string tagName)
		{
			if (string.IsNullOrEmpty(data) || string.IsNullOrEmpty(tagName))
				return string.Empty;

			string startTag = $"<{tagName}>";
			string endTag = $"</{tagName}>";

			int startIndex = data.IndexOf(startTag);
			if (startIndex == -1) return string.Empty;

			startIndex += startTag.Length;
			int endIndex = data.IndexOf(endTag, startIndex);
			if (endIndex == -1) return string.Empty;

			return data[startIndex..endIndex];
		}

		public static List<Tag> MakeTagList(int keySize, int valueSize)
		{
			var tagSet = new List<Tag>();
			for (int i = 0; i < 10; i++)
				tagSet.Add(new Tag() { Key = RandomTextToLong(keySize), Value = RandomTextToLong(valueSize) });

			return tagSet;
		}

		public static Tagging MakeSimpleTagset(int count)
		{
			var tagList = new Tagging() { TagSet = [] };

			for (int i = 0; i < count; i++)
				tagList.TagSet.Add(new Tag() { Key = i.ToString(), Value = i.ToString() });
			return tagList;
		}

		public static string MakeArnResource(string path = "*") => $"arn:aws:s3:::{path}";

		public static JObject MakeJsonStatement(string action, string resource, string effect = MainData.PolicyEffectAllow,
										JObject principal = null, JObject conditions = null)
		{
			principal ??= new JObject() { { "AWS", "*" } };

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

		public static JObject MakeJsonPolicy(string action, string resource, JObject principal = null, JObject conditions = null)
			=> new(){
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray(){ MakeJsonStatement(action, resource, principal: principal, conditions: conditions) } },
			};

		public static JObject MakeJsonPolicy(JArray statements)
			=> new() { { MainData.PolicyVersion, MainData.PolicyVersionDate }, { MainData.PolicyStatement, statements }, };

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

		public static void CheckVersioning(S3Client client, string bucketName, VersionStatus statusCode)
		{
			var response = client.GetBucketVersioning(bucketName);
			Assert.Equal(statusCode, response.VersioningConfig.Status);
		}

		public static void PartsETagCompare(List<PartETag> expected, List<PartDetail> actual)
		{
			Assert.Equal(expected.Count, actual.Count);
			for (int i = 0; i < expected.Count; i++)
			{
				// UploadPart 응답의 ETag에는 따옴표가 붙지만 ListParts 응답에는 붙지 않는다.
				Assert.Equal(expected[i].ETag?.Replace("\"", ""), actual[i].ETag?.Replace("\"", ""));
				Assert.Equal(expected[i].PartNumber, actual[i].PartNumber);
			}
		}

		public static void CheckObjContent(S3Client client, string bucketName, string key, string versionId, string content)
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

		public static void CheckObjVersions(S3Client client, string bucketName, string key, List<string> versionIds, List<string> contents)
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
	}
}
