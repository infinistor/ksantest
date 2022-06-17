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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace s3tests2
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
		Random rand = new Random(Guid.NewGuid().GetHashCode());
		#endregion

		#region Values
		public readonly List<string> EmptyList = new List<string>();
		public MainConfig Config { get; private set; }
		public string ConfigFilePath { get; private set; }
		private List<string> BucketList { get; set; }
		#endregion

		protected TestBase()
		{
			try
			{
				string FileName = Environment.GetEnvironmentVariable(MainData.S3TESTS_INI);
				if (string.IsNullOrWhiteSpace(FileName)) ConfigFilePath = Environment.CurrentDirectory + "\\" + MainConfig.STR_DEF_FILENAME;
				else ConfigFilePath = Environment.CurrentDirectory + "\\" + FileName.Trim();
			}
			catch (Exception) { ConfigFilePath = Environment.CurrentDirectory + "\\" + MainConfig.STR_DEF_FILENAME; }

			Config = new MainConfig(ConfigFilePath);
			Config.GetConfig();

			BucketList = new List<string>();
		}

		public void Dispose() => Clear();
		public void Clear() => BucketClear();

		#region Get Client
		public S3Client GetClient() => new S3Client(Config.S3, Config.SignatureVersion, Config.IsSecure, Config.MainUser);
		public S3Client GetClientV4() => new S3Client(Config.S3, MainConfig.STR_SIGNATURE_VERSION_4, Config.IsSecure, Config.MainUser);
		public S3Client GetClientHttps() => new S3Client(Config.S3, Config.SignatureVersion, true, Config.MainUser);
		public S3Client GetClientHttpsV4() => new S3Client(Config.S3, MainConfig.STR_SIGNATURE_VERSION_4, true, Config.MainUser);
		public S3Client GetAltClient() => new S3Client(Config.S3, Config.SignatureVersion, Config.IsSecure, Config.AltUser);
		public S3Client GetUnauthenticatedClient() => new S3Client(Config.S3, Config.SignatureVersion, Config.IsSecure, null);
		public S3Client GetBadAuthClient(string AccessKey = null, string SecretKey = null)
		{
			if (AccessKey == null) AccessKey = "aaaaaaaaaaaaaaa";
			if (SecretKey == null) SecretKey = "bbbbbbbbbbbbbbb";
			var User = new UserData() { AccessKey = AccessKey, SecretKey = SecretKey };
			return new S3Client(Config.S3, Config.SignatureVersion, Config.IsSecure, User);
		}
		#endregion

		#region Create Data
		public string RandomText(int Length) => new string(Enumerable.Range(0, Length).Select(x => TEXT[rand.Next(0, TEXT.Length)]).ToArray());
		public string RandomTextToLong(int Length) => new string(Enumerable.Range(0, Length).Select(x => TEXT_STRING[rand.Next(0, TEXT_STRING.Length)]).ToArray());

		public string GetBase64EncodedSHA1Hash(string Policy, string SecretKey)
		{
			var Data = Encoding.UTF8.GetBytes(Policy);
			var keyBytes = Encoding.UTF8.GetBytes(SecretKey);

			using HMACSHA1 sha1 = new HMACSHA1(keyBytes);
			return Convert.ToBase64String(sha1.ComputeHash(Data));
		}
		public string GetMD5(string Data)
		{
			using var md5 = MD5.Create();
			var ByteData = Encoding.UTF8.GetBytes(Data);
			var hash = md5.ComputeHash(ByteData, 0, ByteData.Length);
			return Convert.ToBase64String(hash);
		}

		public string GetPrefix() => Config.BucketPrefix.Replace(STR_RANDOM, RandomText(RANDOM_PREFIX_TEXT_LENGTH));
		public string GetNewBucketName(bool Create = true)
		{
			string BucketName = GetPrefix() + RandomText(RANDOM_SUFFIX_TEXT_LENGTH);
			if (BucketName.Length > BUCKET_MAX_LENGTH) BucketName = BucketName.Substring(0, BUCKET_MAX_LENGTH - 1);
			if (Create) BucketList.Add(BucketName);
			return BucketName;
		}
		public string GetNewBucketName(int Length)
		{
			string BucketName = GetPrefix() + RandomText(BUCKET_MAX_LENGTH);
			BucketName = BucketName.Substring(0, Length);
			BucketList.Add(BucketName);
			return BucketName;
		}

		public string GetURL(string BucketName) => $"{MainData.HTTP}{GetHost(BucketName)}";
		public string GetHost(string BucketName)
			=> Config.S3.IsAWS ? $"{BucketName}.s3-{Config.S3.RegionName}.amazonaws.com" : $"{Config.S3.Address}:{Config.S3.Port}/{BucketName}";

		public string HtmlParser(string Data, string Flag1, string Flag2)
		{
			int Index1 = Data.IndexOf(Flag1) + Flag1.Length;
			int Index2 = Data.IndexOf(Flag2);

			return Data.Substring(Index1, Index2 - Index1);
		}

		public List<string> MakePartData(int Size, int PartSize)
		{
			List<string> StringList = new List<string>();

			int RemainSize = Size;
			while (RemainSize > 0)
			{
				int NowPartSize;
				if (RemainSize > PartSize) NowPartSize = PartSize;
				else NowPartSize = RemainSize;

				StringList.Add(RandomTextToLong(NowPartSize));

				RemainSize -= NowPartSize;
			}

			return StringList;
		}
		public List<string> MakePartData(string Data, int PartSize)
		{
			List<string> StringList = new List<string>();

			int StartPoint = 0;
			while (StartPoint < Data.Length)
			{
				int EndPoint;
				if ((StartPoint + PartSize) < Data.Length) EndPoint = PartSize;
				else EndPoint = Data.Length - StartPoint;

				StringList.Add(Data.Substring(StartPoint, EndPoint));
				StartPoint += PartSize;
			}

			return StringList;
		}

		public ByteRange MakeRandomRange(int FileSize)
		{

			var Offset = rand.Next(FileSize - 1000);
			var Length = rand.Next(FileSize - Offset) - 1;

			return new ByteRange(Offset, Offset + Length);
		}

		public List<Tag> MakeTagList(int KeySize, int ValueSize)
		{
			var TagSet = new List<Tag>();
			for (int i = 0; i < 10; i++)
				TagSet.Add(new Tag() { Key = RandomTextToLong(KeySize), Value = RandomTextToLong(ValueSize) });

			return TagSet;
		}
		public Tagging MakeSimpleTagset(int Count)
		{
			var TagList = new Tagging() { TagSet = new List<Tag>() };

			for (int i = 0; i < Count; i++)
				TagList.TagSet.Add(new Tag() { Key = i.ToString(), Value = i.ToString() });
			return TagList;
		}

		public string MakeArnResource(string Path = "*") => $"arn:aws:s3:::{Path}";
		public JObject MakeJsonStatement(string Action, string Resource, string Effect = MainData.PolicyEffectAllow,
										JObject Principal = null, JObject Conditions = null)
		{
			if (Principal == null) Principal = new JObject() { { "AWS", "*" } };

			var Statement = new JObject()
			{
				{ MainData.PolicyEffect, Effect },
				{ MainData.PolicyPrincipal, Principal },
				{ MainData.PolicyAction, Action },
				{ MainData.PolicyResource, Resource },
			};

			if (Conditions != null) Statement.Add(MainData.PolicyCondition, Conditions);

			return Statement;
		}
		public JObject MakeJsonPolicy(string Action, string Resource, JObject Principal = null, JObject Conditions = null)
			=> new JObject(){
				{ MainData.PolicyVersion, MainData.PolicyVersionDate },
				{ MainData.PolicyStatement, new JArray(){ MakeJsonStatement(Action, Resource, Principal: Principal, Conditions: Conditions) } },
			};
		public JObject MakeJsonPolicy(JArray Statements)
			=> new JObject() { { MainData.PolicyVersion, MainData.PolicyVersionDate }, { MainData.PolicyStatement, Statements }, };

		#endregion



		#region POST
		public HttpWebResponse GetObject(string URL)
		{
			HttpWebRequest httpRequest = WebRequest.Create(URL) as HttpWebRequest;
			httpRequest.Method = "GET";
			HttpWebResponse response = httpRequest.GetResponse() as HttpWebResponse;

			return response;
		}
		public HttpWebResponse PutObject(string URL, string Body = null)
		{
			HttpWebRequest httpRequest = WebRequest.Create(URL) as HttpWebRequest;
			httpRequest.Method = "PUT";

			if (Body != null)
			{
				var ByteStream = Encoding.ASCII.GetBytes(Body);
				using Stream dataStream = httpRequest.GetRequestStream();
				dataStream.Write(ByteStream, 0, ByteStream.Length);
			}

			HttpWebResponse Response = httpRequest.GetResponse() as HttpWebResponse;
			return Response;
		}

		public MyResult PostUpload(string BucketName, Dictionary<string, object> parameters)
		{
			//https://spirit32.tistory.com/21
			string boundary = DateTime.Now.Ticks.ToString("x");
			byte[] boundaryBytes = Encoding.ASCII.GetBytes(string.Format("\r\n--{0}\r\n", boundary));

			var url = GetURL(BucketName);
			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
			request.ContentType = "multipart/form-data; boundary=" + boundary;
			request.Method = "POST";
			request.KeepAlive = true;
			request.Credentials = CredentialCache.DefaultCredentials;
			if (Config.S3.IsAWS) request.Host = GetHost(BucketName);

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
				//requestStream.Close();
			}

			try
			{
				var Response = request.GetResponse() as HttpWebResponse;

				StreamReader reader = new StreamReader(Response.GetResponseStream());
				string Data = reader.ReadToEnd();


				return new MyResult()
				{
					URL = Response.ResponseUri.AbsoluteUri,
					StatusCode = Response.StatusCode,
				};
			}
			catch (WebException e)
			{
				var Stream = e.Response.GetResponseStream();
				StreamReader reader = new StreamReader(Stream);
				string Data = reader.ReadToEnd();

				return new MyResult()
				{
					URL = ((HttpWebResponse)e.Response).ResponseUri.AbsoluteUri,
					StatusCode = ((HttpWebResponse)e.Response).StatusCode,
					ErrorCode = HtmlParser(Data, "<Code>", "</Code>"),
					Message = HtmlParser(Data, "<Message>", "</Message>"),
				};
			}
		}
		#endregion

		#region Check
		public bool ErrorCheck(HttpStatusCode StatusCode)
		{
			if (StatusCode.Equals(HttpStatusCode.BadRequest)) return true;
			if (StatusCode.Equals(HttpStatusCode.Forbidden)) return true;
			return false;
		}

		public void CheckVersioning(string BucketName, VersionStatus StatusCode)
		{
			var Client = GetClient();

			var Response = Client.GetBucketVersioning(BucketName);
			Assert.AreEqual(StatusCode, Response.VersioningConfig.Status);
		}
		public void CheckConfigureVersioningRetry(string BucketName, VersionStatus Status)
		{
			var Client = GetClient();

			Client.PutBucketVersioning(BucketName, EnableMfaDelete: false, Status: Status);

			VersionStatus ReadStatus = null;

			for (int i = 0; i < 5; i++)
			{
				try
				{
					var Response = Client.GetBucketVersioning(BucketName);
					ReadStatus = Response.VersioningConfig.Status;

					if (ReadStatus == Status) break;
					Thread.Sleep(1000);
				}
				catch (Exception)
				{
					ReadStatus = null;
				}
			}

			Assert.AreEqual(Status, ReadStatus);
		}

		public void CheckContent(S3Client Client, string BucketName, string Key, string Data, int LoopCount = 1, SSECustomerKey SSEC = null)
		{
			for (int i = 0; i < LoopCount; i++)
			{
				var Response = Client.GetObject(BucketName, Key, SSEC: SSEC);
				var Body = GetBody(Response);
				Assert.AreEqual(Data, Body);
			}
		}
		public void CheckContentUsingRange(S3Client Client, string BucketName, string Key, string Data, long Step, SSECustomerKey SSEC = null)
		{
			var Size = Data.Length;
			long StartpPosition = 0;

			while (StartpPosition < Size)
			{
				var EndPosition = StartpPosition + Step;
				if (EndPosition > Size) EndPosition = Size - 1;
				EndPosition -= 1;


				var Range = new ByteRange(StartpPosition, EndPosition);
				var Response = Client.GetObject(BucketName, Key: Key, Range: Range, SSEC: SSEC);
				var Body = GetBody(Response);
				var Length = EndPosition - StartpPosition + 1;

				Assert.AreEqual(Length, Response.ContentLength);
				Assert.AreEqual(Data.Substring((int)Range.Start, (int)Length), Body);
				StartpPosition += Step;
			}
		}
		public void CheckContentUsingRandomRange(S3Client Client, string BucketName, string Key, string Data, int LoopCount, SSECustomerKey SSEC = null)
		{
			for (int i = 0; i < LoopCount; i++)
			{
				var Range = MakeRandomRange(Data.Length);
				var Length = Range.End - Range.Start;

				var Response = Client.GetObject(BucketName, Key: Key, Range: Range, SSEC: SSEC);
				var Body = GetBody(Response);

				Assert.AreEqual(Length, Response.ContentLength - 1);
				Assert.AreEqual(Data.Substring((int)Range.Start, (int)Length + 1), Body);
			}
		}

		public string ValidateListObjcet(string BucketName, string Prefix, string Delimiter, string Marker,
						int MaxKeys, bool IsTruncated, List<string> CheckKeys, List<string> CheckPrefixs, string NextMarker)
		{
			var Client = GetClient();
			var Response = Client.ListObjects(BucketName, Delimiter: Delimiter, Marker: Marker, MaxKeys: MaxKeys, Prefix: Prefix);

			Assert.AreEqual(IsTruncated, Response.IsTruncated);
			Assert.AreEqual(NextMarker, Response.NextMarker);

			List<string> Keys = GetKeys(Response);
			List<string> Prefixes = Response.CommonPrefixes;

			Assert.AreEqual(CheckKeys.Count, Keys.Count);
			Assert.AreEqual(CheckPrefixs.Count, Prefixes.Count);
			CollectionAssert.AreEqual(CheckKeys, Keys);
			CollectionAssert.AreEqual(CheckPrefixs, Prefixes);

			return Response.NextMarker;
		}

		public string ValidateListObjcetV2(string BucketName, string Prefix, string Delimiter, string ContinuationToken,
						int MaxKeys, bool IsTruncated, List<string> CheckKeys, List<string> CheckPrefixs, bool Last = false)
		{
			var Client = GetClient();
			var Response = Client.ListObjectsV2(BucketName, Delimiter: Delimiter, ContinuationToken: ContinuationToken,
												MaxKeys: MaxKeys, Prefix: Prefix);

			Assert.AreEqual(IsTruncated, Response.IsTruncated);
			if (Last) Assert.IsNull(Response.NextContinuationToken);

			List<string> Keys = GetKeys(Response);
			List<string> Prefixes = Response.CommonPrefixes;

			CollectionAssert.AreEqual(CheckKeys, Keys);
			CollectionAssert.AreEqual(CheckPrefixs, Prefixes);
			Assert.AreEqual(CheckKeys.Count, Keys.Count);
			Assert.AreEqual(CheckPrefixs.Count, Prefixes.Count);

			return Response.NextContinuationToken;
		}

		public void CheckBucketACLGrantCanRead(string BucketName) => GetAltClient().ListObjects(BucketName);
		public void CheckBucketACLGrantCantRead(string BucketName) => Assert.ThrowsException<AmazonS3Exception>(() => GetAltClient().ListObjects(BucketName));
		public void CheckBucketACLGrantCanReadACP(string BucketName) => GetAltClient().GetBucketACL(BucketName);
		public void CheckBucketACLGrantCantReadACP(string BucketName) => Assert.ThrowsException<AmazonS3Exception>(() => GetAltClient().GetBucketACL(BucketName));
		public void CheckBucketACLGrantCanWrite(string BucketName) => GetAltClient().PutObject(BucketName, "foo-write", Body: "bar");
		public void CheckBucketACLGrantCantWrite(string BucketName) => Assert.ThrowsException<AmazonS3Exception>(() => GetAltClient().PutObject(BucketName, "foo-write", Body: "bar"));
		public void CheckBucketACLGrantCanWriteACP(string BucketName) => GetAltClient().PutBucketACL(BucketName, ACL: S3CannedACL.PublicRead);
		public void CheckBucketACLGrantCantWriteACP(string BucketName) => Assert.ThrowsException<AmazonS3Exception>(() => GetAltClient().PutBucketACL(BucketName, ACL: S3CannedACL.PublicRead));

		public void CheckBadBucketName(string BucketName)
		{
			var Client = GetClient();

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutBucket(BucketName));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;
			Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
			Assert.AreEqual(MainData.InvalidBucketName, ErrorCode);
		}
		public void CheckGoodBucketName(string Name, string Prefix = null)
		{
			if (Prefix == null) Prefix = GetPrefix();

			var BucketName = $"{Prefix}{Name}";
			BucketList.Add(BucketName);

			var Client = GetClient();
			var Response = Client.PutBucket(BucketName);
			Assert.AreEqual(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		public void CheckGrants(List<S3Grant> Expected, List<S3Grant> Actual)
		{
			Assert.AreEqual(Expected.Count, Actual.Count);

			Expected = GrantsSort(Expected);
			Actual = GrantsSort(Actual);

			for (int i = 0; i < Expected.Count; i++)
			{
				Assert.AreEqual(Expected[i].Permission, Actual[i].Permission);
				Assert.AreEqual(Expected[i].Grantee.CanonicalUser, Actual[i].Grantee.CanonicalUser);
				Assert.AreEqual(Expected[i].Grantee.EmailAddress, Actual[i].Grantee.EmailAddress);
				Assert.AreEqual(Expected[i].Grantee.Type, Actual[i].Grantee.Type);
				Assert.AreEqual(Expected[i].Grantee.URI, Actual[i].Grantee.URI);
			}
		}
		public void CheckObjectACL(S3Permission Permission)
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";

			Client.PutObject(BucketName, Key);
			var Response = Client.GetObjectACL(BucketName, Key);

			var Policy = Response.AccessControlList;
			Policy.Grants[0].Permission = Permission;

			Client.PutObjectACL(BucketName, Key, AccessControlPolicy: Policy);

			Response = Client.GetObjectACL(BucketName, Key);
			var Grants = Response.AccessControlList.Grants;

			var MainUserId = Config.MainUser.UserId;
			var MainDispalyName = Config.MainUser.DisplayName;

			CheckGrants(new List<S3Grant>()
			{
				new S3Grant()
				{
					 Permission = Permission,
					 Grantee = new S3Grantee()
					 {
						 CanonicalUser = MainUserId,
						 DisplayName = MainDispalyName,
						 URI = null,
						 EmailAddress = null,
					 }
				}
			},
			Grants);
		}

		public void CheckCopyContent(S3Client Client,
									string SrcBucketName, string SrcKey, string DestBucketName, string DestKey,
									string SrcVersionId = null, SSECustomerKey SrcSSEC = null,
									string DestVersionId = null, SSECustomerKey DestSSEC = null)
		{
			var SrcResponse = Client.GetObject(SrcBucketName, SrcKey, VersionId: SrcVersionId, SSEC: SrcSSEC);
			var SrcSize = SrcResponse.ContentLength;
			var SrcData = GetBody(SrcResponse);

			var DestResponse = Client.GetObject(DestBucketName, DestKey, VersionId: DestVersionId, SSEC: DestSSEC);
			var DestSize = DestResponse.ContentLength;
			var DestData = GetBody(DestResponse);

			Assert.AreEqual(SrcSize, DestSize);
			Assert.AreEqual(SrcData, DestData);
		}

		public void CheckCopyContentUsingRange(S3Client Client, string SrcBucketName, string SrcKey, string DestBucketName, string DestKey, string VersionId = null)
		{
			var HeadResponse = Client.GetObjectMetadata(SrcBucketName, SrcKey, VersionId: VersionId);
			var SrcSize = HeadResponse.ContentLength;

			var Response = Client.GetObject(DestBucketName, DestKey);
			var DestSize = Response.ContentLength;
			var DestData = GetBody(Response);
			Assert.IsTrue(SrcSize >= DestSize);

			var Range = new ByteRange(0, DestSize - 1);
			Response = Client.GetObject(SrcBucketName, SrcKey, Range: Range, VersionId: VersionId);
			var SrcData = GetBody(Response);
			Assert.AreEqual(SrcData, DestData);
		}

		public void CheckObjContent(S3Client Client, string BucketName, string Key, string VersionId, string Content)
		{
			var Response = Client.GetObject(BucketName, Key, VersionId: VersionId);
			if (Content != null)
			{
				var Body = GetBody(Response);
				Assert.AreEqual(Content, Body);
			}
			else
				Assert.AreEqual("True", Response.DeleteMarker);
		}
		public void CheckObjVersions(S3Client Client, string BucketName, string Key, List<string> VersionIds, List<string> Contents)
		{
			var Response = Client.ListVersions(BucketName);
			var Versions = GetVersions(Response.Versions);

			Versions.Reverse();

			var index = 0;
			foreach (var version in Versions)
			{
				Assert.AreEqual(version.VersionId, VersionIds[index]);
				if (!string.IsNullOrWhiteSpace(Key)) Assert.AreEqual(Key, version.Key);
				CheckObjContent(Client, BucketName, Key, version.VersionId, Contents[index++]);
			}
		}

		public void CheckUploadMultipartResend(string BucketName, string Key, int Size, List<int> ResendParts)
		{
			var ContentType = "text/bla";
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar") };
			var Client = GetClient();
			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, ContentType: ContentType, MetadataList: Metadata, ResendParts: ResendParts);
			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			var Response = Client.GetObject(BucketName, Key);
			Assert.AreEqual(ContentType, Response.Headers.ContentType);
			CollectionAssert.AreEqual(Metadata, GetMetaData(Response.Metadata));
			var Body = GetBody(Response);
			Assert.AreEqual(Body.Length, Response.ContentLength);
			Assert.AreEqual(UploadData.Body, Body);

			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 1000000);
			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 10000000);
		}

		public void PrefixLifecycleConfigurationCheck(List<LifecycleRule> Expected, List<LifecycleRule> Actual)
		{
			Assert.AreEqual(Expected.Count, Actual.Count);

			for (int i = 0; i < Expected.Count; i++)
			{
				Assert.AreEqual(Expected[i].Id, Actual[i].Id);
				Assert.AreEqual(Expected[i].Expiration.DateUtc, Actual[i].Expiration.DateUtc);
				Assert.AreEqual(Expected[i].Expiration.Days, Actual[i].Expiration.Days);
				Assert.AreEqual(Expected[i].Expiration.ExpiredObjectDeleteMarker, Actual[i].Expiration.ExpiredObjectDeleteMarker);
				Assert.AreEqual((Expected[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix,
							 (Actual[i].Filter.LifecycleFilterPredicate as LifecyclePrefixPredicate).Prefix);

				Assert.AreEqual(Expected[i].Status, Actual[i].Status);
				Assert.AreEqual(Expected[i].NoncurrentVersionExpiration, Actual[i].NoncurrentVersionExpiration);
				CollectionAssert.AreEqual(Expected[i].Transitions, Actual[i].Transitions);
				CollectionAssert.AreEqual(Expected[i].NoncurrentVersionTransitions, Actual[i].NoncurrentVersionTransitions);
				Assert.AreEqual(Expected[i].AbortIncompleteMultipartUpload, Actual[i].AbortIncompleteMultipartUpload);
			}
		}

		public void CorsRequestAndCheck(string Method, string BucketName, List<KeyValuePair<string, string>> Headers, HttpStatusCode StatusCode,
			string ExpectAllowOrigin, string ExpectAllowMethods, string Key = null)
		{
			//https://spirit32.tistory.com/21
			var url = GetURL(BucketName);
			if (Key != null) url += string.Format("/{0}", Key);

			WebRequest request = WebRequest.Create(url);
			Assert.IsNotNull(request);
			request.Method = Method;
			request.Credentials = CredentialCache.DefaultCredentials;
			foreach (var Item in Headers)
			{
				if (Item.Key == "content-length") continue;
				request.Headers.Add(Item.Key, Item.Value);
			}

			MyResult Result;
			try
			{
				var Response = request.GetResponse() as HttpWebResponse;

				Result = new MyResult()
				{
					URL = Response.ResponseUri.AbsoluteUri,
					StatusCode = Response.StatusCode,
					Headers = Response.Headers,
				};
			}
			catch (WebException e)
			{
				var Stream = e.Response.GetResponseStream();
				StreamReader reader = new StreamReader(Stream);
				string Data = reader.ReadToEnd();

				Result = new MyResult()
				{
					URL = ((HttpWebResponse)e.Response).ResponseUri.AbsoluteUri,
					StatusCode = ((HttpWebResponse)e.Response).StatusCode,
					Headers = ((HttpWebResponse)e.Response).Headers,
					ErrorCode = HtmlParser(Data, "<Code>", "</Code>"),
					Message = HtmlParser(Data, "<Message>", "</Message>"),
				};
			}

			Assert.AreEqual(StatusCode, Result.StatusCode);
			Assert.AreEqual(Result.Headers.Get("access-control-allow-origin"), ExpectAllowOrigin);
			Assert.AreEqual(Result.Headers.Get("access-control-allow-methods"), ExpectAllowMethods);
		}

		public void TaggingCompare(List<Tag> Expected, List<Tag> Actual)
		{
			Assert.AreEqual(Expected.Count, Actual.Count);

			var OrderExpected = Expected.OrderBy(x => x.Key).ToList();
			var OrderActual = Actual.OrderBy(x => x.Key).ToList();

			for (int i = 0; i < Expected.Count; i++)
			{
				Assert.AreEqual(OrderExpected[i].Key, OrderActual[i].Key);
				Assert.AreEqual(OrderExpected[i].Value, OrderActual[i].Value);
			}
		}
		public void LockCompare(ObjectLockConfiguration Expected, ObjectLockConfiguration Actual)
		{
			Assert.AreEqual(Expected.ObjectLockEnabled, Actual.ObjectLockEnabled);
			Assert.AreEqual(Expected.Rule.DefaultRetention.Mode, Actual.Rule.DefaultRetention.Mode);
			Assert.AreEqual(Expected.Rule.DefaultRetention.Years, Actual.Rule.DefaultRetention.Years);
			Assert.AreEqual(Expected.Rule.DefaultRetention.Days, Actual.Rule.DefaultRetention.Days);
		}
		public void RetentionCompare(ObjectLockRetention Expected, ObjectLockRetention Actual)
		{
			Assert.AreEqual(Expected.Mode, Actual.Mode);
			Assert.AreEqual(Expected.RetainUntilDate, Actual.RetainUntilDate.ToUniversalTime());

		}
		public void PartsETagCompare(List<PartETag> Expected, List<PartDetail> Actual)
		{
			Assert.AreEqual(Expected.Count, Actual.Count);

			for (int i = 0; i < Expected.Count; i++)
			{
				Assert.AreEqual(Expected[i].PartNumber, Actual[i].PartNumber);
				Assert.AreEqual(Expected[i].ETag, Actual[i].ETag);
			}
		}
		public void VersionIdsCompare(List<S3ObjectVersion> Expected, List<S3ObjectVersion> Actual)
		{
			Assert.AreEqual(Expected.Count, Actual.Count);

			for (int i = 0; i < Expected.Count; i++)
			{
				Assert.AreEqual(Expected[i].VersionId, Actual[i].VersionId);
				Assert.AreEqual(Expected[i].ETag, Actual[i].ETag);
				Assert.AreEqual(Expected[i].Size, Actual[i].Size);
				Assert.AreEqual(Expected[i].Key, Actual[i].Key);
			}
		}
		public void LoggingConfigCompare(S3BucketLoggingConfig Expected, S3BucketLoggingConfig Actual)
		{
			Assert.AreEqual(Expected.TargetBucketName, Actual.TargetBucketName);
			
			if(Expected.TargetPrefix == null) Assert.IsNull(Actual.TargetPrefix);
			else Assert.AreEqual(Expected.TargetPrefix, Actual.TargetPrefix);

			if (Expected.Grants == null && Actual.Grants == null) return;
			CheckGrants(Expected.Grants, Actual.Grants);
		}

		#endregion

		#region  Get Data
		public static string TimeToString(DateTime Time) => Time.ToString(S3Headers.TimeFormat, S3Headers.TimeCulture);
		public static List<string> GetKeys(ListObjectsResponse Response)
		{
			if (Response != null)
			{
				List<string> Temp = new List<string>();

				foreach (var S3Object in Response.S3Objects) Temp.Add(S3Object.Key);

				return Temp;
			}
			return null;
		}
		public static List<string> GetKeys(ListObjectsV2Response Response)
		{
			if (Response != null)
			{
				List<string> Temp = new List<string>();

				foreach (var S3Object in Response.S3Objects) Temp.Add(S3Object.Key);

				return Temp;
			}
			return null;
		}
		public static string GetBody(GetObjectResponse Response)
		{
			string Body = string.Empty;
			if (Response != null && Response.ResponseStream != null)
			{
				var Reader = new StreamReader(Response.ResponseStream);
				Body = Reader.ReadToEnd();
				Reader.Close();
			}
			return Body;
		}
		public static ObjectData GetObjectToKey(string Key, List<ObjectData> KeyList)
		{
			foreach (var Object in KeyList)
			{
				if (Object.Key.Equals(Key)) return Object;
			}
			return null;
		}
		public static List<KeyVersion> GetKeyVersions(List<string> KeyList)
		{
			List<KeyVersion> KeyVersions = new List<KeyVersion>();
			foreach (var Key in KeyList) KeyVersions.Add(new KeyVersion() { Key = Key });

			return KeyVersions;
		}

		public static HttpStatusCode GetStatus(AggregateException e) => (e.InnerException is AmazonS3Exception e2) ? e2.StatusCode : HttpStatusCode.OK;

		public static string GetErrorCode(AggregateException e) => (e.InnerException is AmazonS3Exception e2) ? e2.ErrorCode : null;

		public static long GetBytesUsed(ListObjectsV2Response Response)
		{
			if (Response == null) return 0;
			if (Response.S3Objects == null) return 0;
			if (Response.S3Objects.Count > 1) return 0;

			long Size = 0;

			foreach (var Obj in Response.S3Objects) Size += Obj.Size;

			return Size;
		}
		public static List<KeyValuePair<string, string>> GetMetaData(MetadataCollection Response)
		{
			var MetaDataList = new List<KeyValuePair<string, string>>();

			foreach (var Key in Response.Keys)
				MetaDataList.Add(new KeyValuePair<string, string>(Key, Response[Key]));

			return MetaDataList;
		}
		public List<KeyValuePair<string, string>> GetACLHeader(string UserId = null, string[] Perms = null)
		{
			string[] AllHeaders = { "read", "write", "read-acp", "write-acp", "full-control" };

			var Headers = new List<KeyValuePair<string, string>>();

			if (UserId == null) UserId = Config.AltUser.UserId;
			if (Perms == null)
			{
				foreach (var Perm in AllHeaders)
					Headers.Add(new KeyValuePair<string, string>(string.Format("x-amz-grant-{0}", Perm), string.Format("id={0}", UserId)));
			}
			else
			{
				foreach (var Perm in Perms)
					Headers.Add(new KeyValuePair<string, string>(string.Format("x-amz-grant-{0}", Perm), string.Format("id={0}", UserId)));
			}
			return Headers;
		}
		public List<S3Grant> GetGrantList(string UserId = null, S3Permission[] Perms = null)
		{
			S3Permission[] AllHeaders = { S3Permission.READ, S3Permission.WRITE, S3Permission.READ_ACP, S3Permission.WRITE_ACP, S3Permission.FULL_CONTROL };

			var Headers = new List<S3Grant>();

			if (UserId == null) UserId = Config.AltUser.UserId;
			if (Perms == null)
			{
				foreach (var Perm in AllHeaders)
					Headers.Add(new S3Grant() { Permission = Perm, Grantee = new S3Grantee() { CanonicalUser = UserId } });
			}
			else
			{
				foreach (var Perm in Perms)
					Headers.Add(new S3Grant() { Permission = Perm, Grantee = new S3Grantee() { CanonicalUser = UserId } });
			}
			return Headers;
		}
		public List<string> GetBucketList(ListBucketsResponse Response)
		{
			if (Response == null) return null;
			var Buckets = Response.Buckets;
			var BucketList = new List<string>();

			foreach (var Bucket in Buckets) BucketList.Add(Bucket.BucketName);

			return BucketList;
		}
		public List<string> GetObjectList(string BucketName, string Prefix = null)
		{
			var Client = GetClient();
			var Response = Client.ListObjects(BucketName, Prefix: Prefix);
			return GetKeys(Response);
		}

		public List<S3ObjectVersion> GetVersions(List<S3ObjectVersion> Versions)
		{
			if (Versions == null) return null;

			var Lists = new List<S3ObjectVersion>();
			foreach (var item in Versions)
				if (!item.IsDeleteMarker) Lists.Add(item);
			return Lists;
		}
		public List<string> GetVersionIds(List<S3ObjectVersion> Versions)
		{
			if (Versions == null) return null;

			var Lists = new List<string>();
			foreach (var item in Versions)
				if (!item.IsDeleteMarker) Lists.Add(item.VersionId);
			return Lists;
		}
		public int GetDeleteMarkerCount(List<S3ObjectVersion> Versions)
		{
			if (Versions == null) return 0;
			int Count = 0;
			foreach (var item in Versions)
				if (item.IsDeleteMarker) Count++;
			return Count;
		}
		public List<S3ObjectVersion> GetDeleteMarkers(List<S3ObjectVersion> Versions)
		{
			if (Versions == null) return null;

			var DeleteMarkers = new List<S3ObjectVersion>();

			foreach (var item in Versions)
				if (item.IsDeleteMarker) DeleteMarkers.Add(item);
			return DeleteMarkers;
		}

		public List<S3Grant> GrantsSort(List<S3Grant> Data)
		{
			var newList = new List<S3Grant>();

			var SortMap = new SortedDictionary<string, S3Grant>();

			foreach (var Grant in Data)
				SortMap.Add($"{Grant.Grantee.CanonicalUser}{Grant.Permission}", Grant);

			foreach (var item in SortMap)
			{
				newList.Add(item.Value);
				Console.WriteLine($"{item.Value.Grantee.DisplayName}, {item.Value.Grantee.URI}, {item.Value.Permission}");
			}

			return newList;
		}
		#endregion

		#region Setup
		public string GetNewBucket()
		{
			var BucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(BucketName);
			return BucketName;
		}

		public string SetupMetadata(string Metadata, string BucketName = null)
		{
			if (string.IsNullOrEmpty(BucketName)) BucketName = GetNewBucket();

			var Client = GetClient();
			var KeyName = "foo";
			var MetadataKey = "x-amz-meta-meta1";

			var MetadataList = new List<KeyValuePair<string, string>>
			{
				new KeyValuePair<string, string>(MetadataKey, Metadata)
			};

			Client.PutObject(BucketName, Key: KeyName, Body: "bar", MetadataList: MetadataList);

			var Response = Client.GetObject(BucketName, KeyName);
			return Response.Metadata[MetadataKey];
		}

		public string SetupObjects(List<string> KeyList, string BucketName = null, string Body = null)
		{
			var Client = GetClient();
			if (BucketName == null)
			{
				BucketName = GetNewBucketName();
				Client.PutBucket(BucketName);
			}

			if (KeyList != null && Client != null)
			{
				if (Body != null) foreach (var Key in KeyList) Client.PutObject(BucketName, Key, Body);
				else foreach (var Key in KeyList) Client.PutObject(BucketName, Key, Key);
			}

			return BucketName;
		}
		public string SetupObjectsV4(List<string> KeyList, string BucketName = null, string Body = null,
										bool? UseChunkEncoding = null, bool? DisablePayloadSigning = null)
		{
			var Client = GetClientV4();
			if (DisablePayloadSigning.HasValue)
			{
				if (DisablePayloadSigning.Value)
				{
					Client = GetClientHttpsV4();
				}
			}

			if (BucketName == null)
			{
				BucketName = GetNewBucketName();
				Client.PutBucket(BucketName);
			}

			if (KeyList != null && Client != null)
			{
				foreach (var Key in KeyList)
				{
					if (Body == null) Body = Key;
					Client.PutObject(BucketName, Key, Body, UseChunkEncoding: UseChunkEncoding, DisablePayloadSigning: DisablePayloadSigning);
				}
			}

			return BucketName;
		}

		public string SetupKeyWithRandomContent(string Key, int Size = 7 * MainData.MB, string BucketName = null, S3Client Client = null)
		{
			if (BucketName == null) BucketName = GetNewBucket();
			if (Client == null) Client = GetClient();

			var Data = RandomTextToLong(Size);
			Client.PutObject(BucketName, Key, Body: Data);

			return BucketName;
		}

		public string SetupBucketObjectACL(S3CannedACL BucketACL, S3CannedACL ObjectACL, string Key)
		{
			var BucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(BucketName, ACL: BucketACL);
			Client.PutObject(BucketName, Key: Key, ACL: ObjectACL);
			return BucketName;
		}
		public string SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL BucketACL, S3CannedACL ObjectACL)
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			Key1 = "foo";
			Key2 = "bar";
			NewKey = "new";

			Client.PutBucketACL(BucketName, ACL: BucketACL);
			Client.PutObject(BucketName, Key1, Body: "foocontent");
			Client.PutObjectACL(BucketName, Key1, ACL: ObjectACL);
			Client.PutObject(BucketName, Key2, Body: "barcontent");

			return BucketName;
		}

		public MultipartUploadData SetupMultipartCopy(S3Client Client, string SrcBucketName, string SrcKey, string DestBucketName, string DestKey, int Size,
			int PartSize = 5 * MainData.MB, string VersionId = null, SSECustomerKey SrcSSEC = null, SSECustomerKey DestSSEC = null, ServerSideEncryptionMethod SSE_S3 = null)
		{
			var UploadData = new MultipartUploadData();

			var Response = Client.InitiateMultipartUpload(DestBucketName, DestKey, SSEC: SrcSSEC, SSE_S3: SSE_S3);
			UploadData.UploadId = Response.UploadId;

			int Start = 0;
			while (Start < Size)
			{
				int End = Math.Min(Start + PartSize - 1, Size - 1);
				var PartNumber = UploadData.NextPartNumber;

				var PartResPonse = Client.CopyPart(SrcBucketName, SrcKey, DestBucketName, DestKey, UploadData.UploadId, PartNumber, Start, End,
							VersionId: VersionId, SrcSSEC: SrcSSEC, DestSSEC: DestSSEC);
				UploadData.AddPart(PartNumber, PartResPonse.ETag);

				Start = End + 1;
			}

			return UploadData;
		}

		public MultipartUploadData SetupMultipartUpload(S3Client Client, string BucketName, string Key, int Size, MultipartUploadData UploadData = null,
			int PartSize = 5 * MainData.MB, List<KeyValuePair<string, string>> MetadataList = null, string ContentType = null,
			List<int> ResendParts = null, SSECustomerKey SSEC = null, ServerSideEncryptionMethod SSE_S3 = null)
		{
			if (UploadData == null)
			{
				UploadData = new MultipartUploadData();
				var InitResponse = Client.InitiateMultipartUpload(BucketName, Key, MetadataList: MetadataList, ContentType: ContentType, SSEC: SSEC, SSE_S3: SSE_S3);
				UploadData.UploadId = InitResponse.UploadId;
			}

			var Parts = MakePartData(Size, PartSize);

			foreach (var Part in Parts)
			{
				UploadData.AppendBody(Part);
				var PartNumber = UploadData.NextPartNumber;
				var PartResPonse = Client.UploadPart(BucketName, Key, UploadData.UploadId, Part, PartNumber, SSEC: SSEC);
				UploadData.Parts.Add(new PartETag(PartResPonse.PartNumber, PartResPonse.ETag));

				if (ResendParts != null && ResendParts.Contains(PartNumber)) Client.UploadPart(BucketName, Key, UploadData.UploadId, Part, PartNumber);
			}

			return UploadData;
		}
		public MultipartUploadData SetupMultipartUploadCSE(S3Client Client, string BucketName, string Key, string Data, int PartSize = 5 * MainData.MB,
			List<KeyValuePair<string, string>> MetadataList = null, string ContentType = null)
		{
			var UploadData = new MultipartUploadData();
			if (Client == null) Client = GetClient();

			var InitMultiPartResponse = Client.InitiateMultipartUpload(BucketName, Key, MetadataList: MetadataList, ContentType: ContentType);
			UploadData.UploadId = InitMultiPartResponse.UploadId;

			var Parts = MakePartData(Data, PartSize);

			int UploadCount = 1;
			foreach (var Part in Parts)
			{
				var PartResPonse = Client.UploadPart(BucketName, Key, UploadData.UploadId, Part, UploadCount);
				UploadData.Parts.Add(new PartETag(UploadCount++, PartResPonse.ETag));
			}

			return UploadData;
		}

		public S3AccessControlList AddObjectUserGrant(string BucketName, string Key, S3Grant Grant)
		{
			var Client = GetClient();
			var MainUserId = Config.MainUser.UserId;
			var MainUserDisplayName = Config.MainUser.DisplayName;

			var Response = Client.GetObjectACL(BucketName, Key);
			var Grants = Response.AccessControlList.Grants;
			Grants.Add(Grant);

			var MyGrants = new S3AccessControlList()
			{
				Grants = Grants,
				Owner = new Owner()
				{
					DisplayName = MainUserDisplayName,
					Id = MainUserId,
				}
			};

			return MyGrants;
		}
		public S3AccessControlList AddBucketUserGrant(string BucketName, S3Grant Grant)
		{
			var Client = GetClient();
			var MainUserId = Config.MainUser.UserId;
			var MainUserDisplayName = Config.MainUser.DisplayName;

			var Response = Client.GetBucketACL(BucketName);
			var Grants = Response.AccessControlList.Grants;
			Grants.Add(Grant);

			var MyGrants = new S3AccessControlList()
			{
				Grants = Grants,
				Owner = new Owner()
				{
					DisplayName = MainUserDisplayName,
					Id = MainUserId,
				}
			};

			return MyGrants;
		}

		public void SetupMultipleVersions(S3Client Client, string BucketName, string Key, int NumVersions,
			ref List<string> VersionIds, ref List<string> Contents, bool CheckVersion = true)
		{
			if (VersionIds == null) VersionIds = new List<string>();
			if (Contents == null) Contents = new List<string>();

			for (int i = 0; i < NumVersions; i++)
			{
				var Body = string.Format("content-{0}", i);
				var Response = Client.PutObject(BucketName, Key, Body: Body);
				var VersionId = Response.VersionId;

				Contents.Add(Body);
				VersionIds.Add(VersionId);
			}

			if (CheckVersion) CheckObjVersions(Client, BucketName, Key, VersionIds, Contents);

		}
		public void SetupMultipleVersion(S3Client Client, string BucketName, string Key, int NumVersions, bool CheckVersion = true)
		{
			var VersionIds = new List<string>();
			var Contents = new List<string>();

			for (int i = 0; i < NumVersions; i++)
			{
				var Body = string.Format("content-{0}", i);
				var Response = Client.PutObject(BucketName, Key, Body: Body);
				var VersionId = Response.VersionId;

				Contents.Add(Body);
				VersionIds.Add(VersionId);
			}

			if (CheckVersion) CheckObjVersions(Client, BucketName, Key, VersionIds, Contents);
		}

		public void OverwriteSuspendedVersioningObj(S3Client Client, string BucketName, string Key,
			ref List<string> VersionIds, ref List<string> Contents, string Content)
		{
			Client.PutObject(BucketName, Key, Body: Content);

			Assert.AreEqual(VersionIds.Count, Contents.Count);

			for (int i = VersionIds.Count - 1; i >= 0; i--)
			{
				if (VersionIds[i] == "null")
				{
					VersionIds.RemoveAt(i);
					Contents.RemoveAt(i);
				}
			}
			Contents.Add(Content);
			VersionIds.Add("null");
			Thread.Sleep(100);
		}

		public List<Thread> SetupVersionedObjConcurrent(S3Client Client, string BucketName, string Key, int Num)
		{
			var TList = new List<Thread>();
			for (int i = 0; i < Num; i++)
			{
				var mThread = new Thread(() => Client.PutObject(BucketName, Key, Body: string.Format("Data {0}", i)));
				mThread.Start();
				TList.Add(mThread);
			}
			return TList;
		}

		#endregion

		#region Remove
		public void RemoveObjVersion(S3Client Client, string BucketName, string Key,
			List<string> VersionIds, List<string> Contents, int index)
		{
			Assert.AreEqual(VersionIds.Count, Contents.Count);
			var rmVersionId = VersionIds[index]; VersionIds.RemoveAt(index);
			var rmContent = Contents[index]; Contents.RemoveAt(index);

			CheckObjContent(Client, BucketName, Key, rmVersionId, rmContent);

			Client.DeleteObject(BucketName, Key, VersionId: rmVersionId);

			if (VersionIds.Count != 0)
				CheckObjVersions(Client, BucketName, Key, VersionIds, Contents);
		}

		public void DeleteSuspendedVersioningObj(S3Client Client, string BucketName, string Key,
			ref List<string> VersionIds, ref List<string> Contents)
		{
			Client.DeleteObject(BucketName, Key);

			Assert.AreEqual(VersionIds.Count, Contents.Count);

			for (int i = VersionIds.Count - 1; i >= 0; i--)
			{
				if (VersionIds[i] == "null")
				{
					VersionIds.RemoveAt(i);
					Contents.RemoveAt(i);
				}
			}
			Thread.Sleep(100);
		}

		public List<Thread> ClearVersionedBucketConcurrent(S3Client Client, string BucketName)
		{
			var TList = new List<Thread>();
			var Response = Client.ListVersions(BucketName);

			foreach (var Version in Response.Versions)
			{
				var mThread = new Thread(() => Client.DeleteObject(BucketName, Version.Key, VersionId: Version.VersionId));
				mThread.Start();
				TList.Add(mThread);
			}
			return TList;
		}
		#endregion

		#region test

		public void TestACL(string BucketName, string Key, S3Client Client, bool Pass)
		{
			if (Pass)
			{
				var Response = Client.GetObject(BucketName, Key);
				Assert.AreEqual(Key, Response.Key);
			}
			else
			{
				var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, Key));
				var StatusCode = e.StatusCode;
				var ErrorCode = e.ErrorCode;

				Assert.AreEqual(HttpStatusCode.Forbidden, StatusCode);
				Assert.AreEqual(MainData.AccessDenied, ErrorCode);
			}
		}

		public AmazonS3Exception TestMetadataUnreadable(string Metadata, string BucketName = null)
		{
			if (string.IsNullOrEmpty(BucketName)) BucketName = GetNewBucket();

			var Client = GetClient();
			var KeyName = "foo";
			var MetadataKey = "x-amz-meta-meta1";

			var MetadataList = new List<KeyValuePair<string, string>>
			{
				new KeyValuePair<string, string>(MetadataKey, Metadata)
			};


			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObject(BucketName, Key: KeyName, Body: "bar", MetadataList: MetadataList));

			if (e.InnerException is AmazonS3Exception e2) return e2;
			return null;
		}

		public void TestEncryptionSSECustomerWrite(int FileSize)
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', FileSize);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};
			Client.PutObject(BucketName, Key: Key, Body: Data, SSEC: SSEC);

			var Response = Client.GetObject(BucketName, Key: Key, SSEC: SSEC);
			var Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
		}
		public void TestEncryptionSSES3ustomerWrite(int FileSize)
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "testobj";
			var Data = new string('A', FileSize);

			var SSE_S3_Method = ServerSideEncryptionMethod.AES256;
			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_S3_Method: SSE_S3_Method);

			var Response = Client.GetObject(BucketName, Key: Key);
			var Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
		}
		public void TestEncryptionSSES3Copy(int FileSize)
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Data = new string('A', FileSize);

			var SSEConfig = new ServerSideEncryptionConfiguration()
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
			};

			Client.PutBucketEncryption(BucketName, SSEConfig);

			var Response = Client.GetBucketEncryption(BucketName);
			CollectionAssert.Equals(Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules, SSEConfig.ServerSideEncryptionRules);

			var SourceKey = "bar";
			Client.PutObject(BucketName, SourceKey, Body: Data);

			var SourceResponse = Client.GetObject(BucketName, SourceKey);
			var SourceBody = GetBody(SourceResponse);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, SourceResponse.ServerSideEncryptionMethod);

			var DestKey = "foo";
			Client.CopyObject(BucketName, SourceKey, BucketName, DestKey);
			var DestResponse = Client.GetObject(BucketName, DestKey);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, DestResponse.ServerSideEncryptionMethod);

			var DestBody = GetBody(DestResponse);
			Assert.AreEqual(SourceBody, DestBody);
		}

		public void TestObjectCopy(bool SourceObjectEncryption, bool SourceBucketEncryption,
								   bool DestBucketEncryption, bool DestObjectEncryption, int FileSize)
		{
			var SourceKey = "SourceKey";
			var DestKey = "DestKey";
			var SourceBucketName = GetNewBucket();
			var DestBucketName = GetNewBucket();
			var Client = GetClient();
			var Data = new string('A', FileSize);

			//Source Put Object
			if (SourceObjectEncryption) Client.PutObject(SourceBucketName, SourceKey, Body: Data, SSE_S3_Method: ServerSideEncryptionMethod.AES256);
			else Client.PutObject(SourceBucketName, SourceKey, Body: Data);

			////Source Object Check
			var SourceResponse = Client.GetObject(SourceBucketName, SourceKey);
			var SourceBody = GetBody(SourceResponse);
			if (SourceObjectEncryption) Assert.AreEqual(ServerSideEncryptionMethod.AES256, SourceResponse.ServerSideEncryptionMethod);
			else Assert.IsNull(SourceResponse.ServerSideEncryptionMethod);
			Assert.AreEqual(Data, SourceBody);

			var SSEConfig = new ServerSideEncryptionConfiguration()
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
			};

			//Source Bucket Encryption
			if (SourceBucketEncryption)
			{
				Client.PutBucketEncryption(SourceBucketName, SSEConfig);

				var EncryptionResponse = Client.GetBucketEncryption(SourceBucketName);
				CollectionAssert.Equals(SSEConfig.ServerSideEncryptionRules, EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
			}

			//Dest Bucket Encryption
			if (DestBucketEncryption)
			{
				Client.PutBucketEncryption(DestBucketName, SSEConfig);

				var EncryptionResponse = Client.GetBucketEncryption(DestBucketName);
				CollectionAssert.Equals(SSEConfig.ServerSideEncryptionRules, EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
			}

			//Source Copy Object
			if (DestObjectEncryption) Client.CopyObject(SourceBucketName, SourceKey, DestBucketName, DestKey, SSE_S3_Method: ServerSideEncryptionMethod.AES256);
			else Client.CopyObject(SourceBucketName, SourceKey, DestBucketName, DestKey);

			//Dest Object Check
			var DestResponse = Client.GetObject(DestBucketName, DestKey);
			var DestBody = GetBody(DestResponse);
			if (DestBucketEncryption || DestObjectEncryption) Assert.AreEqual(ServerSideEncryptionMethod.AES256, DestResponse.ServerSideEncryptionMethod);
			else Assert.IsNull(DestResponse.ServerSideEncryptionMethod);
			Assert.AreEqual(SourceBody, DestBody);
		}


		public void TestEncryptionCSEWrite(int FileSize)
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "testobj";

			//AES
			var AES = new AES256();

			var Data = new string('A', FileSize);
			var EncodingData = AES.AESEncrypt(Data);
			var MetadataList = new List<KeyValuePair<string, string>>()
			{
				new KeyValuePair<string, string>("x-amz-meta-key", AES.Key),
			};
			Client.PutObject(BucketName, Key: Key, Body: EncodingData, MetadataList: MetadataList);

			var Response = Client.GetObject(BucketName, Key: Key);
			var EncodingBody = GetBody(Response);
			var Body = AES.AESDecrypt(EncodingBody);
			Assert.AreEqual(Data, Body);
		}

		public void TestBucketCreateNamingGoodLong(int Length)
		{
			var BucketName = GetPrefix();
			if (BucketName.Length < Length) BucketName += RandomText(Length - BucketName.Length);
			else BucketName = BucketName.Substring(0, Length - 1);
			BucketList.Add(BucketName);

			var Client = GetClient();
			var Response = Client.PutBucket(BucketName);
			Assert.AreEqual(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		public string BucketACLGrantUserid(S3Permission Permission)
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var MainUserId = Config.MainUser.UserId;
			var MainDispalyName = Config.MainUser.DisplayName;

			var AltUserId = Config.AltUser.UserId;
			var AltDispalyName = Config.AltUser.DisplayName;

			var Grant = new S3Grant() { Permission = Permission, Grantee = new S3Grantee() { CanonicalUser = AltUserId } };

			var Grants = AddBucketUserGrant(BucketName, Grant);

			Client.PutBucketACL(BucketName, AccessControlPolicy: Grants);

			var Response = Client.GetBucketACL(BucketName);

			var GetGrants = Response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new S3Grant()
				{
					 Permission = S3Permission.FULL_CONTROL,
					 Grantee = new S3Grantee()
					 {
						 CanonicalUser = MainUserId,
						 DisplayName = MainDispalyName,
						 URI = null,
						 EmailAddress = null,
					 }
				},
				new S3Grant()
				{
					 Permission = Permission,
					 Grantee = new S3Grantee()
					 {
						 CanonicalUser = AltUserId,
						 DisplayName = AltDispalyName,
						 URI = null,
						 EmailAddress = null,
					 }
				},
			},
			GetGrants);

			return BucketName;
		}

		public string TestMultipartUploadContents(string BucketName, string Key, int NumParts)
		{
			var Payload = RandomTextToLong(5 * MainData.MB);
			var Client = GetClient();

			var InitResponse = Client.InitiateMultipartUpload(BucketName, Key);
			var UploadId = InitResponse.UploadId;
			var Parts = new List<PartETag>();
			var AllPayload = "";

			for (int i = 0; i < NumParts; i++)
			{
				var PartNumber = i + 1;
				var PartResponse = Client.UploadPart(BucketName, Key, UploadId, Payload, PartNumber);
				Parts.Add(new PartETag(PartNumber, PartResponse.ETag));
				AllPayload += Payload;
			}
			var LestPayload = RandomTextToLong(MainData.MB);
			var LestPartResponse = Client.UploadPart(BucketName, Key, UploadId, LestPayload, NumParts + 1);
			Parts.Add(new PartETag(NumParts + 1, LestPartResponse.ETag));
			AllPayload += LestPayload;

			Client.CompleteMultipartUpload(BucketName, Key, UploadId, Parts);

			var Response = Client.GetObject(BucketName, Key);
			var Text = GetBody(Response);

			Assert.AreEqual(AllPayload, Text);

			return AllPayload;
		}

		public void TestCreateRemoveVersions(S3Client Client, string BucketName, string Key, int Numversions,
			int RemoveStartIdx, int IdxInc)
		{
			List<string> VersionIds = null;
			List<string> Contents = null;
			SetupMultipleVersions(Client, BucketName, Key, Numversions, ref VersionIds, ref Contents);
			var Idx = RemoveStartIdx;

			for (int i = 0; i < Numversions; i++)
			{
				RemoveObjVersion(Client, BucketName, Key, VersionIds, Contents, Idx);
				Idx += IdxInc;
			}
		}

		#endregion

		#region Bucket Clear
		public void BucketClear()
		{
			var Client = GetClient();
			if (Client == null) return;
			if (BucketList == null) return;

			foreach (var BucketName in BucketList)
			{
				if (string.IsNullOrWhiteSpace(BucketName)) continue;

				try
				{
					var ObjectList = Client.ListObjectsV2(BucketName, MaxKeys: 2000);
					foreach (var Object in ObjectList.S3Objects) Client.DeleteObject(BucketName, Object.Key);

					var Versions = Client.ListVersions(BucketName);
					foreach (var Object in Versions.Versions) Client.DeleteObject(BucketName, Object.Key, VersionId: Object.VersionId);

				}
				catch { }
				try
				{
					Client.DeleteBucket(BucketName);
				}
				catch { }
			}
		}
		#endregion
	}
}