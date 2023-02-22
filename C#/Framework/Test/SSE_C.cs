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
using System.Net;
using System.Text;
using Amazon.S3;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace s3tests2
{
	[TestClass]
	public class SSE_C : TestBase
	{
		[TestMethod("test_encrypted_transfer_1b")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encrypted_transfer_1b()
		{
			TestEncryptionSSE_CWrite(1);
		}

		[TestMethod("test_encrypted_transfer_1kb")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encrypted_transfer_1kb()
		{
			TestEncryptionSSE_CWrite(1024);
		}

		[TestMethod("test_encrypted_transfer_1MB")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encrypted_transfer_1MB()
		{
			TestEncryptionSSE_CWrite(1024 * 1024);
		}

		[TestMethod("test_encrypted_transfer_13b")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encrypted_transfer_13b()
		{
			TestEncryptionSSE_CWrite(13);
		}

		[TestMethod("test_encryption_sse_c_method_head")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_method_head()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', 1000);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC);

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObjectMetadata(BucketName, Key));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);

			var Response = Client.GetObjectMetadata(BucketName, Key, SSE_C: SSEC);
			Assert.AreEqual(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		[TestMethod("test_encryption_sse_c_present")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_present()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', 1000);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC);

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, Key));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
		}

		[TestMethod("test_encryption_sse_c_other_key")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_other_key()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', 100);

			var SSEC_A = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};
			var SSEC_B = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=",
				MD5 = "arxBvwY2V4SiOne6yppVPQ==",
			};

			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC_A);

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, Key, SSE_C: SSEC_B));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.Forbidden, StatusCode);
		}

		[TestMethod("test_encryption_sse_c_invalid_md5")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_invalid_md5()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', 100);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "AAAAAAAAAAAAAAAAAAAAAA==",
			};

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC));

			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
		}


		[TestMethod("test_encryption_sse_c_no_md5")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_no_md5()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', 100);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
			};

			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC);
			var Response = Client.GetObject(BucketName, Key: Key, SSE_C: SSEC);
			var Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
		}

		[TestMethod("test_encryption_sse_c_no_key")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_no_key()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', 100);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
			};

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
		}

		[TestMethod("test_encryption_key_no_sse_c")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_key_no_sse_c()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "testobj";
			var Data = new string('A', 100);
			var SSEC = new SSECustomerKey()
			{
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
		}

		[TestMethod("test_encryption_sse_c_multipart_upload")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Multipart")]
		[TestProperty(MainData.Explanation, "멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_multipart_upload()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: Metadata, ContentType: ContentType, SSE_C: SSEC);

			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.AreEqual(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.AreEqual(Size, BytesUsed);

			var GetResponse = Client.GetObject(BucketName, Key, SSE_C: SSEC);
			CollectionAssert.AreEqual(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.AreEqual(ContentType, GetResponse.Headers["content-type"]);

			var Body = GetBody(GetResponse);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Size, GetResponse.ContentLength);

			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 1000000, SSE_C: SSEC);
			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 10000000, SSE_C: SSEC);
			CheckContentUsingRandomRange(Client, BucketName, Key, UploadData.Body, 100, SSE_C: SSEC);
		}

		[TestMethod("test_encryption_sse_c_multipart_bad_download")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Multipart")]
		[TestProperty(MainData.Explanation, "SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_multipart_bad_download()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";
			var PutSSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};
			var GetSSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=",
				MD5 = "arxBvwY2V4SiOne6yppVPQ==",
			};

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: Metadata, ContentType: ContentType, SSE_C: PutSSEC);

			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.AreEqual(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.AreEqual(Size, BytesUsed);

			var GetResponse = Client.GetObject(BucketName, Key, SSE_C: PutSSEC);
			CollectionAssert.AreEqual(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.AreEqual(ContentType, GetResponse.Headers["content-type"]);

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.GetObject(BucketName, Key, SSE_C: GetSSEC));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.Forbidden, StatusCode);

		}

		[TestMethod("test_encryption_sse_c_post_object_authenticated_request")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Post")]
		[TestProperty(MainData.Explanation, "Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_post_object_authenticated_request()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();

			var ContentType = "text/plain";
			var Key = "foo.txt";

			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var PolicyDocument = new JObject()
			{
				{"expiration", DateTime.UtcNow.AddMinutes(100).ToString("yyyy-MM-ddTHH:mm:ssZ") },
				{ "conditions", new JArray()
					{
						{ new JObject() { { "bucket", BucketName } } },
						{ new JArray() { "starts-with", "$key", "foo" } },
						{ new JObject() { { "acl", "private" } } },
						{ new JArray() { "starts-with", "$Content-Type", ContentType } },
						{ new JArray() { "starts-with", "$x-amz-server-side-encryption-customer-algorithm", "" } },
						{ new JArray() { "starts-with", "$x-amz-server-side-encryption-customer-key", "" } },
						{ new JArray() { "starts-with", "$x-amz-server-side-encryption-customer-key-md5", "" } },
						{ new JArray() { "content-length-range", 0, 1024 } },
					}
				},
			};

			var BytesJsonPolicyDocument = Encoding.UTF8.GetBytes(PolicyDocument.ToString());
			var Policy = Convert.ToBase64String(BytesJsonPolicyDocument);

			var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = Key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", Key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "x-amz-server-side-encryption-customer-algorithm", "AES256" },
					{ "x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=" },
					{ "x-amz-server-side-encryption-customer-key-md5", "DWygnHRtgiJ77HCm+1rvHw==" },
					{ "file", FileData },
			};

			var Result = PostUpload(BucketName, Payload);
			Assert.AreEqual(HttpStatusCode.NoContent, Result.StatusCode);

			var Response = Client.GetObject(BucketName, Key, SSE_C: SSEC);
			var Body = GetBody(Response);
			Assert.AreEqual("bar", Body);
		}

		[TestMethod("test_encryption_sse_c_get_object_many")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Get")]
		[TestProperty(MainData.Explanation, "SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_get_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "foo";
			var Data = RandomTextToLong(15 * 1024 * 1024);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC);
			CheckContent(Client, BucketName, Key, Data, LoopCount: 100, SSE_C: SSEC);
		}

		[TestMethod("test_encryption_sse_c_range_object_many")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Get")]
		[TestProperty(MainData.Explanation, "SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_range_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var Key = "foo";
			var Size = 15 * 1024 * 1024;
			var Data = RandomTextToLong(Size);
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_C: SSEC);
			CheckContentUsingRandomRange(Client, BucketName, Key, Data, 100, SSE_C: SSEC);
		}

		[TestMethod("test_sse_c_encryption_multipart_copypart_upload")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Copy")]
		[TestProperty(MainData.Explanation, "SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_c_encryption_multipart_copypart_upload()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var SrcKey = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var UploadData = SetupMultipartUpload(Client, BucketName, SrcKey, Size, MetadataList: Metadata, ContentType: ContentType, SSE_C: SSEC);

			Client.CompleteMultipartUpload(BucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.AreEqual(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.AreEqual(Size, BytesUsed);

			var GetResponse = Client.GetObject(BucketName, SrcKey, SSE_C: SSEC);
			Assert.AreEqual(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.AreEqual(ContentType, GetResponse.Headers["content-type"]);

			var Body = GetBody(GetResponse);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Size, GetResponse.ContentLength);

			// 멀티파트 복사
			var DestKey = "multipart_enc_copy";
			UploadData = SetupMultipartCopy(Client, BucketName, SrcKey, BucketName, DestKey, Size, SrcSSE_C: SSEC, DestSSE_C: SSEC);
			Client.CompleteMultipartUpload(BucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContent(Client, BucketName, SrcKey, BucketName, DestKey, SrcSSE_C: SSEC, DestSSE_C: SSEC);
		}

		[TestMethod("test_sse_c_encryption_multipart_copy_many")]
		[TestProperty(MainData.Major, "SSE-C")]
		[TestProperty(MainData.Minor, "Multipart")]
		[TestProperty(MainData.Explanation, "SSE-C 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_c_encryption_multipart_copy_many()
		{
			var BucketName = GetNewBucket();
			var SrcKey = "mymultipart_enc";
			var Size = 10 * MainData.MB;
			var Client = GetClientHttps();
			var Body = "";
			var SSEC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			// 멀티파트 업로드
			var UploadData = SetupMultipartUpload(Client, BucketName, SrcKey, Size, SSE_C: SSEC);
			Client.CompleteMultipartUpload(BucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, SrcKey, Body, SSE_C: SSEC);

			// 멀티파트 카피
			var DestKey1 = "mymultipart1_enc";
			UploadData = SetupMultipartCopy(Client, BucketName, SrcKey, BucketName, DestKey1, Size, SrcSSE_C: SSEC, DestSSE_C: SSEC);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(Client, BucketName, DestKey1, Size, UploadData: UploadData, SSE_C: SSEC);
			Client.CompleteMultipartUpload(BucketName, DestKey1, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, DestKey1, Body, SSE_C: SSEC);

			// 멀티파트 카피
			var DestKey2 = "mymultipart2_enc";
			UploadData = SetupMultipartCopy(Client, BucketName, DestKey1, BucketName, DestKey2, Size * 2, SrcSSE_C: SSEC, DestSSE_C: SSEC);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(Client, BucketName, DestKey2, Size, UploadData: UploadData, SSE_C: SSEC);
			Client.CompleteMultipartUpload(BucketName, DestKey2, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, DestKey2, Body, SSE_C: SSEC);
		}
	}
}