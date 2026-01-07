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
using Amazon.S3;
using Newtonsoft.Json.Linq;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Xunit;

namespace s3tests.Test
{
	public class sseC : TestBase
	{
		public sseC(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestEncryptedTransfer1b()
		{
			TestEncryptionSSECustomerWrite(1);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestEncryptedTransfer1kb()
		{
			TestEncryptionSSECustomerWrite(1024);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestEncryptedTransfer1MB()
		{
			TestEncryptionSSECustomerWrite(1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_encrypted_transfer_13b()
		{
			TestEncryptionSSECustomerWrite(13);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_method_head()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "testobj";
			var body = new string('A', 1000);

			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC);

			var e = Assert.Throws<AggregateException>(() => Client.GetObjectMetadata(bucketName, key));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));

			var Response = Client.GetObjectMetadata(bucketName, key, sseC: sseC);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_present()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "testobj";
			var body = new string('A', 1000);

			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC);

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_other_key()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "testobj";
			var body = new string('A', 100);

			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var SSEC2 = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=",
				MD5 = "arxBvwY2V4SiOne6yppVPQ==",
			};

			Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC);

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(bucketName, key, sseCustomerKey: SSEC2));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_invalid_md5()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "testobj";
			var body = new string('A', 100);

			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "AAAAAAAAAAAAAAAAAAAAAA==",
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}


		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_no_md5()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "testobj";
			var body = new string('A', 100);

			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
			};

			Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC);
			var Response = Client.GetObject(bucketName, key: key, sseCustomerKey: sseC);
			Assert.Equal(body, S3Utils.GetBody(Response));
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_no_key()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "testobj";
			var body = new string('A', 100);


			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_key_no_sse_c()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "testobj";
			var body = new string('A', 100);
			var sseC = new SSECustomerKey()
			{
				Method = null,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_multipart_upload()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";
			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var UploadData = S3Utils.SetupMultipartUpload(Client, bucketName, key, Size, metadataList: Metadata, contentType: ContentType, sseCustomerKey: sseC);

			Client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(Size, BytesUsed);

			var GetResponse = Client.GetObject(bucketName, key, sseCustomerKey: sseC);
			Assert.Equal(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);

			var body = S3Utils.GetBody(GetResponse);
			Assert.Equal(UploadData.Body, body);
			Assert.Equal(Size, GetResponse.ContentLength);

			CheckContentUsingRange(Client, bucketName, key, UploadData.Body, 1000000, sseC: sseC);
			CheckContentUsingRange(Client, bucketName, key, UploadData.Body, 10000000, sseC: sseC);
			CheckContentUsingRandomRange(Client, bucketName, key, UploadData.Body, 100, sseC: sseC);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_multipart_bad_download()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar"), };
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

			var UploadData = S3Utils.SetupMultipartUpload(Client, bucketName, key, Size, metadataList: Metadata, contentType: ContentType, sseCustomerKey: PutSSEC);

			Client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(Size, BytesUsed);

			var GetResponse = Client.GetObject(bucketName, key, sseCustomerKey: PutSSEC);
			Assert.Equal(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(bucketName, key, sseCustomerKey: GetSSEC));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_encryption_sse_c_post_object_authenticated_request()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();

			var ContentType = "text/plain";
			var key = "foo.txt";

			var sseC = new SSECustomerKey()
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
						{ new JObject() { { "bucket", bucketName } } },
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

			var Signature = S3Utils.GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
			var FileData = new FormFile() { Name = key, ContentType = ContentType, Body = "bar" };
			var Payload = new Dictionary<string, object>() {
					{ "key", key },
					{ "AWSAccessKeyId", Config.MainUser.AccessKey },
					{ "acl", "private" },
					{ "signature", Signature },
					{ "policy", Policy },
					{ "Content-Type", ContentType },
					{ "x-amz-server-side-encryption-customer-algorithm", "AES256" },
					{ "x-amz-server-side-encryption-customer-key", sseC.ProvidedKey },
					{ "x-amz-server-side-encryption-customer-key-md5", sseC.MD5 },
					{ "file", FileData },
			};

			var Result = PostUpload(bucketName, Payload);
			Assert.Equal(HttpStatusCode.NoContent, Result.StatusCode);

			var Response = Client.GetObject(bucketName, key, sseCustomerKey: sseC);
			var body = S3Utils.GetBody(Response);
			Assert.Equal("bar", body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_get_object_many()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "foo";
			var body = RandomTextToLong(15 * 1024 * 1024);
			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC);
			CheckContent(Client, bucketName, key, body, loopCount: 100, sseC: sseC);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_encryption_sse_c_range_object_many()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var key = "foo";
			var Size = 15 * 1024 * 1024;
			var body = RandomTextToLong(Size);
			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			Client.PutObject(bucketName, key: key, body: body, sseCustomerKey: sseC);
			CheckContentUsingRandomRange(Client, bucketName, key, body, 100, sseC: sseC);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_c_encryption_multipart_copypart_upload()
		{
			var bucketName = GetNewBucket();
			var Client = GetClientHttps();
			var SrcKey = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";
			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			var UploadData = S3Utils.SetupMultipartUpload(Client, bucketName, SrcKey, Size, metadataList: Metadata, contentType: ContentType, sseCustomerKey: sseC);

			Client.CompleteMultipartUpload(bucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(Size, BytesUsed);

			var GetResponse = Client.GetObject(bucketName, SrcKey, sseCustomerKey: sseC);
			Assert.Equal(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);

			var body = S3Utils.GetBody(GetResponse);
			Assert.Equal(UploadData.Body, body);
			Assert.Equal(Size, GetResponse.ContentLength);

			// 멀티파트 복사
			var DestKey = "multipart_enc_copy";
			UploadData = SetupMultipartCopy(Client, bucketName, SrcKey, bucketName, DestKey, Size, srcCustomerKey: sseC, destCustomerKey: sseC);
			Client.CompleteMultipartUpload(bucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContent(Client, bucketName, SrcKey, bucketName, DestKey, srcCustomerKey: sseC, destCustomerKey: sseC);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-C")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "SSE-C 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestSseCEncryptionMultipartCopyMany()
		{
			var bucketName = GetNewBucket();
			var SrcKey = "mymultipart_enc";
			var Size = 10 * MainData.MB;
			var Client = GetClientHttps();
			var body = "";
			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			// 멀티파트 업로드
			var UploadData = S3Utils.SetupMultipartUpload(Client, bucketName, SrcKey, Size, sseCustomerKey: sseC);
			Client.CompleteMultipartUpload(bucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(Client, bucketName, SrcKey, body, sseC: sseC);

			// 멀티파트 카피
			var DestKey1 = "mymultipart1_enc";
			UploadData = SetupMultipartCopy(Client, bucketName, SrcKey, bucketName, DestKey1, Size, srcCustomerKey: sseC, destCustomerKey: sseC);
			// 추가파츠 업로드
			UploadData = S3Utils.SetupMultipartUpload(Client, bucketName, DestKey1, Size, uploadData: UploadData, sseCustomerKey: sseC);
			Client.CompleteMultipartUpload(bucketName, DestKey1, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(Client, bucketName, DestKey1, body, sseC: sseC);

			// 멀티파트 카피
			var DestKey2 = "mymultipart2_enc";
			UploadData = SetupMultipartCopy(Client, bucketName, DestKey1, bucketName, DestKey2, Size * 2, srcCustomerKey: sseC, destCustomerKey: sseC);
			// 추가파츠 업로드
			UploadData = S3Utils.SetupMultipartUpload(Client, bucketName, DestKey2, Size, uploadData: UploadData, sseCustomerKey: sseC);
			Client.CompleteMultipartUpload(bucketName, DestKey2, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(Client, bucketName, DestKey2, body, sseC: sseC);
		}
	}
}
