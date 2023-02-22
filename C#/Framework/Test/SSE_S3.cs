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
using System.Collections.Generic;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Amazon.S3.Model;
using System;

namespace s3tests2
{
	[TestClass]
	public class SSE_S3 : TestBase
	{
		[TestMethod("test_sse_s3_encrypted_transfer_1b")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_1b()
		{
			TestEncryptionSSEWrite(1);
		}

		[TestMethod("test_sse_s3_encrypted_transfer_1kb")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_1kb()
		{
			TestEncryptionSSEWrite(1024);
		}

		[TestMethod("test_sse_s3_encrypted_transfer_1MB")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_1MB()
		{
			TestEncryptionSSEWrite(1024 * 1024);
		}

		[TestMethod("test_sse_s3_encrypted_transfer_13b")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_13b()
		{
			TestEncryptionSSEWrite(13);
		}

		[TestMethod("test_sse_s3_encryption_method_head")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_method_head()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "testobj";
			var Data = new string('A', 1000);
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar"), };

			var SSE_S3_Method = ServerSideEncryptionMethod.AES256;

			Client.PutObject(BucketName, Key: Key, Body: Data, SSE_S3_Method: SSE_S3_Method, MetadataList: Metadata);

			var Response = Client.GetObjectMetadata(BucketName, Key);
			Assert.AreEqual(HttpStatusCode.OK, Response.HttpStatusCode);
			CollectionAssert.AreEqual(Metadata, GetMetaData(Response.Metadata));
		}

		[TestMethod("test_sse_s3_encryption_multipart_upload")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Multipart")]
		[TestProperty(MainData.Explanation, "멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_multipart_upload()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar"), };
			var SSE_S3_Method = ServerSideEncryptionMethod.AES256;
			var ContentType = "text/plain";

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: Metadata,
				ContentType: ContentType);

			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.AreEqual(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.AreEqual(Size, BytesUsed);

			var GetResponse = Client.GetObject(BucketName, Key);
			var ResMetaData = GetMetaData(GetResponse.Metadata);
			CollectionAssert.AreEqual(Metadata, ResMetaData);
			Assert.AreEqual(ContentType, GetResponse.Headers["content-type"]);

			var Body = GetBody(GetResponse);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Size, GetResponse.ContentLength);

			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 1000000);
			CheckContentUsingRange(Client, BucketName, Key, UploadData.Body, 10000000);
			CheckContentUsingRandomRange(Client, BucketName, Key, UploadData.Body, 100);
		}


		[TestMethod("test_get_bucket_encryption")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "버킷의 SSE-S3 설정 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_get_bucket_encryption()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Response = Client.GetBucketEncryption(BucketName);

			Assert.AreEqual(0, Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count);
		}

		[TestMethod("test_put_bucket_encryption")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "버킷의 SSE-S3 설정이 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_bucket_encryption()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

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
		}

		[TestMethod("test_delete_bucket_encryption")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "버킷의 SSE-S3 설정 삭제가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_delete_bucket_encryption()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

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
			Assert.AreEqual(1, Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count);

			var DelResponse = Client.DeleteBucketEncryption(BucketName);
			Assert.AreEqual(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);

			Response = Client.GetBucketEncryption(BucketName);
			Assert.AreEqual(0, Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count);
		}

		[TestMethod("test_put_bucket_encryption_and_object_set_check")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_bucket_encryption_and_object_set_check()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

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

			var KeyNames = new List<string>() { "for/bar", "test/" };
			SetupObjects(KeyNames, BucketName: BucketName, Body: "");

			var GetResponse = Client.GetObject(BucketName, KeyNames[0]);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, GetResponse.ServerSideEncryptionMethod);

			GetResponse = Client.GetObject(BucketName, KeyNames[1]);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, GetResponse.ServerSideEncryptionMethod);
		}

		[TestMethod("test_copy_object_encryption_1kb")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "CopyObject")]
		[TestProperty(MainData.Explanation, "버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_object_encryption_1kb()
		{
			TestEncryptionSSECopy(1024);
		}

		[TestMethod("test_copy_object_encryption_256kb")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "CopyObject")]
		[TestProperty(MainData.Explanation, "버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_object_encryption_256kb()
		{
			TestEncryptionSSECopy(256 * 1024);
		}

		[TestMethod("test_copy_object_encryption_1mb")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "CopyObject")]
		[TestProperty(MainData.Explanation, "버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_object_encryption_1mb()
		{
			TestEncryptionSSECopy(1024 * 1024);
		}


		[TestMethod("test_sse_s3_bucket_put_get")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정] 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = Client.GetBucketEncryption(BucketName);
			CollectionAssert.Equals(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules, SSEConfig.ServerSideEncryptionRules);

			var Key = "bar";
			Client.PutObject(BucketName, Key, Body: Data);

			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.AreEqual(Body, Body);
		}

		[TestMethod("test_sse_s3_bucket_put_get_v4")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4] 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_v4()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttpsV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = Client.GetBucketEncryption(BucketName);
			CollectionAssert.Equals(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules, SSEConfig.ServerSideEncryptionRules);

			var Key = "bar";
			Client.PutObject(BucketName, Key, Body: Data);

			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.AreEqual(Body, Body);
		}

		[TestMethod("test_sse_s3_bucket_put_get_use_chunk_encoding")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_use_chunk_encoding()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = Client.GetBucketEncryption(BucketName);
			Assert.AreEqual(1, EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count);

			var Key = "bar";
			Client.PutObject(BucketName, Key, Body: Data, UseChunkEncoding: true);

			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.AreEqual(Body, Body);
		}

		[TestMethod("test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttpsV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = Client.GetBucketEncryption(BucketName);
			Assert.AreEqual(1, EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count);

			var Key = "bar";
			Client.PutObject(BucketName, Key, Body: Data, UseChunkEncoding: true, DisablePayloadSigning: true);

			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.AreEqual(Body, Body);
		}

		[TestMethod("test_sse_s3_bucket_put_get_not_chunk_encoding")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_not_chunk_encoding()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = Client.GetBucketEncryption(BucketName);
			Assert.AreEqual(1, EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count);

			var Key = "bar";
			Client.PutObject(BucketName, Key, Body: Data, UseChunkEncoding: false);

			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.AreEqual(Body, Body);
		}

		[TestMethod("test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Put / Get")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttpsV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = Client.GetBucketEncryption(BucketName);
			Assert.AreEqual(1, EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules.Count);

			var Key = "bar";
			Client.PutObject(BucketName, Key, Body: Data, UseChunkEncoding: false, DisablePayloadSigning: true);

			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.AreEqual(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.AreEqual(Body, Body);
		}

		[TestMethod("test_sse_s3_bucket_presignedurl_put_get")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "PresignedURL")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_presignedurl_put_get()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";


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

			var PutURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
			var PutResponse = PutObject(PutURL, Key);

			Assert.AreEqual(HttpStatusCode.OK, PutResponse.StatusCode);
			PutResponse.Close();

			var GetURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
			var GetResponse = GetObject(GetURL);

			Assert.AreEqual(HttpStatusCode.OK, GetResponse.StatusCode);
			GetResponse.Close();
		}

		[TestMethod("test_sse_s3_bucket_presignedurl_put_get_v4")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "PresignedURL")]
		[TestProperty(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_presignedurl_put_get_v4()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientV4();
			var Key = "foo";

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

			var PutURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
			var PutResponse = PutObject(PutURL, Key);

			Assert.AreEqual(HttpStatusCode.OK, PutResponse.StatusCode);
			PutResponse.Close();

			var GetURL = Client.GeneratePresignedURL(BucketName, Key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
			var GetResponse = GetObject(GetURL);

			Assert.AreEqual(HttpStatusCode.OK, GetResponse.StatusCode);
			GetResponse.Close();
		}

		[TestMethod("test_sse_s3_get_object_many")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Get")]
		[TestProperty(MainData.Explanation, "SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_get_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";
			var Data = RandomTextToLong(15 * 1024 * 1024);

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

			Client.PutObject(BucketName, Key, Body: Data, SSE_S3_Method: ServerSideEncryptionMethod.AES256);
			CheckContent(Client, BucketName, Key, Data, 100);
		}

		[TestMethod("test_sse_s3_range_object_many")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Get")]
		[TestProperty(MainData.Explanation, "SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_range_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";
			var Size = 15 * 1024 * 1024;
			var Data = RandomTextToLong(Size);

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

			Client.PutObject(BucketName, Key, Body: Data, SSE_S3_Method: ServerSideEncryptionMethod.AES256);
			CheckContentUsingRandomRange(Client, BucketName, Key, Data, 100);
		}

		[TestMethod("test_sse_s3_encryption_multipart_copypart_upload")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Copy")]
		[TestProperty(MainData.Explanation, "SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_multipart_copypart_upload()
		{
			var BucketName = GetNewBucket();
			var Client = GetClientHttps();
			var SrcKey = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";

			var UploadData = SetupMultipartUpload(Client, BucketName, SrcKey, Size, MetadataList: Metadata, ContentType: ContentType, SSE_S3: ServerSideEncryptionMethod.AES256);

			Client.CompleteMultipartUpload(BucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.AreEqual(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.AreEqual(Size, BytesUsed);

			var GetResponse = Client.GetObject(BucketName, SrcKey);
			Assert.AreEqual(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.AreEqual(ContentType, GetResponse.Headers["content-type"]);

			var Body = GetBody(GetResponse);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Size, GetResponse.ContentLength);

			// 멀티파트 복사
			var DestKey = "multipart_enc_copy";
			UploadData = SetupMultipartCopy(Client, BucketName, SrcKey, BucketName, DestKey, Size);
			Client.CompleteMultipartUpload(BucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContent(Client, BucketName, SrcKey, BucketName, DestKey);
		}

		[TestMethod("test_sse_s3_encryption_multipart_copy_many")]
		[TestProperty(MainData.Major, "SSE-S3")]
		[TestProperty(MainData.Minor, "Multipart")]
		[TestProperty(MainData.Explanation, "SSE-S3 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_multipart_copy_many()
		{
			var BucketName = GetNewBucket();
			var SrcKey = "mymultipart_enc";
			var Size = 10 * MainData.MB;
			var Client = GetClient();
			var Body = "";
			// 멀티파트 업로드
			var UploadData = SetupMultipartUpload(Client, BucketName, SrcKey, Size, SSE_S3: ServerSideEncryptionMethod.AES256);
			Client.CompleteMultipartUpload(BucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, SrcKey, Body);

			// 멀티파트 카피
			var DestKey1 = "mymultipart1_enc";
			UploadData = SetupMultipartCopy(Client, BucketName, SrcKey, BucketName, DestKey1, Size, SSE_S3: ServerSideEncryptionMethod.AES256);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(Client, BucketName, DestKey1, Size, UploadData: UploadData);
			Client.CompleteMultipartUpload(BucketName, DestKey1, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, DestKey1, Body);

			// 멀티파트 카피
			var DestKey2 = "mymultipart2_enc";
			UploadData = SetupMultipartCopy(Client, BucketName, DestKey1, BucketName, DestKey2, Size * 2, SSE_S3: ServerSideEncryptionMethod.AES256);
			// 추가파츠 업로드
			UploadData = SetupMultipartUpload(Client, BucketName, DestKey2, Size, UploadData: UploadData);
			Client.CompleteMultipartUpload(BucketName, DestKey2, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			Body += UploadData.Body;
			CheckContent(Client, BucketName, DestKey2, Body);
		}
	}
}
