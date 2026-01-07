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
using Amazon.S3.Model;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class SSE_S3 : TestBase
	{
		public SSE_S3(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_1b()
		{
			TestEncryptionSSES3ustomerWrite(1);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_1kb()
		{
			TestEncryptionSSES3ustomerWrite(1024);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_1MB()
		{
			TestEncryptionSSES3ustomerWrite(1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encrypted_transfer_13b()
		{
			TestEncryptionSSES3ustomerWrite(13);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_method_head()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testobj";
			var Data = new string('A', 1000);
			var Metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar"), };

			var sseKey = ServerSideEncryptionMethod.AES256;

			client.PutObject(bucketName, key: key, body: Data, sseKey: sseKey, metadataList: Metadata);

			var Response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
			Assert.Equal(Metadata, GetMetaData(Response.Metadata));
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_multipart_upload()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, Size, metadataList: Metadata, contentType: ContentType, sseKey: ServerSideEncryptionMethod.AES256);

			client.CompleteMultipartUpload(bucketName, key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(Size, BytesUsed);

			var GetResponse = client.GetObject(bucketName, key);
			var ResMetaData = GetMetaData(GetResponse.Metadata);
			Assert.Equal(Metadata, ResMetaData);
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);

			var body = S3Utils.GetBody(GetResponse);
			Assert.Equal(UploadData.Body, body);
			Assert.Equal(Size, GetResponse.ContentLength);

			CheckContentUsingRange(client, bucketName, key, UploadData.Body, 1000000);
			CheckContentUsingRange(client, bucketName, key, UploadData.Body, 10000000);
			CheckContentUsingRandomRange(client, bucketName, key, UploadData.Body, 100);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "버킷의 SSE-S3 설정 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_get_bucket_encryption()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Response = client.GetBucketEncryption(bucketName);

			Assert.Empty(Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
		}

		[Fact]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "버킷의 SSE-S3 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_put_bucket_encryption()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

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

			var Response = client.GetBucketEncryption(bucketName);
			Assert.Single(Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "버킷의 SSE-S3 설정 삭제가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_delete_bucket_encryption()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

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

			var Response = client.GetBucketEncryption(bucketName);
			Assert.Single(Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var DelResponse = client.DeleteBucketEncryption(bucketName);
			Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);

			Response = client.GetBucketEncryption(bucketName);
			Assert.Empty(Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_put_bucket_encryption_and_object_set_check()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

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

			var Response = client.GetBucketEncryption(bucketName);
			Assert.Single(Response.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var KeyNames = new List<string>() { "for/bar", "test" };
			SetupObjects(KeyNames, bucketName: bucketName, body: "");

			var GetResponse = client.GetObject(bucketName, KeyNames[0]);
			Assert.Equal(ServerSideEncryptionMethod.AES256, GetResponse.ServerSideEncryptionMethod);

			GetResponse = client.GetObject(bucketName, KeyNames[1]);
			Assert.Equal(ServerSideEncryptionMethod.AES256, GetResponse.ServerSideEncryptionMethod);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "CopyObject")]
		[Trait(MainData.Explanation, "버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_object_encryption_1kb()
		{
			TestEncryptionSSES3Copy(1024);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "CopyObject")]
		[Trait(MainData.Explanation, "버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_object_encryption_256kb()
		{
			TestEncryptionSSES3Copy(256 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "CopyObject")]
		[Trait(MainData.Explanation, "버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_object_encryption_1mb()
		{
			TestEncryptionSSES3Copy(1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정] 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Data = new string('A', 1024);

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

			var EncryptionResponse = client.GetBucketEncryption(bucketName);
			Assert.Single(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var key = "bar";
			client.PutObject(bucketName, key, body: Data);

			var Response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.Equal(body, body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4] 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_v4()
		{
			var bucketName = GetNewBucket();
			var client = GetClientHttpsV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = client.GetBucketEncryption(bucketName);
			Assert.Single(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var key = "bar";
			client.PutObject(bucketName, key, body: Data);

			var Response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.Equal(body, body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, useChunkEncoding = true] 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_use_chunk_encoding()
		{
			var bucketName = GetNewBucket();
			var client = GetClientV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = client.GetBucketEncryption(bucketName);
			Assert.Single(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var key = "bar";
			client.PutObject(bucketName, key, body: Data, useChunkEncoding: true);

			var Response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.Equal(body, body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, useChunkEncoding = true, disablePayloadSigning = true] 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing()
		{
			var bucketName = GetNewBucket();
			var client = GetClientHttpsV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = client.GetBucketEncryption(bucketName);
			Assert.Single(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var key = "bar";
			client.PutObject(bucketName, key, body: Data, useChunkEncoding: true, disablePayloadSigning: true);

			var Response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.Equal(body, body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, useChunkEncoding = false] 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_not_chunk_encoding()
		{
			var bucketName = GetNewBucket();
			var client = GetClientV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = client.GetBucketEncryption(bucketName);
			Assert.Single(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var key = "bar";
			client.PutObject(bucketName, key, body: Data, useChunkEncoding: false);

			var Response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.Equal(body, body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4, useChunkEncoding = false, disablePayloadSigning = true] 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing()
		{
			var bucketName = GetNewBucket();
			var client = GetClientHttpsV4();
			var Data = new string('A', 1024);

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

			var EncryptionResponse = client.GetBucketEncryption(bucketName);
			Assert.Single(EncryptionResponse.ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			var key = "bar";
			client.PutObject(bucketName, key, body: Data, useChunkEncoding: false, disablePayloadSigning: true);

			var Response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(Response);
			Assert.Equal(ServerSideEncryptionMethod.AES256, Response.ServerSideEncryptionMethod);
			Assert.Equal(body, body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_presignedurl_put_get()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";


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

			var PutURL = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
			var PutResponse = PutObject(PutURL, key);

			Assert.Equal(HttpStatusCode.OK, PutResponse.StatusCode);

			var GetURL = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
			var GetResponse = GetObject(GetURL);

			Assert.Equal(HttpStatusCode.OK, GetResponse.StatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "PresignedURL")]
		[Trait(MainData.Explanation, "[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_bucket_presignedurl_put_get_v4()
		{
			var bucketName = GetNewBucket();
			var client = GetClientV4();
			var key = "foo";

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

			var PutURL = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddSeconds(100000), HttpVerb.PUT);
			var PutResponse = PutObject(PutURL, key);

			Assert.Equal(HttpStatusCode.OK, PutResponse.StatusCode);

			var GetURL = client.GeneratePresignedURL(bucketName, key, DateTime.Now.AddSeconds(100000), HttpVerb.GET);
			var GetResponse = GetObject(GetURL);

			Assert.Equal(HttpStatusCode.OK, GetResponse.StatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_get_object_many()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";
			var Data = S3Utils.RandomTextToLong(15 * 1024 * 1024);

			client.PutObject(bucketName, key, body: Data, sseKey: ServerSideEncryptionMethod.AES256);
			CheckContent(client, bucketName, key, Data, loopCount: 100);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_range_object_many()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";
			var Size = 15 * 1024 * 1024;
			var Data = S3Utils.RandomTextToLong(Size);

			client.PutObject(bucketName, key, body: Data, sseKey: ServerSideEncryptionMethod.AES256);
			CheckContentUsingRandomRange(client, bucketName, key, Data, 100);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_multipart_copypart_upload()
		{
			var bucketName = GetNewBucket();
			var client = GetClientHttps();
			var SrcKey = "multipart_enc";
			var Size = 50 * MainData.MB;
			var Metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar"), };
			var ContentType = "text/plain";

			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, SrcKey, Size, metadataList: Metadata, contentType: ContentType, sseKey: ServerSideEncryptionMethod.AES256);

			client.CompleteMultipartUpload(bucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = client.ListObjectsV2(bucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			var BytesUsed = GetBytesUsed(HeadResponse);
			Assert.Equal(Size, BytesUsed);

			var GetResponse = client.GetObject(bucketName, SrcKey);
			Assert.Equal(Metadata, GetMetaData(GetResponse.Metadata));
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);

			var body = S3Utils.GetBody(GetResponse);
			Assert.Equal(UploadData.Body, body);
			Assert.Equal(Size, GetResponse.ContentLength);

			// 멀티파트 복사
			var DestKey = "multipart_enc_copy";
			UploadData = SetupMultipartCopy(client, bucketName, SrcKey, bucketName, DestKey, Size);
			client.CompleteMultipartUpload(bucketName, DestKey, UploadData.UploadId, UploadData.Parts);
			CheckCopyContent(client, bucketName, SrcKey, bucketName, DestKey);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "SSE-S3 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_sse_s3_encryption_multipart_copy_many()
		{
			var bucketName = GetNewBucket();
			var SrcKey = "mymultipart_enc";
			var Size = 10 * MainData.MB;
			var client = GetClient();
			var body = "";
			// 멀티파트 업로드
			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, SrcKey, Size, sseKey: ServerSideEncryptionMethod.AES256);
			client.CompleteMultipartUpload(bucketName, SrcKey, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, SrcKey, body);

			// 멀티파트 카피
			var DestKey1 = "mymultipart1_enc";
			UploadData = SetupMultipartCopy(client, bucketName, SrcKey, bucketName, DestKey1, Size, SSE_S3: ServerSideEncryptionMethod.AES256);
			// 추가파츠 업로드
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, DestKey1, Size, uploadData: UploadData);
			client.CompleteMultipartUpload(bucketName, DestKey1, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, DestKey1, body);

			// 멀티파트 카피
			var DestKey2 = "mymultipart2_enc";
			UploadData = SetupMultipartCopy(client, bucketName, DestKey1, bucketName, DestKey2, Size * 2, SSE_S3: ServerSideEncryptionMethod.AES256);
			// 추가파츠 업로드
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, DestKey2, Size, uploadData: UploadData);
			client.CompleteMultipartUpload(bucketName, DestKey2, UploadData.UploadId, UploadData.Parts);

			// 업로드가 올바르게 되었는지 확인
			body += UploadData.Body;
			CheckContent(client, bucketName, DestKey2, body);
		}

		[Fact]
		[Trait(MainData.Major, "SSE-S3")]
		[Trait(MainData.Minor, "Retroactive")]
		[Trait(MainData.Explanation, "sse-s3설정은 소급적용 되지 않음을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		//
		public void test_sse_s3_not_retroactive()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var PutKey = "PutKey";
			var CopyKey = "CopyKey";
			var MultiKey = "MultiKey";
			var PutData = S3Utils.RandomTextToLong(1000);

			var SSEConfig = new ServerSideEncryptionConfiguration() { ServerSideEncryptionRules = [new() { ServerSideEncryptionByDefault = new ServerSideEncryptionByDefault() { ServerSideEncryptionAlgorithm = new ServerSideEncryptionMethod(ServerSideEncryptionMethod.AES256) } }] };

			client.PutObject(bucketName, PutKey, PutData);
			client.CopyObject(bucketName, PutKey, bucketName, CopyKey);
			var UploadData = S3Utils.SetupMultipartUpload(client, bucketName, MultiKey, 1000, sseKey: ServerSideEncryptionMethod.AES256);

			// SSE-S3 설정
			client.PutBucketEncryption(bucketName, SSEConfig);
			Assert.Single(client.GetBucketEncryption(bucketName).ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			// 오브젝트 다운로드 확인
			var Response = client.GetObject(bucketName, PutKey);
			Assert.Equal(PutData, S3Utils.GetBody(Response));
			Response = client.GetObject(bucketName, CopyKey);
			Assert.Equal(PutData, S3Utils.GetBody(Response));
			Response = client.GetObject(bucketName, MultiKey);
			Assert.Equal(UploadData.Body, S3Utils.GetBody(Response));

			// 오브젝트 업로드
			var PutKey2 = "key2";
			var PutData2 = S3Utils.RandomTextToLong(1000);
			var CopyKey2 = "CopyKey2";
			var MultiKey2 = "MultiKey2";

			client.PutObject(bucketName, PutKey2, PutData2);
			client.CopyObject(bucketName, PutKey2, bucketName, CopyKey2);
			UploadData = S3Utils.SetupMultipartUpload(client, bucketName, MultiKey2, 1000, sseKey: ServerSideEncryptionMethod.AES256);

			// SSE-S3 설정 해제
			Assert.Equal(HttpStatusCode.NoContent, client.DeleteBucketEncryption(bucketName).HttpStatusCode);
			Assert.Empty(client.GetBucketEncryption(bucketName).ServerSideEncryptionConfiguration.ServerSideEncryptionRules);

			// 오브젝트 다운로드 확인
			Response = client.GetObject(bucketName, PutKey2);
			Assert.Equal(PutData2, S3Utils.GetBody(Response));
			Response = client.GetObject(bucketName, CopyKey2);
			Assert.Equal(PutData2, S3Utils.GetBody(Response));
			Response = client.GetObject(bucketName, MultiKey2);
			Assert.Equal(UploadData.Body, S3Utils.GetBody(Response));

		}
	}
}
