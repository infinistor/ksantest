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
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Xunit;

namespace s3tests
{
	public class CSE : TestBase
	{
		public CSE(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_cse_encrypted_transfer_1b")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_encrypted_transfer_1b()
		{
			TestEncryptionCSEWrite(1);
		}

		[Fact(DisplayName = "test_cse_encrypted_transfer_1kb")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_encrypted_transfer_1kb()
		{
			TestEncryptionCSEWrite(1024);
		}

		[Fact(DisplayName = "test_cse_encrypted_transfer_1MB")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_encrypted_transfer_1MB()
		{
			TestEncryptionCSEWrite(1024 * 1024);
		}

		[Fact(DisplayName = "test_cse_encrypted_transfer_13b")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_encrypted_transfer_13b()
		{
			TestEncryptionCSEWrite(13);
		}

		[Fact(DisplayName = "test_cse_encryption_method_head")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "[AES256] 암호화하고 메타데이터에 공개키를 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_encryption_method_head()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "testobj";

			//AES
			var AES = new AES256();
			var Data = new string('A', 1000);
			var EncodingData = AES.AESEncrypt(Data);
			var MetadataList = new List<KeyValuePair<string, string>>()
			{
				new KeyValuePair<string, string>("x-amz-meta-key", AES.Key),
			};
			Client.PutObject(BucketName, Key: Key, Body: EncodingData, MetadataList: MetadataList);

			var Response = Client.GetObjectMetadata(BucketName, Key: Key);
			var ResMetaData = GetMetaData(Response.Metadata);
			Assert.Equal(MetadataList, ResMetaData);
		}

		[Fact(DisplayName = "test_cse_encryption_non_decryption")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_cse_encryption_non_decryption()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "testobj";

			//AES
			var AES = new AES256();

			var Data = new string('A', 1000);
			var EncodingData = AES.AESEncrypt(Data);
			var MetadataList = new List<KeyValuePair<string, string>>()
			{
				new KeyValuePair<string, string>("x-amz-meta-key", AES.Key),
			};
			Client.PutObject(BucketName, Key: Key, Body: EncodingData, MetadataList: MetadataList);

			var Response = Client.GetObject(BucketName, Key: Key);
			var Body = GetBody(Response);
			Assert.NotEqual(Data, Body);
		}


		[Fact(DisplayName = "test_cse_non_encryption_decryption")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_cse_non_encryption_decryption()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "testobj";

			//AES
			var AES = new AES256();

			var Data = new string('A', 1000);
			var EncodingData = AES.AESEncrypt(Data);
			var MetadataList = new List<KeyValuePair<string, string>>()
			{
				new KeyValuePair<string, string>("x-amz-meta-key", AES.Key),
			};
			Client.PutObject(BucketName, Key: Key, Body: Data, MetadataList: MetadataList);

			var Response = Client.GetObject(BucketName, Key: Key);
			var EncodingBody = GetBody(Response);

			var e = Assert.Throws<CryptographicException>(() => AES.AESDecrypt(EncodingBody));
		}

		[Fact(DisplayName = "test_cse_encryption_range_read")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Range Read")]
		[Trait(MainData.Explanation, "[AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_encryption_range_read()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "testobj";

			//AES
			var AES = new AES256();

			var Data = RandomTextToLong(1024 * 1024);
			var EncodingData = AES.AESEncrypt(Data);
			var MetadataList = new List<KeyValuePair<string, string>>()
			{
				new KeyValuePair<string, string>("x-amz-meta-key", AES.Key),
			};
			Client.PutObject(BucketName, Key: Key, Body: EncodingData, MetadataList: MetadataList);

			var r = new Random();
			var StartPoint = r.Next(0, 1024 * 1024 - 1001);
			var Range = new ByteRange(StartPoint, StartPoint + 1000);
			var Response = Client.GetObject(BucketName, Key: Key, Range: Range);
			var EncodingBody = GetBody(Response);
			Assert.Equal(EncodingData.Substring((int)Range.Start, (int)(Range.End - Range.Start + 1)), EncodingBody);
		}


		[Fact(DisplayName = "test_cse_encryption_multipart_upload")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "[AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_encryption_multipart_upload()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "multipart_enc";
			var Size = 50 * MainData.MB;
			var ContentType = "text/plain";

			//AES
			var AES = new AES256();

			var Data = RandomTextToLong(Size);
			var EncodingData = AES.AESEncrypt(Data);
			var MetadataList = new List<KeyValuePair<string, string>>()
			{
				new KeyValuePair<string, string>("x-amz-meta-key", AES.Key),
			};

			var UploadData = SetupMultipartUploadData(Client, BucketName, Key, EncodingData, MetadataList: MetadataList, ContentType: ContentType);

			Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);

			var HeadResponse = Client.ListObjectsV2(BucketName);
			var ObjectCount = HeadResponse.KeyCount;
			Assert.Equal(1, ObjectCount);
			Assert.Equal(EncodingData.Length, GetBytesUsed(HeadResponse));

			var GetResponse = Client.GetObject(BucketName, Key);
			Assert.Equal(MetadataList, GetMetaData(GetResponse.Metadata));
			Assert.Equal(ContentType, GetResponse.Headers["content-type"]);

			var EncodingBody = GetBody(GetResponse);
			var Body = AES.AESDecrypt(EncodingBody);
			Assert.Equal(Size, Body.Length);
			Assert.Equal(Data, Body);

			CheckContentUsingRange(Client, BucketName, Key, EncodingData, 1000000);
			CheckContentUsingRange(Client, BucketName, Key, EncodingData, 10000000);
			CheckContentUsingRandomRange(Client, BucketName, Key, EncodingData, 100);
		}

		[Fact(DisplayName = "test_cse_get_object_many")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_get_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";

			//AES
			var AES = new AES256();
			var Data = RandomTextToLong(15 * 1024 * 1024);
			var EncodingData = AES.AESEncrypt(Data);

			Client.PutObject(BucketName, Key, Body: EncodingData);
			CheckContent(Client, BucketName, Key, EncodingData, LoopCount: 100);
		}

		[Fact(DisplayName = "test_cse_range_object_many")]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_cse_range_object_many()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo";

			//AES
			var AES = new AES256();
			var Data = RandomTextToLong(15 * 1024 * 1024);
			var EncodingData = AES.AESEncrypt(Data);

			Client.PutObject(BucketName, Key, Body: EncodingData);
			CheckContentUsingRandomRange(Client, BucketName, Key, EncodingData, 100);
		}
	}
}
