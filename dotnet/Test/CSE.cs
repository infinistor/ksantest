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

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseEncryptedTransfer1b()
		{
			TestEncryptionCSEWrite(1);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseEncryptedTransfer1kb()
		{
			TestEncryptionCSEWrite(1024);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseEncryptedTransfer1MB()
		{
			TestEncryptionCSEWrite(1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Put / Get")]
		[Trait(MainData.Explanation, "[AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseEncryptedTransfer13b()
		{
			TestEncryptionCSEWrite(13);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "[AES256] 암호화하고 메타데이터에 공개키를 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseEncryptionMethodHead()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testobj";

			var aes = new AES256();
			var data = new string('A', 1000);
			var encodedData = aes.AESEncrypt(data);
			List<KeyValuePair<string, string>> metadataList = new()
			{
				new("x-amz-meta-key", aes.Key),
			};
			client.PutObject(bucketName, key: key, body: encodedData, metadataList: metadataList);

			var response = client.GetObjectMetadata(bucketName, key: key);
			var resMetaData = GetMetaData(response.Metadata);
			Assert.Equal(metadataList, resMetaData);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCseEncryptionNonDecryption()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testobj";

			var aes = new AES256();
			var data = new string('A', 1000);
			var encodedData = aes.AESEncrypt(data);
			var metadataList = new List<KeyValuePair<string, string>>()
			{
				new("x-amz-meta-key", aes.Key),
			};
			client.PutObject(bucketName, key: key, body: encodedData, metadataList: metadataList);

			var response = client.GetObject(bucketName, key: key);
			var body = GetBody(response);
			Assert.NotEqual(data, body);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCseNonEncryptionDecryption()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testobj";

			var aes = new AES256();
			var data = new string('A', 1000);
			var metadataList = new List<KeyValuePair<string, string>>()
			{
				new("x-amz-meta-key", aes.Key),
			};
			client.PutObject(bucketName, key: key, body: data, metadataList: metadataList);

			var response = client.GetObject(bucketName, key: key);
			var encodedBody = GetBody(response);

			Assert.Throws<CryptographicException>(() => aes.AESDecrypt(encodedBody));
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "range Read")]
		[Trait(MainData.Explanation, "[AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseEncryptionRangeRead()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testobj";

			var aes = new AES256();
			var data = RandomTextToLong(1024 * 1024);
			var encodedData = aes.AESEncrypt(data);
			var metadataList = new List<KeyValuePair<string, string>>()
			{
				new("x-amz-meta-key", aes.Key),
			};
			client.PutObject(bucketName, key: key, body: encodedData, metadataList: metadataList);

			var r = new Random();
			var startPoint = r.Next(0, 1024 * 1024 - 1001);
			var range = new ByteRange(startPoint, startPoint + 1000);
			var response = client.GetObject(bucketName, key: key, range: range);
			var encodedBody = GetBody(response);
			Assert.Equal(encodedData.Substring((int)range.Start, (int)(range.End - range.Start + 1)), encodedBody);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "[AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseEncryptionMultipartUpload()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "multipart_enc";
			var size = 50 * MainData.MB;
			var contentType = "text/plain";

			var aes = new AES256();
			var data = RandomTextToLong(size);
			var encodedData = aes.AESEncrypt(data);
			var metadataList = new List<KeyValuePair<string, string>>()
			{
				new("x-amz-meta-key", aes.Key),
			};

			var uploadData = SetupMultipartUploadData(client, bucketName, key, encodedData, metadataList: metadataList, contentType: contentType);

			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);

			var headResponse = client.ListObjectsV2(bucketName);
			var objectCount = headResponse.KeyCount;
			Assert.Equal(1, objectCount);
			Assert.Equal(encodedData.Length, GetBytesUsed(headResponse));

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(metadataList, GetMetaData(getResponse.Metadata));
			Assert.Equal(contentType, getResponse.Headers["content-type"]);

			var encodedBody = GetBody(getResponse);
			var body = aes.AESDecrypt(encodedBody);
			Assert.Equal(size, body.Length);
			Assert.Equal(data, body);

			CheckContentUsingRange(client, bucketName, key, encodedData, 1000000);
			CheckContentUsingRange(client, bucketName, key, encodedData, 10000000);
			CheckContentUsingRandomRange(client, bucketName, key, encodedData, 100);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseGetObjectMany()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";

			var aes = new AES256();
			var data = RandomTextToLong(15 * 1024 * 1024);
			var encodedData = aes.AESEncrypt(data);

			client.PutObject(bucketName, key, body: encodedData);
			CheckContent(client, bucketName, key, encodedData, loopCount: 100);
		}

		[Fact]
		[Trait(MainData.Major, "CSE")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "CSE설정한 오브젝트를 여러번 반복하여 range 다운로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCseRangeObjectMany()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";

			var aes = new AES256();
			var data = RandomTextToLong(15 * 1024 * 1024);
			var encodedData = aes.AESEncrypt(data);

			client.PutObject(bucketName, key, body: encodedData);
			CheckContentUsingRandomRange(client, bucketName, key, encodedData, 100);
		}
	}
}
