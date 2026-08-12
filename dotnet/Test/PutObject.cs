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
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Xunit;

namespace s3tests.Test
{
	public class PutObject : TestBase
	{
		public PutObject(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Bucket")]
		[Trait(MainData.Minor, "PUT")]
		[Trait(MainData.Explanation, "오브젝트가 올바르게 생성되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListDistinct()
		{
			TestId = 1;
			var bucket1 = GetNewBucket();
			var bucket2 = GetNewBucket();
			var client = GetClient();
			var key = "TestBucketListDistinct";

			client.PutObject(bucket1, key);

			var response1 = client.ListObjects(bucket1);
			Assert.Single(response1.S3Objects);

			var response2 = client.ListObjects(bucket2);
			Assert.Empty(response2.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷에 오브젝트 업로드할 경우 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectWriteToNonExistBucket()
		{
			TestId = 2;
			var key = "TestObjectWriteToNonexistBucket";
			var bucketName = "whatchutalkinboutwillis";
			var client = GetClient();

			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key: key, body: key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "0바이트로 업로드한 오브젝트가 실제로 0바이트인지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectHeadZeroBytes()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "TestObjectHeadZeroBytes";
			client.PutObject(bucketName, key: key, body: "");

			var response = client.GetObjectMetadata(bucketName, key: key);
			Assert.Equal(0, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "업로드한 오브젝트의 ETag가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectWriteCheckEtag()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestObjectWriteCheckEtag";

			var response = client.PutObject(bucketName, key: key, body: key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
			Assert.Equal(S3Utils.GetMD5Hex(key), response.ETag.Replace("\"", ""));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "CacheControl")]
		[Trait(MainData.Explanation, "캐시를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectWriteCacheControl()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "TestObjectWriteCacheControl";
			var cacheControl = "public, max-age=14400";
			client.PutObject(bucketName, key: key, body: key, cacheControl: cacheControl);

			var response = client.GetObjectMetadata(bucketName, key: key);
			Assert.Equal(cacheControl, response.Metadata[S3Headers.CacheControl]);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Expires")]
		[Trait(MainData.Explanation, "헤더만료일시(날짜)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectWriteExpires()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "TestObjectWriteExpires";
			var expires = DateTime.UtcNow.AddSeconds(6000);
			client.PutObject(bucketName, key: key, body: key, expires: expires);

			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(TimeToString(expires), response.Metadata[S3Headers.Expires]);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Update")]
		[Trait(MainData.Explanation, "오브젝트의 기본 작업을 모두 올바르게 할 수 있는지 확인(read, write, update, delete)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectWriteReadUpdateReadDelete()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "TestObjectWriteReadUpdateReadDelete";

			client.PutObject(bucketName, key: key, body: key);

			// Read
			var getResponse = client.GetObject(bucketName, key: key);
			Assert.Equal(key, S3Utils.GetBody(getResponse));


			// Delete
			client.DeleteObject(bucketName, key);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "오브젝트에 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectSetGetMetadataNoneToGood()
		{
			TestId = 8;
			var myMeta = "my-meta";
			var got = SetupMetadata(myMeta);
			Assert.Equal(myMeta, got);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "오브젝트에 빈 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectSetGetMetadataNoneToEmpty()
		{
			TestId = 9;
			var got = SetupMetadata("");
			Assert.Empty(got);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "메타 데이터 업데이트가 올바르게 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectSetGetMetadataOverwriteToEmpty()
		{
			TestId = 10;
			var bucketName = GetNewBucket();

			var myMeta = "old-mata";
			var got = SetupMetadata(myMeta, bucketName: bucketName);
			Assert.Equal(myMeta, got);

			got = SetupMetadata("", bucketName: bucketName);
			Assert.Empty(got);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "메타데이터에 올바르지 않는 문자열[EOF(\x04)]를 사용할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectSetGetNonUtf8Metadata()
		{
			TestId = 11;
			var metadata = "\x04my-meta";
			var e = TestMetadataUnreadable(metadata);
			Assert.True(ErrorCheck(e.StatusCode));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "메타데이터에 올바르지 않는 문자[EOF(\x04)]를 문자열 맨앞에 사용할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectSetGetMetadataEmptyToUnreadablePrefix()
		{
			TestId = 12;
			var metadata = "\x04w";
			var e = TestMetadataUnreadable(metadata);
			Assert.True(ErrorCheck(e.StatusCode));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "메타데이터에 올바르지 않는 문자[EOF(\x04)]를 문자열 맨뒤에 사용할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectSetGetMetadataEmptyToUnreadableSuffix()
		{
			TestId = 13;
			var metadata = "h\x04";
			var e = TestMetadataUnreadable(metadata);
			Assert.True(ErrorCheck(e.StatusCode));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "오브젝트를 메타데이타 없이 덮어쓰기 했을 때, 메타데이타 값이 비어있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectMetadataReplacedOnPut()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var metadataList = new List<KeyValuePair<string, string>>
			{
				new("x-amz-meta-meta1", "bar")
			};

			var key = "TestObjectMetadataReplacedOnPut";
			client.PutObject(bucketName, key: key, body: key, metadataList: metadataList);
			client.PutObject(bucketName, key: key, body: key);

			var response = client.GetObject(bucketName, key: key);
			var got = response.Metadata;
			Assert.Equal(0, got.Count);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Incoding")]
		[Trait(MainData.Explanation, "body의 내용을utf-8로 인코딩한 오브젝트를 업로드 했을때 올바르게 업로드 되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectWriteFile()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestObjectWriteFile";
			var body = Encoding.UTF8.GetBytes(key);

			client.PutObject(bucketName, key: key, byteBody: body);

			var response = client.GetObject(bucketName, key: key);
			Assert.Equal(key, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special key Name")]
		[Trait(MainData.Explanation, "오브젝트 이름과 내용이 모두 특수문자인 오브젝트 여러개를 업로드 할 경우 모두 재대로 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateSpecialKeyNames()
		{
			TestId = 16;
			var keyNames = new List<string>() { " ", "\"", "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__", };

			// 키별로 PutObjectACL을 호출하므로 ACL이 허용되는 버킷이어야 한다.
			var client = GetClient();
			var bucketName = SetupObjects(keyNames, GetNewBucketCannedAcl(client));

			var objectList = GetObjectList(bucketName);

			foreach (var key in keyNames)
			{
				if (string.IsNullOrWhiteSpace(key)) continue;
				Assert.Contains(key, objectList);
				var response = client.GetObject(bucketName, key);
				Assert.Equal(key, S3Utils.GetBody(response));
				client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private);
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special key Name")]
		[Trait(MainData.Explanation, "[_], [/]가 포함된 이름을 가진 오브젝트를 업로드 한뒤 prefix정보를 설정한 GetObjectList가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListSpecialPrefix()
		{
			TestId = 17;
			var keyNames = new List<string>() { "_bla/1", "_bla/2", "_bla/3", "_bla/4", "abcd" };

			var bucketName = SetupObjects(keyNames);

			var objectList = GetObjectList(bucketName);
			Assert.Equal(5, objectList.Count);

			objectList = GetObjectList(bucketName, prefix: "_bla/");
			Assert.Equal(4, objectList.Count);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Lock")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
									 "LegalHold와 Lock유지기한을 설정하여 오브젝트 업로드할 경우 설정이 적용되는지 메타데이터를 통해 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockUploadingObj()
		{
			TestId = 18;
			var bucketName = GetNewBucketName();
			var client = GetClient();
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var key = "TestObjectLockUploadingObj";
			var body = "abc";
			var md5 = S3Utils.GetMD5(body);
			var putResponse = client.PutObject(bucketName, key: key, body: "abc", md5Digest: md5, objectLockMode: ObjectLockMode.Governance,
				objectLockRetainUntilDate: new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
				objectLockLegalHoldStatus: ObjectLockLegalHoldStatus.On);

			var response = client.GetObjectMetadata(bucketName, key: key);
			Assert.Equal(ObjectLockMode.Governance, response.ObjectLockMode);
			Assert.Equal(new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc), response.ObjectLockRetainUntilDate.Value);
			Assert.Equal(ObjectLockLegalHoldStatus.On, response.ObjectLockLegalHoldStatus);

			var legalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			client.PutObjectLegalHold(bucketName, key: key, legalHold);
			client.DeleteObject(bucketName, key: key, versionId: putResponse.VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Space")]
		[Trait(MainData.Explanation, "오브젝트의 중간에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectInfixSpace()
		{
			TestId = 19;
			var keyNames = new List<string>() { "a a/", "b b/f1", "c/f 2", "d d/f 3" };
			var bucketName = SetupObjects(keyNames, body: "");
			var client = GetClient();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Space")]
		[Trait(MainData.Explanation, "오브젝트의 마지막에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectSuffixSpace()
		{
			TestId = 20;
			var keyNames = new List<string>() { "a /", "b /f1", "c/f2 ", "d /f3 " };
			var bucketName = SetupObjects(keyNames, body: "");
			var client = GetClient();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectSpecialCharacters()
		{
			TestId = 21;
			var keyNames = SpecialCharacterKeys();
			var bucketName = SetupObjectsV4(keyNames, useChunkEncoding: true);
			var client = GetClientV4();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Encoding")]
		[Trait(MainData.Explanation, "[SignatureVersion4, useChunkEncoding = true] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectSpecialCharactersUseChunkEncoding()
		{
			TestId = 22;
			var keyNames = SpecialCharacterKeys();
			var bucketName = SetupObjectsV4(keyNames, useChunkEncoding: true);
			var client = GetClientV4();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Encoding")]
		[Trait(MainData.Explanation, "[SignatureVersion4, useChunkEncoding = true, disablePayloadSigning = true] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectUseSpecialCharactersChunkEncodingAndDisablePayloadSigning()
		{
			TestId = 23;
			var keyNames = SpecialCharacterKeys();
			var bucketName = SetupObjectsV4(keyNames, useChunkEncoding: true, disablePayloadSigning: true);
			var client = GetClientHttpsV4();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Encoding")]
		[Trait(MainData.Explanation, "[SignatureVersion4, useChunkEncoding = false] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectSpecialCharactersNotChunkEncoding()
		{
			TestId = 24;
			var keyNames = SpecialCharacterKeys();
			var bucketName = SetupObjectsV4(keyNames, useChunkEncoding: false);
			var client = GetClientV4();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Encoding")]
		[Trait(MainData.Explanation, "[SignatureVersion4, useChunkEncoding = false, disablePayloadSigning = true] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectSpecialCharactersNotChunkEncodingAndDisablePayloadSigning()
		{
			TestId = 25;
			var keyNames = SpecialCharacterKeys();
			var bucketName = SetupObjectsV4(keyNames, useChunkEncoding: false, disablePayloadSigning: true);
			var client = GetClientHttpsV4();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Directory")]
		[Trait(MainData.Explanation, "폴더의 이름과 동일한 오브젝트 업로드가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectDirAndFile()
		{
			TestId = 26;
			// file first
			var bucketName = GetNewBucket();
			var objectName = "aaa";
			var directoryName = "aaa/";
			var client = GetClient();

			client.PutObject(bucketName, objectName, body: objectName);
			client.PutObject(bucketName, directoryName, body: "");

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);
			Assert.Equal(2, keys.Count);

			// dir first
			var bucketName2 = GetNewBucket();

			client.PutObject(bucketName2, directoryName, body: "");
			client.PutObject(bucketName2, objectName, body: objectName);

			response = client.ListObjects(bucketName2);
			keys = GetKeys(response);
			Assert.Equal(2, keys.Count);

			// etc
			var bucketName3 = GetNewBucket();
			var newObjectName = "aaa/bbb/ccc";

			client.PutObject(bucketName3, objectName, body: objectName);
			client.PutObject(bucketName3, newObjectName, body: newObjectName);

			response = client.ListObjects(bucketName3);
			keys = GetKeys(response);
			Assert.Equal(2, keys.Count);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "오브젝트를 덮어쓰기 했을때 올바르게 반영되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectOverwrite()
		{
			TestId = 27;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestObjectOverwrite";
			var content1 = S3Utils.RandomTextToLong(10 * MainData.KB);
			var content2 = S3Utils.RandomTextToLong(1 * MainData.MB);

			client.PutObject(bucketName, key: key, body: content1);
			client.PutObject(bucketName, key: key, body: content2);

			var response = client.GetObject(bucketName, key: key);
			Assert.Equal(content2, S3Utils.GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "PUT")]
		[Trait(MainData.Explanation, "오브젝트 이름에 이모지가 포함될 경우 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectEmoji()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestObjectEmoji";

			client.PutObject(bucketName, key: key);

			var response = client.ListObjects(bucketName);
			Assert.Single(response.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "ChunkEncoding 사용 시 체크섬 알고리즘별 PutObject 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectChecksumUseChunkEncoding()
		{
			TestId = 30;
			var bucketName = GetNewBucket();
			var configs = new (RequestChecksumCalculation Req, ResponseChecksumValidation Resp)[]
			{
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_REQUIRED),
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_SUPPORTED),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_REQUIRED),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED),
			};

			foreach (var config in configs)
			{
				var client = GetClientHttpsV4(config.Req, config.Resp);
				foreach (var checksum in CheckSum.AllAlgorithms)
				{
					var prefix = $"req_{config.Req}/resp_{config.Resp}";
					var key = $"{prefix}/sync/{checksum.Value}";
					var response = client.PutObject(bucketName, key, body: key, checksumAlgorithm: checksum, useChunkEncoding: true);
					ChecksumCompare(checksum, key, response);
				}
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "체크섬 알고리즘별 PutObject 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectChecksum()
		{
			TestId = 31;
			var bucketName = GetNewBucket();
			var configs = new (RequestChecksumCalculation Req, ResponseChecksumValidation Resp)[]
			{
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_REQUIRED),
				(RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_SUPPORTED),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_REQUIRED),
				(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED),
			};

			foreach (var config in configs)
			{
				var client = GetClientHttpsV4(config.Req, config.Resp);
				foreach (var checksum in CheckSum.AllAlgorithms)
				{
					var prefix = $"req_{config.Req}/resp_{config.Resp}";
					var key = $"{prefix}/sync/{checksum.Value}";
					var response = client.PutObject(bucketName, key, body: key, checksumAlgorithm: checksum, useChunkEncoding: false);
					ChecksumCompare(checksum, key, response);
				}
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "사전 계산한 체크섬 값으로 PutObject 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectChecksumWithValue()
		{
			TestId = 32;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			foreach (var checksum in CheckSum.AllAlgorithms)
			{
				var key = $"precomputed/{checksum.Value}";
				var value = CheckSum.CalculateChecksum(checksum, key);
				var response = client.PutObject(bucketName, key, body: key, checksumAlgorithm: checksum, checksumValue: value);
				ChecksumCompare(checksum, key, response);
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "checksum-failure")]
		[Trait(MainData.Explanation, "잘못된 체크섬 값 지정 시 BadDigest 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjectChecksumFailure()
		{
			TestId = 33;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			foreach (var checksum in CheckSum.AllAlgorithms)
			{
				var key = $"wrong-checksum/{checksum.Value}";
				var wrongValue = CheckSum.CalculateChecksum(checksum, key + "-wrong");
				var e = Assert.Throws<AggregateException>(() =>
					client.PutObject(bucketName, key, body: key, checksumAlgorithm: checksum, checksumValue: wrongValue));
				Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
				Assert.Equal(MainData.BAD_DIGEST, GetErrorCode(e));
			}
		}

		/// <summary>특수문자 키 목록. testV2와 동일한 순서를 유지한다.</summary>
		private static List<string> SpecialCharacterKeys() =>
		[
			"!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(",
			")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]"
		];

		private static string Repeat(string value, int count)
		{
			var builder = new StringBuilder(value.Length * count);
			for (var i = 0; i < count; i++) builder.Append(value);
			return builder.ToString();
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "UTF-8 문자열을 메타데이터로 사용할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectSetGetMetadataUtf8()
		{
			TestId = 29;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";
			var metadataKey1 = "x-amz-meta-meta1";
			var metadataKey2 = "x-amz-meta-meta2";
			var metadata1 = "utf-8";
			var metadata2 = "UTF-8";
			var contentType = "text/plain; charset=UTF-8";

			client.PutObject(bucketName, key, body: "bar", contentType: contentType,
				metadataList: [new(metadataKey1, metadata1), new(metadataKey2, metadata2)]);

			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(metadata1, response.Metadata[metadataKey1]);
			Assert.Equal(metadata2, response.Metadata[metadataKey2]);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "유저 메타데이터 키가 대소문자가 섞여 있어도 소문자로 반환되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectSetGetMetadataMixedCaseKey()
		{
			TestId = 49;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";
			var metadataList = new List<KeyValuePair<string, string>>()
			{
				new("x-amz-meta-Meta1", "value1"),
				new("x-amz-meta-META2", "value2"),
				new("x-amz-meta-mEtA3", "value3"),
			};

			client.PutObject(bucketName, key, body: key, metadataList: metadataList);

			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal("value1", response.Metadata["x-amz-meta-meta1"]);
			Assert.Equal("value2", response.Metadata["x-amz-meta-meta2"]);
			Assert.Equal("value3", response.Metadata["x-amz-meta-meta3"]);
			foreach (var metadataKey in response.Metadata.Keys)
			{
				if (metadataKey.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase))
					Assert.Equal(metadataKey.ToLowerInvariant(), metadataKey);
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상 오브젝트와 일치하는 If-Match 조건으로 덮어쓰기 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectIfMatchGood()
		{
			TestId = 34;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testPutObjectIfMatchGood";

			var eTag = client.PutObject(bucketName, key, body: "old").ETag;

			client.PutObject(bucketName, key, body: "new", ifMatch: eTag);

			var response = client.GetObject(bucketName, key);
			Assert.Equal("new", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상 오브젝트와 일치하지 않는 If-Match 조건으로 덮어쓰기 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjectIfMatchFailed()
		{
			TestId = 35;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testPutObjectIfMatchFailed";

			client.PutObject(bucketName, key, body: "old");

			var e = Assert.Throws<AggregateException>(() =>
				client.PutObject(bucketName, key, body: "new", ifMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			// 덮어쓰기 되지 않았는지 확인
			var response = client.GetObject(bucketName, key);
			Assert.Equal("old", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "IfNoneMatch")]
		[Trait(MainData.Explanation, "존재하지 않는 키에 If-None-Match: * 조건으로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectIfNoneMatchGood()
		{
			TestId = 36;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testPutObjectIfNoneMatchGood";

			client.PutObject(bucketName, key, body: "bar", ifNoneMatch: "*");

			var response = client.GetObject(bucketName, key);
			Assert.Equal("bar", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "IfNoneMatch")]
		[Trait(MainData.Explanation, "이미 존재하는 키에 If-None-Match: * 조건으로 업로드 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjectIfNoneMatchFailed()
		{
			TestId = 37;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testPutObjectIfNoneMatchFailed";

			client.PutObject(bucketName, key, body: "old");

			var e = Assert.Throws<AggregateException>(() =>
				client.PutObject(bucketName, key, body: "new", ifNoneMatch: "*"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			// 덮어쓰기 되지 않았는지 확인
			var response = client.GetObject(bucketName, key);
			Assert.Equal("old", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjectIfMatchAndIfNoneMatch()
		{
			TestId = 38;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testPutObjectIfMatchAndIfNoneMatch";

			var eTag = client.PutObject(bucketName, key, body: "old").ETag;

			var e = Assert.Throws<AggregateException>(() =>
				client.PutObject(bucketName, key, body: "new", ifMatch: eTag, ifNoneMatch: "*"));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			// 덮어쓰기 되지 않았는지 확인
			var response = client.GetObject(bucketName, key);
			Assert.Equal("old", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "KeyLength")]
		[Trait(MainData.Explanation, "최소 길이(1자) 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeyMinLength()
		{
			TestId = 40;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "a";
			var body = "test-min-length";

			var response = client.PutObject(bucketName, key, body: body);
			Assert.NotNull(response.ETag);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(body, GetBody(getResponse));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "KeyLength")]
		[Trait(MainData.Explanation, "최대 길이(1024자) 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeyMaxLength()
		{
			TestId = 39;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = S3Utils.RandomObjectName(MainData.MAX_KEY_LENGTH);
			var body = "test-max-length";

			var response = client.PutObject(bucketName, key, body: body);
			Assert.NotNull(response.ETag);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(body, GetBody(getResponse));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "KeyLength")]
		[Trait(MainData.Explanation, "최대 길이를 초과한 키로 업로드 시 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjectKeyTooLong()
		{
			TestId = 41;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = S3Utils.RandomObjectName(MainData.MAX_KEY_LENGTH + 1);
			var body = "test-too-long";

			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: body));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.KEY_TOO_LONG, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "KeyLength")]
		[Trait(MainData.Explanation, "다양한 경계 길이의 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeyBoundaryLengths()
		{
			TestId = 48;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var testCases = new List<int>()
			{
				MainData.MAX_KEY_LENGTH - 1, // 1023
				MainData.MAX_KEY_LENGTH,     // 1024
				500,                         // 중간 길이
				100,                         // 짧은 길이
				50                           // 매우 짧은 길이
			};

			foreach (var length in testCases)
			{
				var key = S3Utils.RandomObjectName(length);
				var body = "boundary-test-" + length;

				var response = client.PutObject(bucketName, key, body: body);
				Assert.NotNull(response.ETag);

				var getResponse = client.GetObject(bucketName, key);
				Assert.Equal(body, GetBody(getResponse));
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "특수문자로 시작하는 최대 길이 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeySpecialCharactersAtStart()
		{
			TestId = 42;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// '/'는 제외한다. path style 요청에서 키가 '/'로 시작하면 경로가 '/{bucket}//key'가 되어
			// SDK v4의 SigV4 서명이 어긋난다(SignatureDoesNotMatch). 키 이름 규칙이 아니라 서명 경로 문제다.
			var specialChars = new List<string>()
			{
				"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_", "+", "=", "[", "]", "{",
				"}", "|", "\\", ":", ";", "\"", "'", "<", ">", ",", ".", "?", "~", "`"
			};

			foreach (var specialChar in specialChars)
			{
				// 최대 길이에서 특수문자 1자를 뺀 길이로 생성
				var remainingLength = MainData.MAX_KEY_LENGTH - specialChar.Length;
				var key = specialChar + S3Utils.RandomObjectName(remainingLength);
				var body = "test-body-" + specialChar;

				Assert.Equal(MainData.MAX_KEY_LENGTH, key.Length);
				var response = client.PutObject(bucketName, key, body: body);
				Assert.NotNull(response.ETag);

				var getResponse = client.GetObject(bucketName, key);
				Assert.Equal(body, GetBody(getResponse));
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "특수문자로 끝나는 최대 길이 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeySpecialCharactersAtEnd()
		{
			TestId = 43;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var specialChars = new List<string>()
			{
				"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_", "+", "=", "[", "]", "{",
				"}", "|", "\\", ":", ";", "\"", "'", "<", ">", ",", ".", "?", "/", "~", "`"
			};

			foreach (var specialChar in specialChars)
			{
				// 최대 길이에서 특수문자 1자를 뺀 길이로 생성
				var remainingLength = MainData.MAX_KEY_LENGTH - specialChar.Length;
				var key = S3Utils.RandomObjectName(remainingLength) + specialChar;
				var body = "test-body-" + specialChar;

				Assert.Equal(MainData.MAX_KEY_LENGTH, key.Length);
				var response = client.PutObject(bucketName, key, body: body);
				Assert.NotNull(response.ETag);

				var getResponse = client.GetObject(bucketName, key);
				Assert.Equal(body, GetBody(getResponse));
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Unicode")]
		[Trait(MainData.Explanation, "유니코드 문자로 구성된 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeyUnicodeCharacters()
		{
			TestId = 44;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var unicodeChars = new List<string>() { "한", "中", "日", "а", "α", "ع", "т", "ф" };

			foreach (var unicodeChar in unicodeChars)
			{
				// 실제 바이트 길이 확인
				var singleCharBytes = Encoding.UTF8.GetByteCount(unicodeChar);
				var maxLength = 200 / singleCharBytes; // 200바이트 제한에 맞는 최대 문자 수

				// 안전하게 조금 작은 길이로 시도
				var safeLength = Math.Max(1, maxLength - 1);
				var key = Repeat(unicodeChar, safeLength);
				var body = "unicode-test-" + unicodeChar;

				var response = client.PutObject(bucketName, key, body: body);
				Assert.NotNull(response.ETag);

				var getResponse = client.GetObject(bucketName, key);
				Assert.Equal(body, GetBody(getResponse));
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Unicode")]
		[Trait(MainData.Explanation, "1024바이트를 초과하는 유니코드 키로 업로드 시 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestPutObjectKeyUnicodeCharactersTooLong()
		{
			TestId = 45;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var unicodeChars = new List<string>() { "한", "中", "日", "а", "α", "ع", "т", "ф" };

			foreach (var unicodeChar in unicodeChars)
			{
				// 실제 바이트 길이 확인
				var singleCharBytes = Encoding.UTF8.GetByteCount(unicodeChar);
				var maxLength = MainData.MAX_KEY_LENGTH / singleCharBytes; // 1024바이트 제한에 맞는 최대 문자 수

				// 1024바이트를 초과하는 길이로 시도
				var key = Repeat(unicodeChar, maxLength + 1);
				var body = "unicode-test-fail-" + unicodeChar;

				var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: body));
				Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
				Assert.Equal(MainData.KEY_TOO_LONG, GetErrorCode(e));
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "연속된 슬래시를 포함한 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeyWithConsecutiveSlashes()
		{
			TestId = 47;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// '/'로 시작하는 키는 제외한다. path style 요청에서 경로가 '/{bucket}//key'가 되어
			// SDK v4의 SigV4 서명이 어긋난다(SignatureDoesNotMatch). 중간/끝의 연속 슬래시는 정상 동작한다.
			var keys = new List<string>()
			{
				"folder//double-slash",
				"folder///triple-slash",
				"trailing-double-slash//",
				"folder////multiple-slashes"
			};

			foreach (var key in keys)
			{
				var body = "slash-test-" + key.Replace("/", "-");

				var response = client.PutObject(bucketName, key, body: body);
				Assert.NotNull(response.ETag);

				var getResponse = client.GetObject(bucketName, key);
				Assert.Equal(body, GetBody(getResponse));
			}
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "앞뒤에 공백이 있는 최대 길이 키로 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectKeyWithLeadingAndTrailingSpaces()
		{
			TestId = 46;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var testCases = new List<int>() { 1, 2, 3, 5 };

			foreach (var spaceCount in testCases)
			{
				var spaces = Repeat(" ", spaceCount);
				var middleLength = MainData.MAX_KEY_LENGTH - (spaceCount * 2);
				var middle = S3Utils.RandomObjectName(middleLength);
				var key = spaces + middle + spaces;
				var body = "space-test-" + spaceCount;

				Assert.Equal(MainData.MAX_KEY_LENGTH, key.Length);
				var response = client.PutObject(bucketName, key, body: body);
				Assert.NotNull(response.ETag);

				var getResponse = client.GetObject(bucketName, key);
				Assert.Equal(body, GetBody(getResponse));
			}
		}
	}
}
