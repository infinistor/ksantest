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
		public void TestObjectWriteToNonexistBucket()
		{
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestObjectWriteCheckEtag";

			var response = client.PutObject(bucketName, key: key, body: key);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
			Assert.Equal(S3Utils.GetMD5(key), response.ETag.Replace("\"", ""));
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "CacheControl")]
		[Trait(MainData.Explanation, "캐시를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectWriteCacheControl()
		{
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
			var keyNames = new List<string>() { " ", "\"", "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__", };

			var bucketName = SetupObjects(keyNames);

			var objectList = GetObjectList(bucketName);

			var client = GetClient();

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
		[Trait(MainData.Explanation, "[SignatureVersion2] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutEmptyObjectSignatureVersion2()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t /", "t :", "t [", "t ]" };
			var bucketName = SetupObjects(keyNames, body: "");
			var client = GetClient();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutEmptyObjectSignatureVersion4()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var bucketName = SetupObjectsV4(keyNames, body: "");
			var client = GetClientV4();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "[SignatureVersion2] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectSignatureVersion2()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var response = client.ListObjects(bucketName);
			var keys = GetKeys(response);

			Assert.Equal(keyNames, keys);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "Special Characters")]
		[Trait(MainData.Explanation, "[SignatureVersion4] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectSignatureVersion4()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var bucketName = SetupObjectsV4(keyNames);
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
		public void TestPutObjectUseChunkEncoding()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
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
		public void TestPutObjectUseChunkEncodingAndDisablePayloadSigning()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
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
		public void TestPutObjectNotChunkEncoding()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
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
		public void TestPutObjectNotChunkEncodingAndDisablePayloadSigning()
		{
			var keyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestObjectEmoji";

			client.PutObject(bucketName, key: key);

			var response = client.ListObjects(bucketName);
			Assert.Single(response.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "PutObject")]
		[Trait(MainData.Minor, "PUT")]
		[Trait(MainData.Explanation, "오브젝트 이름에 이모지가 포함될 경우 올바르게 업로드 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectChecksum()
		{
			var client = GetClientHttpsV4(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_SUPPORTED);
			var bucketName = GetNewBucket(client);
			var key = "TestPutObjectChecksum";
			var checksums = new List<ChecksumAlgorithm>
			{
				ChecksumAlgorithm.CRC32,
				// ChecksumAlgorithm.CRC32C,
				// ChecksumAlgorithm.SHA1,
				// ChecksumAlgorithm.SHA256
			};
			foreach (var checksum in checksums)
			{
				client.PutObject(bucketName, key: $"{key}/00", body: key, checksumAlgorithm: checksum, useChunkEncoding: false, disablePayloadSigning: false);
				client.PutObject(bucketName, key: $"{key}/01", body: key, checksumAlgorithm: checksum, useChunkEncoding: true, disablePayloadSigning: false);
				client.PutObject(bucketName, key: $"{key}/10", body: key, checksumAlgorithm: checksum, useChunkEncoding: false, disablePayloadSigning: true);
				client.PutObject(bucketName, key: $"{key}/11", body: key, checksumAlgorithm: checksum);

				// var response = client.GetObject(bucketName, key: $"{key}/00");
				// Assert.Equal(key, S3Utils.GetBody(response));

				// response = client.GetObject(bucketName, key: $"{key}/01");
				// Assert.Equal(key, S3Utils.GetBody(response));

				// response = client.GetObject(bucketName, key: $"{key}/10");
				// Assert.Equal(key, S3Utils.GetBody(response));

				// response = client.GetObject(bucketName, key: $"{key}/11");
				// Assert.Equal(key, S3Utils.GetBody(response));
			}
		}
	}
}
