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
using System.Threading;
using Xunit;

namespace s3tests.Test
{
	public class CopyObject : TestBase
	{
		public CopyObject(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트의 크기가 0일때 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyZeroSize()
		{
			TestId = 1;
			var key = "foo123bar";
			var newKey = "bar321foo";
			var bucketName = SetupObjects([key]);
			var client = GetClient();

			client.PutObject(bucketName, key, body: "");

			client.CopyObject(bucketName, key, bucketName, newKey);

			var response = client.GetObject(bucketName, newKey);
			Assert.Equal(0, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "동일한 버킷에서 오브젝트 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopySameBucket()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";
			var newKey = "bar321foo";

			client.PutObject(bucketName, key, body: "foo");

			client.CopyObject(bucketName, key, bucketName, newKey);

			var response = client.GetObject(bucketName, newKey);
			var body = S3Utils.GetBody(response);
			Assert.Equal("foo", body);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "ContentType")]
		[Trait(MainData.Explanation, "ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyVerifyContentType()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";
			var newKey = "bar321foo";
			var contentType = "text/bla";

			client.PutObject(bucketName, key, body: "foo", contentType: contentType);

			client.CopyObject(bucketName, key, bucketName, newKey);

			var response = client.GetObject(bucketName, newKey);
			var body = S3Utils.GetBody(response);
			Assert.Equal("foo", body);
			var responseContentType = response.Headers.ContentType;
			Assert.Equal(contentType, responseContentType);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyToItself()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";
			var contentType = "text/bla";

			client.PutObject(bucketName, key, body: "foo", contentType: contentType);

			var e = Assert.Throws<AggregateException>(() => client.CopyObject(bucketName, key, bucketName, key));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "다른 버킷으로 오브젝트 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyDiffBucket()
		{
			TestId = 6;
			var bucketName1 = GetNewBucket();
			var bucketName2 = GetNewBucket();

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			var client = GetClient();
			client.PutObject(bucketName1, key1, body: "foo");

			client.CopyObject(bucketName1, key1, bucketName2, key2);

			var response = client.GetObject(bucketName2, key2);
			var body = S3Utils.GetBody(response);
			Assert.Equal("foo", body);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "[bucket1:created main user, object:created main user / bucket2:created sub user] " +
									 "메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyNotOwnedBucket()
		{
			TestId = 7;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName1 = GetNewBucketName();
			var bucketName2 = GetNewBucketName();

			client.PutBucket(bucketName1);
			altClient.PutBucket(bucketName2);

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			client.PutObject(bucketName1, key1, body: "foo");

			var e = Assert.Throws<AggregateException>(() => altClient.CopyObject(bucketName1, key1, bucketName2, key2));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "[bucket_acl = main:full control,sub : full control | object_acl = main:full control,sub : full control]" +
			"서브유저가 접근권한이 있는 버킷에 들어있는 접근권한이 있는 오브젝트를 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyNotOwnedObjectBucket()
		{
			TestId = 8;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucketCannedAcl(client);

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			client.PutObject(bucketName, key1, body: "foo");

			var altUserId = Config.AltUser.UserId;

			var grant = new S3Grant() { Grantee = new S3Grantee() { CanonicalUser = altUserId }, Permission = S3Permission.FULL_CONTROL };
			var grants = AddObjectUserGrant(bucketName, key1, grant);

			var response = client.PutObjectACL(bucketName, key1, accessControlPolicy: grants);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);


			grants = AddBucketUserGrant(bucketName, grant);
			var bucketAclResponse = client.PutBucketACL(bucketName, accessControlPolicy: grants);
			Assert.Equal(HttpStatusCode.OK, bucketAclResponse.HttpStatusCode);

			var response2 = altClient.GetObject(bucketName, key1);
			Assert.Equal(HttpStatusCode.OK, response2.HttpStatusCode);

			var response3 = altClient.CopyObject(bucketName, key1, bucketName, key2);
			Assert.Equal(HttpStatusCode.OK, response3.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "권한정보를 포함하여 복사할때 올바르게 적용되는지 확인 " +
									 "메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyCannedAcl()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var altClient = GetAltClient();
			var key1 = "foo123bar";
			var key2 = "bar321foo";

			var response = client.PutObject(bucketName, key1, body: "foo");
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			var response2 = client.CopyObject(bucketName, key1, bucketName, key2, acl: S3CannedACL.PublicRead);
			Assert.Equal(HttpStatusCode.OK, response2.HttpStatusCode);

			var response3 = altClient.GetObject(bucketName, key2);
			Assert.Equal(HttpStatusCode.OK, response3.HttpStatusCode);

			var metaData = new List<KeyValuePair<string, string>>() { new("x-amz-meta-abc", "def") };

			var response4 = client.CopyObject(bucketName, key2, bucketName, key1, metadataList: metaData, acl: S3CannedACL.PublicRead, metadataDirective: S3MetadataDirective.REPLACE);
			Assert.Equal(HttpStatusCode.OK, response4.HttpStatusCode);

			var response5 = altClient.GetObject(bucketName, key1);
			Assert.Equal(HttpStatusCode.OK, response5.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "크고 작은 용량의 오브젝트가 복사되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyRetainingMetadata()
		{
			TestId = 10;
			foreach (var size in new List<int>() { 3, 1024 * 1024 })
			{
				var bucketName = GetNewBucket();
				var client = GetClient();
				var contentType = "audio/ogg";

				var key1 = "foo123bar";
				var key2 = "bar321foo";

				var metaData = new List<KeyValuePair<string, string>>()
				{
					new("x-amz-meta-key1", "value1"),
					new("x-amz-meta-key2", "value2")
				};
				client.PutObject(bucketName, key1, metadataList: metaData, contentType: contentType, body: S3Utils.RandomTextToLong(size));

				client.CopyObject(bucketName, key1, bucketName, key2);

				var response = client.GetObject(bucketName, key2);
				Assert.Equal(contentType, response.Headers.ContentType);
				CheckMetaData(metaData, response.Metadata);
				Assert.Equal(size, response.ContentLength);
			}
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyReplacingMetadata()
		{
			TestId = 11;
			foreach (var size in new List<int>() { 3, 1024 * 1024 })
			{
				var bucketName = GetNewBucket();
				var client = GetClient();
				var contentType = "audio/ogg";

				var key1 = "foo123bar";
				var key2 = "bar321foo";

				var metaData = new List<KeyValuePair<string, string>>()
				{
					new("x-amz-meta-key1", "value1"),
					new("x-amz-meta-key2", "value2")
				};
				client.PutObject(bucketName, key1, metadataList: metaData, contentType: contentType, body: S3Utils.RandomTextToLong(size));

				var newMetaData = new List<KeyValuePair<string, string>>()
				{
					new("x-amz-meta-key2", "value2"),
					new("x-amz-meta-key3", "value3"),
				};

				client.CopyObject(bucketName, key1, bucketName, key2, metadataList: newMetaData, metadataDirective: S3MetadataDirective.REPLACE, contentType: contentType);

				var response = client.GetObject(bucketName, key2);
				Assert.Equal(contentType, response.Headers.ContentType);
				Assert.Equal(newMetaData, GetMetaData(response.Metadata));
				Assert.Equal(size, response.ContentLength);
			}
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyBucketNotFound()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => client.CopyObject(bucketName + "-fake", "foo123bar", bucketName, "bar321foo"));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 오브젝트 복사 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyKeyNotFound()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => client.CopyObject(bucketName, "foo123bar", bucketName, "bar321foo"));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "버저닝된 오브젝트 복사 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyVersioningBucket()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var size = 1 * 5;
			var data = S3Utils.RandomTextToLong(size);
			var key1 = "foo123bar";
			var key2 = "bar321foo";
			var key3 = "bar321foo2";
			client.PutObject(bucketName, key1, body: data);

			var response = client.GetObject(bucketName, key1);
			var versionId = response.VersionId;

			client.CopyObject(bucketName, key1, bucketName, key2, versionId: versionId);
			response = client.GetObject(bucketName, key2);
			var body = S3Utils.GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var versionId2 = response.VersionId;
			client.CopyObject(bucketName, key2, bucketName, key3, versionId: versionId2);
			response = client.GetObject(bucketName, key3);
			body = S3Utils.GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var bucketName2 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName2, VersionStatus.Enabled);
			var key4 = "bar321foo3";
			client.CopyObject(bucketName, key1, bucketName2, key4, versionId: versionId);
			response = client.GetObject(bucketName2, key4);
			body = S3Utils.GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var bucketName3 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName3, VersionStatus.Enabled);
			var key5 = "bar321foo4";
			client.CopyObject(bucketName, key1, bucketName3, key5, versionId: versionId);
			response = client.GetObject(bucketName3, key5);
			body = S3Utils.GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var key6 = "foo123bar2";
			client.CopyObject(bucketName3, key5, bucketName, key6);
			response = client.GetObject(bucketName, key6);
			body = S3Utils.GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "[버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyVersioningUrlEncoding()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var srcKey = "foo?bar";

			client.PutObject(bucketName, srcKey);
			var response = client.GetObject(bucketName, srcKey);

			var dstKey = "bar&foo";
			client.CopyObject(bucketName, srcKey, bucketName, dstKey, versionId: response.VersionId);
			client.GetObject(bucketName, dstKey);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "[버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyVersioningMultipartUpload()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var size = 50 * MainData.MB;
			var key1 = "srcmultipart";
			var key1MetaData = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var contentType = "text/bla";

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key1, size, contentType: contentType, metadataList: key1MetaData);
			client.CompleteMultipartUpload(bucketName, key1, uploadData.UploadId, uploadData.Parts);

			var response = client.GetObject(bucketName, key1);
			var key1Size = response.ContentLength;
			var versionId = response.VersionId;

			var key2 = "dstmultipart";
			client.CopyObject(bucketName, key1, bucketName, key2, versionId: versionId);
			response = client.GetObject(bucketName, key2);
			var versionId2 = response.VersionId;
			var body = S3Utils.GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);


			var key3 = "dstmultipart2";
			client.CopyObject(bucketName, key2, bucketName, key3, versionId: versionId2);
			response = client.GetObject(bucketName, key3);
			body = S3Utils.GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);

			var bucketName2 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName2, VersionStatus.Enabled);
			var key4 = "dstmultipart3";
			client.CopyObject(bucketName, key1, bucketName2, key4, versionId: versionId);
			response = client.GetObject(bucketName2, key4);
			body = S3Utils.GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);

			var bucketName3 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName3, VersionStatus.Enabled);
			var key5 = "dstmultipart4";
			client.CopyObject(bucketName, key1, bucketName3, key5, versionId: versionId);
			response = client.GetObject(bucketName3, key5);
			body = S3Utils.GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);

			var key6 = "dstmultipart5";
			client.CopyObject(bucketName3, key5, bucketName, key6);
			response = client.GetObject(bucketName, key6);
			body = S3Utils.GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Imatch")]
		[Trait(MainData.Explanation, "ifmatch 값을 추가하여 오브젝트를 복사할 경우 성공확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectIfMatchGood()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var putResponse = client.PutObject(bucketName, "foo", body: "bar");

			client.CopyObject(bucketName, "foo", bucketName, "bar", eTagToMatch: putResponse.ETag);
			var getResponse = client.GetObject(bucketName, "bar");
			var body = S3Utils.GetBody(getResponse);
			Assert.Equal("bar", body);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Imatch")]
		[Trait(MainData.Explanation, "ifmatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectIfMatchFailed()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutObject(bucketName, "foo", body: "bar");

			var e = Assert.Throws<AggregateException>(() => client.CopyObject(bucketName, "foo", bucketName, "bar", eTagToMatch: "ABCORZ"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(HttpStatusCode.PreconditionFailed.ToString(), GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyNorSrcToNorBucketAndObj()
		{
			TestId = 36;
			TestObjectCopy(false, false, false, false, 1024);
			TestObjectCopy(false, false, false, false, 256 * 1024);
			TestObjectCopy(false, false, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyNorSrcToNorBucketEncryptionObj()
		{
			TestId = 37;
			TestObjectCopy(false, false, false, true, 1024);
			TestObjectCopy(false, false, false, true, 256 * 1024);
			TestObjectCopy(false, false, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyNorSrcToEncryptionBucketNorObj()
		{
			TestId = 38;
			TestObjectCopy(false, false, true, false, 1024);
			TestObjectCopy(false, false, true, false, 256 * 1024);
			TestObjectCopy(false, false, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyNorSrcToEncryptionBucketAndObj()
		{
			TestId = 39;
			TestObjectCopy(false, false, true, true, 1024);
			TestObjectCopy(false, false, true, true, 256 * 1024);
			TestObjectCopy(false, false, true, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionSrcToNorBucketAndObj()
		{
			TestId = 40;
			TestObjectCopy(true, false, false, false, 1024);
			TestObjectCopy(true, false, false, false, 256 * 1024);
			TestObjectCopy(true, false, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionSrcToNorBucketEncryptionObj()
		{
			TestId = 41;
			TestObjectCopy(true, false, false, true, 1024);
			TestObjectCopy(true, false, false, true, 256 * 1024);
			TestObjectCopy(true, false, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionSrcToEncryptionBucketNorObj()
		{
			TestId = 42;
			TestObjectCopy(true, false, true, false, 1024);
			TestObjectCopy(true, false, true, false, 256 * 1024);
			TestObjectCopy(true, false, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionSrcToEncryptionBucketAndObj()
		{
			TestId = 43;
			TestObjectCopy(true, false, true, true, 1024);
			TestObjectCopy(true, false, true, true, 256 * 1024);
			TestObjectCopy(true, false, true, true, 1024 * 1024);
		}


		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketNorObjToNorBucketAndObj()
		{
			TestId = 44;
			TestObjectCopy(false, true, false, false, 1024);
			TestObjectCopy(false, true, false, false, 256 * 1024);
			TestObjectCopy(false, true, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketNorObjToNorBucketEncryptionObj()
		{
			TestId = 45;
			TestObjectCopy(false, true, false, true, 1024);
			TestObjectCopy(false, true, false, true, 256 * 1024);
			TestObjectCopy(false, true, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketNorObjToEncryptionBucketNorObj()
		{
			TestId = 46;
			TestObjectCopy(false, true, true, false, 1024);
			TestObjectCopy(false, true, true, false, 256 * 1024);
			TestObjectCopy(false, true, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketNorObjToEncryptionBucketAndObj()
		{
			TestId = 47;
			TestObjectCopy(false, true, true, true, 1024);
			TestObjectCopy(false, true, true, true, 256 * 1024);
			TestObjectCopy(false, true, true, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketAndObjToNorBucketAndObj()
		{
			TestId = 48;
			TestObjectCopy(true, true, false, false, 1024);
			TestObjectCopy(true, true, false, false, 256 * 1024);
			TestObjectCopy(true, true, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketAndObjToNorBucketEncryptionObj()
		{
			TestId = 49;
			TestObjectCopy(true, true, false, true, 1024);
			TestObjectCopy(true, true, false, true, 256 * 1024);
			TestObjectCopy(true, true, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketAndObjToEncryptionBucketNorObj()
		{
			TestId = 50;
			TestObjectCopy(true, true, true, false, 1024);
			TestObjectCopy(true, true, true, false, 256 * 1024);
			TestObjectCopy(true, true, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyEncryptionBucketAndObjToEncryptionBucketAndObj()
		{
			TestId = 51;
			TestObjectCopy(true, true, true, true, 1024);
			TestObjectCopy(true, true, true, true, 256 * 1024);
			TestObjectCopy(true, true, true, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "일반 오브젝트에서 다양한 방식으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyToNormalSource()
		{
			TestId = 52;
			var size1 = 1024;
			var size2 = 256 * 1024;
			var size3 = 1024 * 1024;

			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, size1);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, size2);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, size3);

			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, size1);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, size2);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, size3);

			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, size1);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, size2);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, size3);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyToSseS3Source()
		{
			TestId = 53;
			var size1 = 1024;
			var size2 = 256 * 1024;
			var size3 = 1024 * 1024;

			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, size1);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, size2);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, size3);

			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, size1);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, size2);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, size3);

			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, size1);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, size2);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, size3);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyToSseCSource()
		{
			TestId = 54;
			var size1 = 1024;
			var size2 = 256 * 1024;
			var size3 = 1024 * 1024;

			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, size1);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, size2);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, size3);

			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, size1);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, size2);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, size3);

			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, size1);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, size2);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, size3);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyToItselfWithMetadata()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";

			client.PutObject(bucketName, key, body: "foo");

			var metaData = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };

			client.CopyObject(bucketName, key, bucketName, key, metadataList: metaData, metadataDirective: S3MetadataDirective.REPLACE);
			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(metaData, GetMetaData(response.Metadata));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인(Versioning 설정)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectVersioningCopyToItselfWithMetadata()
		{
			TestId = 57;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			client.PutObject(bucketName, key, body: "foo");

			var metaData = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };

			client.CopyObject(bucketName, key, bucketName, key, metadataList: metaData, metadataDirective: S3MetadataDirective.REPLACE);
			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(metaData, GetMetaData(response.Metadata));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 변경하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyToItselfWithMetadataOverwrite()
		{
			TestId = 58;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";
			var metaData = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };

			client.PutObject(bucketName, key, body: "foo", metadataList: metaData);

			metaData[0] = new KeyValuePair<string, string>("x-amz-meta-foo", "bar2");

			client.CopyObject(bucketName, key, bucketName, key, metadataList: metaData, metadataDirective: S3MetadataDirective.REPLACE);
			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(metaData, GetMetaData(response.Metadata));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 변경하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인(Versioning 설정)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectVersioningCopyToItselfWithMetadataOverwrite()
		{
			TestId = 59;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var metaData = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			client.PutObject(bucketName, key, body: "foo", metadataList: metaData);

			metaData[0] = new KeyValuePair<string, string>("x-amz-meta-foo", "bar2");

			client.CopyObject(bucketName, key, bucketName, key, metadataList: metaData, metadataDirective: S3MetadataDirective.REPLACE);
			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(metaData, GetMetaData(response.Metadata));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "checksum")]
		[Trait(MainData.Explanation, "ChunkEncoding 환경에서 CopyObject 체크섬 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectChecksumUseChunkEncoding()
		{
			TestId = 61;
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
					var sourceKey = $"req_{config.Req}/resp_{config.Resp}/src/{checksum.Value}";
					var destKey = $"req_{config.Req}/resp_{config.Resp}/dst/{checksum.Value}";
					var putResponse = client.PutObject(bucketName, sourceKey, body: sourceKey,
						checksumAlgorithm: checksum, useChunkEncoding: true);
					ChecksumCompare(checksum, sourceKey, putResponse);

					var copyResponse = client.CopyObject(bucketName, sourceKey, bucketName, destKey,
						checksumAlgorithm: checksum);
					ChecksumCompare(checksum, sourceKey, copyResponse);
				}
			}
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectIfNoneMatchGood()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfNoneMatchGoodSource";
			var target = "testCopyObjectIfNoneMatchGoodTarget";

			client.PutObject(bucketName, source, body: source);

			client.CopyObject(bucketName, source, bucketName, target, eTagToNotMatch: "ABC");
			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 복사 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectIfNoneMatchFailed()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfNoneMatchFailedSource";
			var target = "testCopyObjectIfNoneMatchFailedTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, eTagToNotMatch: eTag));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectIfModifiedSinceGood()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfModifiedSinceGoodSource";
			var target = "testCopyObjectIfModifiedSinceGoodTarget";

			client.PutObject(bucketName, source, body: source);

			var days = new DateTime(1994, 9, 29, 19, 43, 31, DateTimeKind.Utc);

			client.CopyObject(bucketName, source, bucketName, target, modifiedSince: days);
			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 복사 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectIfModifiedSinceFailed()
		{
			TestId = 22;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfModifiedSinceFailedSource";
			var target = "testCopyObjectIfModifiedSinceFailedTarget";

			client.PutObject(bucketName, source, body: source);

			// 미래 날짜는 RFC 7232에 따라 무시되므로 업로드 시간 + 1초를 지정하고 1초 대기
			var lastModified = client.GetObjectMetadata(bucketName, source).LastModified.Value;
			var after = lastModified.AddSeconds(1);

			Thread.Sleep(1000);

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, modifiedSince: after));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectIfUnmodifiedSinceGood()
		{
			TestId = 23;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfUnmodifiedSinceGoodSource";
			var target = "testCopyObjectIfUnmodifiedSinceGoodTarget";

			client.PutObject(bucketName, source, body: source);

			var days = new DateTime(2100, 9, 29, 19, 43, 31, DateTimeKind.Utc);

			client.CopyObject(bucketName, source, bucketName, target, unmodifiedSince: days);
			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 복사 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectIfUnmodifiedSinceFailed()
		{
			TestId = 24;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfUnmodifiedSinceFailedSource";
			var target = "testCopyObjectIfUnmodifiedSinceFailedTarget";

			client.PutObject(bucketName, source, body: source);

			var days = new DateTime(1994, 9, 29, 19, 43, 31, DateTimeKind.Utc);

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, unmodifiedSince: days));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "copy-source-if-match(일치)와 copy-source-if-unmodified-since(불일치)를 함께 사용할 경우 ETag 조건이 우선되어 복사에 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectIfMatchWithIfUnmodifiedSince()
		{
			TestId = 25;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfMatchWithIfUnmodifiedSinceSource";
			var target = "testCopyObjectIfMatchWithIfUnmodifiedSinceTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;

			var days = new DateTime(1994, 9, 29, 19, 43, 31, DateTimeKind.Utc);

			// copy-source-if-match: true, copy-source-if-unmodified-since: false -> 200 OK
			client.CopyObject(bucketName, source, bucketName, target, eTagToMatch: eTag, unmodifiedSince: days);
			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "If Match")]
		[Trait(MainData.Explanation, "copy-source-if-none-match(불일치)와 copy-source-if-modified-since(일치)를 함께 사용할 경우 ETag 조건이 우선되어 412가 반환되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectIfNoneMatchWithIfModifiedSince()
		{
			TestId = 26;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfNoneMatchWithIfModifiedSinceSource";
			var target = "testCopyObjectIfNoneMatchWithIfModifiedSinceTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;

			var days = new DateTime(1994, 9, 29, 19, 43, 31, DateTimeKind.Utc);

			// copy-source-if-none-match: false, copy-source-if-modified-since: true -> 412
			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, eTagToNotMatch: eTag, modifiedSince: days));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "copy-source-if-match와 copy-source-if-none-match에 동일한 ETag를 지정하면 412가 반환되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectIfMatchAndIfNoneMatch()
		{
			TestId = 27;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfMatchAndIfNoneMatchSource";
			var target = "testCopyObjectIfMatchAndIfNoneMatchTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, eTagToMatch: eTag, eTagToNotMatch: eTag));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "copy-source-if-match와 copy-source-if-none-match: * 를 함께 지정하면 412가 반환되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectIfMatchAndIfNoneMatchAny()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectIfMatchAndIfNoneMatchAnySource";
			var target = "testCopyObjectIfMatchAndIfNoneMatchAnyTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, eTagToMatch: eTag, eTagToNotMatch: "*"));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상 오브젝트와 일치하는 If-Match 조건으로 덮어쓰기 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectDestinationIfMatchGood()
		{
			TestId = 29;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectDestinationIfMatchGoodSource";
			var target = "testCopyObjectDestinationIfMatchGoodTarget";

			client.PutObject(bucketName, source, body: source);
			var targetETag = client.PutObject(bucketName, target, body: "old").ETag;

			client.CopyObject(bucketName, source, bucketName, target, ifMatch: targetETag);
			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상 오브젝트와 일치하지 않는 If-Match 조건으로 복사 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectDestinationIfMatchFailed()
		{
			TestId = 30;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectDestinationIfMatchFailedSource";
			var target = "testCopyObjectDestinationIfMatchFailedTarget";

			client.PutObject(bucketName, source, body: source);
			client.PutObject(bucketName, target, body: "old");

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, ifMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			// 덮어쓰기 되지 않았는지 확인
			var response = client.GetObject(bucketName, target);
			Assert.Equal("old", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfNoneMatch")]
		[Trait(MainData.Explanation, "존재하지 않는 대상 키에 If-None-Match: * 조건으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectDestinationIfNoneMatchGood()
		{
			TestId = 31;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectDestinationIfNoneMatchGoodSource";
			var target = "testCopyObjectDestinationIfNoneMatchGoodTarget";

			client.PutObject(bucketName, source, body: source);

			client.CopyObject(bucketName, source, bucketName, target, ifNoneMatch: "*");
			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfNoneMatch")]
		[Trait(MainData.Explanation, "이미 존재하는 대상 키에 If-None-Match: * 조건으로 복사 시 412 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectDestinationIfNoneMatchFailed()
		{
			TestId = 32;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectDestinationIfNoneMatchFailedSource";
			var target = "testCopyObjectDestinationIfNoneMatchFailedTarget";

			client.PutObject(bucketName, source, body: source);
			client.PutObject(bucketName, target, body: "old");

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, ifNoneMatch: "*"));

			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			// 덮어쓰기 되지 않았는지 확인
			var response = client.GetObject(bucketName, target);
			Assert.Equal("old", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectDestinationIfMatchAndIfNoneMatch()
		{
			TestId = 33;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectDestinationIfMatchAndIfNoneMatchSource";
			var target = "testCopyObjectDestinationIfMatchAndIfNoneMatchTarget";

			client.PutObject(bucketName, source, body: source);
			var targetETag = client.PutObject(bucketName, target, body: "old").ETag;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, ifMatch: targetETag, ifNoneMatch: targetETag));

			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			// 덮어쓰기 되지 않았는지 확인
			var response = client.GetObject(bucketName, target);
			Assert.Equal("old", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "대상에 If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyObjectDestinationIfMatchAndIfNoneMatchAny()
		{
			TestId = 34;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectDestinationIfMatchAndIfNoneMatchAnySource";
			var target = "testCopyObjectDestinationIfMatchAndIfNoneMatchAnyTarget";

			client.PutObject(bucketName, source, body: source);
			var targetETag = client.PutObject(bucketName, target, body: "old").ETag;

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target, ifMatch: targetETag, ifNoneMatch: "*"));

			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			// 덮어쓰기 되지 않았는지 확인
			var response = client.GetObject(bucketName, target);
			Assert.Equal("old", GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "IfMatch")]
		[Trait(MainData.Explanation, "소스 If-Match와 대상 If-None-Match: * 를 함께 사용해 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectSourceIfMatchWithDestinationIfNoneMatch()
		{
			TestId = 35;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyObjectSourceIfMatchWithDestinationIfNoneMatchSource";
			var target = "testCopyObjectSourceIfMatchWithDestinationIfNoneMatchTarget";

			var eTag = client.PutObject(bucketName, source, body: source).ETag;

			client.CopyObject(bucketName, source, bucketName, target, eTagToMatch: eTag, ifNoneMatch: "*");
			var response = client.GetObject(bucketName, target);
			Assert.Equal(source, GetBody(response));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "복사시 메타데이터와 태그가 유지되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectMetadataAndTags()
		{
			TestId = 62;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var sourceKey = "testCopyObjectMetadataAndTagsSource";
			var targetKey = "testCopyObjectMetadataAndTagsTarget";

			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var tagSet = new List<Tag>() { new() { Key = "tag1", Value = "value1" } };

			client.PutObject(bucketName, sourceKey, body: sourceKey, metadataList: metadataList, tagSet: tagSet);

			var response = client.GetObject(bucketName, sourceKey);
			Assert.Equal("bar", response.Metadata["x-amz-meta-foo"]);

			var tagResponse = client.GetObjectTagging(bucketName, sourceKey);
			TaggingCompare(tagSet, tagResponse.Tagging);

			client.CopyObject(bucketName, sourceKey, bucketName, targetKey);

			response = client.GetObject(bucketName, targetKey);
			Assert.Equal("bar", response.Metadata["x-amz-meta-foo"]);

			tagResponse = client.GetObjectTagging(bucketName, targetKey);
			TaggingCompare(tagSet, tagResponse.Tagging);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "SSE-C로 암호화된 오브젝트를 복사할 때 대상 암호화 알고리즘을 지정하지 않으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyRevokeSseAlgorithm()
		{
			TestId = 60;
			var client = GetClientHttps();
			var bucketName = GetNewBucket(client);
			UnblockSseC(bucketName);
			var sourceKey = "testCopyRevokeSseAlgorithmSource";
			var targetKey = "testCopyRevokeSseAlgorithmTarget";
			var data = S3Utils.RandomTextToLong(1024);

			var sseC = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.AES256,
				ProvidedKey = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
				MD5 = "DWygnHRtgiJ77HCm+1rvHw==",
			};

			client.PutObject(bucketName, sourceKey, body: data, sseCustomerKey: sseC);

			// 키/MD5만 넘기고 알고리즘(x-amz-copy-source-server-side-encryption-customer-algorithm)은 생략한다.
			var noAlgorithm = new SSECustomerKey()
			{
				Method = ServerSideEncryptionCustomerMethod.None,
				ProvidedKey = sseC.ProvidedKey,
				MD5 = sseC.MD5,
			};

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, sourceKey, bucketName, targetKey, srcCustomerKey: noAlgorithm));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "삭제된 오브젝트를 복사할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyToDeletedObject()
		{
			TestId = 55;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyToDeletedObjectSource";
			var target = "testCopyToDeletedObjectTarget";

			client.PutObject(bucketName, source, body: source);
			client.DeleteObject(bucketName, source);

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "삭제 마커가 있는 오브젝트를 복사할 경우 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCopyToDeleteMarkerObject()
		{
			TestId = 56;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var source = "testCopyToDeleteMarkerObjectSource";
			var target = "testCopyToDeleteMarkerObjectTarget";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			client.PutObject(bucketName, source, body: source);
			client.DeleteObject(bucketName, source);

			var e = Assert.Throws<AggregateException>(() =>
				client.CopyObject(bucketName, source, bucketName, target));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}
	}
}
