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
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
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
			var key = "foo123bar";
			var newKey = "bar321foo";
			var bucketName = SetupObjects(new List<string>() { key });
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";
			var newKey = "bar321foo";

			client.PutObject(bucketName, key, body: "foo");

			client.CopyObject(bucketName, key, bucketName, newKey);

			var response = client.GetObject(bucketName, newKey);
			var body = GetBody(response);
			Assert.Equal("foo", body);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "ContentType")]
		[Trait(MainData.Explanation, "ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyVerifyContentType()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo123bar";
			var newKey = "bar321foo";
			var contentType = "text/bla";

			client.PutObject(bucketName, key, body: "foo", contentType: contentType);

			client.CopyObject(bucketName, key, bucketName, newKey);

			var response = client.GetObject(bucketName, newKey);
			var body = GetBody(response);
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
			var bucketName1 = GetNewBucket();
			var bucketName2 = GetNewBucket();

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			var client = GetClient();
			client.PutObject(bucketName1, key1, body: "foo");

			client.CopyObject(bucketName1, key1, bucketName2, key2);

			var response = client.GetObject(bucketName2, key2);
			var body = GetBody(response);
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
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucketName();
			client.PutBucket(bucketName);

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			client.PutObject(bucketName, key1, body: "foo");

			var altUserId = Config.AltUser.UserId;

			var grant = new S3Grant() { Grantee = new S3Grantee() { CanonicalUser = altUserId }, Permission = S3Permission.FULL_CONTROL };
			var grants = AddObjectUserGrant(bucketName, key1, grant);

			var response = client.PutObjectACL(bucketName, key1, accessControlPolicy: grants);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);


			grants = AddBucketUserGrant(bucketName, grant);
			response = client.PutBucketACL(bucketName, accessControlPolicy: grants);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

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
		public void TestObjectCopyCannedACL()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
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
				client.PutObject(bucketName, key1, metadataList: metaData, contentType: contentType, body: RandomTextToLong(size));

				client.CopyObject(bucketName, key1, bucketName, key2);

				var response = client.GetObject(bucketName, key2);
				Assert.Equal(contentType, response.Headers.ContentType);
				Assert.Equal(metaData, GetMetaData(response.Metadata));
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
				client.PutObject(bucketName, key1, metadataList: metaData, contentType: contentType, body: RandomTextToLong(size));

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
		public void TestObjectCopyVersionedBucket()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var size = 1 * 5;
			var data = RandomTextToLong(size);
			var key1 = "foo123bar";
			var key2 = "bar321foo";
			var key3 = "bar321foo2";
			client.PutObject(bucketName, key1, body: data);

			var response = client.GetObject(bucketName, key1);
			var versionId = response.VersionId;

			client.CopyObject(bucketName, key1, bucketName, key2, versionId: versionId);
			response = client.GetObject(bucketName, key2);
			var body = GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var versionId2 = response.VersionId;
			client.CopyObject(bucketName, key2, bucketName, key3, versionId: versionId2);
			response = client.GetObject(bucketName, key3);
			body = GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var bucketName2 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName2, VersionStatus.Enabled);
			var key4 = "bar321foo3";
			client.CopyObject(bucketName, key1, bucketName2, key4, versionId: versionId);
			response = client.GetObject(bucketName2, key4);
			body = GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var bucketName3 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName3, VersionStatus.Enabled);
			var key5 = "bar321foo4";
			client.CopyObject(bucketName, key1, bucketName3, key5, versionId: versionId);
			response = client.GetObject(bucketName3, key5);
			body = GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);

			var key6 = "foo123bar2";
			client.CopyObject(bucketName3, key5, bucketName, key6);
			response = client.GetObject(bucketName, key6);
			body = GetBody(response);
			Assert.Equal(data, body);
			Assert.Equal(size, response.ContentLength);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "[버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyVersionedUrlEncoding()
		{
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
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var size = 50 * MainData.MB;
			var key1 = "srcmultipart";
			var key1MetaData = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			var contentType = "text/bla";

			var uploadData = SetupMultipartUpload(client, bucketName, key1, size, contentType: contentType, metadataList: key1MetaData);
			client.CompleteMultipartUpload(bucketName, key1, uploadData.UploadId, uploadData.Parts);

			var response = client.GetObject(bucketName, key1);
			var key1Size = response.ContentLength;
			var versionId = response.VersionId;

			var key2 = "dstmultipart";
			client.CopyObject(bucketName, key1, bucketName, key2, versionId: versionId);
			response = client.GetObject(bucketName, key2);
			var versionId2 = response.VersionId;
			var body = GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);


			var key3 = "dstmultipart2";
			client.CopyObject(bucketName, key2, bucketName, key3, versionId: versionId2);
			response = client.GetObject(bucketName, key3);
			body = GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);

			var bucketName2 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName2, VersionStatus.Enabled);
			var key4 = "dstmultipart3";
			client.CopyObject(bucketName, key1, bucketName2, key4, versionId: versionId);
			response = client.GetObject(bucketName2, key4);
			body = GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);

			var bucketName3 = GetNewBucket();
			CheckConfigureVersioningRetry(bucketName3, VersionStatus.Enabled);
			var key5 = "dstmultipart4";
			client.CopyObject(bucketName, key1, bucketName3, key5, versionId: versionId);
			response = client.GetObject(bucketName3, key5);
			body = GetBody(response);
			Assert.Equal(uploadData.Body, body);
			Assert.Equal(key1Size, response.ContentLength);
			Assert.Equal(key1MetaData, GetMetaData(response.Metadata));
			Assert.Equal(contentType, response.Headers.ContentType);

			var key6 = "dstmultipart5";
			client.CopyObject(bucketName3, key5, bucketName, key6);
			response = client.GetObject(bucketName, key6);
			body = GetBody(response);
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
		public void TestObjectCopyIfmatchGood()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var putResponse = client.PutObject(bucketName, "foo", body: "bar");

			client.CopyObject(bucketName, "foo", bucketName, "bar", eTagToMatch: putResponse.ETag);
			var getResponse = client.GetObject(bucketName, "bar");
			var body = GetBody(getResponse);
			Assert.Equal("bar", body);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "Imatch")]
		[Trait(MainData.Explanation, "ifmatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectCopyIfmatchFailed()
		{
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
		public void TestObjectCopyNorSrcToNorBucketAndObj()
		{
			TestObjectCopy(false, false, false, false, 1024);
			TestObjectCopy(false, false, false, false, 256 * 1024);
			TestObjectCopy(false, false, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyNorSrcToNorBucketEncryptionObj()
		{
			TestObjectCopy(false, false, false, true, 1024);
			TestObjectCopy(false, false, false, true, 256 * 1024);
			TestObjectCopy(false, false, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyNorSrcToEncryptionBucketNorObj()
		{
			TestObjectCopy(false, false, true, false, 1024);
			TestObjectCopy(false, false, true, false, 256 * 1024);
			TestObjectCopy(false, false, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyNorSrcToEncryptionBucketAndObj()
		{
			TestObjectCopy(false, false, true, true, 1024);
			TestObjectCopy(false, false, true, true, 256 * 1024);
			TestObjectCopy(false, false, true, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionSrcToNorBucketAndObj()
		{
			TestObjectCopy(true, false, false, false, 1024);
			TestObjectCopy(true, false, false, false, 256 * 1024);
			TestObjectCopy(true, false, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionSrcToNorBucketEncryptionObj()
		{
			TestObjectCopy(true, false, false, true, 1024);
			TestObjectCopy(true, false, false, true, 256 * 1024);
			TestObjectCopy(true, false, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionSrcToEncryptionBucketNorObj()
		{
			TestObjectCopy(true, false, true, false, 1024);
			TestObjectCopy(true, false, true, false, 256 * 1024);
			TestObjectCopy(true, false, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionSrcToEncryptionBucketAndObj()
		{
			TestObjectCopy(true, false, true, true, 1024);
			TestObjectCopy(true, false, true, true, 256 * 1024);
			TestObjectCopy(true, false, true, true, 1024 * 1024);
		}


		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketNorObjToNorBucketAndObj()
		{
			TestObjectCopy(false, true, false, false, 1024);
			TestObjectCopy(false, true, false, false, 256 * 1024);
			TestObjectCopy(false, true, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketNorObjToNorBucketEncryptionObj()
		{
			TestObjectCopy(false, true, false, true, 1024);
			TestObjectCopy(false, true, false, true, 256 * 1024);
			TestObjectCopy(false, true, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketNorObjToEncryptionBucketNorObj()
		{
			TestObjectCopy(false, true, true, false, 1024);
			TestObjectCopy(false, true, true, false, 256 * 1024);
			TestObjectCopy(false, true, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketNorObjToEncryptionBucketAndObj()
		{
			TestObjectCopy(false, true, true, true, 1024);
			TestObjectCopy(false, true, true, true, 256 * 1024);
			TestObjectCopy(false, true, true, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketAndObjToNorBucketAndObj()
		{
			TestObjectCopy(true, true, false, false, 1024);
			TestObjectCopy(true, true, false, false, 256 * 1024);
			TestObjectCopy(true, true, false, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketAndObjToNorBucketEncryptionObj()
		{
			TestObjectCopy(true, true, false, true, 1024);
			TestObjectCopy(true, true, false, true, 256 * 1024);
			TestObjectCopy(true, true, false, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketAndObjToEncryptionBucketNorObj()
		{
			TestObjectCopy(true, true, true, false, 1024);
			TestObjectCopy(true, true, true, false, 256 * 1024);
			TestObjectCopy(true, true, true, false, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyEncryptionBucketAndObjToEncryptionBucketAndObj()
		{
			TestObjectCopy(true, true, true, true, 1024);
			TestObjectCopy(true, true, true, true, 256 * 1024);
			TestObjectCopy(true, true, true, true, 1024 * 1024);
		}

		[Fact]
		[Trait(MainData.Major, "CopyObject")]
		[Trait(MainData.Minor, "encryption")]
		[Trait(MainData.Explanation, "일반 오브젝트에서 다양한 방식으로 복사 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectCopyToNormalSource()
		{
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
		public void TestObjectCopyToSseS3Source()
		{
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
		public void TestObjectCopyToSseCSource()
		{
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
		public void TestObjectCopyVersioningCopyToItselfWithMetadata()
		{
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
		public void TestObjectCopyVersioningCopyToItselfWithMetadataOverwrite()
		{
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
	}
}
