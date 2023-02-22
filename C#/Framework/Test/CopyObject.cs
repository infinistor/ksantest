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
using Amazon.S3.Model;
using System.Collections.Generic;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace s3tests2
{
	[TestClass]
	public class CopyObject : TestBase
	{
		[TestMethod("test_object_copy_zero_size")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "오브젝트의 크기가 0일때 복사가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_zero_size()
		{
			var Key = "foo123bar";
			var NewKey = "bar321foo";
			var BucketName = SetupObjects(new List<string>() { Key });
			var Client = GetClient();

			Client.PutObject(BucketName, Key, Body: "");

			Client.CopyObject(BucketName, Key, BucketName, NewKey);

			var Response = Client.GetObject(BucketName, NewKey);
			Assert.AreEqual(0, Response.ContentLength);
		}

		[TestMethod("test_object_copy_same_bucket")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "동일한 버킷에서 오브젝트 복사가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_same_bucket()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo123bar";
			var NewKey = "bar321foo";

			Client.PutObject(BucketName, Key, Body: "foo");

			Client.CopyObject(BucketName, Key, BucketName, NewKey);

			var Response = Client.GetObject(BucketName, NewKey);
			var Body = GetBody(Response);
			Assert.AreEqual("foo", Body);
		}

		[TestMethod("test_object_copy_verify_contenttype")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "ContentType")]
		[TestProperty(MainData.Explanation, "ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_verify_contenttype()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo123bar";
			var NewKey = "bar321foo";
			var ContentType = "text/bla";

			Client.PutObject(BucketName, Key, Body: "foo", ContentType: ContentType);

			Client.CopyObject(BucketName, Key, BucketName, NewKey);

			var Response = Client.GetObject(BucketName, NewKey);
			var Body = GetBody(Response);
			Assert.AreEqual("foo", Body);
			var ResponseContentType = Response.Headers.ContentType;
			Assert.AreEqual(ContentType, ResponseContentType);
		}

		[TestMethod("test_object_copy_to_itself")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "OverWrite")]
		[TestProperty(MainData.Explanation, "복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_to_itself()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo123bar";
			var ContentType = "text/bla";

			Client.PutObject(BucketName, Key, Body: "foo", ContentType: ContentType);

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.CopyObject(BucketName, Key, BucketName, Key));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;

			Assert.AreEqual(HttpStatusCode.BadRequest, StatusCode);
			Assert.AreEqual(MainData.InvalidRequest, ErrorCode);
		}

		[TestMethod("test_object_copy_to_itself_with_metadata")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "OverWrite")]
		[TestProperty(MainData.Explanation, "복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 " +
									 "모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_to_itself_with_metadata()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "foo123bar";

			Client.PutObject(BucketName, Key, Body: "foo");

			var MetaData = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar") };

			Client.CopyObject(BucketName, Key, BucketName, Key, MetadataList: MetaData, MetadataDirective: S3MetadataDirective.REPLACE);
			var Response = Client.GetObject(BucketName, Key);
			CollectionAssert.AreEqual(MetaData, GetMetaData(Response.Metadata));
		}

		[TestMethod("test_object_copy_diff_bucket")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "다른 버킷으로 오브젝트 복사가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_diff_bucket()
		{
			var BucketName1 = GetNewBucket();
			var BucketName2 = GetNewBucket();

			var Key1 = "foo123bar";
			var Key2 = "bar321foo";

			var Client = GetClient();
			Client.PutObject(BucketName1, Key1, Body: "foo");

			Client.CopyObject(BucketName1, Key1, BucketName2, Key2);

			var Response = Client.GetObject(BucketName2, Key2);
			var Body = GetBody(Response);
			Assert.AreEqual("foo", Body);
		}

		[TestMethod("test_object_copy_not_owned_bucket")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "[bucket1:created main user, object:created main user / bucket2:created sub user] " +
									 "메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_not_owned_bucket()
		{
			var Client = GetClient();
			var AltClient = GetAltClient();
			var BucketName1 = GetNewBucketName();
			var BucketName2 = GetNewBucketName();

			Client.PutBucket(BucketName1);
			AltClient.PutBucket(BucketName2);

			var Key1 = "foo123bar";
			var Key2 = "bar321foo";

			Client.PutObject(BucketName1, Key1, Body: "foo");

			var e = Assert.ThrowsException<AmazonS3Exception>(() => AltClient.CopyObject(BucketName1, Key1, BucketName2, Key2));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;

			Assert.AreEqual(HttpStatusCode.Forbidden, StatusCode);
		}

		[TestMethod("test_object_copy_not_owned_object_bucket")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "[bucket_acl = main:full control,sub : full control | object_acl = main:full control,sub : full control]" +
			"서브유저가 접근권한이 있는 버킷에 들어있는 접근권한이 있는 오브젝트를 복사가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_not_owned_object_bucket()
		{
			var Client = GetClient();
			var AltClient = GetAltClient();
			var BucketName = GetNewBucketName();
			Client.PutBucket(BucketName);

			var Key1 = "foo123bar";
			var Key2 = "bar321foo";

			Client.PutObject(BucketName, Key1, Body: "foo");

			var AltUserId = Config.AltUser.UserId;

			var Grant = new S3Grant() { Grantee = new S3Grantee() { CanonicalUser = AltUserId }, Permission = S3Permission.FULL_CONTROL };
			var Grants = AddObjectUserGrant(BucketName, Key1, Grant);
			Client.PutObjectACL(BucketName, Key1, AccessControlPolicy: Grants);

			Grants = AddBucketUserGrant(BucketName, Grant);
			Client.PutBucketACL(BucketName, AccessControlPolicy: Grants);

			AltClient.GetObject(BucketName, Key1);

			AltClient.CopyObject(BucketName, Key1, BucketName, Key2);
		}

		[TestMethod("test_object_copy_canned_acl")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "OverWrite")]
		[TestProperty(MainData.Explanation, "권한정보를 포함하여 복사할때 올바르게 적용되는지 확인 " +
									 "메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_canned_acl()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var AltClient = GetAltClient();
			var Key1 = "foo123bar";
			var Key2 = "bar321foo";

			Client.PutObject(BucketName, Key1, Body: "foo");

			Client.CopyObject(BucketName, Key1, BucketName, Key2, ACL: S3CannedACL.PublicRead);
			AltClient.GetObject(BucketName, Key2);

			var MetaData = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-abc", "def") };

			Client.CopyObject(BucketName, Key2, BucketName, Key1, MetadataList: MetaData, ACL: S3CannedACL.PublicRead, MetadataDirective: S3MetadataDirective.REPLACE);
			AltClient.GetObject(BucketName, Key1);
		}

		[TestMethod("test_object_copy_retaining_metadata")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "크고 작은 용량의 오브젝트가 복사되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_retaining_metadata()
		{
			foreach (var Size in new List<int>() { 3, 1024 * 1024 })
			{
				var BucketName = GetNewBucket();
				var Client = GetClient();
				var ContentType = "audio/ogg";

				var Key1 = "foo123bar";
				var Key2 = "bar321foo";

				var MetaData = new List<KeyValuePair<string, string>>()
				{
					new KeyValuePair<string, string>("x-amz-meta-key1", "value1"),
					new KeyValuePair<string, string>("x-amz-meta-key2", "value2")
				};
				Client.PutObject(BucketName, Key1, MetadataList: MetaData, ContentType: ContentType, Body: RandomTextToLong(Size));

				Client.CopyObject(BucketName, Key1, BucketName, Key2);

				var Response = Client.GetObject(BucketName, Key2);
				Assert.AreEqual(ContentType, Response.Headers.ContentType);
				CollectionAssert.AreEqual(MetaData, GetMetaData(Response.Metadata));
				Assert.AreEqual(Size, Response.ContentLength);
			}
		}

		[TestMethod("test_object_copy_replacing_metadata")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Check")]
		[TestProperty(MainData.Explanation, "크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_replacing_metadata()
		{
			foreach (var Size in new List<int>() { 3, 1024 * 1024 })
			{
				var BucketName = GetNewBucket();
				var Client = GetClient();
				var ContentType = "audio/ogg";

				var Key1 = "foo123bar";
				var Key2 = "bar321foo";

				var MetaData = new List<KeyValuePair<string, string>>()
				{
					new KeyValuePair<string, string>("x-amz-meta-key1", "value1"),
					new KeyValuePair<string, string>("x-amz-meta-key2", "value2")
				};
				Client.PutObject(BucketName, Key1, MetadataList: MetaData, ContentType: ContentType, Body: RandomTextToLong(Size));

				var NewMetaData = new List<KeyValuePair<string, string>>()
				{
					new KeyValuePair<string, string>("x-amz-meta-key2", "value2"),
					new KeyValuePair<string, string>("x-amz-meta-key3", "value3"),
				};

				Client.CopyObject(BucketName, Key1, BucketName, Key2, MetadataList: NewMetaData, MetadataDirective: S3MetadataDirective.REPLACE, ContentType: ContentType);

				var Response = Client.GetObject(BucketName, Key2);
				Assert.AreEqual(ContentType, Response.Headers.ContentType);
				CollectionAssert.AreEqual(NewMetaData, GetMetaData(Response.Metadata));
				Assert.AreEqual(Size, Response.ContentLength);
			}
		}

		[TestMethod("test_object_copy_bucket_not_found")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_bucket_not_found()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.CopyObject(BucketName + "-fake", "foo123bar", BucketName, "bar321foo"));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.NotFound, StatusCode);
		}

		[TestMethod("test_object_copy_key_not_found")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "존재하지 않는 오브젝트 복사 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_copy_key_not_found()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.CopyObject(BucketName, "foo123bar", BucketName, "bar321foo"));
			var StatusCode = e.StatusCode;
			Assert.AreEqual(HttpStatusCode.NotFound, StatusCode);
		}

		[TestMethod("test_object_copy_versioned_bucket")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Version")]
		[TestProperty(MainData.Explanation, "버저닝된 오브젝트 복사 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_versioned_bucket()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			var Size = 1 * 5;
			var Data = RandomTextToLong(Size);
			var Key1 = "foo123bar";
			var Key2 = "bar321foo";
			var Key3 = "bar321foo2";
			Client.PutObject(BucketName, Key1, Body: Data);

			var Response = Client.GetObject(BucketName, Key1);
			var VersionId = Response.VersionId;

			Client.CopyObject(BucketName, Key1, BucketName, Key2, VersionId: VersionId);
			Response = Client.GetObject(BucketName, Key2);
			var Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
			Assert.AreEqual(Size, Response.ContentLength);

			var VersionId2 = Response.VersionId;
			Client.CopyObject(BucketName, Key2, BucketName, Key3, VersionId: VersionId2);
			Response = Client.GetObject(BucketName, Key3);
			Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
			Assert.AreEqual(Size, Response.ContentLength);

			var BucketName2 = GetNewBucket();
			CheckConfigureVersioningRetry(BucketName2, VersionStatus.Enabled);
			var Key4 = "bar321foo3";
			Client.CopyObject(BucketName, Key1, BucketName2, Key4, VersionId: VersionId);
			Response = Client.GetObject(BucketName2, Key4);
			Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
			Assert.AreEqual(Size, Response.ContentLength);

			var BucketName3 = GetNewBucket();
			CheckConfigureVersioningRetry(BucketName3, VersionStatus.Enabled);
			var Key5 = "bar321foo4";
			Client.CopyObject(BucketName, Key1, BucketName3, Key5, VersionId: VersionId);
			Response = Client.GetObject(BucketName3, Key5);
			Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
			Assert.AreEqual(Size, Response.ContentLength);

			var Key6 = "foo123bar2";
			Client.CopyObject(BucketName3, Key5, BucketName, Key6);
			Response = Client.GetObject(BucketName, Key6);
			Body = GetBody(Response);
			Assert.AreEqual(Data, Body);
			Assert.AreEqual(Size, Response.ContentLength);
		}

		[TestMethod("test_object_copy_versioned_url_encoding")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Version")]
		[TestProperty(MainData.Explanation, "[버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_versioned_url_encoding()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			var SrcKey = "foo?bar";

			Client.PutObject(BucketName, SrcKey);
			var Response = Client.GetObject(BucketName, SrcKey);

			var DstKey = "bar&foo";
			Client.CopyObject(BucketName, SrcKey, BucketName, DstKey, VersionId: Response.VersionId);
			Client.GetObject(BucketName, DstKey);
		}

		[TestMethod("test_object_copy_versioning_multipart_upload")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Multipart")]
		[TestProperty(MainData.Explanation, "[버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_copy_versioning_multipart_upload()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			var Size = 50 * MainData.MB;
			var Key1 = "srcmultipart";
			var Key1MetaData = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("x-amz-meta-foo", "bar") };
			var ContentType = "text/bla";

			var UploadData = SetupMultipartUpload(Client, BucketName, Key1, Size, ContentType: ContentType, MetadataList: Key1MetaData);
			Client.CompleteMultipartUpload(BucketName, Key1, UploadData.UploadId, UploadData.Parts);

			var Response = Client.GetObject(BucketName, Key1);
			var Key1Size = Response.ContentLength;
			var VersionId = Response.VersionId;

			var Key2 = "dstmultipart";
			Client.CopyObject(BucketName, Key1, BucketName, Key2, VersionId: VersionId);
			Response = Client.GetObject(BucketName, Key2);
			var VersionId2 = Response.VersionId;
			var Body = GetBody(Response);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Key1Size, Response.ContentLength);
			CollectionAssert.AreEqual(Key1MetaData, GetMetaData(Response.Metadata));
			Assert.AreEqual(ContentType, Response.Headers.ContentType);


			var Key3 = "dstmultipart2";
			Client.CopyObject(BucketName, Key2, BucketName, Key3, VersionId: VersionId2);
			Response = Client.GetObject(BucketName, Key3);
			Body = GetBody(Response);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Key1Size, Response.ContentLength);
			CollectionAssert.AreEqual(Key1MetaData, GetMetaData(Response.Metadata));
			Assert.AreEqual(ContentType, Response.Headers.ContentType);

			var BucketName2 = GetNewBucket();
			CheckConfigureVersioningRetry(BucketName2, VersionStatus.Enabled);
			var Key4 = "dstmultipart3";
			Client.CopyObject(BucketName, Key1, BucketName2, Key4, VersionId: VersionId);
			Response = Client.GetObject(BucketName2, Key4);
			Body = GetBody(Response);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Key1Size, Response.ContentLength);
			CollectionAssert.AreEqual(Key1MetaData, GetMetaData(Response.Metadata));
			Assert.AreEqual(ContentType, Response.Headers.ContentType);

			var BucketName3 = GetNewBucket();
			CheckConfigureVersioningRetry(BucketName3, VersionStatus.Enabled);
			var Key5 = "dstmultipart4";
			Client.CopyObject(BucketName, Key1, BucketName3, Key5, VersionId: VersionId);
			Response = Client.GetObject(BucketName3, Key5);
			Body = GetBody(Response);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Key1Size, Response.ContentLength);
			CollectionAssert.AreEqual(Key1MetaData, GetMetaData(Response.Metadata));
			Assert.AreEqual(ContentType, Response.Headers.ContentType);

			var Key6 = "dstmultipart5";
			Client.CopyObject(BucketName3, Key5, BucketName, Key6);
			Response = Client.GetObject(BucketName, Key6);
			Body = GetBody(Response);
			Assert.AreEqual(UploadData.Body, Body);
			Assert.AreEqual(Key1Size, Response.ContentLength);
			CollectionAssert.AreEqual(Key1MetaData, GetMetaData(Response.Metadata));
			Assert.AreEqual(ContentType, Response.Headers.ContentType);
		}

		[TestMethod("test_copy_object_ifmatch_good")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Imatch")]
		[TestProperty(MainData.Explanation, "ifmatch 값을 추가하여 오브젝트를 복사할 경우 성공확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_object_ifmatch_good()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var PutResponse = Client.PutObject(BucketName, "foo", Body: "bar");

			Client.CopyObject(BucketName, "foo", BucketName, "bar", ETagToMatch: PutResponse.ETag);
			var GetResponse = Client.GetObject(BucketName, "bar");
			var Body = GetBody(GetResponse);
			Assert.AreEqual("bar", Body);
		}

		[TestMethod("test_copy_object_ifmatch_failed")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "Imatch")]
		[TestProperty(MainData.Explanation, "ifmatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_copy_object_ifmatch_failed()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(BucketName, "foo", Body: "bar");

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.CopyObject(BucketName, "foo", BucketName, "bar", ETagToMatch: "ABCORZ"));
			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;
			Assert.AreEqual(HttpStatusCode.PreconditionFailed, StatusCode);
			Assert.AreEqual(HttpStatusCode.PreconditionFailed.ToString(), ErrorCode);
		}

		[TestMethod("test_copy_nor_src_to_nor_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_nor_src_to_nor_bucket_and_obj()
		{
			TestObjectCopy(false, false, false, false, 1024);
			TestObjectCopy(false, false, false, false, 256 * 1024);
			TestObjectCopy(false, false, false, false, 1024 * 1024);
		}

		[TestMethod("test_copy_nor_src_to_nor_bucket_encryption_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_nor_src_to_nor_bucket_encryption_obj()
		{
			TestObjectCopy(false, false, false, true, 1024);
			TestObjectCopy(false, false, false, true, 256 * 1024);
			TestObjectCopy(false, false, false, true, 1024 * 1024);
		}

		[TestMethod("test_copy_nor_src_to_encryption_bucket_nor_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_nor_src_to_encryption_bucket_nor_obj()
		{
			TestObjectCopy(false, false, true, false, 1024);
			TestObjectCopy(false, false, true, false, 256 * 1024);
			TestObjectCopy(false, false, true, false, 1024 * 1024);
		}

		[TestMethod("test_copy_nor_src_to_encryption_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_nor_src_to_encryption_bucket_and_obj()
		{
			TestObjectCopy(false, false, true, true, 1024);
			TestObjectCopy(false, false, true, true, 256 * 1024);
			TestObjectCopy(false, false, true, true, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_src_to_nor_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_src_to_nor_bucket_and_obj()
		{
			TestObjectCopy(true, false, false, false, 1024);
			TestObjectCopy(true, false, false, false, 256 * 1024);
			TestObjectCopy(true, false, false, false, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_src_to_nor_bucket_encryption_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_src_to_nor_bucket_encryption_obj()
		{
			TestObjectCopy(true, false, false, true, 1024);
			TestObjectCopy(true, false, false, true, 256 * 1024);
			TestObjectCopy(true, false, false, true, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_src_to_encryption_bucket_nor_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_src_to_encryption_bucket_nor_obj()
		{
			TestObjectCopy(true, false, true, false, 1024);
			TestObjectCopy(true, false, true, false, 256 * 1024);
			TestObjectCopy(true, false, true, false, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_src_to_encryption_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_src_to_encryption_bucket_and_obj()
		{
			TestObjectCopy(true, false, true, true, 1024);
			TestObjectCopy(true, false, true, true, 256 * 1024);
			TestObjectCopy(true, false, true, true, 1024 * 1024);
		}


		[TestMethod("test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj()
		{
			TestObjectCopy(false, true, false, false, 1024);
			TestObjectCopy(false, true, false, false, 256 * 1024);
			TestObjectCopy(false, true, false, false, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj()
		{
			TestObjectCopy(false, true, false, true, 1024);
			TestObjectCopy(false, true, false, true, 256 * 1024);
			TestObjectCopy(false, true, false, true, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj()
		{
			TestObjectCopy(false, true, true, false, 1024);
			TestObjectCopy(false, true, true, false, 256 * 1024);
			TestObjectCopy(false, true, true, false, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj()
		{
			TestObjectCopy(false, true, true, true, 1024);
			TestObjectCopy(false, true, true, true, 256 * 1024);
			TestObjectCopy(false, true, true, true, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj()
		{
			TestObjectCopy(true, true, false, false, 1024);
			TestObjectCopy(true, true, false, false, 256 * 1024);
			TestObjectCopy(true, true, false, false, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj()
		{
			TestObjectCopy(true, true, false, true, 1024);
			TestObjectCopy(true, true, false, true, 256 * 1024);
			TestObjectCopy(true, true, false, true, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj()
		{
			TestObjectCopy(true, true, true, false, 1024);
			TestObjectCopy(true, true, true, false, 256 * 1024);
			TestObjectCopy(true, true, true, false, 1024 * 1024);
		}

		[TestMethod("test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj()
		{
			TestObjectCopy(true, true, true, true, 1024);
			TestObjectCopy(true, true, true, true, 256 * 1024);
			TestObjectCopy(true, true, true, true, 1024 * 1024);
		}

		[TestMethod("test_copy_to_normal_source")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "일반 오브젝트에서 다양한 방식으로 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_to_normal_source()
		{
			var Size1 = 1024;
			var Size2 = 256 * 1024;
			var Size3 = 1024 * 1024;

			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, Size1);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, Size2);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, Size3);

			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, Size1);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, Size2);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, Size3);

			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, Size1);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, Size2);
			TestObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, Size3);
		}

		[TestMethod("test_copy_to_sse_s3_source")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_to_sse_s3_source()
		{
			var Size1 = 1024;
			var Size2 = 256 * 1024;
			var Size3 = 1024 * 1024;

			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, Size1);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, Size2);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, Size3);

			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, Size1);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, Size2);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, Size3);

			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, Size1);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, Size2);
			TestObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, Size3);
		}

		[TestMethod("test_copy_to_sse_c_source")]
		[TestProperty(MainData.Major, "CopyObject")]
		[TestProperty(MainData.Minor, "encryption")]
		[TestProperty(MainData.Explanation, "SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_copy_to_sse_c_source()
		{
			var Size1 = 1024;
			var Size2 = 256 * 1024;
			var Size3 = 1024 * 1024;

			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, Size1);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, Size2);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, Size3);

			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, Size1);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, Size2);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, Size3);

			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, Size1);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, Size2);
			TestObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, Size3);
		}
	}
}
