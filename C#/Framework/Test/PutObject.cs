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
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace s3tests2
{
	[TestClass]
	public class PutObject : TestBase
	{
		[TestMethod("test_bucket_list_distinct")]
		[TestProperty(MainData.Major, "Bucket")]
		[TestProperty(MainData.Minor, "PUT")]
		[TestProperty(MainData.Explanation, "오브젝트가 올바르게 생성되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_distinct()
		{
			var Bucket1 = GetNewBucket();
			var Bucket2 = GetNewBucket();
			var Client = GetClient();

			Client.PutObject(Bucket1, "str");

			var Response1 = Client.ListObjects(Bucket1);
			Assert.AreEqual(1, Response1.S3Objects.Count);

			var Response2 = Client.ListObjects(Bucket2);
			Assert.AreEqual(0, Response2.S3Objects.Count);
		}

		[TestMethod("test_object_write_to_nonexist_bucket")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "ERROR")]
		[TestProperty(MainData.Explanation, "존재하지 않는 버킷에 오브젝트 업로드할 경우 실패")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_write_to_nonexist_bucket()
		{
			var KeyName = "foo";
			var BucketName = "whatchutalkinboutwillis";
			var Client = GetClient();

			var e = Assert.ThrowsException<AmazonS3Exception>(() => Client.PutObject(BucketName, Key: KeyName, Body: KeyName));

			var StatusCode = e.StatusCode;
			var ErrorCode = e.ErrorCode;

			Assert.AreEqual(HttpStatusCode.NotFound, StatusCode);
			Assert.AreEqual(MainData.NoSuchBucket, ErrorCode);
		}

		[TestMethod("test_object_head_zero_bytes")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "0바이트로 업로드한 오브젝트가 실제로 0바이트인지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_head_zero_bytes()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var KeyName = "foo";
			Client.PutObject(BucketName, Key: KeyName, Body: "");

			var Response = Client.GetObjectMetadata(BucketName, Key: KeyName);
			Assert.AreEqual(0, Response.ContentLength);
		}

		[TestMethod("test_object_write_check_etag")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "업로드한 오브젝트의 ETag가 올바른지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_write_check_etag()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var Response = Client.PutObject(BucketName, Key: "foo", Body: "bar");
			Assert.AreEqual(HttpStatusCode.OK, Response.HttpStatusCode);
			Assert.AreEqual("\"37b51d194a7513e45b56f6524f2d51f2\"", Response.ETag);
		}

		[TestMethod("test_object_write_cache_control")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, " Cache control")]
		[TestProperty(MainData.Explanation, "캐시를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_write_cache_control()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var KeyName = "foo";
			var Body = "bar";
			var CacheControl = "public, max-age=14400";
			Client.PutObject(BucketName, Key: KeyName, Body: Body, CacheControl: CacheControl);

			var Response = Client.GetObjectMetadata(BucketName, Key: KeyName);
			Assert.AreEqual(CacheControl, Response.Metadata[S3Headers.CacheControl]);
		}

		[TestMethod("test_object_write_expires")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Expires")]
		[TestProperty(MainData.Explanation, "헤더만료일시(날짜)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_write_expires()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			var KeyName = "foo";
			var Body = "bar";
			var Expires = DateTime.UtcNow;
			Expires.AddSeconds(6000);
			Client.PutObject(BucketName, Key: KeyName, Body: Body, Expires: Expires);

			var Response = Client.GetObjectMetadata(BucketName, Key: KeyName);
			Assert.AreEqual(TimeToString(Expires), Response.Metadata[S3Headers.Expires]);
		}

		[TestMethod("test_object_write_read_update_read_delete")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Update")]
		[TestProperty(MainData.Explanation, "오브젝트의 기본 작업을 모두 올바르게 할 수 있는지 확인(read, write, update, delete)")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_write_read_update_read_delete()
		{
			var Client = GetClient();
			var BucketName = GetNewBucket();

			var KeyName = "foo";
			var Body = "bar";

			//Write
			Client.PutObject(BucketName, Key: KeyName, Body: Body);

			//Read
			var GetResponse = Client.GetObject(BucketName, Key: KeyName);
			var ResponseBody = GetBody(GetResponse);
			Assert.AreEqual(Body, ResponseBody);

			//Update
			var Body2 = "soup";
			Client.PutObject(BucketName, Key: KeyName, Body: Body2);

			//Read
			GetResponse = Client.GetObject(BucketName, Key: KeyName);
			ResponseBody = GetBody(GetResponse);
			Assert.AreEqual(Body2, ResponseBody);

			//Delete
			Client.DeleteObject(BucketName, KeyName);
		}

		[TestMethod("test_object_set_get_metadata_none_to_good")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "오브젝트에 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_set_get_metadata_none_to_good()
		{
			var MyMeta = "mymeta";
			var got = SetupMetadata(MyMeta);
			Assert.AreEqual(MyMeta, got);
		}

		[TestMethod("test_object_set_get_metadata_none_to_empty")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "오브젝트에 빈 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_set_get_metadata_none_to_empty()
		{
			var got = SetupMetadata("");
			Assert.IsTrue(string.IsNullOrWhiteSpace(got));
		}

		[TestMethod("test_object_set_get_metadata_overwrite_to_empty")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "메타 데이터 업데이트가 올바르게 적용되었는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_set_get_metadata_overwrite_to_empty()
		{
			var BucketName = GetNewBucket();

			var MyMeta = "oldmata";
			var got = SetupMetadata(MyMeta, BucketName: BucketName);
			Assert.AreEqual(MyMeta, got);

			got = SetupMetadata("", BucketName: BucketName);
			Assert.IsTrue(string.IsNullOrWhiteSpace(got));
		}

		[TestMethod("test_object_set_get_non_utf8_metadata")]
		[Ignore("NetFramework에서는 매끄럽게 예외처리되지 못함")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "메타데이터에 올바르지 않는 문자열[EOF(\x04)]를 사용할 경우 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_set_get_non_utf8_metadata()
		{
			var Metadata = "\x04mymeta";
			var e = TestMetadataUnreadable(Metadata);
			Assert.IsTrue(ErrorCheck(e.StatusCode));
		}

		[TestMethod("test_object_set_get_metadata_empty_to_unreadable_prefix")]
		[Ignore("NetFramework에서는 매끄럽게 예외처리되지 못함")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "메타데이터에 올바르지 않는 문자[EOF(\x04)]를 문자열 맨앞에 사용할 경우 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_set_get_metadata_empty_to_unreadable_prefix()
		{
			var Metadata = "\x04w";
			var e = TestMetadataUnreadable(Metadata);
			Assert.IsTrue(ErrorCheck(e.StatusCode));
		}

		[TestMethod("test_object_set_get_metadata_empty_to_unreadable_suffix")]
		[Ignore("NetFramework에서는 매끄럽게 예외처리되지 못함")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "메타데이터에 올바르지 않는 문자[EOF(\x04)]를 문자열 맨뒤에 사용할 경우 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_set_get_metadata_empty_to_unreadable_suffix()
		{
			var Metadata = "h\x04";
			var e = TestMetadataUnreadable(Metadata);
			Assert.IsTrue(ErrorCheck(e.StatusCode));
		}

		[TestMethod("test_object_set_get_metadata_empty_to_unreadable_infix")]
		[Ignore("AWS에서 문자열 중간에 읽을수 없는 특수문자는 무시하고 저장하기 때문에 에러가 발생하지 않음.")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "메타데이터에 올바르지 않는 문자[EOF(\x04)]를 문자열 중간에 사용할 경우 실패 확인")]
		[TestProperty(MainData.Result, MainData.ResultFailure)]
		public void test_object_set_get_metadata_empty_to_unreadable_infix()
		{
			var Metadata = "h\x04w";
			var e = TestMetadataUnreadable(Metadata);
			Assert.IsTrue(ErrorCheck(e.StatusCode));
		}

		[TestMethod("test_object_metadata_replaced_on_put")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Metadata")]
		[TestProperty(MainData.Explanation, "오브젝트를 메타데이타 없이 덮어쓰기 했을 때, 메타데이타 값이 비어있는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_metadata_replaced_on_put()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var MetadataList = new List<KeyValuePair<string, string>>
			{
				new KeyValuePair<string, string>("x-amz-meta-mata1", "bar")
			};

			var KeyName = "foo";
			var Body = "bar";
			Client.PutObject(BucketName, KeyName, Body: Body, MetadataList: MetadataList);
			Client.PutObject(BucketName, KeyName, Body: Body);

			var Response = Client.GetObject(BucketName, KeyName);
			var got = Response.Metadata;
			Assert.AreEqual(0, got.Count);
		}

		[TestMethod("test_object_write_file")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Incoding")]
		[TestProperty(MainData.Explanation, "body의 내용을utf-8로 인코딩한 오브젝트를 업로드 했을때 올바르게 업로드 되었는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_write_file()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var KeyName = "foo";
			var Data_str = "bar";
			var Data = Encoding.UTF8.GetBytes(Data_str);

			Client.PutObject(BucketName, KeyName, ByteBody: Data);

			var Response = Client.GetObject(BucketName, KeyName);
			var Body = GetBody(Response);
			Assert.AreEqual(Data_str, Body);
		}

		[TestMethod("test_bucket_create_special_key_names")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Special Key Name")]
		[TestProperty(MainData.Explanation, "오브젝트 이름과 내용이 모두 특수문자인 오브젝트 여러개를 업로드 할 경우 모두 재대로 업로드 되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_create_special_key_names()
		{
			var KeyNames = new List<string>() { " ", "\"", "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__", };

			var BucketName = SetupObjects(KeyNames);

			var ObjectList = GetObjectList(BucketName);

			var Client = GetClient();

			foreach (var Name in KeyNames)
			{
				if (!string.IsNullOrWhiteSpace(Name)) Assert.IsTrue(ObjectList.Contains(Name));
				var Response = Client.GetObject(BucketName, Name);
				var Body = GetBody(Response);
				Assert.AreEqual(Name, Body);
				Client.PutObjectACL(BucketName, Name, ACL: S3CannedACL.Private);
			}
		}

		[TestMethod("test_bucket_list_special_prefix")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Special Key Name")]
		[TestProperty(MainData.Explanation, "[_], [/]가 포함된 이름을 가진 오브젝트를 업로드 한뒤 prefix정보를 설정한 GetObjectList가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_list_special_prefix()
		{
			var KeyNames = new List<string>() { "_bla/1", "_bla/2", "_bla/3", "_bla/4", "abcd" };

			var BucketName = SetupObjects(KeyNames);

			var ObjectList = GetObjectList(BucketName);
			Assert.AreEqual(5, ObjectList.Count);

			ObjectList = GetObjectList(BucketName, Prefix: "_bla/");
			Assert.AreEqual(4, ObjectList.Count);
		}

		[TestMethod("test_object_lock_uploading_obj")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Lock")]
		[TestProperty(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
									 "LegalHold와 Lock유지기한을 설정하여 오브젝트 업로드할 경우 설정이 적용되는지 메타데이터를 통해 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_uploading_obj()
		{
			var BucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

			var Key = "file1";
			var Body = "abc";
			var MD5 = GetMD5(Body);
			var PutResponse = Client.PutObject(BucketName, Key, Body: "abc", MD5Digest: MD5, ObjectLockMode: ObjectLockMode.Governance,
				ObjectLockRetainUntilDate: new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
				ObjectLockLegalHoldStatus: ObjectLockLegalHoldStatus.On);

			var Response = Client.GetObjectMetadata(BucketName, Key);
			Assert.AreEqual(ObjectLockMode.Governance, Response.ObjectLockMode);
			Assert.AreEqual(new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc), Response.ObjectLockRetainUntilDate.ToUniversalTime());
			Assert.AreEqual(ObjectLockLegalHoldStatus.On, Response.ObjectLockLegalHoldStatus);

			var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			Client.PutObjectLegalHold(BucketName, Key, LegalHold);
			Client.DeleteObject(BucketName, Key, VersionId: PutResponse.VersionId, BypassGovernanceRetention: true);
		}

		[TestMethod("test_object_infix_space")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Space")]
		[TestProperty(MainData.Explanation, "오브젝트의 중간에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_infix_space()
		{
			var KeyNames = new List<string>() { "a a/", "b b/f1", "c/f 2", "d d/f 3" };
			var BucketName = SetupObjects(KeyNames, Body: "");
			var Client = GetClient();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_object_suffix_space")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Space")]
		[TestProperty(MainData.Explanation, "오브젝트의 마지막에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_suffix_space()
		{
			var KeyNames = new List<string>() { "a /", "b /f1", "c/f2 ", "d /f3 " };
			var BucketName = SetupObjects(KeyNames, Body: "");
			var Client = GetClient();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_empty_object_signature_version_2")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Special Characters")]
		[TestProperty(MainData.Explanation, "[SignatureVersion2] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_empty_object_signature_version_2()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjects(KeyNames, Body: "");
			var Client = GetClient();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_empty_object_signature_version_4")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Special Characters")]
		[TestProperty(MainData.Explanation, "[SignatureVersion4] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_empty_object_signature_version_4()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjectsV4(KeyNames, Body: "");
			var Client = GetClientV4();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_object_signature_version_2")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Special Characters")]
		[TestProperty(MainData.Explanation, "[SignatureVersion2] 특수문자를 포함한 비어있는 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_signature_version_2()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjects(KeyNames);
			var Client = GetClient();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_object_signature_version_4")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Special Characters")]
		[TestProperty(MainData.Explanation, "[SignatureVersion4] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_signature_version_4()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjectsV4(KeyNames);
			var Client = GetClientV4();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_object_use_chunk_encoding")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Encoding")]
		[TestProperty(MainData.Explanation, "[SignatureVersion4, UseChunkEncoding = true] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_use_chunk_encoding()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjectsV4(KeyNames, UseChunkEncoding: true);
			var Client = GetClientV4();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_object_use_chunk_encoding_and_disable_payload_signing")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Encoding")]
		[TestProperty(MainData.Explanation, "[SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_use_chunk_encoding_and_disable_payload_signing()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjectsV4(KeyNames, UseChunkEncoding: true, DisablePayloadSigning: true);
			var Client = GetClientHttpsV4();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_object_not_chunk_encoding")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Encoding")]
		[TestProperty(MainData.Explanation, "[SignatureVersion4, UseChunkEncoding = false] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_not_chunk_encoding()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjectsV4(KeyNames, UseChunkEncoding: false);
			var Client = GetClientV4();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_object_not_chunk_encoding_and_disable_payload_signing")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Encoding")]
		[TestProperty(MainData.Explanation, "[SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 특수문자를 포함한 오브젝트 업로드 성공 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_not_chunk_encoding_and_disable_payload_signing()
		{
			var KeyNames = new List<string>() { "t !", "t $", "t '", "t (", "t )", "t *", "t :", "t [", "t ]" };
			var BucketName = SetupObjectsV4(KeyNames, UseChunkEncoding: false, DisablePayloadSigning: true);
			var Client = GetClientHttpsV4();

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);

			CollectionAssert.AreEqual(KeyNames, Keys);
		}

		[TestMethod("test_put_object_dir_and_file")]
		[TestProperty(MainData.Major, "PutObject")]
		[TestProperty(MainData.Minor, "Directory")]
		[TestProperty(MainData.Explanation, "폴더의 이름과 동일한 오브젝트 업로드가 가능한지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_put_object_dir_and_file()
		{
			//file first
			var BucketName = GetNewBucket();
			var ObjectName = "aaa";
			var DirectoryName = "aaa/";
			var Client = GetClient();

			Client.PutObject(BucketName, ObjectName, Body: ObjectName);
			Client.PutObject(BucketName, DirectoryName, Body: "");

			var Response = Client.ListObjects(BucketName);
			var Keys = GetKeys(Response);
			Assert.AreEqual(2, Keys.Count);

			//dir first
			var BucketName2 = GetNewBucket();

			Client.PutObject(BucketName2, DirectoryName, Body: "");
			Client.PutObject(BucketName2, ObjectName, Body: ObjectName);

			Response = Client.ListObjects(BucketName2);
			Keys = GetKeys(Response);
			Assert.AreEqual(2, Keys.Count);

			//etc
			var BucketName3 = GetNewBucket();
			var NewObjectName = "aaa/bbb/ccc";

			Client.PutObject(BucketName3, ObjectName, Body: ObjectName);
			Client.PutObject(BucketName3, NewObjectName, Body: NewObjectName);

			Response = Client.ListObjects(BucketName3);
			Keys = GetKeys(Response);
			Assert.AreEqual(2, Keys.Count);
		}
		
		[TestMethod("test_object_emoji")]
		[TestProperty(MainData.Major, "Bucket")]
		[TestProperty(MainData.Minor, "PUT")]
		[TestProperty(MainData.Explanation, "오브젝트 이름에 이모지가 포함될 경우 올바르게 업로드 되는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_object_emoji()
		{
			var Bucket = GetNewBucket();
			var Client = GetClient();
			var Key = "test❤🍕🍔🚗";

			Client.PutObject(Bucket, Key);

			var Response1 = Client.ListObjects(Bucket);
			Assert.AreEqual(1, Response1.S3Objects.Count);
		}
	}
}
