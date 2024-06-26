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
using System.Threading;
using Xunit;

namespace s3tests
{
	public class Versioning : TestBase
	{
		public Versioning(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact(DisplayName = "test_versioning_bucket_create_suspend")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 버저닝 옵션 변경 가능 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_bucket_create_suspend()
		{
			var BucketName = GetNewBucket();
			CheckVersioning(BucketName, VersionStatus.Off);

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Suspended);
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Suspended);
		}

		[Fact(DisplayName = "test_versioning_obj_create_read_remove")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버저닝 오브젝트의 생성/읽기/삭제 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_read_remove()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			Client.PutBucketVersioning(BucketName, EnableMfaDelete: false, VersionStatus.Enabled);
			var Key = "testobj";
			var NumVersions = 5;

			TestCreateRemoveVersions(Client, BucketName, Key, NumVersions, 4, -1);
			TestCreateRemoveVersions(Client, BucketName, Key, NumVersions, 0, 0);
		}

		[Fact(DisplayName = "test_versioning_obj_create_read_remove_head")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_read_remove_head()
		{
			var BucketName = GetNewBucket();

			var Client = GetClient();
			Client.PutBucketVersioning(BucketName, EnableMfaDelete: false, VersionStatus.Enabled);
			var Key = "testobj";
			var NumVersions = 5;

			List<string> VersionIds = null;
			List<string> Contents = null;
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			var RemovedVersionId = VersionIds[0];
			VersionIds.RemoveAt(0);
			Contents.RemoveAt(0);
			NumVersions--;

			Client.DeleteObject(BucketName, Key, VersionId: RemovedVersionId);
			var GetResponse = Client.GetObject(BucketName, Key);
			var Body = GetBody(GetResponse);
			Assert.Equal(Contents[^1], Body);

			var DelResponse = Client.DeleteObject(BucketName, Key);
			Assert.Equal("true", DelResponse.DeleteMarker);

			var DeleteMarkerVersionId = DelResponse.VersionId;
			VersionIds.Add(DeleteMarkerVersionId);

			var ListResponse = Client.ListVersions(BucketName);
			Assert.Equal(NumVersions, GetVersions(ListResponse.Versions).Count);
			Assert.Equal(1, GetDeleteMarkerCount(ListResponse.Versions));
			Assert.Equal(DeleteMarkerVersionId, GetDeleteMarkers(ListResponse.Versions)[0].VersionId);
		}

		[Fact(DisplayName = "test_versioning_obj_plain_null_version_removal")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_versioning_obj_plain_null_version_removal()
		{
			var BucketName = GetNewBucket();
			CheckVersioning(BucketName, VersionStatus.Off);

			var Client = GetClient();
			var Key = "testobjfoo";
			var Content = "fooz";
			Client.PutObject(BucketName, Key, Body: Content);

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			Client.DeleteObject(BucketName, Key, VersionId: "null");

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NoSuchKey, GetErrorCode(e));

			var ListResponse = Client.ListVersions(BucketName);
			Assert.Empty(ListResponse.Versions);
		}

		[Fact(DisplayName = "test_versioning_obj_plain_null_version_overwrite")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[버킷에 버저닝 설정이 되어있는 상태] " +
									 "null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_versioning_obj_plain_null_version_overwrite()
		{
			var BucketName = GetNewBucket();
			CheckVersioning(BucketName, VersionStatus.Off);

			var Client = GetClient();
			var Key = "testobjfoo";
			var Content = "fooz";
			Client.PutObject(BucketName, Key, Body: Content);

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Content2 = "zzz";
			Client.PutObject(BucketName, Key, Body: Content2);
			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.Equal(Content2, Body);

			var VersionId = Response.VersionId;
			Client.DeleteObject(BucketName, Key, VersionId: VersionId);
			Response = Client.GetObject(BucketName, Key);
			Body = GetBody(Response);
			Assert.Equal(Content, Body);

			Client.DeleteObject(BucketName, Key, VersionId: "null");

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NoSuchKey, GetErrorCode(e));

			var ListResponse = Client.ListVersions(BucketName);
			Assert.Empty(ListResponse.Versions);
		}

		[Fact(DisplayName = "test_versioning_obj_plain_null_version_overwrite_suspended")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[버킷에 버저닝 설정이 되어있지만 중단된 상태일때] " +
									 "null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_versioning_obj_plain_null_version_overwrite_suspended()
		{
			var BucketName = GetNewBucket();
			CheckVersioning(BucketName, VersionStatus.Off);

			var Client = GetClient();
			var Key = "testobjfoo";
			var Content = "fooz";
			Client.PutObject(BucketName, Key, Body: Content);

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Suspended);

			var Content2 = "zzz";
			Client.PutObject(BucketName, Key, Body: Content2);
			var Response = Client.GetObject(BucketName, Key);
			var Body = GetBody(Response);
			Assert.Equal(Content2, Body);

			var ListResponse = Client.ListVersions(BucketName);
			Assert.Single(ListResponse.Versions);

			Client.DeleteObject(BucketName, Key, VersionId: "null");

			var e = Assert.Throws<AggregateException>(() => Client.GetObject(BucketName, Key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NoSuchKey, GetErrorCode(e));
		}

		[Fact(DisplayName = "test_versioning_obj_suspend_versions")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버전관리를 일시중단했을때 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_suspend_versions()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			var Key = "testobj";
			var NumVersions = 5;

			List<string> VersionIds = null;
			List<string> Contents = null;
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Suspended);
			DeleteSuspendedVersioningObj(Client, BucketName, Key, ref VersionIds, ref Contents);
			DeleteSuspendedVersioningObj(Client, BucketName, Key, ref VersionIds, ref Contents);

			OverwriteSuspendedVersioningObj(Client, BucketName, Key, ref VersionIds, ref Contents, "null content 1");
			OverwriteSuspendedVersioningObj(Client, BucketName, Key, ref VersionIds, ref Contents, "null content 2");
			DeleteSuspendedVersioningObj(Client, BucketName, Key, ref VersionIds, ref Contents);
			OverwriteSuspendedVersioningObj(Client, BucketName, Key, ref VersionIds, ref Contents, "null content 3");
			DeleteSuspendedVersioningObj(Client, BucketName, Key, ref VersionIds, ref Contents);

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			SetupMultipleVersions(Client, BucketName, Key, 3, ref VersionIds, ref Contents);
			NumVersions += 3;

			for (int i = 0; i < NumVersions; i++)
				RemoveObjVersion(Client, BucketName, Key, VersionIds, Contents, 0);

			Assert.Empty(VersionIds);
			Assert.Empty(Contents);

		}

		[Fact(DisplayName = "test_versioning_obj_create_versions_remove_all")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "오브젝트하나의 여러버전을 모두 삭제 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_versions_remove_all()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "testobj";
			var NumVersions = 10;

			List<string> VersionIds = null;
			List<string> Contents = null;
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			for (int i = 0; i < NumVersions; i++)
				RemoveObjVersion(Client, BucketName, Key, VersionIds, Contents, 0);

			var Response = Client.ListVersions(BucketName);
			Assert.Empty(Response.Versions);
		}

		[Fact(DisplayName = "test_versioning_obj_create_versions_remove_special_names")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_versions_remove_special_names()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Keys = new List<string>() { "_testobj", "_", ":", " " };
			var NumVersions = 10;

			List<string> VersionIds = null;
			List<string> Contents = null;
			foreach (var Key in Keys)
			{
				SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

				for (int i = 0; i < NumVersions; i++)
					RemoveObjVersion(Client, BucketName, Key, VersionIds, Contents, 0);

				var Response = Client.ListVersions(BucketName);
				Assert.Empty(Response.Versions);
			}
		}

		[Fact(DisplayName = "test_versioning_obj_create_overwrite_multipart")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_overwrite_multipart()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "testobj";
			var NumVersions = 3;
			var VersionIds = new List<string>();
			var Contents = new List<string>();

			for (int i = 0; i < NumVersions; i++)
				Contents.Add(TestMultipartUploadContents(BucketName, Key, 3));

			var Response = Client.ListVersions(BucketName);
			foreach (var Version in Response.Versions) VersionIds.Add(Version.VersionId);

			VersionIds.Reverse();
			CheckObjVersions(Client, BucketName, Key, VersionIds, Contents);

			for (int i = 0; i < NumVersions; i++)
				RemoveObjVersion(Client, BucketName, Key, VersionIds, Contents, 0);

			Response = Client.ListVersions(BucketName);
			Assert.Empty(Response.Versions);
		}

		[Fact(DisplayName = "test_versioning_obj_list_marker")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트의 해당 버전 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_list_marker()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "testobj";
			var Key2 = "testobj-1";
			var NumVersions = 5;


			var VersionIds = new List<string>();
			var Contents = new List<string>();
			var VersionIds2 = new List<string>();
			var Contents2 = new List<string>();

			for (int i = 0; i < NumVersions; i++)
			{
				var Body = string.Format("content-{0}", i);
				var Response = Client.PutObject(BucketName, Key, Body: Body);
				var VersionId = Response.VersionId;

				Contents.Add(Body);
				VersionIds.Add(VersionId);
			}

			for (int i = 0; i < NumVersions; i++)
			{
				var Body = string.Format("content-{0}", i);
				var Response = Client.PutObject(BucketName, Key2, Body: Body);
				var VersionId = Response.VersionId;

				Contents2.Add(Body);
				VersionIds2.Add(VersionId);
			}

			var ListResponse = Client.ListVersions(BucketName);
			var Versions = GetVersions(ListResponse.Versions);
			Versions.Reverse();

			int index = 0;
			for (int i = 0; i < 5; i++, index++)
			{
				var Version = Versions[index];
				Assert.Equal(Version.VersionId, VersionIds2[i]);
				Assert.Equal(Version.Key, Key2);
				CheckObjContent(Client, BucketName, Key2, Version.VersionId, Contents2[i]);
			}

			for (int i = 0; i < 5; i++, index++)
			{
				var Version = Versions[index];
				Assert.Equal(Version.VersionId, VersionIds[i]);
				Assert.Equal(Version.Key, Key);
				CheckObjContent(Client, BucketName, Key, Version.VersionId, Contents[i]);
			}
		}

		[Fact(DisplayName = "test_versioning_copy_obj_version")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "오브젝트의 버전별 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_copy_obj_version()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "testobj";
			var NumVersions = 3;


			var VersionIds = new List<string>();
			var Contents = new List<string>();
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			for (int i = 0; i < NumVersions; i++)
			{
				var NewKeyName = string.Format("key_{0}", i);
				Client.CopyObject(BucketName, Key, BucketName, NewKeyName, VersionId: VersionIds[i]);
				var GetResponse = Client.GetObject(BucketName, NewKeyName);
				var Content = GetBody(GetResponse);
				Assert.Equal(Contents[i], Content);
			}

			var AnotherBucketName = GetNewBucket();

			for (int i = 0; i < NumVersions; i++)
			{
				var NewKeyName = string.Format("key_{0}", i);
				Client.CopyObject(BucketName, Key, AnotherBucketName, NewKeyName, VersionId: VersionIds[i]);
				var GetResponse = Client.GetObject(BucketName, NewKeyName);
				var Content = GetBody(GetResponse);
				Assert.Equal(Contents[i], Content);
			}

			var NewKeyName2 = "new_key";
			Client.CopyObject(BucketName, Key, AnotherBucketName, NewKeyName2);

			var Response = Client.GetObject(AnotherBucketName, NewKeyName2);
			var Body = GetBody(Response);
			Assert.Equal(Body, Contents[^1]);
		}

		[Fact(DisplayName = "test_versioning_multi_object_delete")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_multi_object_delete()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "key";
			var NumVersions = 2;

			var VersionIds = new List<string>();
			var Contents = new List<string>();
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			var ListResponse = Client.ListVersions(BucketName);
			var Versions = GetVersions(ListResponse.Versions);
			Versions.Reverse();

			foreach (var Version in Versions)
				Client.DeleteObject(BucketName, Key, VersionId: Version.VersionId);

			ListResponse = Client.ListVersions(BucketName);
			Assert.Empty(ListResponse.Versions);

			foreach (var Version in Versions)
				Client.DeleteObject(BucketName, Key, VersionId: Version.VersionId);

			ListResponse = Client.ListVersions(BucketName);
			Assert.Empty(ListResponse.Versions);
		}

		[Fact(DisplayName = "test_versioning_multi_object_delete_with_marker")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Delete Marker")]
		[Trait(MainData.Explanation, "버전이 여러개인 오브젝트에 대한 삭제마커가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_multi_object_delete_with_marker()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "key";
			var NumVersions = 2;

			var VersionIds = new List<string>();
			var Contents = new List<string>();
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			Client.DeleteObject(BucketName, Key);
			var Response = Client.ListVersions(BucketName);
			var Versions = GetVersions(Response.Versions);
			var DeleteMarkers = GetDeleteMarkers(Response.Versions);

			VersionIds.Add(DeleteMarkers[0].VersionId);
			Assert.Equal(3, VersionIds.Count);
			Assert.Single(DeleteMarkers);

			foreach (var Version in Versions)
				Client.DeleteObject(BucketName, Key, VersionId: Version.VersionId);

			foreach (var DeleteMarker in DeleteMarkers)
				Client.DeleteObject(BucketName, Key, VersionId: DeleteMarker.VersionId);

			Response = Client.ListVersions(BucketName);
			Assert.Empty(GetVersions(Response.Versions));
			Assert.Empty(GetDeleteMarkers(Response.Versions));


			foreach (var Version in Versions)
				Client.DeleteObject(BucketName, Key, VersionId: Version.VersionId);

			foreach (var DeleteMarker in DeleteMarkers)
				Client.DeleteObject(BucketName, Key, VersionId: DeleteMarker.VersionId);

			Response = Client.ListVersions(BucketName);
			Assert.Empty(GetVersions(Response.Versions));
			Assert.Empty(GetDeleteMarkers(Response.Versions));
		}

		[Fact(DisplayName = "test_versioning_multi_object_delete_with_marker_create")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Delete Marker")]
		[Trait(MainData.Explanation, "존재하지않는 오브젝트를 삭제할경우 삭제마커가 생성되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_multi_object_delete_with_marker_create()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "key";

			var DelResponse = Client.DeleteObject(BucketName, Key);
			var DeleteMarkerVersionId = DelResponse.VersionId;

			var Response = Client.ListVersions(BucketName);
			var DeleteMarker = GetDeleteMarkers(Response.Versions);

			Assert.Single(DeleteMarker);
			Assert.Equal(DeleteMarkerVersionId, DeleteMarker[0].VersionId);
			Assert.Equal(Key, DeleteMarker[0].Key);
		}

		[Fact(DisplayName = "test_versioned_object_acl")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioned_object_acl()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "xyz";
			var NumVersions = 3;

			var VersionIds = new List<string>();
			var Contents = new List<string>();
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			var VersionId = VersionIds[1];

			var Response = Client.GetObjectACL(BucketName, Key, VersionId: VersionId);

			var DisplayName = Config.MainUser.DisplayName;
			var UserId = Config.MainUser.UserId;

			if (!Config.S3.IsAWS) Assert.Equal(DisplayName, Response.AccessControlList.Owner.DisplayName);
			Assert.Equal(UserId, Response.AccessControlList.Owner.Id);

			var GetGrants = Response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new S3Grant()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new S3Grantee()
					{
						CanonicalUser = UserId,
						DisplayName = DisplayName,
						URI = null,
						EmailAddress = null,
					}
				},
			},
			GetGrants);
		}

		[Fact(DisplayName = "test_versioned_object_acl_no_version_specified")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioned_object_acl_no_version_specified()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "xyz";
			var NumVersions = 3;

			var VersionIds = new List<string>();
			var Contents = new List<string>();
			SetupMultipleVersions(Client, BucketName, Key, NumVersions, ref VersionIds, ref Contents);

			var GetResponse = Client.GetObject(BucketName, Key);
			var VersionId = GetResponse.VersionId;

			var Response = Client.GetObjectACL(BucketName, Key, VersionId: VersionId);

			var DisplayName = Config.MainUser.DisplayName;
			var UserId = Config.MainUser.UserId;

			if (!Config.S3.IsAWS) Assert.Equal(DisplayName, Response.AccessControlList.Owner.DisplayName);
			Assert.Equal(UserId, Response.AccessControlList.Owner.Id);

			var GetGrants = Response.AccessControlList.Grants;
			var DefaultPolicy = new List<S3Grant>()
			{
				new S3Grant()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new S3Grantee()
					{
						CanonicalUser = UserId,
						DisplayName = DisplayName,
						URI = null,
						EmailAddress = null,
					}
				},
			};
			CheckGrants(DefaultPolicy, GetGrants);

			Client.PutObjectACL(BucketName, Key, ACL: S3CannedACL.PublicRead);

			Response = Client.GetObjectACL(BucketName, Key, VersionId: VersionId);
			GetGrants = Response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new S3Grant()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new S3Grantee()
					{
						CanonicalUser = UserId,
						DisplayName = DisplayName,
						URI = null,
						EmailAddress = null,
					}
				},
				new S3Grant()
				{
					Permission = S3Permission.READ,
					Grantee = new S3Grantee()
					{
						CanonicalUser = null,
						DisplayName = null,
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
						EmailAddress = null,
					}
				},
			},
			GetGrants);
		}

		[Fact(DisplayName = "test_versioned_concurrent_object_create_and_remove")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioned_concurrent_object_create_and_remove()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var Key = "myobj";
			var NumVersions = 3;

			var AllTasks = new List<Thread>();

			for (int i = 0; i < 3; i++)
			{
				var TList = SetupVersionedObjConcurrent(Client, BucketName, Key, NumVersions);
				AllTasks.AddRange(TList);

				var TList2 = DoClearVersionedBucketConcurrent(Client, BucketName);
				AllTasks.AddRange(TList2);
			}

			foreach (var mTask in AllTasks) mTask.Join();

			var TList3 = DoClearVersionedBucketConcurrent(Client, BucketName);
			foreach (var mTask in TList3) mTask.Join();

			var Response = Client.ListVersions(BucketName);
			Assert.Empty(Response.Versions);
		}

		[Fact(DisplayName = "test_versioning_bucket_atomic_upload_return_version_id")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_bucket_atomic_upload_return_version_id()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "bar";

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			var PutResponse = Client.PutObject(BucketName, Key);
			var VersionId = PutResponse.VersionId;

			var ListResponse = Client.ListVersions(BucketName);
			var Versions = GetVersions(ListResponse.Versions);
			foreach (var Version in Versions)
				Assert.Equal(VersionId, Version.VersionId);

			BucketName = GetNewBucket();
			Key = "baz";
			PutResponse = Client.PutObject(BucketName, Key);
			Assert.Null(PutResponse.VersionId);

			BucketName = GetNewBucket();
			Key = "baz";
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Suspended);
			PutResponse = Client.PutObject(BucketName, Key);
			Assert.Null(PutResponse.VersionId);
		}

		[Fact(DisplayName = "test_versioning_bucket_multipart_upload_return_version_id")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "MultiPart")]
		[Trait(MainData.Explanation, "버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_bucket_multipart_upload_return_version_id()
		{
			var ContentType = "text/bla";
			var Size = 50 * MainData.MB;

			var BucketName = GetNewBucket();
			var Client = GetClient();
			var Key = "bar";
			var Metadata = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("foo", "baz") };

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: Metadata, ContentType: ContentType);

			var CompResponse = Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);
			var VersionId = CompResponse.VersionId;

			var ListResponse = Client.ListVersions(BucketName);
			var Versions = GetVersions(ListResponse.Versions);
			foreach (var Version in Versions)
				Assert.Equal(VersionId, Version.VersionId);

			BucketName = GetNewBucket();
			Key = "baz";

			UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: Metadata, ContentType: ContentType);
			CompResponse = Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);
			Assert.Null(CompResponse.VersionId);

			BucketName = GetNewBucket();
			Key = "foo";

			UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList: Metadata, ContentType: ContentType);
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Suspended);
			CompResponse = Client.CompleteMultipartUpload(BucketName, Key, UploadData.UploadId, UploadData.Parts);
			Assert.Null(CompResponse.VersionId);
		}

		[Fact(DisplayName = "test_versioning_get_object_head")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_get_object_head()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var KeyName = "foo";
			var VersionList = new List<string>();

			for (int i = 1; i <= 5; i++)
			{
				var Response = Client.PutObject(BucketName, Key: KeyName, RandomTextToLong(i));
				VersionList.Add(Response.VersionId);
			}

			for (int i = 0; i < 5; i++)
			{
				var Response = Client.GetObjectMetadata(BucketName, Key: KeyName, VersionId: VersionList[i]);
				Assert.Equal(i + 1, Response.ContentLength);
			}
		}

		// 버전이 여러개인 오브젝트의 최신 버전을 삭제 했을때 이전버전이 최신버전으로 변경되는지 확인
		[Fact(DisplayName = "test_versioning_latest")]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_latest()
		{
			var BucketName = GetNewBucket();
			var Client = GetClient();
			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);

			var KeyName = "foo";
			var VersionStack = new Stack<string>();

			for (int i = 1; i <= 5; i++)
			{
				var Response = Client.PutObject(BucketName, KeyName, RandomTextToLong(i));
				VersionStack.Push(Response.VersionId);
			}

			var LastVersionId = VersionStack.Pop();
			while (VersionStack.Count > 1)
			{
				Client.DeleteObject(BucketName, KeyName, LastVersionId);
				LastVersionId = VersionStack.Pop();
				
				var Response = Client.GetObjectMetadata(BucketName, KeyName);
				Assert.Equal(LastVersionId, Response.VersionId);
			}
		}
	}
}
