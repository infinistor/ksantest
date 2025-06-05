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

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 버저닝 옵션 변경 가능 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningBucketCreateSuspend()
		{
			var bucketName = GetNewBucket();
			CheckVersioning(bucketName, VersionStatus.Off);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버저닝 오브젝트의 생성/읽기/삭제 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningObjCreateReadRemove()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketVersioning(bucketName, status: VersionStatus.Enabled);
			var key = "testobj";
			var numVersions = 5;

			TestCreateRemoveVersions(client, bucketName, key, numVersions, 4, -1);
			TestCreateRemoveVersions(client, bucketName, key, numVersions, 0, 0);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_read_remove_head()
		{
			var bucketName = GetNewBucket();

			var client = GetClient();
			client.PutBucketVersioning(bucketName, status: VersionStatus.Enabled);
			var key = "testobj";
			var numVersions = 5;

			List<string> versionIds = null;
			List<string> contents = null;
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			var removedVersionId = versionIds[0];
			versionIds.RemoveAt(0);
			contents.RemoveAt(0);
			numVersions--;

			client.DeleteObject(bucketName, key, versionId: removedVersionId);
			var getResponse = client.GetObject(bucketName, key);
			var body = GetBody(getResponse);
			Assert.Equal(contents[^1], body);

			var delResponse = client.DeleteObject(bucketName, key);
			Assert.Equal("true", delResponse.DeleteMarker);

			var deleteMarkerVersionId = delResponse.VersionId;
			versionIds.Add(deleteMarkerVersionId);

			var listResponse = client.ListVersions(bucketName);
			Assert.Equal(numVersions, GetVersions(listResponse.Versions).Count);
			Assert.Equal(1, GetDeleteMarkerCount(listResponse.Versions));
			Assert.Equal(deleteMarkerVersionId, GetDeleteMarkers(listResponse.Versions)[0].VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_versioning_obj_plain_null_version_removal()
		{
			var bucketName = GetNewBucket();
			CheckVersioning(bucketName, VersionStatus.Off);

			var client = GetClient();
			var key = "testobjfoo";
			var content = "fooz";
			client.PutObject(bucketName, key, body: content);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			client.DeleteObject(bucketName, key, versionId: "null");

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));

			var listResponse = client.ListVersions(bucketName);
			Assert.Empty(listResponse.Versions);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[버킷에 버저닝 설정이 되어있는 상태] " +
									 "null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_versioning_obj_plain_null_version_overwrite()
		{
			var bucketName = GetNewBucket();
			CheckVersioning(bucketName, VersionStatus.Off);

			var client = GetClient();
			var key = "testobjfoo";
			var content = "fooz";
			client.PutObject(bucketName, key, body: content);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var content2 = "zzz";
			client.PutObject(bucketName, key, body: content2);
			var response = client.GetObject(bucketName, key);
			var body = GetBody(response);
			Assert.Equal(content2, body);

			var versionId = response.VersionId;
			client.DeleteObject(bucketName, key, versionId: versionId);
			response = client.GetObject(bucketName, key);
			body = GetBody(response);
			Assert.Equal(content, body);

			client.DeleteObject(bucketName, key, versionId: "null");

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));

			var listResponse = client.ListVersions(bucketName);
			Assert.Empty(listResponse.Versions);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[버킷에 버저닝 설정이 되어있지만 중단된 상태일때] " +
									 "null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_versioning_obj_plain_null_version_overwrite_suspended()
		{
			var bucketName = GetNewBucket();
			CheckVersioning(bucketName, VersionStatus.Off);

			var client = GetClient();
			var key = "testobjfoo";
			var content = "fooz";
			client.PutObject(bucketName, key, body: content);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);

			var content2 = "zzz";
			client.PutObject(bucketName, key, body: content2);
			var response = client.GetObject(bucketName, key);
			var body = GetBody(response);
			Assert.Equal(content2, body);

			var listResponse = client.ListVersions(bucketName);
			Assert.Single(listResponse.Versions);

			client.DeleteObject(bucketName, key, versionId: "null");

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버전관리를 일시중단했을때 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_suspend_versions()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var key = "testobj";
			var numVersions = 5;

			List<string> versionIds = null;
			List<string> contents = null;
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			DeleteSuspendedVersioningObj(client, bucketName, key, ref versionIds, ref contents);
			DeleteSuspendedVersioningObj(client, bucketName, key, ref versionIds, ref contents);

			OverwriteSuspendedVersioningObj(client, bucketName, key, ref versionIds, ref contents, "null content 1");
			OverwriteSuspendedVersioningObj(client, bucketName, key, ref versionIds, ref contents, "null content 2");
			DeleteSuspendedVersioningObj(client, bucketName, key, ref versionIds, ref contents);
			OverwriteSuspendedVersioningObj(client, bucketName, key, ref versionIds, ref contents, "null content 3");
			DeleteSuspendedVersioningObj(client, bucketName, key, ref versionIds, ref contents);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			SetupMultipleVersions(client, bucketName, key, 3, ref versionIds, ref contents);
			numVersions += 3;

			for (int i = 0; i < numVersions; i++)
				RemoveObjVersion(client, bucketName, key, versionIds, contents, 0);

			Assert.Empty(versionIds);
			Assert.Empty(contents);

		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "오브젝트하나의 여러버전을 모두 삭제 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_versions_remove_all()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "testobj";
			var numVersions = 10;

			List<string> versionIds = null;
			List<string> contents = null;
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			for (int i = 0; i < numVersions; i++)
				RemoveObjVersion(client, bucketName, key, versionIds, contents, 0);

			var response = client.ListVersions(bucketName);
			Assert.Empty(response.Versions);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_versions_remove_special_names()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var keys = new List<string>() { "_testobj", "_", ":", " " };
			var numVersions = 10;

			List<string> versionIds = null;
			List<string> contents = null;
			foreach (var key in keys)
			{
				SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

				for (int i = 0; i < numVersions; i++)
					RemoveObjVersion(client, bucketName, key, versionIds, contents, 0);

				var response = client.ListVersions(bucketName);
				Assert.Empty(response.Versions);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_create_overwrite_multipart()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "testobj";
			var numVersions = 3;
			var versionIds = new List<string>();
			var contents = new List<string>();

			for (int i = 0; i < numVersions; i++)
				contents.Add(TestMultipartUploadContents(bucketName, key, 3));

			var response = client.ListVersions(bucketName);
			foreach (var version in response.Versions) versionIds.Add(version.VersionId);

			versionIds.Reverse();
			CheckObjVersions(client, bucketName, key, versionIds, contents);

			for (int i = 0; i < numVersions; i++)
				RemoveObjVersion(client, bucketName, key, versionIds, contents, 0);

			response = client.ListVersions(bucketName);
			Assert.Empty(response.Versions);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트의 해당 버전 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_obj_list_marker()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "testobj";
			var key2 = "testobj-1";
			var numVersions = 5;


			var versionIds = new List<string>();
			var contents = new List<string>();
			var versionIds2 = new List<string>();
			var contents2 = new List<string>();

			for (int i = 0; i < numVersions; i++)
			{
				var body = string.Format("content-{0}", i);
				var response = client.PutObject(bucketName, key, body: body);
				var versionId = response.VersionId;

				contents.Add(body);
				versionIds.Add(versionId);
			}

			for (int i = 0; i < numVersions; i++)
			{
				var body = string.Format("content-{0}", i);
				var response = client.PutObject(bucketName, key2, body: body);
				var versionId = response.VersionId;

				contents2.Add(body);
				versionIds2.Add(versionId);
			}

			var listResponse = client.ListVersions(bucketName);
			var versions = GetVersions(listResponse.Versions);
			versions.Reverse();

			int index = 0;
			for (int i = 0; i < 5; i++, index++)
			{
				var version = versions[index];
				Assert.Equal(version.VersionId, versionIds2[i]);
				Assert.Equal(version.Key, key2);
				CheckObjContent(client, bucketName, key2, version.VersionId, contents2[i]);
			}

			for (int i = 0; i < 5; i++, index++)
			{
				var version = versions[index];
				Assert.Equal(version.VersionId, versionIds[i]);
				Assert.Equal(version.Key, key);
				CheckObjContent(client, bucketName, key, version.VersionId, contents[i]);
			}
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Copy")]
		[Trait(MainData.Explanation, "오브젝트의 버전별 복사가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_copy_obj_version()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "testobj";
			var numVersions = 3;


			var versionIds = new List<string>();
			var contents = new List<string>();
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			for (int i = 0; i < numVersions; i++)
			{
				var newKeyName = string.Format("key_{0}", i);
				client.CopyObject(bucketName, key, bucketName, newKeyName, versionId: versionIds[i]);
				var getResponse = client.GetObject(bucketName, newKeyName);
				var content = GetBody(getResponse);
				Assert.Equal(contents[i], content);
			}

			var anotherBucketName = GetNewBucket();

			for (int i = 0; i < numVersions; i++)
			{
				var newKeyName = string.Format("key_{0}", i);
				client.CopyObject(bucketName, key, anotherBucketName, newKeyName, versionId: versionIds[i]);
				var getResponse = client.GetObject(bucketName, newKeyName);
				var content = GetBody(getResponse);
				Assert.Equal(contents[i], content);
			}

			var newKeyName2 = "new_key";
			client.CopyObject(bucketName, key, anotherBucketName, newKeyName2);

			var response = client.GetObject(anotherBucketName, newKeyName2);
			var body = GetBody(response);
			Assert.Equal(body, contents[^1]);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_multi_object_delete()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "key";
			var numVersions = 2;

			var versionIds = new List<string>();
			var contents = new List<string>();
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			var listResponse = client.ListVersions(bucketName);
			var versions = GetVersions(listResponse.Versions);
			versions.Reverse();

			foreach (var version in versions)
				client.DeleteObject(bucketName, key, versionId: version.VersionId);

			listResponse = client.ListVersions(bucketName);
			Assert.Empty(listResponse.Versions);

			foreach (var version in versions)
				client.DeleteObject(bucketName, key, versionId: version.VersionId);

			listResponse = client.ListVersions(bucketName);
			Assert.Empty(listResponse.Versions);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Delete Marker")]
		[Trait(MainData.Explanation, "버전이 여러개인 오브젝트에 대한 삭제마커가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_multi_object_delete_with_marker()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "key";
			var numVersions = 2;

			var versionIds = new List<string>();
			var contents = new List<string>();
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			client.DeleteObject(bucketName, key);
			var response = client.ListVersions(bucketName);
			var versions = GetVersions(response.Versions);
			var deleteMarkers = GetDeleteMarkers(response.Versions);

			versionIds.Add(deleteMarkers[0].VersionId);
			Assert.Equal(3, versionIds.Count);
			Assert.Single(deleteMarkers);

			foreach (var version in versions)
				client.DeleteObject(bucketName, key, versionId: version.VersionId);

			foreach (var deleteMarker in deleteMarkers)
				client.DeleteObject(bucketName, key, versionId: deleteMarker.VersionId);

			response = client.ListVersions(bucketName);
			Assert.Empty(GetVersions(response.Versions));
			Assert.Empty(GetDeleteMarkers(response.Versions));


			foreach (var version in versions)
				client.DeleteObject(bucketName, key, versionId: version.VersionId);

			foreach (var deleteMarker in deleteMarkers)
				client.DeleteObject(bucketName, key, versionId: deleteMarker.VersionId);

			response = client.ListVersions(bucketName);
			Assert.Empty(GetVersions(response.Versions));
			Assert.Empty(GetDeleteMarkers(response.Versions));
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Delete Marker")]
		[Trait(MainData.Explanation, "존재하지않는 오브젝트를 삭제할경우 삭제마커가 생성되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_multi_object_delete_with_marker_create()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "key";

			var delResponse = client.DeleteObject(bucketName, key);
			var deleteMarkerVersionId = delResponse.VersionId;

			var response = client.ListVersions(bucketName);
			var deleteMarker = GetDeleteMarkers(response.Versions);

			Assert.Single(deleteMarker);
			Assert.Equal(deleteMarkerVersionId, deleteMarker[0].VersionId);
			Assert.Equal(key, deleteMarker[0].Key);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioned_object_acl()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "xyz";
			var numVersions = 3;

			var versionIds = new List<string>();
			var contents = new List<string>();
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			var versionId = versionIds[1];

			var response = client.GetObjectACL(bucketName, key, versionId: versionId);

			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			if (!Config.S3.IsAWS) Assert.Equal(displayName, response.AccessControlList.Owner.DisplayName);
			Assert.Equal(userId, response.AccessControlList.Owner.Id);

			var getGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new S3Grantee()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
						URI = null,
						EmailAddress = null,
					}
				},
			},
			getGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioned_object_acl_no_version_specified()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "xyz";
			var numVersions = 3;

			var versionIds = new List<string>();
			var contents = new List<string>();
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			var getResponse = client.GetObject(bucketName, key);
			var versionId = getResponse.VersionId;

			var response = client.GetObjectACL(bucketName, key, versionId: versionId);

			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			if (!Config.S3.IsAWS) Assert.Equal(displayName, response.AccessControlList.Owner.DisplayName);
			Assert.Equal(userId, response.AccessControlList.Owner.Id);

			var getGrants = response.AccessControlList.Grants;
			var defaultPolicy = new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new S3Grantee()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
						URI = null,
						EmailAddress = null,
					}
				},
			};
			CheckGrants(defaultPolicy, getGrants);

			client.PutObjectACL(bucketName, key, acl: S3CannedACL.PublicRead);

			response = client.GetObjectACL(bucketName, key, versionId: versionId);
			getGrants = response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new S3Grantee()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
						URI = null,
						EmailAddress = null,
					}
				},
				new()
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
			getGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioned_concurrent_object_create_and_remove()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "myobj";
			var numVersions = 3;

			var allTasks = new List<Thread>();

			for (int i = 0; i < 3; i++)
			{
				var tList = SetupVersionedObjConcurrent(client, bucketName, key, numVersions);
				allTasks.AddRange(tList);

				var tList2 = DoClearVersionedBucketConcurrent(client, bucketName);
				allTasks.AddRange(tList2);
			}

			foreach (var mTask in allTasks) mTask.Join();

			var tList3 = DoClearVersionedBucketConcurrent(client, bucketName);
			foreach (var mTask in tList3) mTask.Join();

			var response = client.ListVersions(bucketName);
			Assert.Empty(response.Versions);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_bucket_atomic_upload_return_version_id()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "bar";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var putResponse = client.PutObject(bucketName, key);
			var versionId = putResponse.VersionId;

			var listResponse = client.ListVersions(bucketName);
			var versions = GetVersions(listResponse.Versions);
			foreach (var version in versions)
				Assert.Equal(versionId, version.VersionId);

			bucketName = GetNewBucket();
			key = "baz";
			putResponse = client.PutObject(bucketName, key);
			Assert.Null(putResponse.VersionId);

			bucketName = GetNewBucket();
			key = "baz";
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			putResponse = client.PutObject(bucketName, key);
			Assert.Null(putResponse.VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "MultiPart")]
		[Trait(MainData.Explanation, "버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_bucket_multipart_upload_return_version_id()
		{
			var contentType = "text/bla";
			var size = 50 * MainData.MB;

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "bar";
			var metadata = new List<KeyValuePair<string, string>>() { new("foo", "baz") };

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var uploadData = SetupMultipartUpload(client, bucketName, key, size, metadataList: metadata, contentType: contentType);

			var compResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			var versionId = compResponse.VersionId;

			var listResponse = client.ListVersions(bucketName);
			var versions = GetVersions(listResponse.Versions);
			foreach (var version in versions)
				Assert.Equal(versionId, version.VersionId);

			bucketName = GetNewBucket();
			key = "baz";

			uploadData = SetupMultipartUpload(client, bucketName, key, size, metadataList: metadata, contentType: contentType);
			compResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			Assert.Null(compResponse.VersionId);

			bucketName = GetNewBucket();
			key = "foo";

			uploadData = SetupMultipartUpload(client, bucketName, key, size, metadataList: metadata, contentType: contentType);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			compResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			Assert.Null(compResponse.VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_versioning_get_object_head()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var keyName = "foo";
			var versionList = new List<string>();

			for (int i = 1; i <= 5; i++)
			{
				var response = client.PutObject(bucketName, key: keyName, RandomTextToLong(i));
				versionList.Add(response.VersionId);
			}

			for (int i = 0; i < 5; i++)
			{
				var response = client.GetObjectMetadata(bucketName, key: keyName, versionId: versionList[i]);
				Assert.Equal(i + 1, response.ContentLength);
			}
		}

		// 버전이 여러개인 오브젝트의 최신 버전을 삭제 했을때 이전버전이 최신버전으로 변경되는지 확인
		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningLatest()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var keyName = "foo";
			var versionStack = new Stack<string>();

			for (int i = 1; i <= 5; i++)
			{
				var response = client.PutObject(bucketName, keyName, RandomTextToLong(i));
				versionStack.Push(response.VersionId);
			}

			var lastVersionId = versionStack.Pop();
			while (versionStack.Count > 1)
			{
				client.DeleteObject(bucketName, keyName, lastVersionId);
				lastVersionId = versionStack.Pop();

				var response = client.GetObjectMetadata(bucketName, keyName);
				Assert.Equal(lastVersionId, response.VersionId);
			}
		}
	}
}
