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
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Xunit;

namespace s3tests.Test
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
			TestId = 1;
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
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketVersioning(bucketName, status: VersionStatus.Enabled);
			var key = "TestVersioningObjCreateReadRemove";
			var numVersions = 5;

			TestCreateRemoveVersions(client, bucketName, key, numVersions, 4, -1);
			TestCreateRemoveVersions(client, bucketName, key, numVersions, 0, 0);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningObjCreateReadRemoveHead()
		{
			TestId = 3;
			var bucketName = GetNewBucket();

			var client = GetClient();
			client.PutBucketVersioning(bucketName, status: VersionStatus.Enabled);
			var key = "TestVersioningObjCreateReadRemoveHead";
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
			var body = S3Utils.GetBody(getResponse);
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
		public void TestVersioningObjPlainNullVersionRemoval()
		{
			TestId = 4;
			var bucketName = GetNewBucket();
			CheckVersioning(bucketName, VersionStatus.Off);

			var client = GetClient();
			var key = "TestVersioningObjPlainNullVersionRemoval";
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
		public void TestVersioningObjPlainNullVersionOverwrite()
		{
			TestId = 5;
			var bucketName = GetNewBucket();
			CheckVersioning(bucketName, VersionStatus.Off);

			var client = GetClient();
			var key = "TestVersioningObjPlainNullVersionOverwrite";
			var content = "fooz";
			client.PutObject(bucketName, key, body: content);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var content2 = "zzz";
			client.PutObject(bucketName, key, body: content2);
			var response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(response);
			Assert.Equal(content2, body);

			var versionId = response.VersionId;
			client.DeleteObject(bucketName, key, versionId: versionId);
			response = client.GetObject(bucketName, key);
			body = S3Utils.GetBody(response);
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
		public void TestVersioningObjPlainNullVersionOverwriteSuspended()
		{
			TestId = 6;
			var bucketName = GetNewBucket();
			CheckVersioning(bucketName, VersionStatus.Off);

			var client = GetClient();
			var key = "TestVersioningObjPlainNullVersionOverwriteSuspended";
			var content = "fooz";
			client.PutObject(bucketName, key, body: content);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);

			var content2 = "zzz";
			client.PutObject(bucketName, key, body: content2);
			var response = client.GetObject(bucketName, key);
			var body = S3Utils.GetBody(response);
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
		public void TestVersioningObjSuspendVersions()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var key = "TestVersioningObjSuspendVersions";
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
		public void TestVersioningObjCreateVersionsRemoveAll()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningObjCreateVersionsRemoveAll";
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
		public void TestVersioningObjCreateVersionsRemoveSpecialNames()
		{
			TestId = 9;
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
		public void TestVersioningObjCreateOverwriteMultipart()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningObjCreateOverwriteMultipart";
			var numVersions = 3;
			var versionIds = new List<string>();
			var contents = new List<string>();

			for (int i = 0; i < numVersions; i++)
				contents.Add(DoTestMultipartUploadContents(bucketName, key, 3));

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
		public void TestVersioningObjListMarker()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningObjListMarker";
			var key2 = "TestVersioningObjListMarker-001";
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
		public void TestVersioningCopyObjVersion()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningCopyObjVersion";
			var numVersions = 3;


			var versionIds = new List<string>();
			var contents = new List<string>();
			SetupMultipleVersions(client, bucketName, key, numVersions, ref versionIds, ref contents);

			for (int i = 0; i < numVersions; i++)
			{
				var newKeyName = string.Format("key_{0}", i);
				client.CopyObject(bucketName, key, bucketName, newKeyName, versionId: versionIds[i]);
				var getResponse = client.GetObject(bucketName, newKeyName);
				var content = S3Utils.GetBody(getResponse);
				Assert.Equal(contents[i], content);
			}

			var anotherBucketName = GetNewBucket();

			for (int i = 0; i < numVersions; i++)
			{
				var newKeyName = string.Format("key_{0}", i);
				client.CopyObject(bucketName, key, anotherBucketName, newKeyName, versionId: versionIds[i]);
				var getResponse = client.GetObject(bucketName, newKeyName);
				var content = S3Utils.GetBody(getResponse);
				Assert.Equal(contents[i], content);
			}

			var newKeyName2 = "TestVersioningCopyObjVersionDestination-001";
			client.CopyObject(bucketName, key, anotherBucketName, newKeyName2);

			var response = client.GetObject(anotherBucketName, newKeyName2);
			var body = S3Utils.GetBody(response);
			Assert.Equal(body, contents[^1]);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningMultiObjectDelete()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningMultiObjectDelete";
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
		public void TestVersioningMultiObjectDeleteWithMarker()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningMultiObjectDeleteWithMarker";
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
		public void TestVersioningMultiObjectDeleteWithMarkerCreate()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningMultiObjectDeleteWithMarkerCreate";

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
		public void TestVersionedObjectAcl()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersionedObjectAcl";
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
			CheckGrants(
			[
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
			],
			getGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "ACL")]
		[Trait(MainData.Explanation, "버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersionedObjectAclNoVersionSpecified()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersionedObjectAclNoVersionSpecified";
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

			CheckGrants(
			[
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
			],
			getGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersionedConcurrentObjectCreateAndRemove()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersionedConcurrentObjectCreateAndRemove";
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
		public void TestVersioningBucketAtomicUploadReturnVersionId()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningBucketAtomicUploadReturnVersionId";

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
		public void TestVersioningBucketMultipartUploadReturnVersionId()
		{
			TestId = 21;
			var contentType = "text/bla";
			var size = 50 * MainData.MB;

			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningBucketMultipartUploadReturnVersionId";
			var metadata = new List<KeyValuePair<string, string>>() { new("foo", "baz") };

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, metadataList: metadata, contentType: contentType);

			var compResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			var versionId = compResponse.VersionId;

			var listResponse = client.ListVersions(bucketName);
			var versions = GetVersions(listResponse.Versions);
			foreach (var version in versions)
				Assert.Equal(versionId, version.VersionId);

			bucketName = GetNewBucket();
			key = "baz";

			uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, metadataList: metadata, contentType: contentType);
			compResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			Assert.Null(compResponse.VersionId);

			bucketName = GetNewBucket();
			key = "foo";

			uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size, metadataList: metadata, contentType: contentType);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			compResponse = client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			Assert.Null(compResponse.VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningGetObjectHead()
		{
			TestId = 22;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var keyName = "TestVersioningGetObjectHead";
			var versionList = new List<string>();

			for (int i = 1; i <= 5; i++)
			{
				var response = client.PutObject(bucketName, key: keyName, S3Utils.RandomTextToLong(i));
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
			TestId = 23;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var keyName = "TestVersioningLatest";
			var versionStack = new Stack<string>();

			for (int i = 1; i <= 5; i++)
			{
				var response = client.PutObject(bucketName, keyName, S3Utils.RandomTextToLong(i));
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

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Mixed")]
		[Trait(MainData.Explanation, "버저닝 버킷에서 PutObject와 Multipart 혼합 업로드 후 버전별 조회 검증")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningObjMixPutAndMultipart()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var key = "TestVersioningObjMixPutAndMultipart";
			var versionIds = new List<string>();
			var contents = new List<string>();

			// PutObject 1KB
			var content1kb = S3Utils.RandomTextToLong(1 * MainData.KB);
			var put1kb = client.PutObject(bucketName, key, body: content1kb);
			Assert.NotNull(put1kb.VersionId);
			versionIds.Add(put1kb.VersionId);
			contents.Add(content1kb);

			// MultipartUpload 50MB
			var upload50mb = S3Utils.SetupMultipartUpload(client, bucketName, key, 50 * MainData.MB);
			var comp50mb = client.CompleteMultipartUpload(bucketName, key, upload50mb.UploadId, upload50mb.Parts);
			Assert.NotNull(comp50mb.VersionId);
			versionIds.Add(comp50mb.VersionId);
			contents.Add(upload50mb.Body);

			// PutObject 1MB
			var content1mb = S3Utils.RandomTextToLong(1 * MainData.MB);
			var put1mb = client.PutObject(bucketName, key, body: content1mb);
			Assert.NotNull(put1mb.VersionId);
			versionIds.Add(put1mb.VersionId);
			contents.Add(content1mb);

			// MultipartUpload 10MB
			var upload10mb = S3Utils.SetupMultipartUpload(client, bucketName, key, 10 * MainData.MB);
			var comp10mb = client.CompleteMultipartUpload(bucketName, key, upload10mb.UploadId, upload10mb.Parts);
			Assert.NotNull(comp10mb.VersionId);
			versionIds.Add(comp10mb.VersionId);
			contents.Add(upload10mb.Body);

			// ListVersions: 최신 버전부터 반환
			var listResponse = client.ListVersions(bucketName);
			var versions = GetVersions(listResponse.Versions);
			Assert.Equal(4, versions.Count);
			for (int i = 0; i < versions.Count; i++)
			{
				var version = versions[i];
				Assert.Equal(key, version.Key);
				Assert.Equal(versionIds[versionIds.Count - 1 - i], version.VersionId);
				Assert.Equal(contents[contents.Count - 1 - i].Length, version.Size);
			}

			// 업로드 순서대로 versionId 지정 GetObject 후 내용 검증
			for (int i = 0; i < versionIds.Count; i++)
			{
				var getResponse = client.GetObject(bucketName, key, versionId: versionIds[i]);
				Assert.Equal(contents[i], S3Utils.GetBody(getResponse));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 VersionId로 조회시 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestVersioningInvalidVersionId()
		{
			TestId = 24;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningInvalidVersionId";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			client.PutObject(bucketName, key, body: key);

			var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key, versionId: "f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59"));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_VERSION, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "CopyObject")]
		[Trait(MainData.Explanation, "버저닝 상태에 따른 CopyObject의 버전 처리 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningCopyObject()
		{
			TestId = 25;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var sourceKey = "TestVersioningCopyObjectSource";
			var targetKey = "TestVersioningCopyObjectTarget";
			var content = "content-version1";
			var expectedVersions = new List<string>();

			// 버저닝 설정
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// PutObject - 첫 번째 버전 생성
			var putResponse = client.PutObject(bucketName, sourceKey, body: content);
			expectedVersions.Add(putResponse.VersionId);

			// CopyObject - 복제가 정상적인지 확인
			var copyResponse = client.CopyObject(bucketName, sourceKey, bucketName, targetKey);
			var targetVersion1 = copyResponse.VersionId;
			expectedVersions.Add(targetVersion1);

			var getResponse = client.GetObject(bucketName, targetKey);
			Assert.Equal(content, S3Utils.GetBody(getResponse));
			Assert.Equal(targetVersion1, getResponse.VersionId);

			// ListVersions 확인 - source(1), target(1)
			var listResponse = client.ListVersions(bucketName);
			Assert.Equal(2, GetVersions(listResponse.Versions).Count);
			foreach (var version in GetVersions(listResponse.Versions))
				Assert.Contains(version.VersionId, expectedVersions);

			copyResponse = client.CopyObject(bucketName, sourceKey, bucketName, targetKey);
			var targetVersion2 = copyResponse.VersionId;
			expectedVersions.Add(targetVersion2);

			// 복제가 정상적인지 확인
			getResponse = client.GetObject(bucketName, targetKey);
			Assert.Equal(content, S3Utils.GetBody(getResponse));
			Assert.Equal(targetVersion2, getResponse.VersionId);

			// ListVersions - source(1), target(2)
			listResponse = client.ListVersions(bucketName);
			Assert.Equal(3, GetVersions(listResponse.Versions).Count);
			Assert.Equal(expectedVersions.Count, GetVersions(listResponse.Versions).Count);
			foreach (var version in GetVersions(listResponse.Versions))
				Assert.Contains(version.VersionId, expectedVersions);

			// CopyObject(metadata only overwrite) - 메타데이터만 변경하여 복사
			var metadata = new List<KeyValuePair<string, string>>() { new("x-amz-meta-test-key", "test-value") };
			copyResponse = client.CopyObject(bucketName, sourceKey, bucketName, targetKey,
				metadataList: metadata, metadataDirective: S3MetadataDirective.REPLACE, contentType: "text/plain");
			var targetVersion3 = copyResponse.VersionId;
			expectedVersions.Add(targetVersion3);

			// 복제가 정상적인지 확인
			var metadataResponse = client.GetObjectMetadata(bucketName, targetKey);
			Assert.Equal("test-value", metadataResponse.Metadata["x-amz-meta-test-key"]);
			Assert.Equal("text/plain", metadataResponse.Headers.ContentType);
			Assert.Equal(targetVersion3, metadataResponse.VersionId);

			// ListVersions - source(1), target(3)
			listResponse = client.ListVersions(bucketName);
			Assert.Equal(4, GetVersions(listResponse.Versions).Count);
			Assert.Equal(expectedVersions.Count, GetVersions(listResponse.Versions).Count);
			foreach (var version in GetVersions(listResponse.Versions))
				Assert.Contains(version.VersionId, expectedVersions);

			// 버저닝 중단
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);

			// CopyObject - 버저닝 중단 상태에서 기존 버전 복사
			copyResponse = client.CopyObject(bucketName, sourceKey, bucketName, targetKey);
			var targetVersion4 = copyResponse.VersionId;
			Assert.True(targetVersion4 == null || targetVersion4 == "null");
			expectedVersions.Add("null");

			// 복제가 정상적인지 확인
			getResponse = client.GetObject(bucketName, targetKey);
			Assert.Equal(content, S3Utils.GetBody(getResponse));

			// ListVersions - source(1), target(3+null)
			listResponse = client.ListVersions(bucketName);
			Assert.Equal(5, GetVersions(listResponse.Versions).Count);
			Assert.Equal(expectedVersions.Count, GetVersions(listResponse.Versions).Count);
			foreach (var version in GetVersions(listResponse.Versions))
				Assert.Contains(version.VersionId ?? "null", expectedVersions);

			// CopyObject(overwrite) - 버저닝 중단 상태에서 다시 덮어쓰기
			copyResponse = client.CopyObject(bucketName, sourceKey, bucketName, targetKey);
			var targetVersion5 = copyResponse.VersionId;
			Assert.True(targetVersion5 == null || targetVersion5 == "null");
			// null 버전은 덮어쓰기되므로 expectedVersions에 추가하지 않음

			// 복제가 정상적인지 확인
			getResponse = client.GetObject(bucketName, targetKey);
			Assert.Equal(content, S3Utils.GetBody(getResponse));

			// ListVersions - null 버전은 덮어써지므로 개수 유지
			listResponse = client.ListVersions(bucketName);
			Assert.Equal(5, GetVersions(listResponse.Versions).Count);
			Assert.Equal(expectedVersions.Count, GetVersions(listResponse.Versions).Count);
			foreach (var version in GetVersions(listResponse.Versions))
				Assert.Contains(version.VersionId ?? "null", expectedVersions);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "VersionId")]
		[Trait(MainData.Explanation, "버저닝 미설정 버킷에서 모든 업로드 방식의 VersionId가 null인지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningUnversionedAllVersionId()
		{
			TestId = 26;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningUnversionedAllVersionId";
			var multipartKey = key + "-multipart";
			var copyKey = key + "-copy";
			var content = "testContent";
			var size = 5 * MainData.MB;

			var putResponse = client.PutObject(bucketName, key, body: content);
			Assert.Null(putResponse.VersionId);

			var headResponse = client.GetObjectMetadata(bucketName, key);
			Assert.Null(headResponse.VersionId);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Null(getResponse.VersionId);
			Assert.Equal(content, S3Utils.GetBody(getResponse));

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, multipartKey, size);
			var compResponse = client.CompleteMultipartUpload(bucketName, multipartKey, uploadData.UploadId, uploadData.Parts);
			Assert.Null(compResponse.VersionId);

			var copyResponse = client.CopyObject(bucketName, key, bucketName, copyKey);
			Assert.Null(copyResponse.VersionId);

			var listObjects = client.ListObjects(bucketName);
			Assert.Equal(3, listObjects.S3Objects.Count);

			var listVersions = client.ListVersions(bucketName);
			var versions = GetVersions(listVersions.Versions);
			Assert.Equal(3, versions.Count);
			foreach (var version in versions)
				Assert.Equal("null", version.VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "VersionId")]
		[Trait(MainData.Explanation, "버저닝 설정 버킷에서 모든 업로드 방식의 VersionId가 존재하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningEnabledAllVersionId()
		{
			TestId = 27;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningEnabledAllVersionId";
			var multipartKey = key + "-multipart";
			var copyKey = key + "-copy";
			var content = "testContent";
			var size = 5 * MainData.MB;

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var putResponse = client.PutObject(bucketName, key, body: content);
			var versionId = putResponse.VersionId;
			Assert.NotNull(versionId);

			var headResponse = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(versionId, headResponse.VersionId);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(versionId, getResponse.VersionId);
			Assert.Equal(content, S3Utils.GetBody(getResponse));

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, multipartKey, size);
			var compResponse = client.CompleteMultipartUpload(bucketName, multipartKey, uploadData.UploadId, uploadData.Parts);
			var multipartVersionId = compResponse.VersionId;
			Assert.NotNull(multipartVersionId);

			var copyResponse = client.CopyObject(bucketName, key, bucketName, copyKey);
			var copyVersionId = copyResponse.VersionId;
			Assert.NotNull(copyVersionId);

			var listObjects = client.ListObjects(bucketName);
			Assert.Equal(3, listObjects.S3Objects.Count);

			var listVersions = client.ListVersions(bucketName);
			var versionIds = GetVersionIds(listVersions.Versions);
			Assert.Equal(3, GetVersions(listVersions.Versions).Count);
			Assert.Contains(versionId, versionIds);
			Assert.Contains(multipartVersionId, versionIds);
			Assert.Contains(copyVersionId, versionIds);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "VersionId")]
		[Trait(MainData.Explanation, "버저닝 중단 버킷에서 모든 업로드 방식의 VersionId가 null인지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningSuspendedAllVersionId()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningSuspendedAllVersionId";
			var multipartKey = key + "-multipart";
			var copyKey = key + "-copy";
			var content = "testContent";
			var size = 5 * MainData.MB;

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);

			var putResponse = client.PutObject(bucketName, key, body: content);
			Assert.Null(putResponse.VersionId);

			var headResponse = client.GetObjectMetadata(bucketName, key);
			Assert.Equal("null", headResponse.VersionId);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal("null", getResponse.VersionId);
			Assert.Equal(content, S3Utils.GetBody(getResponse));

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, multipartKey, size);
			var compResponse = client.CompleteMultipartUpload(bucketName, multipartKey, uploadData.UploadId, uploadData.Parts);
			Assert.Null(compResponse.VersionId);

			var copyResponse = client.CopyObject(bucketName, key, bucketName, copyKey);
			Assert.True(copyResponse.VersionId == null || copyResponse.VersionId == "null");

			var listObjects = client.ListObjects(bucketName);
			Assert.Equal(3, listObjects.S3Objects.Count);

			var listVersions = client.ListVersions(bucketName);
			var versions = GetVersions(listVersions.Versions);
			Assert.Equal(3, versions.Count);
			foreach (var version in versions)
				Assert.Equal("null", version.VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "동일 key에 OFF→ENABLED→SUSPENDED 순서로 업로드시 버전 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningListVersionsOffEnabledSuspended()
		{
			TestId = 29;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningListVersionsOffEnabledSuspended";
			var contentOff = "content-off";
			var contentEnabled = "content-enabled";
			var contentSuspended = "content-suspended";

			// 1. OFF: put
			var offResponse = client.PutObject(bucketName, key, body: contentOff);
			Assert.Null(offResponse.VersionId);

			// 2. ENABLED: put (새 versionId 추가 → null + versionId)
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var enabledResponse = client.PutObject(bucketName, key, body: contentEnabled);
			var enabledVersionId = enabledResponse.VersionId;
			Assert.NotNull(enabledVersionId);

			// 3. SUSPENDED: put (기존 null 버전을 덮어씀 → 여전히 2개)
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			var suspendedResponse = client.PutObject(bucketName, key, body: contentSuspended);
			Assert.Null(suspendedResponse.VersionId);

			var listObjects = client.ListObjects(bucketName);
			Assert.Single(listObjects.S3Objects);
			Assert.Equal(key, listObjects.S3Objects[0].Key);

			var listVersions = client.ListVersions(bucketName);
			var versions = GetVersions(listVersions.Versions);
			Assert.Equal(2, versions.Count);

			var versionIds = GetVersionIds(listVersions.Versions);
			Assert.Contains(enabledVersionId, versionIds);
			Assert.Contains("null", versionIds);

			// current는 suspended put으로 덮어쓴 null 버전
			var latest = versions.Find(v => v.IsLatest == true);
			Assert.NotNull(latest);
			Assert.Equal("null", latest.VersionId);
			Assert.Equal(contentSuspended.Length, latest.Size);

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(contentSuspended, S3Utils.GetBody(getResponse));

			// ENABLED 때 만든 versionId로 Get하면 해당 내용이 반환되어야 함
			var getByVersion = client.GetObject(bucketName, key, versionId: enabledVersionId);
			Assert.Equal(contentEnabled, S3Utils.GetBody(getByVersion));
			Assert.Equal(enabledVersionId, getByVersion.VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "서로 다른 key에 OFF→ENABLED→SUSPENDED 순서로 업로드시 버전 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningListVersionsOffEnabledSuspendedDifferentKeys()
		{
			TestId = 30;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var keyOff = "TestVersioningListVersionsOffEnabledSuspendedDifferentKeysOff";
			var keyEnabled = "TestVersioningListVersionsOffEnabledSuspendedDifferentKeysEnabled";
			var keySuspended = "TestVersioningListVersionsOffEnabledSuspendedDifferentKeysSuspended";
			var contentOff = "content-off";
			var contentEnabled = "content-enabled";
			var contentSuspended = "content-suspended";

			// 1. OFF: put (key별 null 버전)
			var offResponse = client.PutObject(bucketName, keyOff, body: contentOff);
			Assert.Null(offResponse.VersionId);

			// 2. ENABLED: put (다른 key → versionId)
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var enabledResponse = client.PutObject(bucketName, keyEnabled, body: contentEnabled);
			var enabledVersionId = enabledResponse.VersionId;
			Assert.NotNull(enabledVersionId);

			// 3. SUSPENDED: put (또 다른 key → null 버전)
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			var suspendedResponse = client.PutObject(bucketName, keySuspended, body: contentSuspended);
			Assert.Null(suspendedResponse.VersionId);

			var listObjects = client.ListObjects(bucketName);
			Assert.Equal(3, listObjects.S3Objects.Count);

			var listVersions = client.ListVersions(bucketName);
			var versions = GetVersions(listVersions.Versions);
			Assert.Equal(3, versions.Count);

			var versionByKey = new Dictionary<string, string>();
			foreach (var version in versions) versionByKey[version.Key] = version.VersionId;
			Assert.Equal("null", versionByKey[keyOff]);
			Assert.Equal(enabledVersionId, versionByKey[keyEnabled]);
			Assert.Equal("null", versionByKey[keySuspended]);

			var nullVersionCount = versions.FindAll(v => v.VersionId == "null").Count;
			Assert.Equal(2, nullVersionCount);

			// key별 Head/Get versionId 확인
			Assert.Equal("null", client.GetObjectMetadata(bucketName, keyOff).VersionId);
			Assert.Equal(contentOff, S3Utils.GetBody(client.GetObject(bucketName, keyOff)));

			Assert.Equal(enabledVersionId, client.GetObjectMetadata(bucketName, keyEnabled).VersionId);
			Assert.Equal(contentEnabled, S3Utils.GetBody(client.GetObject(bucketName, keyEnabled)));

			Assert.Equal("null", client.GetObjectMetadata(bucketName, keySuspended).VersionId);
			Assert.Equal(contentSuspended, S3Utils.GetBody(client.GetObject(bucketName, keySuspended)));
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "버저닝 중단 후 null 버전 삭제시 current가 ENABLED 버전이 되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningDeleteNullVersionAfterSuspend()
		{
			TestId = 31;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningDeleteNullVersionAfterSuspend";
			var contentEnabled = "content-enabled";
			var contentSuspended = "content-suspended";

			client.PutObject(bucketName, key, body: "content-off");

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			var enabledVersionId = client.PutObject(bucketName, key, body: contentEnabled).VersionId;
			Assert.NotNull(enabledVersionId);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			client.PutObject(bucketName, key, body: contentSuspended);
			Assert.Equal(contentSuspended, S3Utils.GetBody(client.GetObject(bucketName, key)));

			// null 버전 삭제 후 current는 ENABLED 버전이 되어야 함
			client.DeleteObject(bucketName, key, versionId: "null");

			var getResponse = client.GetObject(bucketName, key);
			Assert.Equal(contentEnabled, S3Utils.GetBody(getResponse));
			Assert.Equal(enabledVersionId, getResponse.VersionId);

			var listVersions = client.ListVersions(bucketName);
			var versions = GetVersions(listVersions.Versions);
			Assert.Single(versions);
			Assert.Equal(enabledVersionId, versions[0].VersionId);
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "Transition")]
		[Trait(MainData.Explanation, "ENABLED에서 여러 버전 생성 후 SUSPENDED로 전환시 버전 목록 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestVersioningListVersionsMultipleEnabledThenSuspended()
		{
			TestId = 32;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningListVersionsMultipleEnabledThenSuspended";
			var enabledVersionIds = new List<string>();

			client.PutObject(bucketName, key, body: "content-off");

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			for (int i = 1; i <= 3; i++)
			{
				var versionId = client.PutObject(bucketName, key, body: "content-enabled-" + i).VersionId;
				Assert.NotNull(versionId);
				enabledVersionIds.Add(versionId);
			}

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Suspended);
			client.PutObject(bucketName, key, body: "content-suspended");

			// ENABLED 3개 + null 1개
			var listVersions = client.ListVersions(bucketName);
			var versions = GetVersions(listVersions.Versions);
			Assert.Equal(4, versions.Count);

			var versionIds = GetVersionIds(listVersions.Versions);
			foreach (var enabledVersionId in enabledVersionIds)
				Assert.Contains(enabledVersionId, versionIds);
			Assert.Contains("null", versionIds);

			var latest = versions.Find(v => v.IsLatest == true);
			Assert.NotNull(latest);
			Assert.Equal("null", latest.VersionId);
			Assert.Equal("content-suspended", S3Utils.GetBody(client.GetObject(bucketName, key)));
		}

		[Fact]
		[Trait(MainData.Major, "Versioning")]
		[Trait(MainData.Minor, "DeleteMarker")]
		[Trait(MainData.Explanation, "오브젝트 삭제로 생성된 DeleteMarker에 대한 HeadObject가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestVersioningHeadObjectDeleteMarker()
		{
			TestId = 33;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "TestVersioningHeadObjectDeleteMarker";
			var content = "testContent";

			// 1. 버킷 생성 및 버저닝 설정
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 2. 오브젝트 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);
			var versionId = putResponse.VersionId;

			// 3. 업로드 확인
			var headResponse = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(content.Length, headResponse.ContentLength);
			Assert.Equal(versionId, headResponse.VersionId);

			// 4. 오브젝트 삭제
			client.DeleteObject(bucketName, key);

			// 5. DeleteMarker 생성 확인
			var listResponse = client.ListVersions(bucketName);
			Assert.Single(GetVersions(listResponse.Versions));
			Assert.Equal(1, GetDeleteMarkerCount(listResponse.Versions));
			Assert.Equal(key, GetDeleteMarkers(listResponse.Versions)[0].Key);

			// 6. HeadObject 실패 확인
			var e = Assert.Throws<AggregateException>(() => client.GetObjectMetadata(bucketName, key));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
		}
	}
}
