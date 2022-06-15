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
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace s3tests2
{
	[TestClass]
	public class DeleteObjects : TestBase
	{
		[TestMethod("test_multi_object_delete")]
		[TestProperty(MainData.Major, "DeleteObjects")]
		[TestProperty(MainData.Minor, "ListObject")]
		[TestProperty(MainData.Explanation, "버킷에 존재하는 오브젝트 여러개를 한번에 삭제")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_multi_object_delete()
		{
			var KeyNames = new List<string>() { "key0", "key1", "key2" };
			var BucketName = SetupObjects(KeyNames);
			var Client = GetClient();

			var ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(KeyNames.Count, ListResponse.S3Objects.Count);

			var ObjectList = GetKeyVersions(KeyNames);
			var DelResponse = Client.DeleteObjects(BucketName, ObjectList);

			Assert.AreEqual(KeyNames.Count, DelResponse.DeletedObjects.Count);
			Assert.AreEqual(0, DelResponse.DeleteErrors.Count);

			ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(0, ListResponse.S3Objects.Count);

			DelResponse = Client.DeleteObjects(BucketName, ObjectList);
			Assert.AreEqual(KeyNames.Count, DelResponse.DeletedObjects.Count);
			Assert.AreEqual(0, DelResponse.DeleteErrors.Count);

			ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(0, ListResponse.S3Objects.Count);
		}

		[TestMethod("test_multi_objectv2_delete")]
		[TestProperty(MainData.Major, "DeleteObjects")]
		[TestProperty(MainData.Minor, "ListObjectsV2")]
		[TestProperty(MainData.Explanation, "버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_multi_objectv2_delete()
		{
			var KeyNames = new List<string>() { "key0", "key1", "key2" };
			var BucketName = SetupObjects(KeyNames);
			var Client = GetClient();

			var ListResponse = Client.ListObjectsV2(BucketName);
			Assert.AreEqual(KeyNames.Count, ListResponse.S3Objects.Count);

			var ObjectList = GetKeyVersions(KeyNames);
			var DelResponse = Client.DeleteObjects(BucketName, ObjectList);

			Assert.AreEqual(KeyNames.Count, DelResponse.DeletedObjects.Count);
			Assert.AreEqual(0, DelResponse.DeleteErrors.Count);

			ListResponse = Client.ListObjectsV2(BucketName);
			Assert.AreEqual(0, ListResponse.S3Objects.Count);

			DelResponse = Client.DeleteObjects(BucketName, ObjectList);
			Assert.AreEqual(KeyNames.Count, DelResponse.DeletedObjects.Count);
			Assert.AreEqual(0, DelResponse.DeleteErrors.Count);

			ListResponse = Client.ListObjectsV2(BucketName);
			Assert.AreEqual(0, ListResponse.S3Objects.Count);
		}

		[TestMethod("test_multi_object_delete_versions")]
		[TestProperty(MainData.Major, "DeleteObjects")]
		[TestProperty(MainData.Minor, "Versioning")]
		[TestProperty(MainData.Explanation, "버킷에 존재하는 버저닝 오브젝트 여러개를 한번에 삭제")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_multi_object_delete_versions()
		{
			var KeyNames = new List<string>() { "key0", "key1", "key2" };
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			foreach (var Key in KeyNames)
				SetupMultipleVersion(Client, BucketName, Key, 3, false);

			var ListResponse = Client.ListObjectsV2(BucketName);
			Assert.AreEqual(KeyNames.Count, ListResponse.S3Objects.Count);

			var ObjectList = GetKeyVersions(KeyNames);
			var DelResponse = Client.DeleteObjects(BucketName, ObjectList);

			Assert.AreEqual(KeyNames.Count, DelResponse.DeletedObjects.Count);
			Assert.Empty(DelResponse.DeleteErrors);

			ListResponse = Client.ListObjectsV2(BucketName);
			Assert.Empty(ListResponse.S3Objects);

			DelResponse = Client.DeleteObjects(BucketName, ObjectList);
			Assert.AreEqual(KeyNames.Count, DelResponse.DeletedObjects.Count);
			Assert.Empty(DelResponse.DeleteErrors);

			ListResponse = Client.ListObjectsV2(BucketName);
			Assert.Empty(ListResponse.S3Objects);
		}

		[TestMethod("test_multi_object_delete_quiet")]
		[TestProperty(MainData.Major, "DeleteObjects")]
		[TestProperty(MainData.Minor, "Quiet")]
		[TestProperty(MainData.Explanation, "quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_multi_object_delete_quiet()
		{
			var KeyNames = new List<string>() { "key0", "key1", "key2" };
			var BucketName = SetupObjects(KeyNames);
			var Client = GetClient();

			var ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(KeyNames.Count, ListResponse.S3Objects.Count);

			var ObjectList = GetKeyVersions(KeyNames);
			var DelResponse = Client.DeleteObjects(BucketName, ObjectList, Quiet: true);

			Assert.AreEqual(0, DelResponse.DeletedObjects.Count);

			ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(0, ListResponse.S3Objects.Count);
		}

		[TestMethod("test_directory_delete")]
		[TestProperty(MainData.Major, "DeleteObjects")]
		[TestProperty(MainData.Minor, "Directory")]
		[TestProperty(MainData.Explanation, "업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_directory_delete()
		{
			var KeyNames = new List<string>() { "a/b/", "a/b/c/d/obj1", "a/b/c/d/obj2", "1/2/", "1/2/3/4/obj1", "q/w/e/r/obj" };
			var BucketName = SetupObjects(KeyNames, Body: "");
			var Client = GetClient();

			var ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(KeyNames.Count, ListResponse.S3Objects.Count);

			Client.DeleteObject(BucketName, "a/b/");
			Client.DeleteObject(BucketName, "1/2/");
			Client.DeleteObject(BucketName, "q/w/");

			ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(4, ListResponse.S3Objects.Count);

			Client.DeleteObject(BucketName, "a/b/");
			Client.DeleteObject(BucketName, "1/2/");
			Client.DeleteObject(BucketName, "q/w/");

			ListResponse = Client.ListObjects(BucketName);
			Assert.AreEqual(4, ListResponse.S3Objects.Count);
		}

		[TestMethod("test_directory_delete_versions")]
		[TestProperty(MainData.Major, "DeleteObjects")]
		[TestProperty(MainData.Minor, "Versioning")]
		[TestProperty(MainData.Explanation, "버저닝 된 버킷에 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")]
		[TestProperty(MainData.Result, MainData.ResultSuccess)]
		public void test_directory_delete_versions()
		{
			var KeyNames = new List<string>() { "a/", "a/obj1", "a/obj2", "b/", "b/obj1" };
			var BucketName = GetNewBucket();
			var Client = GetClient();

			CheckConfigureVersioningRetry(BucketName, VersionStatus.Enabled);
			foreach (var Key in KeyNames)
				SetupMultipleVersion(Client, BucketName, Key, 3, false);

			var ListResponse = Client.ListObjectsV2(BucketName);
			Assert.AreEqual(KeyNames.Count, ListResponse.S3Objects.Count);

			var VerResponse = Client.ListVersions(BucketName);
			Assert.AreEqual(15, VerResponse.Versions.Count);

			Client.DeleteObject(BucketName, "a/");

			ListResponse = Client.ListObjectsV2(BucketName);
			Assert.AreEqual(KeyNames.Count, ListResponse.S3Objects.Count);

			VerResponse = Client.ListVersions(BucketName);
			Assert.AreEqual(16, VerResponse.Versions.Count);

			var DeleteList = new List<string> { "a/obj1", "a/obj2" };
			var ObjectList = GetKeyVersions(DeleteList);

			var DelResponse = Client.DeleteObjects(BucketName, ObjectList);
			Assert.AreEqual(2, DelResponse.DeletedObjects.Count);

			VerResponse = Client.ListVersions(BucketName);
			Assert.AreEqual(18, VerResponse.Versions.Count);
		}
	}
}
