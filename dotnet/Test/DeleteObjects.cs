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
using System.Collections.Generic;
using Amazon.S3;
using Xunit;

namespace s3tests.Test
{
	public class DeleteObjects : TestBase
	{
		public DeleteObjects(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "ListObject")]
		[Trait(MainData.Explanation, "버킷에 존재하는 오브젝트 여러개를 한번에 삭제")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultiObjectDelete()
		{
			var keyNames = new List<string>() { "key0", "key1", "key2" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var listResponse = client.ListObjects(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			var objectList = GetKeyVersions(keyNames);
			var delResponse = client.DeleteObjects(bucketName, objectList);

			Assert.Equal(keyNames.Count, delResponse.DeletedObjects.Count);
			Assert.Empty(delResponse.DeleteErrors);

			listResponse = client.ListObjects(bucketName);
			Assert.Empty(listResponse.S3Objects);

			delResponse = client.DeleteObjects(bucketName, objectList);
			Assert.Equal(keyNames.Count, delResponse.DeletedObjects.Count);
			Assert.Empty(delResponse.DeleteErrors);

			listResponse = client.ListObjects(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "ListObjectsV2")]
		[Trait(MainData.Explanation, "버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultiObjectV2Delete()
		{
			var keyNames = new List<string>() { "key0", "key1", "key2" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var listResponse = client.ListObjectsV2(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			var objectList = GetKeyVersions(keyNames);
			var delResponse = client.DeleteObjects(bucketName, objectList);

			Assert.Equal(keyNames.Count, delResponse.DeletedObjects.Count);
			Assert.Empty(delResponse.DeleteErrors);

			listResponse = client.ListObjectsV2(bucketName);
			Assert.Empty(listResponse.S3Objects);

			delResponse = client.DeleteObjects(bucketName, objectList);
			Assert.Equal(keyNames.Count, delResponse.DeletedObjects.Count);
			Assert.Empty(delResponse.DeleteErrors);

			listResponse = client.ListObjectsV2(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "버킷에 존재하는 버저닝 오브젝트 여러개를 한번에 삭제")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multi_object_delete_versions()
		{
			var keyNames = new List<string>() { "key0", "key1", "key2" };
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			foreach (var key in keyNames)
				SetupMultipleVersion(client, bucketName, key, 3, false);

			var listResponse = client.ListObjectsV2(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			var objectList = GetKeyVersions(keyNames);
			var delResponse = client.DeleteObjects(bucketName, objectList);

			Assert.Equal(keyNames.Count, delResponse.DeletedObjects.Count);
			Assert.Empty(delResponse.DeleteErrors);

			listResponse = client.ListObjectsV2(bucketName);
			Assert.Empty(listResponse.S3Objects);

			delResponse = client.DeleteObjects(bucketName, objectList);
			Assert.Equal(keyNames.Count, delResponse.DeletedObjects.Count);
			Assert.Empty(delResponse.DeleteErrors);

			listResponse = client.ListObjectsV2(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Quiet")]
		[Trait(MainData.Explanation, "quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_multi_object_delete_quiet()
		{
			var keyNames = new List<string>() { "key0", "key1", "key2" };
			var bucketName = SetupObjects(keyNames);
			var client = GetClient();

			var listResponse = client.ListObjects(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			var objectList = GetKeyVersions(keyNames);
			var delResponse = client.DeleteObjects(bucketName, objectList, quiet: true);

			Assert.Empty(delResponse.DeletedObjects);

			listResponse = client.ListObjects(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Directory")]
		[Trait(MainData.Explanation, "업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDirectoryDelete()
		{
			var keyNames = new List<string>() { "a/b/", "a/b/c/d/obj1", "a/b/c/d/obj2", "1/2/", "1/2/3/4/obj1", "q/w/e/r/obj" };
			var bucketName = SetupObjects(keyNames, body: "");
			var client = GetClient();

			var listResponse = client.ListObjects(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			client.DeleteObject(bucketName, "a/b/");
			client.DeleteObject(bucketName, "1/2/");
			client.DeleteObject(bucketName, "q/w/");

			listResponse = client.ListObjects(bucketName);
			Assert.Equal(4, listResponse.S3Objects.Count);

			client.DeleteObject(bucketName, "a/b/");
			client.DeleteObject(bucketName, "1/2/");
			client.DeleteObject(bucketName, "q/w/");

			listResponse = client.ListObjects(bucketName);
			Assert.Equal(4, listResponse.S3Objects.Count);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "버저닝 된 버킷에 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDirectoryDeleteVersions()
		{
			var keyNames = new List<string>() { "a/", "a/obj1", "a/obj2", "b/", "b/obj1" };
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			foreach (var key in keyNames)
				SetupMultipleVersion(client, bucketName, key, 3, false);

			var listResponse = client.ListObjectsV2(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			var verResponse = client.ListVersions(bucketName);
			Assert.Equal(15, verResponse.Versions.Count);

			client.DeleteObject(bucketName, "a/");

			listResponse = client.ListObjectsV2(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			verResponse = client.ListVersions(bucketName);
			Assert.Equal(16, verResponse.Versions.Count);

			var deleteList = new List<string> { "a/obj1", "a/obj2" };
			var objectList = GetKeyVersions(deleteList);

			var delResponse = client.DeleteObjects(bucketName, objectList);
			Assert.Equal(2, delResponse.DeletedObjects.Count);

			verResponse = client.ListVersions(bucketName);
			Assert.Equal(18, verResponse.Versions.Count);
		}
	}
}
