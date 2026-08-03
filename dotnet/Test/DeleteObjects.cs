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
using Amazon.S3.Model;
using System;
using System.Linq;
using System.Net;
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
			TestId = 1;
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
			TestId = 2;
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
		public void TestMultiObjectDeleteVersions()
		{
			TestId = 3;
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
		public void TestMultiObjectDeleteQuiet()
		{
			TestId = 4;
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
			TestId = 5;
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
			TestId = 6;
			var keyNames = new List<string>() { "a/", "a/obj1", "a/obj2", "b/", "b/obj1" };
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			foreach (var key in keyNames)
				SetupMultipleVersion(client, bucketName, key, 3, false);

			var listResponse = client.ListObjectsV2(bucketName);
			Assert.Equal(keyNames.Count, listResponse.S3Objects.Count);

			var verResponse = client.ListVersions(bucketName);
			Assert.Equal(15, GetVersions(verResponse.Versions).Count);

			client.DeleteObject(bucketName, "a/");

			listResponse = client.ListObjectsV2(bucketName);
			Assert.Equal(4, listResponse.S3Objects.Count);

			verResponse = client.ListVersions(bucketName);
			Assert.Equal(15, GetVersions(verResponse.Versions).Count);
			Assert.Single(GetDeleteMarkers(verResponse.Versions));

			var deleteList = new List<string> { "a/obj1", "a/obj2" };
			var objectList = GetKeyVersions(deleteList);

			var delResponse = client.DeleteObjects(bucketName, objectList);
			Assert.Equal(2, delResponse.DeletedObjects.Count);

			verResponse = client.ListVersions(bucketName);
			Assert.Equal(15, GetVersions(verResponse.Versions).Count);
			Assert.Equal(3, GetDeleteMarkers(verResponse.Versions).Count);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "DeleteObjects")]
		public void TestDeleteObjects()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			const int keyCount = 100;
			var keyNames = new List<string>();

			for (var i = 0; i < keyCount; i++)
			{
				var key = string.Format("key-{0:D3}", i);
				keyNames.Add(key);
				client.PutObject(bucketName, key, body: key);
			}

			var listResponse = client.ListObjects(bucketName);
			Assert.Equal(keyCount, listResponse.S3Objects.Count);

			var objectList = GetKeyVersions(keyNames);
			var delResponse = client.DeleteObjects(bucketName, objectList);
			Assert.Equal(keyCount, delResponse.DeletedObjects.Count);

			listResponse = client.ListObjects(bucketName);
			Assert.Empty(listResponse.S3Objects);

			foreach (var key in keyNames)
			{
				var e = Assert.Throws<AggregateException>(() => client.GetObject(bucketName, key));
				Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			}
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Versioning")]
		public void TestDeleteObjectsWithVersioning()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			const string methodName = "testDeleteObjectsWithVersioning";
			var keyNames = new List<string>
			{
				methodName + "-0",
				methodName + "-1",
				methodName + "-2",
				methodName + "-3",
				methodName + "-4"
			};

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			foreach (var key in keyNames)
				SetupMultipleVersion(client, bucketName, key, 2, false);

			var initialVersResponse = client.ListVersions(bucketName);
			var nonCurrentVersions = new List<KeyVersion>();
			foreach (var key in keyNames)
			{
				var keyVersions = GetVersions(initialVersResponse.Versions).Where(v => v.Key == key).ToList();
				if (keyVersions.Count > 0)
				{
					var oldestVersion = keyVersions[^1];
					nonCurrentVersions.Add(new KeyVersion { Key = oldestVersion.Key, VersionId = oldestVersion.VersionId });
				}
			}

			var objectList = GetKeyVersions(keyNames);
			var mixedDeleteList = new List<KeyVersion>(objectList);
			mixedDeleteList.AddRange(nonCurrentVersions);

			var delResponse = client.DeleteObjects(bucketName, mixedDeleteList);
			Assert.Equal(keyNames.Count + nonCurrentVersions.Count, delResponse.DeletedObjects.Count);

			var versResponse = client.ListVersions(bucketName);
			Assert.Equal(5, GetDeleteMarkers(versResponse.Versions).Count);
			Assert.Equal(5, GetVersions(versResponse.Versions).Count);

			// dotnet SDK의 Versions는 삭제 마커까지 포함하므로 한 번의 순회로 전체 삭제 목록을 만든다.
			var deleteList = new List<KeyVersion>();
			foreach (var version in versResponse.Versions)
				deleteList.Add(new KeyVersion { Key = version.Key, VersionId = version.VersionId });

			delResponse = client.DeleteObjects(bucketName, deleteList);
			Assert.Equal(versResponse.Versions.Count, delResponse.DeletedObjects.Count);

			versResponse = client.ListVersions(bucketName);
			Assert.Empty(GetVersions(versResponse.Versions));
			Assert.Empty(GetDeleteMarkers(versResponse.Versions));
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Versioning")]
		public void TestDeleteObjectsWithVersioningDeleteMarker()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectsWithVersioningDeleteMarker";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			client.PutObject(bucketName, key, body: key);
			client.DeleteObject(bucketName, key);

			var versResponse = client.ListVersions(bucketName);
			Assert.Single(GetVersions(versResponse.Versions));
			Assert.Single(GetDeleteMarkers(versResponse.Versions));
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Versioning")]
		public void TestVersioningMultiObjectDeleteWithMarker()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var keyNames = new List<string>
			{
				"testVersioningMultiObjectDeleteWithMarker-0",
				"testVersioningMultiObjectDeleteWithMarker-1",
				"testVersioningMultiObjectDeleteWithMarker-2"
			};

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			foreach (var key in keyNames)
				client.PutObject(bucketName, key, body: key);

			client.DeleteObjects(bucketName, GetKeyVersions(keyNames));

			var versResponse = client.ListVersions(bucketName);
			Assert.Equal(keyNames.Count, GetVersions(versResponse.Versions).Count);
			Assert.Equal(keyNames.Count, GetDeleteMarkers(versResponse.Versions).Count);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Versioning")]
		public void TestVersioningMultiObjectDeleteWithMarkerCreate()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testVersioningMultiObjectDeleteWithMarkerCreate";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			for (var i = 0; i < 10; i++)
				client.DeleteObject(bucketName, key);

			var versResponse = client.ListVersions(bucketName);
			Assert.Empty(GetVersions(versResponse.Versions));
			Assert.Equal(10, GetDeleteMarkers(versResponse.Versions).Count);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "Versioning")]
		public void TestVersioningMultiObjectDeleteWithMarkerCreateObjects()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testVersioningMultiObjectDeleteWithMarkerCreateObjects";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);
			for (var i = 0; i < 10; i++)
				client.DeleteObjects(bucketName, GetKeyVersions(new List<string> { key }));

			var versResponse = client.ListVersions(bucketName);
			Assert.Empty(GetVersions(versResponse.Versions));
			Assert.Equal(10, GetDeleteMarkers(versResponse.Versions).Count);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectIfMatchGood()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectIfMatchGood";

			var eTag = client.PutObject(bucketName, key, body: key).ETag;
			client.DeleteObject(bucketName, key, ifMatch: eTag);

			var listResponse = client.ListObjects(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectIfMatchFailed()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectIfMatchFailed";

			client.PutObject(bucketName, key, body: key);

			var e = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key, ifMatch: "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
			Assert.Equal(HttpStatusCode.PreconditionFailed, GetStatus(e));
			Assert.Equal(MainData.PRECONDITION_FAILED, GetErrorCode(e));

			var listResponse = client.ListObjects(bucketName);
			Assert.Single(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectIfMatchAny()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectIfMatchAny";

			client.PutObject(bucketName, key, body: key);
			client.DeleteObject(bucketName, key, ifMatch: "*");

			var listResponse = client.ListObjects(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectIfMatchAndIfNoneMatch()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectIfMatchAndIfNoneMatch";

			var eTag = client.PutObject(bucketName, key, body: key).ETag;
			var e = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key, ifMatch: eTag,
				headerList: [new KeyValuePair<string, string>("If-None-Match", eTag)]));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			var listResponse = client.ListObjects(bucketName);
			Assert.Single(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectIfMatchAndIfNoneMatchAny()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectIfMatchAndIfNoneMatchAny";

			var eTag = client.PutObject(bucketName, key, body: key).ETag;
			var e = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key, ifMatch: eTag,
				headerList: [new KeyValuePair<string, string>("If-None-Match", "*")]));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			var listResponse = client.ListObjects(bucketName);
			Assert.Single(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectsIfMatchGood()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var keyNames = new List<string> { "testDeleteObjectsIfMatchGood0", "testDeleteObjectsIfMatchGood1" };
			var objectList = new List<KeyVersion>();

			foreach (var key in keyNames)
			{
				var eTag = client.PutObject(bucketName, key, body: key).ETag;
				objectList.Add(new KeyVersion { Key = key, ETag = eTag });
			}

			var delResponse = client.DeleteObjects(bucketName, objectList);
			Assert.Equal(keyNames.Count, delResponse.DeletedObjects.Count);

			var listResponse = client.ListObjects(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectsIfMatchMixed()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			const string goodKey = "testDeleteObjectsIfMatchMixedGood";
			const string badKey = "testDeleteObjectsIfMatchMixedBad";

			var goodETag = client.PutObject(bucketName, goodKey, body: goodKey).ETag;
			client.PutObject(bucketName, badKey, body: badKey);

			var objectList = new List<KeyVersion>
			{
				new() { Key = goodKey, ETag = goodETag },
				new() { Key = badKey, ETag = "\"ABCDEFGHIJKLMNOPQRSTUVWXYZ\"" }
			};

			// .NET SDK는 일부만 실패해도 DeleteObjectsException을 던지므로 예외에서 응답을 꺼낸다.
			DeleteObjectsResponse delResponse;
			try { delResponse = client.DeleteObjects(bucketName, objectList); }
			catch (AggregateException e) when (e.InnerException is DeleteObjectsException de) { delResponse = de.Response; }

			Assert.Single(delResponse.DeletedObjects);
			Assert.Equal(goodKey, delResponse.DeletedObjects[0].Key);
			Assert.Single(delResponse.DeleteErrors);
			Assert.Equal(badKey, delResponse.DeleteErrors[0].Key);
			Assert.Equal(MainData.PRECONDITION_FAILED, delResponse.DeleteErrors[0].Code);

			var listResponse = client.ListObjects(bucketName);
			Assert.Single(listResponse.S3Objects);
			Assert.Equal(badKey, listResponse.S3Objects[0].Key);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectsIfMatchAndIfNoneMatch()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectsIfMatchAndIfNoneMatch";

			var eTag = client.PutObject(bucketName, key, body: key).ETag;
			var objectList = new List<KeyVersion> { new() { Key = key } };
			var e = Assert.Throws<AggregateException>(() => client.DeleteObjects(bucketName, objectList,
				headerList:
				[
					new KeyValuePair<string, string>("If-Match", eTag),
					new KeyValuePair<string, string>("If-None-Match", eTag)
				]));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			var listResponse = client.ListObjects(bucketName);
			Assert.Single(listResponse.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "DeleteObjects")]
		[Trait(MainData.Minor, "IfMatch")]
		public void TestDeleteObjectsIfMatchAndIfNoneMatchAny()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testDeleteObjectsIfMatchAndIfNoneMatchAny";

			var eTag = client.PutObject(bucketName, key, body: key).ETag;
			var objectList = new List<KeyVersion> { new() { Key = key } };
			var e = Assert.Throws<AggregateException>(() => client.DeleteObjects(bucketName, objectList,
				headerList:
				[
					new KeyValuePair<string, string>("If-Match", eTag),
					new KeyValuePair<string, string>("If-None-Match", "*")
				]));
			Assert.Equal(HttpStatusCode.NotImplemented, GetStatus(e));
			Assert.Equal(MainData.NOT_IMPLEMENTED, GetErrorCode(e));

			var listResponse = client.ListObjects(bucketName);
			Assert.Single(listResponse.S3Objects);
		}
	}
}
