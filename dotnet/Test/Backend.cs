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
using System;
using System.Collections.Generic;
using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using s3tests.Utils;
using Xunit;

namespace s3tests.Test
{
	public class Backend : TestBase
	{
		public Backend(Xunit.Abstractions.ITestOutputHelper output) => Output = output;

		private const string AllUsers = "http://acs.amazonaws.com/groups/global/AllUsers";

		/// <summary>메인 유저의 FULL_CONTROL 권한과 AllUsers 그룹의 권한으로 구성된 acl 목록을 생성한다.</summary>
		private List<S3Grant> PublicAcl(params S3Permission[] permissions)
		{
			var grants = new List<S3Grant>()
			{
				new() { Permission = S3Permission.FULL_CONTROL, Grantee = new S3Grantee() { CanonicalUser = Config.MainUser.UserId, DisplayName = Config.MainUser.DisplayName } }
			};
			foreach (var permission in permissions)
				grants.Add(new S3Grant() { Permission = permission, Grantee = new S3Grantee() { URI = AllUsers } });
			return grants;
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "PutObject")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트를 업로드할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObject()
		{
			TestId = 1;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testPutObject";
			var content = "test content";

			// Backend 클라이언트로 업로드
			var response = backendClient.PutObject(bucketName, key, body: content);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 다운로드하여 확인
			var getResponse = client.GetObject(bucketName, key);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "GetObject")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트를 다운로드할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObject()
		{
			TestId = 2;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testGetObject";
			var content = "test content";

			// 일반 클라이언트로 업로드
			client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 다운로드
			var response = backendClient.GetObject(bucketName, key);
			var body = GetBody(response);
			Assert.Equal(content, body);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "DeleteObject")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트를 삭제할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteObject()
		{
			TestId = 3;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testDeleteObject";
			var content = "test content";

			// 일반 클라이언트로 업로드
			client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 삭제
			var response = backendClient.DeleteObject(bucketName, key);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);

			// 일반 클라이언트로 오브젝트 목록 확인
			var listResponse = client.ListObjectsV2(bucketName);
			Assert.Empty(listResponse.S3Objects);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "CopyObject")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트를 복사할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObject()
		{
			TestId = 4;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var sourceBucket = GetNewBucket();
			var targetBucket = GetNewBucket();
			var sourceKey = "sourceKey";
			var targetKey = "targetKey";
			var content = "test content";

			// 일반 클라이언트로 소스 오브젝트 업로드
			client.PutObject(sourceBucket, sourceKey, body: content);

			// Backend 클라이언트로 복사
			var response = backendClient.CopyObject(sourceBucket, sourceKey, targetBucket, targetKey);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObject(targetBucket, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 멀티파트 업로드를 수행할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUpload()
		{
			TestId = 5;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testMultipartUpload";
			var size = 10 * MainData.MB;

			// Backend 클라이언트로 멀티파트 업로드
			var uploadData = S3Utils.SetupMultipartUpload(backendClient, bucketName, key, size);
			var completeResponse = backendClient.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			var versionId = completeResponse.VersionId;

			// 일반 클라이언트로 확인
			var response = client.GetObjectMetadata(bucketName, key, versionId: versionId);
			Assert.Equal(size, response.ContentLength);

			CheckContentUsingRange(client, bucketName, key, uploadData.Body, MainData.MB);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "PutObjectAcl")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트 ACL을 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectAcl()
		{
			TestId = 6;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "testPutObjectAcl";
			var content = "test content";

			// 일반 클라이언트로 업로드
			client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 ACL 설정
			var response = backendClient.PutObjectACL(bucketName, key, acl: S3CannedACL.PublicRead);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 ACL 확인
			var aclResponse = client.GetObjectACL(bucketName, key);
			Assert.Equal(2, aclResponse.AccessControlList.Grants.Count);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "GetObjectAcl")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트 ACL을 조회할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAcl()
		{
			TestId = 7;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "testGetObjectAcl";
			var content = "test content";

			// 일반 클라이언트로 업로드 및 ACL 설정
			client.PutObject(bucketName, key, body: content, acl: S3CannedACL.PublicRead);

			// Backend 클라이언트로 ACL 조회
			var response = backendClient.GetObjectACL(bucketName, key);
			Assert.Equal(2, response.AccessControlList.Grants.Count);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "PutObjectTagging")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트 태그를 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectTagging()
		{
			TestId = 8;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testPutObjectTagging";
			var content = "test content";

			// 일반 클라이언트로 업로드
			client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 태그 설정
			var tagging = new Tagging() { TagSet = [new() { Key = "testKey", Value = "testValue" }] };
			var response = backendClient.PutObjectTagging(bucketName, key, tagging);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 태그 확인
			var getResponse = client.GetObjectTagging(bucketName, key);
			Assert.Single(getResponse.Tagging);
			Assert.Equal("testKey", getResponse.Tagging[0].Key);
			Assert.Equal("testValue", getResponse.Tagging[0].Value);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "GetObjectTagging")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트 태그를 조회할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectTagging()
		{
			TestId = 9;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testGetObjectTagging";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 일반 클라이언트로 업로드 및 태그 설정
			client.PutObject(bucketName, key, body: content, tagSet: tagSet);

			// Backend 클라이언트로 태그 조회
			var response = backendClient.GetObjectTagging(bucketName, key);
			Assert.Single(response.Tagging);
			Assert.Equal("testKey", response.Tagging[0].Key);
			Assert.Equal("testValue", response.Tagging[0].Value);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "DeleteObjectTagging")]
		[Trait(MainData.Explanation, "Backend 헤더를 사용하여 오브젝트 태그를 삭제할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteObjectTagging()
		{
			TestId = 10;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testDeleteObjectTagging";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 일반 클라이언트로 업로드 및 태그 설정
			client.PutObject(bucketName, key, body: content, tagSet: tagSet);

			// Backend 클라이언트로 태그 삭제
			var response = backendClient.DeleteObjectTagging(bucketName, key);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);

			// 일반 클라이언트로 태그 확인
			var getResponse = client.GetObjectTagging(bucketName, key);
			Assert.Empty(getResponse.Tagging);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] PutObject가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectVersioning()
		{
			TestId = 11;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testPutObjectVersioning";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// Backend 클라이언트로 업로드
			var response = backendClient.PutObject(bucketName, key, body: content);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 다운로드하여 확인
			var getResponse = client.GetObject(bucketName, key);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] PutObject 버전 정보 추가시 정상 동작 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectVersioningWithVersionId()
		{
			TestId = 12;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testPutObjectVersioningWithVersionIdSource";
			var key2 = "testPutObjectVersioningWithVersionIdTarget";
			var content = "test content";
			var content2 = "test content2";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 업로드
			var response = backendClient.PutObject(bucketName, key2, body: content2,
				headerList: [
					new(BackendHeaders.IFS_VERSION_ID, versionId),
					new(BackendHeaders.KSAN_VERSION_ID, versionId),
				]);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 다운로드하여 버전 정보가 일치하는지 확인
			var getResponse = client.GetObject(bucketName, key2, versionId: versionId);
			var body = GetBody(getResponse);
			Assert.Equal(content2, body);
			Assert.Equal(versionId, getResponse.VersionId);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] GetObject가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectVersioning()
		{
			TestId = 13;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testGetObjectVersioning";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 다운로드
			var response = backendClient.GetObject(bucketName, key, versionId: versionId);
			var body = GetBody(response);
			Assert.Equal(content, body);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] DeleteObject가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteObjectVersioning()
		{
			TestId = 14;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testDeleteObjectVersioning";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 삭제 (삭제 마커 생성)
			var response = backendClient.DeleteObject(bucketName, key);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);

			// 일반 클라이언트로 버전 목록 확인 (삭제 마커가 생성됨)
			var listResponse = client.ListVersions(bucketName);
			Assert.Equal(1, GetDeleteMarkerCount(listResponse.Versions));

			// Backend 클라이언트로 버전 포함하여 삭제
			var deleteResponse = backendClient.DeleteObject(bucketName, key, versionId: versionId);
			Assert.Equal(HttpStatusCode.NoContent, deleteResponse.HttpStatusCode);

			// 일반 클라이언트로 버전 목록 확인
			listResponse = client.ListVersions(bucketName);
			Assert.Empty(GetVersions(listResponse.Versions));
			Assert.Equal(1, GetDeleteMarkerCount(listResponse.Versions));
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] DeleteObjects가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteObjectsVersioning()
		{
			TestId = 15;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var methodName = "testDeleteObjectsVersioning";
			var keyNames = new List<string>()
			{
				methodName + "-0",
				methodName + "-1",
				methodName + "-2",
				methodName + "-3",
				methodName + "-4",
			};
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 5개의 파일을 일반 클라이언트로 업로드
			foreach (var key in keyNames) client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 DeleteObjects 삭제 (삭제 마커 생성)
			var deleteResponse = backendClient.DeleteObjects(bucketName, GetKeyVersions(keyNames));
			Assert.Equal(5, deleteResponse.DeletedObjects.Count);

			// 일반 클라이언트로 버전 목록 확인 (삭제 마커가 생성됨)
			var listResponse = client.ListVersions(bucketName);
			Assert.Equal(5, GetDeleteMarkerCount(listResponse.Versions));
			Assert.Equal(5, GetVersions(listResponse.Versions).Count);

			// Backend 클라이언트로 버전 정보 포함하여 DeleteObjects로 삭제
			// 모든 버전과 삭제 마커를 삭제 리스트에 추가
			var deleteList = new List<KeyVersion>();
			foreach (var version in listResponse.Versions)
				deleteList.Add(new KeyVersion() { Key = version.Key, VersionId = version.VersionId });

			var finalDeleteResponse = backendClient.DeleteObjects(bucketName, deleteList);
			Assert.Equal(10, finalDeleteResponse.DeletedObjects.Count);

			// 일반 클라이언트로 버전 목록 확인 (모두 삭제됨)
			listResponse = client.ListVersions(bucketName);
			Assert.Empty(GetVersions(listResponse.Versions));
			Assert.Equal(0, GetDeleteMarkerCount(listResponse.Versions));
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] HeadObject가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestHeadObjectVersioning()
		{
			TestId = 16;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testHeadObjectVersioning";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 헤더 조회
			var response = backendClient.GetObjectMetadata(bucketName, key, versionId: versionId);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
			Assert.Equal(content.Length, response.ContentLength);
			Assert.Equal(versionId, response.VersionId);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] CopyObject가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectVersioning()
		{
			TestId = 17;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var sourceBucket = GetNewBucket();
			var targetBucket = GetNewBucket();
			var sourceKey = "sourceKey";
			var sourceKey2 = "sourceKey2";
			var targetKey = "targetKey";
			var content = "test content";

			// 소스/타겟 버킷에 버저닝 활성화
			CheckConfigureVersioningRetry(sourceBucket, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(targetBucket, VersionStatus.Enabled);

			// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
			var putResponse = client.PutObject(sourceBucket, sourceKey, body: content);
			var sourceVid = putResponse.VersionId;

			var copyResponse = client.CopyObject(sourceBucket, sourceKey, sourceBucket, sourceKey2, versionId: sourceVid);
			var targetVid = copyResponse.VersionId;

			// Backend 클라이언트로 복사
			backendClient.CopyObject(sourceBucket, sourceKey2, targetBucket, targetKey, versionId: targetVid,
				headerList: [
					new(BackendHeaders.IFS_VERSION_ID, targetVid),
					new(BackendHeaders.KSAN_VERSION_ID, targetVid),
				]);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObject(targetBucket, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(targetVid, getResponse.VersionId);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] MultipartUpload가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadVersioning()
		{
			TestId = 18;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testMultipartUploadVersioning";
			var size = 10 * MainData.MB;

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// Backend 클라이언트로 멀티파트 업로드
			var uploadData = S3Utils.SetupMultipartUpload(backendClient, bucketName, key, size);
			var completeResponse = backendClient.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);
			var versionId = completeResponse.VersionId;

			// 일반 클라이언트로 확인
			var response = client.GetObjectMetadata(bucketName, key, versionId: versionId);
			Assert.Equal(size, response.ContentLength);

			CheckContentUsingRange(client, bucketName, key, uploadData.Body, MainData.MB);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] PutObjectAcl가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectAclVersioning()
		{
			TestId = 19;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "testPutObjectAclVersioning";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 ACL 설정
			var response = backendClient.PutObjectACL(bucketName, key, acl: S3CannedACL.PublicRead, versionId: versionId);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 ACL 확인
			var aclResponse = client.GetObjectACL(bucketName, key, versionId: versionId);
			Assert.Equal(2, aclResponse.AccessControlList.Grants.Count);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] GetObjectAcl가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAclVersioning()
		{
			TestId = 20;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var key = "testGetObjectAclVersioning";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드 및 ACL 설정
			client.PutObject(bucketName, key, body: content, acl: S3CannedACL.PublicRead);

			// Backend 클라이언트로 ACL 조회
			var response = backendClient.GetObjectACL(bucketName, key);
			Assert.Equal(2, response.AccessControlList.Grants.Count);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] PutObjectTagging가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectTaggingVersioning()
		{
			TestId = 21;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testPutObjectTaggingVersioning";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 태그 설정
			var tagging = new Tagging() { TagSet = [new() { Key = "testKey", Value = "testValue" }] };
			var response = backendClient.PutObjectTagging(bucketName, key, tagging);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			// 일반 클라이언트로 태그 확인
			var getResponse = client.GetObjectTagging(bucketName, key);
			Assert.Single(getResponse.Tagging);
			Assert.Equal("testKey", getResponse.Tagging[0].Key);
			Assert.Equal("testValue", getResponse.Tagging[0].Value);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] GetObjectTagging가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectTaggingVersioning()
		{
			TestId = 22;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testGetObjectTaggingVersioning";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드 및 태그 설정
			client.PutObject(bucketName, key, body: content, tagSet: tagSet);

			// Backend 클라이언트로 태그 조회
			var response = backendClient.GetObjectTagging(bucketName, key);
			Assert.Single(response.Tagging);
			Assert.Equal("testKey", response.Tagging[0].Key);
			Assert.Equal("testValue", response.Tagging[0].Value);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] DeleteObjectTagging가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteObjectTaggingVersioning()
		{
			TestId = 23;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var key = "testDeleteObjectTaggingVersioning";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드 및 태그 설정
			client.PutObject(bucketName, key, body: content, tagSet: tagSet);

			// Backend 클라이언트로 태그 삭제
			var response = backendClient.DeleteObjectTagging(bucketName, key);
			Assert.Equal(HttpStatusCode.NoContent, response.HttpStatusCode);

			// 일반 클라이언트로 태그 확인
			var getResponse = client.GetObjectTagging(bucketName, key);
			Assert.Empty(getResponse.Tagging);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] PutObjectRetention가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectRetentionVersioning()
		{
			TestId = 24;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketName();
			var key = "testPutObjectRetentionVersioning";
			var content = "test content";

			// 버킷 생성
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 보존 설정
			var retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};
			var response = backendClient.PutObjectRetention(bucketName, key, retention, bypassGovernanceRetention: true);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);

			client.DeleteObject(bucketName, key, versionId: putResponse.VersionId, bypassGovernanceRetention: true);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] GetObjectRetention가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectRetentionVersioning()
		{
			TestId = 25;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketName();
			var key = "testGetObjectRetentionVersioning";
			var content = "test content";

			// 버킷 생성
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 보존 설정 조회
			// 보존 설정이 되어있지 않은 경우 예외 발생
			Assert.Throws<AggregateException>(() => backendClient.GetObjectRetention(bucketName, key));
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "[Versioning] PutObjectRetention 후 GetObjectRetention으로 조회가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutAndGetObjectRetentionVersioning()
		{
			TestId = 26;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketName();
			var key = "testPutAndGetObjectRetentionVersioning";
			var content = "test content";

			// 버킷 생성
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, key, body: content);

			// Backend 클라이언트로 보존 설정
			var retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};
			var putRetentionResponse = backendClient.PutObjectRetention(bucketName, key, retention, bypassGovernanceRetention: true);
			Assert.Equal(HttpStatusCode.OK, putRetentionResponse.HttpStatusCode);

			// Backend 클라이언트로 보존 설정 조회
			var getRetentionResponse = backendClient.GetObjectRetention(bucketName, key);
			Assert.Equal(HttpStatusCode.OK, getRetentionResponse.HttpStatusCode);
			Assert.Equal(ObjectLockRetentionMode.Governance, getRetentionResponse.Retention.Mode);
			Assert.Equal(retention.RetainUntilDate, getRetentionResponse.Retention.RetainUntilDate);

			// 정리
			client.DeleteObject(bucketName, key, versionId: putResponse.VersionId, bypassGovernanceRetention: true);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "PutObject 복제가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectReplication()
		{
			TestId = 27;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var sourceBucketName = GetNewBucket();
			var targetBucketName = GetNewBucket();
			var key = "testBackendReplication";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(sourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(targetBucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(sourceBucketName, key, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendPutObject(backendClient, sourceBucketName, key, targetBucketName, key, versionId);

			// 일반 클라이언트로 확인
			var getResponse = client.GetObject(targetBucketName, key);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(versionId, getResponse.VersionId);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "PutObject 태그가 복제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectWithTaggingReplication()
		{
			TestId = 28;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var sourceBucketName = GetNewBucket();
			var targetBucketName = GetNewBucket();
			var key = "testBackendReplicationTagging";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(sourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(targetBucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(sourceBucketName, key, body: content, tagSet: tagSet);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendPutObject(backendClient, sourceBucketName, key, targetBucketName, key, versionId);

			// 일반 클라이언트로 확인
			var getResponse = client.GetObject(targetBucketName, key);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(versionId, getResponse.VersionId);

			var tagResponse = client.GetObjectTagging(targetBucketName, key);
			TaggingCompare(tagSet, tagResponse.Tagging);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "PutObject 헤더와 메타데이터가 복제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectWithMetadataReplication()
		{
			TestId = 29;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var sourceBucketName = GetNewBucket();
			var targetBucketName = GetNewBucket();
			var key = "testBackendReplicationMetadata";
			var content = "test content";
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-test-key", "testValue") };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(sourceBucketName, VersionStatus.Enabled);
			CheckConfigureVersioningRetry(targetBucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(sourceBucketName, key, body: content, metadataList: metadataList);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendPutObject(backendClient, sourceBucketName, key, targetBucketName, key, versionId);

			// 일반 클라이언트로 확인
			var getResponse = client.GetObject(targetBucketName, key);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(versionId, getResponse.VersionId);
			Assert.Equal("testValue", getResponse.Metadata["x-amz-meta-test-key"]);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "CopyObject 복제가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectReplication()
		{
			TestId = 30;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucket = GetNewBucket();
			var sourceKey = "sourceKey";
			var sourceKey2 = "sourceKey2";
			var targetKey = "targetKey";
			var content = "test content";

			// 버킷에 버저닝 활성화
			CheckConfigureVersioningRetry(bucket, VersionStatus.Enabled);

			// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
			var putResponse = client.PutObject(bucket, sourceKey, body: content);
			var sourceVid = putResponse.VersionId;

			var copyResponse = client.CopyObject(bucket, sourceKey, bucket, sourceKey2, versionId: sourceVid);
			var targetVid = copyResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObject(bucket, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(targetVid, getResponse.VersionId);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "CopyObject 태그가 복제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectWithTaggingReplication()
		{
			TestId = 31;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucket = GetNewBucket();
			var sourceKey = "sourceKey";
			var sourceKey2 = "sourceKey2";
			var targetKey = "targetKey";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 버킷에 버저닝 활성화
			CheckConfigureVersioningRetry(bucket, VersionStatus.Enabled);

			// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
			var putResponse = client.PutObject(bucket, sourceKey, body: content, tagSet: tagSet);
			var sourceVid = putResponse.VersionId;

			var copyResponse = client.CopyObject(bucket, sourceKey, bucket, sourceKey2, versionId: sourceVid);
			var targetVid = copyResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObject(bucket, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(targetVid, getResponse.VersionId);

			var tagResponse = client.GetObjectTagging(bucket, targetKey);
			TaggingCompare(tagSet, tagResponse.Tagging);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "CopyObject 헤더와 메타데이터가 복제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectWithMetadataReplication()
		{
			TestId = 32;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucket = GetNewBucket();
			var sourceKey = "sourceKey";
			var sourceKey2 = "sourceKey2";
			var targetKey = "targetKey";
			var content = "test content";
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-test-key", "testValue") };

			// 버킷에 버저닝 활성화
			CheckConfigureVersioningRetry(bucket, VersionStatus.Enabled);

			// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
			var putResponse = client.PutObject(bucket, sourceKey, body: content, metadataList: metadataList);
			var sourceVid = putResponse.VersionId;

			var copyResponse = client.CopyObject(bucket, sourceKey, bucket, sourceKey2, versionId: sourceVid);
			var targetVid = copyResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObject(bucket, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(targetVid, getResponse.VersionId);
			Assert.Equal("testValue", getResponse.Metadata["x-amz-meta-test-key"]);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "CopyObject 메타데이터가 Replace되었을 경우 복제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCopyObjectMetadataReplaceReplication()
		{
			TestId = 33;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucket = GetNewBucket();
			var sourceKey = "sourceKey";
			var sourceKey2 = "sourceKey2";
			var targetKey = "targetKey";
			var content = "test content";
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-test-key", "testValue") };
			var metadataList2 = new List<KeyValuePair<string, string>>() { new("x-amz-meta-test-key2", "testValue2") };

			// 버킷에 버저닝 활성화
			CheckConfigureVersioningRetry(bucket, VersionStatus.Enabled);

			// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
			var putResponse = client.PutObject(bucket, sourceKey, body: content, metadataList: metadataList);
			var sourceVid = putResponse.VersionId;

			var copyResponse = client.CopyObject(bucket, sourceKey, bucket, sourceKey2, versionId: sourceVid,
				metadataList: metadataList2, metadataDirective: S3MetadataDirective.REPLACE);
			var targetVid = copyResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObject(bucket, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(targetVid, getResponse.VersionId);
			Assert.Equal("testValue2", getResponse.Metadata["x-amz-meta-test-key2"]);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "MultipartUpload 복제가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadReplication()
		{
			TestId = 34;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var sourceKey = "testMultipartUploadReplicationSource";
			var targetKey = "testMultipartUploadReplicationTarget";
			var size = 10 * MainData.MB;

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 멀티파트 업로드
			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, sourceKey, size);
			var completeResponse = client.CompleteMultipartUpload(bucketName, sourceKey, uploadData.UploadId, uploadData.Parts);
			var versionId = completeResponse.VersionId;

			// Backend 클라이언트로 멀티파트 업로드
			BackendMultipartUpload(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObjectMetadata(bucketName, targetKey, versionId: versionId);
			Assert.Equal(size, getResponse.ContentLength);
			Assert.Equal(versionId, getResponse.VersionId);

			CheckContentUsingRange(client, bucketName, targetKey, uploadData.Body, MainData.MB);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "MultipartUpload 태그가 복제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadWithTaggingReplication()
		{
			TestId = 35;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var sourceKey = "testMultipartUploadTaggingReplicationSource";
			var targetKey = "testMultipartUploadTaggingReplicationTarget";
			var size = 10 * MainData.MB;
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 멀티파트 업로드 (태그 포함)
			var initUploadData = new MultipartUploadData
			{
				UploadId = client.InitiateMultipartUpload(bucketName, sourceKey, tagSet: tagSet).UploadId
			};
			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, sourceKey, size, uploadData: initUploadData);

			var completeResponse = client.CompleteMultipartUpload(bucketName, sourceKey, uploadData.UploadId, uploadData.Parts);
			var versionId = completeResponse.VersionId;

			// 일반 클라이언트로 태그 확인
			var tagResponse = client.GetObjectTagging(bucketName, sourceKey);
			TaggingCompare(tagSet, tagResponse.Tagging);

			// Backend 클라이언트로 멀티파트 업로드
			BackendMultipartUpload(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObjectMetadata(bucketName, targetKey, versionId: versionId);
			Assert.Equal(size, getResponse.ContentLength);
			Assert.Equal(versionId, getResponse.VersionId);

			// 태그 확인
			tagResponse = client.GetObjectTagging(bucketName, targetKey);
			TaggingCompare(tagSet, tagResponse.Tagging);

			CheckContentUsingRange(client, bucketName, targetKey, uploadData.Body, MainData.MB);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "MultipartUpload 헤더와 메타데이터가 복제되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestMultipartUploadWithMetadataReplication()
		{
			TestId = 36;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var sourceKey = "testMultipartUploadMetadataReplicationSource";
			var targetKey = "testMultipartUploadMetadataReplicationTarget";
			var size = 10 * MainData.MB;
			var metadataList = new List<KeyValuePair<string, string>>() { new("x-amz-meta-test-key", "testValue") };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 멀티파트 업로드 (메타데이터 포함)
			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, sourceKey, size, metadataList: metadataList);
			var completeResponse = client.CompleteMultipartUpload(bucketName, sourceKey, uploadData.UploadId, uploadData.Parts);
			var versionId = completeResponse.VersionId;

			// 일반 클라이언트로 메타데이터 확인
			var metadataResponse = client.GetObjectMetadata(bucketName, sourceKey);
			Assert.Equal("testValue", metadataResponse.Metadata["x-amz-meta-test-key"]);

			// Backend 클라이언트로 멀티파트 업로드
			BackendMultipartUpload(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 타겟 오브젝트 확인
			var getResponse = client.GetObjectMetadata(bucketName, targetKey, versionId: versionId);
			Assert.Equal(size, getResponse.ContentLength);
			Assert.Equal(versionId, getResponse.VersionId);
			Assert.Equal("testValue", getResponse.Metadata["x-amz-meta-test-key"]);

			CheckContentUsingRange(client, bucketName, targetKey, uploadData.Body, MainData.MB);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "PutObjectAcl 복제가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectAclReplication()
		{
			TestId = 37;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucketCannedAcl(client);
			var sourceKey = "testPutObjectAclReplicationSource";
			var targetKey = "testPutObjectAclReplicationTarget";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, sourceKey, body: content);
			var versionId = putResponse.VersionId;

			// 일반 클라이언트로 ACL 변경
			client.PutObjectACL(bucketName, sourceKey, acl: S3CannedACL.PublicRead);

			// Backend 클라이언트로 복사
			BackendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// Backend 클라이언트로 ACL 설정
			BackendPutObjectAcl(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 복제 확인
			var getResponse = client.GetObject(bucketName, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(versionId, getResponse.VersionId);

			// ACL 확인
			var aclResponse = client.GetObjectACL(bucketName, targetKey);
			Assert.Equal(2, aclResponse.AccessControlList.Grants.Count);
			CheckGrants(PublicAcl(S3Permission.READ), aclResponse.AccessControlList.Grants);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "putObjectTagging 복제가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestPutObjectTaggingReplication()
		{
			TestId = 38;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var sourceKey = "testPutObjectTaggingReplicationSource";
			var targetKey = "testPutObjectTaggingReplicationTarget";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, sourceKey, body: content, tagSet: tagSet);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// Backend 클라이언트로 태그 설정
			BackendPutObjectTagging(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 복제 확인
			var getResponse = client.GetObject(bucketName, targetKey);
			var body = GetBody(getResponse);
			Assert.Equal(content, body);
			Assert.Equal(versionId, getResponse.VersionId);

			// 태그 확인
			var tagResponse = client.GetObjectTagging(bucketName, targetKey);
			TaggingCompare(tagSet, tagResponse.Tagging);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "deleteObject 복제가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteObjectReplication()
		{
			TestId = 39;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var sourceKey = "testDeleteObjectReplicationSource";
			var targetKey = "testDeleteObjectReplicationTarget";
			var content = "test content";

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, sourceKey, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 삭제
			var deleteResponse = client.DeleteObject(bucketName, sourceKey);
			var markerVersionId = deleteResponse.VersionId;

			// Backend 클라이언트로 삭제
			BackendDeleteObject(backendClient, bucketName, targetKey, markerVersionId);

			// 일반 클라이언트로 DeleteMarker 확인
			var listResponse = client.ListVersions(bucketName);

			var deleteMarkers = GetDeleteMarkers(listResponse.Versions);
			Assert.Equal(2, deleteMarkers.Count);
			Assert.Equal(markerVersionId, deleteMarkers[0].VersionId);
			Assert.Equal(markerVersionId, deleteMarkers[1].VersionId);

			var versions = GetVersions(listResponse.Versions);
			Assert.Equal(2, versions.Count);
			Assert.Equal(versionId, versions[0].VersionId);
			Assert.Equal(versionId, versions[1].VersionId);
		}

		[SkippableFact]
		[Trait(MainData.Major, "Backend")]
		[Trait(MainData.Minor, "Replication")]
		[Trait(MainData.Explanation, "deleteObjectTagging 복제가 정상 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestDeleteObjectTaggingReplication()
		{
			TestId = 40;
			SkipIfAws();
			var client = GetClient();
			var backendClient = GetBackendClient();
			var bucketName = GetNewBucket();
			var sourceKey = "testDeleteObjectTaggingReplicationSource";
			var targetKey = "testDeleteObjectTaggingReplicationTarget";
			var content = "test content";
			var tagSet = new List<Tag>() { new() { Key = "testKey", Value = "testValue" } };

			// 버저닝 활성화
			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			// 일반 클라이언트로 업로드
			var putResponse = client.PutObject(bucketName, sourceKey, body: content);
			var versionId = putResponse.VersionId;

			// Backend 클라이언트로 복사
			BackendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 태그 설정
			client.PutObjectTagging(bucketName, sourceKey, new Tagging() { TagSet = tagSet });

			// Backend 클라이언트로 태그 복사
			BackendPutObjectTagging(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

			// 일반 클라이언트로 태그 확인
			var tagResponse = client.GetObjectTagging(bucketName, targetKey);
			TaggingCompare(tagSet, tagResponse.Tagging);

			var tagResponse2 = client.GetObjectTagging(bucketName, sourceKey);
			TaggingCompare(tagSet, tagResponse2.Tagging);

			// 일반 클라이언트로 태그 삭제
			client.DeleteObjectTagging(bucketName, targetKey);

			// Backend 클라이언트로 태그 삭제
			BackendDeleteObjectTagging(backendClient, bucketName, targetKey, versionId);

			// 일반 클라이언트로 태그 확인
			var tagResponse3 = client.GetObjectTagging(bucketName, sourceKey);
			TaggingCompare(tagSet, tagResponse3.Tagging);

			var tagResponse4 = client.GetObjectTagging(bucketName, targetKey);
			Assert.Empty(tagResponse4.Tagging);
		}
	}
}
