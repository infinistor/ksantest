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
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using s3tests.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class GetObjectAttributes : TestBase
	{
		public GetObjectAttributes(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "Basic")]
		[Trait(MainData.Explanation, "기본 GetObjectAttributes — 모든 속성 요청 및 응답 검증")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesBasic()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesBasic";

			client.PutObject(bucketName, key, body: key);

			var response = client.GetObjectAttributes(bucketName, key,
			[
				ObjectAttributes.ObjectSize,
				ObjectAttributes.StorageClass,
				ObjectAttributes.ETag
			]);

			Assert.NotNull(response);
			Assert.Equal(key.Length, response.ObjectSize);
			Assert.Equal(S3StorageClass.Standard, response.StorageClass);
			Assert.NotNull(response.ETag);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "SpecificAttributes")]
		[Trait(MainData.Explanation, "특정 속성만 요청하는 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesSpecificAttributes()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesSpecificAttributes";

			client.PutObject(bucketName, key, body: key);

			var sizeResponse = client.GetObjectAttributes(bucketName, key, [ObjectAttributes.ObjectSize]);
			Assert.NotNull(sizeResponse);
			Assert.Equal(key.Length, sizeResponse.ObjectSize);
			Assert.Null(sizeResponse.Checksum);

			var etagResponse = client.GetObjectAttributes(bucketName, key, [ObjectAttributes.ETag]);
			Assert.NotNull(etagResponse);
			Assert.NotNull(etagResponse.ETag);
			Assert.Null(etagResponse.ObjectSize);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "Multipart")]
		[Trait(MainData.Explanation, "멀티파트 업로드 객체 GetObjectAttributes (ObjectParts 포함)")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesMultipart()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesMultipart";
			var size = 10 * MainData.MB;

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, size);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);

			var response = client.GetObjectAttributes(bucketName, key,
			[
				ObjectAttributes.ObjectSize,
				ObjectAttributes.StorageClass,
				ObjectAttributes.ETag,
				ObjectAttributes.ObjectParts
			]);

			Assert.NotNull(response);
			Assert.Equal(size, response.ObjectSize);
			Assert.Equal(S3StorageClass.Standard, response.StorageClass);
			Assert.NotNull(response.ETag);
			Assert.NotNull(response.ObjectParts);
			Assert.True(response.ObjectParts.TotalPartsCount > 0);
			Assert.Equal(uploadData.Parts.Count, response.ObjectParts.TotalPartsCount);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "Checksum")]
		[Trait(MainData.Explanation, "체크섬 알고리즘을 사용한 객체 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesWithChecksum()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesWithChecksum";
			var checksumAlgorithm = ChecksumAlgorithm.SHA256;

			client.PutObject(bucketName, key, body: key, checksumAlgorithm: checksumAlgorithm);

			var response = client.GetObjectAttributes(bucketName, key, [ObjectAttributes.Checksum]);

			Assert.NotNull(response);
			Assert.NotNull(response.Checksum);
			ChecksumCompare(checksumAlgorithm, key, response);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 객체 GetObjectAttributes 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectAttributesNonExistentObject()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesNonExistentObject";

			var e = Assert.Throws<AggregateException>(() => client.GetObjectAttributes(bucketName, key,
				[ObjectAttributes.ObjectSize]));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_KEY, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷 GetObjectAttributes 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectAttributesNonExistentBucket()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = "non-existent-bucket-" + S3Utils.RandomText(10).ToLowerInvariant();
			var key = "testGetObjectAttributesNonExistentBucket";

			var e = Assert.Throws<AggregateException>(() => client.GetObjectAttributes(bucketName, key,
				[ObjectAttributes.ObjectSize]));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "속성 미지정 GetObjectAttributes 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectAttributesNoAttributes()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesNoAttributes";

			client.PutObject(bucketName, key, body: key);

			var e = Assert.Throws<AggregateException>(() => client.GetObjectAttributes(bucketName, key));

			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "Versioning")]
		[Trait(MainData.Explanation, "버전 ID를 사용한 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesWithVersionId()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesWithVersionId";

			CheckConfigureVersioningRetry(bucketName, VersionStatus.Enabled);

			var content1 = key + "-v1";
			client.PutObject(bucketName, key, body: content1);

			var content2 = key + "-v2";
			client.PutObject(bucketName, key, body: content2);

			var listResponse = client.ListVersions(bucketName, prefix: key);
			var versions = listResponse.Versions.Where(v => v.IsDeleteMarker != true).ToList();
			Assert.Equal(2, versions.Count);

			var firstVersionId = versions[1].VersionId;
			var firstVersionResponse = client.GetObjectAttributes(bucketName, key, [ObjectAttributes.ObjectSize],
				versionId: firstVersionId);

			var secondVersionId = versions[0].VersionId;
			var secondVersionResponse = client.GetObjectAttributes(bucketName, key, [ObjectAttributes.ObjectSize],
				versionId: secondVersionId);

			Assert.Equal(content1.Length, firstVersionResponse.ObjectSize);
			Assert.Equal(content2.Length, secondVersionResponse.ObjectSize);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "잘못된 버전 ID GetObjectAttributes 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectAttributesInvalidVersionId()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesInvalidVersionId";

			client.PutObject(bucketName, key, body: key);

			var e = Assert.Throws<AggregateException>(() => client.GetObjectAttributes(bucketName, key,
				[ObjectAttributes.ObjectSize], versionId: "f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59"));

			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_VERSION, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "LargeMultipart")]
		[Trait(MainData.Explanation, "대용량 멀티파트 객체 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesLargeMultipart()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesLargeMultipart";
			var size = 100 * MainData.MB;
			var partSize = 5 * MainData.MB;

			var initResponse = client.InitiateMultipartUpload(bucketName, key);
			var uploadId = initResponse.UploadId;
			var parts = new List<PartETag>();
			var partCount = size / partSize;

			for (int i = 0; i < partCount; i++)
			{
				var partNumber = i + 1;
				var partContent = S3Utils.RandomTextToLong(partSize);
				var partResponse = client.UploadPart(bucketName, key, uploadId, partContent, partNumber);
				parts.Add(new PartETag(partNumber, partResponse.ETag));
			}

			client.CompleteMultipartUpload(bucketName, key, uploadId, parts);

			var response = client.GetObjectAttributes(bucketName, key,
			[
				ObjectAttributes.ObjectSize,
				ObjectAttributes.ObjectParts
			]);

			Assert.NotNull(response);
			Assert.Equal(size, response.ObjectSize);
			Assert.NotNull(response.ObjectParts);
			Assert.Equal(partCount, response.ObjectParts.TotalPartsCount);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "Metadata")]
		[Trait(MainData.Explanation, "메타데이터가 있는 객체 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesWithMetadata()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesWithMetadata";
			var metadata = new List<KeyValuePair<string, string>>
			{
				new("custom-key1", "custom-value1"),
				new("custom-key2", "custom-value2")
			};

			client.PutObject(bucketName, key, body: key, metadataList: metadata);

			var response = client.GetObjectAttributes(bucketName, key,
			[
				ObjectAttributes.ObjectSize,
				ObjectAttributes.ETag
			]);

			Assert.NotNull(response);
			Assert.Equal(key.Length, response.ObjectSize);

			var headResponse = client.GetObjectMetadata(bucketName, key);
			CheckMetaData(metadata, headResponse.Metadata);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "Encryption")]
		[Trait(MainData.Explanation, "SSE-S3 암호화 객체 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesWithSSES3()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesWithSSES3";

			client.PutObject(bucketName, key, body: key, sseKey: ServerSideEncryptionMethod.AES256);

			var response = client.GetObjectAttributes(bucketName, key,
			[
				ObjectAttributes.ObjectSize,
				ObjectAttributes.ETag
			]);

			Assert.NotNull(response);
			Assert.Equal(key.Length, response.ObjectSize);

			var headResponse = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(ServerSideEncryptionMethod.AES256, headResponse.ServerSideEncryptionMethod);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "Async")]
		[Trait(MainData.Explanation, "비동기 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesAsync()
		{
			TestId = 13;
			var client = GetClient();
			var asyncClient = GetClient(RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesAsync";

			client.PutObject(bucketName, key, body: key);

			var request = new GetObjectAttributesRequest
			{
				BucketName = bucketName,
				Key = key,
				ObjectAttributes = [ObjectAttributes.ObjectSize, ObjectAttributes.ETag]
			};
			var response = asyncClient.Client.GetObjectAttributesAsync(request).GetAwaiter().GetResult();

			Assert.NotNull(response);
			Assert.Equal(key.Length, response.ObjectSize);
			Assert.NotNull(response.ETag);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "비동기 GetObjectAttributes 에러")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetObjectAttributesAsyncError()
		{
			TestId = 14;
			var asyncClient = GetClient(RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
			var bucketName = GetNewBucketName(false);
			var key = "testGetObjectAttributesAsyncError";

			var request = new GetObjectAttributesRequest
			{
				BucketName = bucketName,
				Key = key,
				ObjectAttributes = [ObjectAttributes.ObjectSize]
			};

			var e = Assert.Throws<AmazonS3Exception>(() =>
				asyncClient.Client.GetObjectAttributesAsync(request).GetAwaiter().GetResult());

			Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
			Assert.Equal(MainData.NO_SUCH_BUCKET, e.ErrorCode);
		}

		[Fact]
		[Trait(MainData.Major, "GetObjectAttributes")]
		[Trait(MainData.Minor, "AllAttributes")]
		[Trait(MainData.Explanation, "모든 가능한 속성 GetObjectAttributes")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestGetObjectAttributesAllAttributes()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "testGetObjectAttributesAllAttributes";
			var size = 10 * MainData.MB;
			var checksumType = ChecksumType.FULL_OBJECT;
			var checksumAlgorithm = ChecksumAlgorithm.CRC64NVME;

			var initResponse = client.InitiateMultipartUpload(bucketName, key,
				checksumAlgorithm: checksumAlgorithm, checksumType: checksumType);
			var uploadId = initResponse.UploadId;
			var partContent = S3Utils.RandomTextToLong(size);
			var partResponse = client.UploadPart(bucketName, key, uploadId, partContent, 1,
				checksumAlgorithm: checksumAlgorithm);
			var parts = new List<PartETag> { new(1, partResponse.ETag) };

			client.CompleteMultipartUpload(bucketName, key, uploadId, parts, checksumType: checksumType);

			var response = client.GetObjectAttributes(bucketName, key,
			[
				ObjectAttributes.ObjectSize,
				ObjectAttributes.StorageClass,
				ObjectAttributes.ETag,
				ObjectAttributes.ObjectParts,
				ObjectAttributes.Checksum
			]);

			Assert.NotNull(response);
			Assert.Equal(size, response.ObjectSize);
			Assert.Equal(S3StorageClass.Standard, response.StorageClass);
			Assert.NotNull(response.ETag);
			Assert.NotNull(response.ObjectParts);
			Assert.Equal(1, response.ObjectParts.TotalPartsCount);
			Assert.NotNull(response.Checksum);
		}
	}
}
