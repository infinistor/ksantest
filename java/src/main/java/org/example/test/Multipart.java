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
package org.example.test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class Multipart extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("Multipart Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Multipart End");
	}

	@Test
	@Tag("ERROR")
	// 비어있는 오브젝트를 멀티파트로 업로드 실패 확인
	public void test_multipart_upload_empty() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key1 = "mymultipart";
		var Size = 0;

		var UploadData = SetupMultipartUpload(client, bucketName, Key1, Size, null);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key1, UploadData.uploadId, UploadData.parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.MalformedXML, ErrorCode);
	}

	@Test
	@Tag("Check")
	// 파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인
	public void test_multipart_upload_small() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key1 = "mymultipart";
		var Size = 1;

		var UploadData = SetupMultipartUpload(client, bucketName, Key1, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key1, UploadData.uploadId, UploadData.parts));
		var Response = client.getObject(bucketName, Key1);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Copy")
	// 버킷a에서 버킷b로 멀티파트 복사 성공확인
	public void test_multipart_copy_small() {
		var SourceKey = "foo";
		var SourceBucketName = createKeyWithRandomContent(SourceKey, 0, null, null);

		var TargetBucketName = getNewBucket();
		var TargetKey = "mymultipart";
		var Size = 1;
		var client = getClient();

		var UploadData = MultipartCopy(SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, client, 0, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId, UploadData.parts));

		var Response = client.getObject(TargetBucketName, TargetKey);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey, MainData.MB);
	}

	@Test
	@Tag("ERROR")
	// 범위설정을 잘못한 멀티파트 복사 실패 확인
	public void test_multipart_copy_invalid_range() {
		var client = getClient();
		var SourceKey = "source";
		var SourceBucketName = createKeyWithRandomContent(SourceKey, 5, null, client);

		var TargetKey = "dest";
		var Response = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(SourceBucketName, TargetKey));
		var UploadID = Response.getUploadId();

		var e = assertThrows(AmazonServiceException.class, () -> client.copyPart(new CopyPartRequest()
				.withSourceBucketName(SourceBucketName).withSourceKey(SourceKey)
				.withDestinationBucketName(SourceBucketName).withDestinationKey(TargetKey).withUploadId(UploadID)
				.withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 21)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertTrue(new ArrayList<>(Arrays.asList(new Integer[] { 400, 416 })).contains(StatusCode));
		assertEquals(MainData.InvalidArgument, ErrorCode);
	}

	@Test
	@Tag("Range")
	// 범위를 지정한 멀티파트 복사 성공확인
	public void test_multipart_copy_without_range() {
		var client = getClient();
		var SourceKey = "source";
		var SourceBucketName = createKeyWithRandomContent(SourceKey, 10, null, client);
		var TargetBucketName = getNewBucket();
		var TargetKey = "mymultipartcopy";

		var InitResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey));
		var UploadID = InitResponse.getUploadId();
		var Parts = new ArrayList<PartETag>();

		var CopyResponse = client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
				.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
				.withUploadId(UploadID).withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 9));
		Parts.add(new PartETag(1, CopyResponse.getETag()));
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadID, Parts));

		var Response = client.getObject(TargetBucketName, TargetKey);
		assertEquals(10, Response.getObjectMetadata().getContentLength());
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);
	}

	@Test
	@Tag("SpecialNames")
	// 특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인
	public void test_multipart_copy_special_names() {
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();

		var TargetKey = "mymultipart";
		var Size = 1;
		var client = getClient();

		for (var SourceKey : new String[] { " ", "_", "__", "?versionId" }) {
			createKeyWithRandomContent(SourceKey, 0, SourceBucketName, null);
			var UploadData = MultipartCopy(client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size,
					null);
			client.completeMultipartUpload(
					new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId,
							UploadData.parts));
			var Response = client.getObject(TargetBucketName, TargetKey);
			assertEquals(Size, Response.getObjectMetadata().getContentLength());
			CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey, MainData.MB);
		}
	}

	@Test
	@Tag("Put")
	// 멀티파트 업로드 확인
	public void test_multipart_upload() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var ContentType = "text/bla";
		var Size = 50 * MainData.MB;
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata("x-amz-meta-foo", "bar");
		MetadataList.setContentType(ContentType);
		var client = getClient();

		var UploadData = SetupMultipartUpload(client, bucketName, Key, Size, MetadataList);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts));

		var HeadResponse = client.listObjectsV2(bucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(1, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(Size, BytesUsed);

		var GetResponse = client.getObjectMetadata(bucketName, Key);
		assertEquals(ContentType, GetResponse.getContentType());
		assertEquals(MetadataList.getUserMetadata(), GetResponse.getUserMetadata());

		var Body = UploadData.getBody();

		CheckContentUsingRange(bucketName, Key, Body, MainData.MB);
		CheckContentUsingRange(bucketName, Key, Body, 10 * MainData.MB);
		CheckContentUsingRandomRange(bucketName, Key, Body, 100);
	}

	@Test
	@Tag("Copy")
	// 버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인
	public void test_multipart_copy_versioned() {
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();

		var TargetKey = "mymultipart";
		checkVersioning(SourceBucketName, BucketVersioningConfiguration.OFF);

		var SourceKey = "foo";
		checkConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);

		var Size = 15 * MainData.MB;
		createKeyWithRandomContent(SourceKey, Size, SourceBucketName, null);
		createKeyWithRandomContent(SourceKey, Size, SourceBucketName, null);
		createKeyWithRandomContent(SourceKey, Size, SourceBucketName, null);

		var VersionID = new ArrayList<String>();
		var client = getClient();
		var ListResponse = client.listVersions(SourceBucketName, null);
		for (var version : ListResponse.getVersionSummaries())
			VersionID.add(version.getVersionId());

		for (var VID : VersionID) {
			var UploadData = MultipartCopy(SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null, 0,
					VID);
			client.completeMultipartUpload(new CompleteMultipartUploadRequest(TargetBucketName, TargetKey,
					UploadData.uploadId, UploadData.parts));
			var Response = client.getObject(TargetBucketName, TargetKey);
			assertEquals(Size, Response.getObjectMetadata().getContentLength());
			CheckCopyContent(SourceBucketName, SourceKey, TargetBucketName, TargetKey, VID);
		}
	}

	@Test
	@Tag("Duplicate")
	// 멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인
	public void test_multipart_upload_resend_part() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var Size = 50 * MainData.MB;

		CheckUploadMultipartResend(bucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 0 })));
		CheckUploadMultipartResend(bucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 1 })));
		CheckUploadMultipartResend(bucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 2 })));
		CheckUploadMultipartResend(bucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 1, 2 })));
		CheckUploadMultipartResend(bucketName, Key, Size,
				new ArrayList<>(Arrays.asList(new Integer[] { 0, 1, 2, 3, 4, 5 })));
	}

	@Test
	@Tag("Put")
	// 한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인
	public void test_multipart_upload_multiple_sizes() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var client = getClient();

		var Size = 5 * MainData.MB;
		var UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts));

		Size = 5 * MainData.MB + 100 * MainData.KB;
		UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts));

		Size = 5 * MainData.MB + 600 * MainData.KB;
		UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts));

		Size = 10 * MainData.MB;
		UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts));

		Size = 10 * MainData.MB + 100 * MainData.KB;
		UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts));

		Size = 10 * MainData.MB + 600 * MainData.KB;
		UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts));

	}

	@Test
	@Tag("Copy")
	// 한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인
	public void test_multipart_copy_multiple_sizes() {
		var SourceKey = "foo";
		var SourceBucketName = createKeyWithRandomContent(SourceKey, 12 * MainData.MB, null, null);

		var TargetBucketName = getNewBucket();
		var TargetKey = "mymultipart";
		var client = getClient();

		var Size = 5 * MainData.MB;
		var UploadData = MultipartCopy(client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId, UploadData.parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 5 * MainData.MB + 100 * MainData.KB;
		UploadData = MultipartCopy(client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId, UploadData.parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 5 * MainData.MB + 600 * MainData.KB;
		UploadData = MultipartCopy(client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId, UploadData.parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 10 * MainData.MB;
		UploadData = MultipartCopy(client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId, UploadData.parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 10 * MainData.MB + 100 * MainData.KB;
		UploadData = MultipartCopy(client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId, UploadData.parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 10 * MainData.MB + 600 * MainData.KB;
		UploadData = MultipartCopy(client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.uploadId, UploadData.parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);
	}

	@Test
	@Tag("ERROR")
	// 멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인
	public void test_multipart_upload_size_too_small() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var client = getClient();

		var Size = 1 * MainData.MB;
		var UploadData = SetupMultipartUpload(client, bucketName, Key, Size, 10 * MainData.KB, null, null);
		var e = assertThrows(AmazonServiceException.class, () -> client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, Key, UploadData.uploadId, UploadData.parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.EntityTooSmall, ErrorCode);
	}

	@Test
	@Tag("Check")
	// 내용물을 채운 멀티파트 업로드 성공 확인
	public void test_multipart_upload_contents() {
		var bucketName = getNewBucket();
		DoTestMultipartUploadContents(bucketName, "mymultipart", 3);
	}

	@Test
	@Tag("OverWrite")
	// 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
	public void test_multipart_upload_overwrite_existing_object() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "mymultipart";
		var Payload = Utils.randomTextToLong(5 * MainData.MB);
		var NumParts = 2;

		client.putObject(bucketName, Key, Payload);

		var InitResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, Key));
		var UploadID = InitResponse.getUploadId();
		var Parts = new ArrayList<PartETag>();
		var AllPayload = "";

		for (int i = 0; i < NumParts; i++) {
			var PartNumber = i + 1;
			var PartResponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(Key)
					.withUploadId(UploadID).withInputStream(createBody(Payload)).withPartNumber(PartNumber)
					.withPartSize(Payload.length()));
			Parts.add(new PartETag(PartNumber, PartResponse.getETag()));
			AllPayload += Payload;
		}

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, Key, UploadID, Parts));

		var Response = client.getObject(bucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertTrue(AllPayload.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("Cancel")
	// 멀티파트 업로드하는 도중 중단 성공 확인
	public void test_abort_multipart_upload() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var Size = 10 * MainData.MB;
		var client = getClient();

		var UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, UploadData.uploadId));

		var HeadResponse = client.listObjectsV2(bucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(0, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(0, BytesUsed);
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않은 멀티파트 업로드 중단 실패 확인
	public void test_abort_multipart_upload_not_found() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "mymultipart";
		client.putObject(bucketName, Key, "");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, "56788")));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchUpload, ErrorCode);
	}

	@Test
	@Tag("List")
	// 멀티파트 업로드 중인 목록 확인
	public void test_list_multipart_upload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "mymultipart";
		var Key2 = "mymultipart2";

		var UploadIDs = new ArrayList<>(Arrays.asList(
				new String[] { SetupMultipartUpload(client, bucketName, Key, 5 * MainData.MB, null).uploadId,
						SetupMultipartUpload(client, bucketName, Key, 6 * MainData.MB, null).uploadId,
						SetupMultipartUpload(client, bucketName, Key2, 5 * MainData.MB, null).uploadId, }));

		var Response = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		var GetUploadIDs = new ArrayList<String>();

		for (var UploadData : Response.getMultipartUploads())
			GetUploadIDs.add(UploadData.getUploadId());

		for (var UploadID : UploadIDs)
			assertTrue(GetUploadIDs.contains(UploadID));

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, UploadIDs.get(0)));
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, UploadIDs.get(1)));
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key2, UploadIDs.get(2)));
	}

	@Test
	@Tag("ERROR")
	// 업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인
	public void test_multipart_upload_missing_part() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "mymultipart";
		var Body = "test";

		var InitResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, Key));
		var UploadID = InitResponse.getUploadId();

		var Parts = new ArrayList<PartETag>();
		var PartResponse = client
				.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(Key).withUploadId(UploadID)
						.withInputStream(createBody(Body)).withPartNumber(1).withPartSize(Body.length()));
		Parts.add(new PartETag(9999, PartResponse.getETag()));

		var e = assertThrows(AmazonServiceException.class, () -> client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, Key, UploadID, Parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidPart, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	// 잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인
	public void test_multipart_upload_incorrect_etag() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "mymultipart";

		var InitResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, Key));
		var UploadID = InitResponse.getUploadId();

		var Parts = new ArrayList<PartETag>();
		client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(Key).withUploadId(UploadID)
				.withInputStream(createBody("\\00")).withPartNumber(1).withPartSize("\\00".length()));
		Parts.add(new PartETag(1, "ffffffffffffffffffffffffffffffff"));

		var e = assertThrows(AmazonServiceException.class, () -> client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, Key, UploadID, Parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidPart, ErrorCode);
	}

	@Test
	@Tag("Overwrite")
	// 버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인
	public void test_atomic_multipart_upload_write() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "foo";
		client.putObject(bucketName, Key, "bar");

		var InitResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, Key));
		var UploadID = InitResponse.getUploadId();

		var Response = client.getObject(bucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, UploadID));

		Response = client.getObject(bucketName, Key);
		Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("List")
	// 멀티파트 업로드 목록 확인
	public void test_multipart_upload_list() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var ContentType = "text/bla";
		var Size = 50 * MainData.MB;
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata("x-amz-meta-foo", "bar");
		MetadataList.setContentType(ContentType);
		var client = getClient();

		var UploadData = SetupMultipartUpload(client, bucketName, Key, Size, MetadataList);

		var Response = client.listParts(new ListPartsRequest(bucketName, Key, UploadData.uploadId));
		PartsETagCompare(UploadData.parts, Response.getParts());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, UploadData.uploadId));
	}

	@Test
	@Tag("Cancel")
	// 멀티파트 업로드하는 도중 중단 성공 확인
	public void test_abort_multipart_upload_list() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var Size = 10 * MainData.MB;
		var client = getClient();

		var UploadData = SetupMultipartUpload(client, bucketName, Key, Size, null);
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, UploadData.uploadId));

		var ListResponse = client.listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
		assertEquals(0, ListResponse.getMultipartUploads().size());
	}

	@Test
	@Tag("Copy")
	// 멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인
	public void test_multipart_copy_many() {
		var bucketName = getNewBucket();
		var SourceKey = "mymultipart";
		var Size = 10 * MainData.MB;
		var client = getClient();
		var Body = new StringBuilder();

		// 멀티파트 업로드
		var UploadData = SetupMultipartUpload(client, bucketName, SourceKey, Size, null);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, SourceKey, UploadData.uploadId, UploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.body);
		CheckContentUsingRange(bucketName, SourceKey, Body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey1 = "mymultipart1";
		UploadData = MultipartCopy(bucketName, SourceKey, bucketName, TargetKey1, Size, client, 0, null);
		// 추가파츠 업로드
		UploadData = MultipartUpload(client, bucketName, TargetKey1, Size, UploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, TargetKey1, UploadData.uploadId, UploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.body);
		CheckContentUsingRange(bucketName, TargetKey1, Body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey2 = "mymultipart2";
		UploadData = MultipartCopy(bucketName, TargetKey1, bucketName, TargetKey2, Size * 2, client, 0, null);
		// 추가파츠 업로드
		UploadData = MultipartUpload(client, bucketName, TargetKey2, Size, UploadData);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, TargetKey2, UploadData.uploadId, UploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.body);
		CheckContentUsingRange(bucketName, TargetKey2, Body.toString(), MainData.MB);
	}

	@Test
	@Tag("List")
	// 멀티파트 목록 확인
	public void test_multipart_list_parts() {
		var bucketName = getNewBucket();
		var Key = "mymultipart";
		var ContentType = "text/bla";
		var Size = 50 * MainData.MB;
		var MetadataList = new ObjectMetadata();
		MetadataList.setContentType(ContentType);
		var client = getClient();

		var UploadData = SetupMultipartUpload(client, bucketName, Key, Size, 1 * MainData.MB, MetadataList, null);

		for (int i = 0; i < 41; i += 10) {
			var Response = client.listParts(
					new ListPartsRequest(bucketName, Key, UploadData.uploadId)
							.withMaxParts(10)
							.withPartNumberMarker(i));

			assertEquals(10, Response.getParts().size());
			PartsETagCompare(UploadData.parts.subList(i, i + 10), Response.getParts());
		}
		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, Key, UploadData.uploadId));
	}
}
