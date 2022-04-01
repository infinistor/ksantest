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

import org.example.s3tests.MainData;
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

public class Multipart extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Multipart Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Multipart End");
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("비어있는 오브젝트를 멀티파트로 업로드 실패 확인")
	public void test_multipart_upload_empty() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key1 = "mymultipart";
		var Size = 0;

		var UploadData = SetupMultipartUpload(Client, BucketName, Key1, Size, null);
		var e = assertThrows(AmazonServiceException.class, () -> Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key1, UploadData.UploadId, UploadData.Parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.MalformedXML, ErrorCode);
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	//@Tag("파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인")
	public void test_multipart_upload_small() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key1 = "mymultipart";
		var Size = 1;

		var UploadData = SetupMultipartUpload(Client, BucketName, Key1, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key1, UploadData.UploadId, UploadData.Parts));
		var Response = Client.getObject(BucketName, Key1);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Copy")
	@Tag("KSAN")
	//@Tag("버킷a에서 버킷b로 멀티파트 복사 성공확인")
	public void test_multipart_copy_small() {
		var SourceKey = "foo";
		var SourceBucketName = CreateKeyWithRandomContent(SourceKey, 0, null, null);

		var TargetBucketName = GetNewBucket();
		var TargetKey = "mymultipart";
		var Size = 1;
		var Client = GetClient();

		var UploadData = MultipartCopy(SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, Client, 0, null);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));

		var Response = Client.getObject(TargetBucketName, TargetKey);
		assertEquals(Size, Response.getObjectMetadata().getContentLength());
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey, MainData.MB);
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("범위설정을 잘못한 멀티파트 복사 실패 확인")
	public void test_multipart_copy_invalid_range() {
		var Client = GetClient();
		var SourceKey = "source";
		var SourceBucketName = CreateKeyWithRandomContent(SourceKey, 5, null, Client);

		var TargetKey = "dest";
		var Response = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(SourceBucketName, TargetKey));
		var UploadID = Response.getUploadId();

		var e = assertThrows(AmazonServiceException.class,() -> Client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName).withSourceKey(SourceKey)
																		.withDestinationBucketName(SourceBucketName).withDestinationKey(TargetKey).withUploadId(UploadID)
																		.withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 21)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertTrue(new ArrayList<>(Arrays.asList(new Integer[] { 400, 416 })).contains(StatusCode));
		assertEquals(MainData.InvalidArgument, ErrorCode);
	}

	@Test
	@Tag("Range")
	@Tag("KSAN")
	//@Tag("범위를 지정한 멀티파트 복사 성공확인")
	public void test_multipart_copy_without_range() {
		var Client = GetClient();
		var SourceKey = "source";
		var SourceBucketName = CreateKeyWithRandomContent(SourceKey, 10, null, Client);
		var TargetBucketName = GetNewBucket();
		var TargetKey = "mymultipartcopy";

		var InitResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey));
		var UploadID = InitResponse.getUploadId();
		var Parts = new ArrayList<PartETag>();

		var CopyResponse = Client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
				.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
				.withUploadId(UploadID).withPartNumber(1).withFirstByte((long) 0).withLastByte((long) 9));
		Parts.add(new PartETag(1, CopyResponse.getETag()));
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadID, Parts));

		var Response = Client.getObject(TargetBucketName, TargetKey);
		assertEquals(10, Response.getObjectMetadata().getContentLength());
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);
	}

	@Test
	@Tag("SpecialNames")
	@Tag("KSAN")
	//@Tag("특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인")
	public void test_multipart_copy_special_names() {
		var SourceBucketName = GetNewBucket();
		var TargetBucketName = GetNewBucket();

		var TargetKey = "mymultipart";
		var Size = 1;
		var Client = GetClient();

		for (var SourceKey : new String[] { " ", "_", "__", "?versionId" }) {
			CreateKeyWithRandomContent(SourceKey, 0, SourceBucketName, null);
			var UploadData = MultipartCopy(Client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
			Client.completeMultipartUpload(
					new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
			var Response = Client.getObject(TargetBucketName, TargetKey);
			assertEquals(Size, Response.getObjectMetadata().getContentLength());
			CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey, MainData.MB);
		}
	}

	@Test
	@Tag("Put")
	@Tag("KSAN")
	//@Tag("멀티파트 업로드 확인")
	public void test_multipart_upload() {
		var BucketName = GetNewBucket();
		var Key = "mymultipart";
		var ContentType = "text/bla";
		var Size = 50 * MainData.MB;
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata("x-amz-meta-foo", "bar");
		MetadataList.setContentType(ContentType);
		var Client = GetClient();

		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		var HeadResponse = Client.listObjectsV2(BucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(1, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(Size, BytesUsed);

		var GetResponse = Client.getObjectMetadata(BucketName, Key);
		assertEquals(ContentType, GetResponse.getContentType());
		assertEquals(MetadataList.getUserMetadata(), GetResponse.getUserMetadata());

		var Body = UploadData.GetBody();

		CheckContentUsingRange(BucketName, Key, Body, MainData.MB);
		CheckContentUsingRange(BucketName, Key, Body, 10 * MainData.MB);
		CheckContentUsingRandomRange(BucketName, Key, Body, 100);
	}

	@Test
	@Tag("Copy")
	//@Tag("버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인")
	public void test_multipart_copy_versioned() {
		var SourceBucketName = GetNewBucket();
		var TargetBucketName = GetNewBucket();

		var TargetKey = "mymultipart";
		CheckVersioning(SourceBucketName, BucketVersioningConfiguration.OFF);

		var SourceKey = "foo";
		CheckConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);

		var Size = 15 * MainData.MB;
		CreateKeyWithRandomContent(SourceKey, Size, SourceBucketName, null);
		CreateKeyWithRandomContent(SourceKey, Size, SourceBucketName, null);
		CreateKeyWithRandomContent(SourceKey, Size, SourceBucketName, null);

		var VersionID = new ArrayList<String>();
		var Client = GetClient();
		var ListResponse = Client.listVersions(SourceBucketName, null);
		for (var version : ListResponse.getVersionSummaries())
			VersionID.add(version.getVersionId());

		for (var VID : VersionID) {
			var UploadData = MultipartCopy(SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null, 0, VID);
			Client.completeMultipartUpload(new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
			var Response = Client.getObject(TargetBucketName, TargetKey);
			assertEquals(Size, Response.getObjectMetadata().getContentLength());
			CheckCopyContent(SourceBucketName, SourceKey, TargetBucketName, TargetKey, VID);
		}
	}

	@Test
	@Tag("Duplicate")
	@Tag("KSAN")
	//@Tag("멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인")
	public void test_multipart_upload_resend_part() {
		var BucketName = GetNewBucket();
		var Key = "mymultipart";
		var Size = 50 * MainData.MB;

		CheckUploadMultipartResend(BucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 0 })));
		CheckUploadMultipartResend(BucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 1 })));
		CheckUploadMultipartResend(BucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 2 })));
		CheckUploadMultipartResend(BucketName, Key, Size, new ArrayList<>(Arrays.asList(new Integer[] { 1, 2 })));
		CheckUploadMultipartResend(BucketName, Key, Size,
				new ArrayList<>(Arrays.asList(new Integer[] { 0, 1, 2, 3, 4, 5 })));
	}

	@Test
	@Tag("Put")
	@Tag("KSAN")
	//@Tag("한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인")
	public void test_multipart_upload_multiple_sizes() {
		var BucketName = GetNewBucket();
		var Key = "mymultipart";
		var Client = GetClient();

		var Size = 5 * MainData.MB;
		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		Size = 5 * MainData.MB + 100 * MainData.KB;
		UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		Size = 5 * MainData.MB + 600 * MainData.KB;
		UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		Size = 10 * MainData.MB;
		UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		Size = 10 * MainData.MB + 100 * MainData.KB;
		UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		Size = 10 * MainData.MB + 600 * MainData.KB;
		UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

	}

	@Test
	@Tag("Copy")
	//@Tag("한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인")
	public void test_multipart_copy_multiple_sizes() {
		var SourceKey = "foo";
		var SourceBucketName = CreateKeyWithRandomContent(SourceKey, 12 * MainData.MB, null, null);

		var TargetBucketName = GetNewBucket();
		var TargetKey = "mymultipart";
		var Client = GetClient();

		var Size = 5 * MainData.MB;
		var UploadData = MultipartCopy(Client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 5 * MainData.MB + 100 * MainData.KB;
		UploadData = MultipartCopy(Client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 5 * MainData.MB + 600 * MainData.KB;
		UploadData = MultipartCopy(Client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 10 * MainData.MB;
		UploadData = MultipartCopy(Client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 10 * MainData.MB + 100 * MainData.KB;
		UploadData = MultipartCopy(Client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		Size = 10 * MainData.MB + 600 * MainData.KB;
		UploadData = MultipartCopy(Client, SourceBucketName, SourceKey, TargetBucketName, TargetKey, Size, null);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(TargetBucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
		CheckCopyContentUsingRange(SourceBucketName, SourceKey, TargetBucketName, TargetKey);
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인")
	public void test_multipart_upload_size_too_small() {
		var BucketName = GetNewBucket();
		var Key = "mymultipart";
		var Client = GetClient();

		var Size = 1 * MainData.MB;
		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, 10 * MainData.KB, null, null);
		var e = assertThrows(AmazonServiceException.class, () -> Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.EntityTooSmall, ErrorCode);
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	//@Tag("내용물을 채운 멀티파트 업로드 성공 확인")
	public void test_multipart_upload_contents() {
		var BucketName = GetNewBucket();
		DoTestMultipartUploadContents(BucketName, "mymultipart", 3);
	}

	@Test
	@Tag("OverWrite")
	@Tag("KSAN")
	//@Tag("업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인")
	public void test_multipart_upload_overwrite_existing_object() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "mymultipart";
		var Payload = RandomTextToLong(5 * MainData.MB);
		var NumParts = 2;

		Client.putObject(BucketName, Key, Payload);

		var InitResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key));
		var UploadID = InitResponse.getUploadId();
		var Parts = new ArrayList<PartETag>();
		var AllPayload = "";

		for (int i = 0; i < NumParts; i++) {
			var PartNumber = i + 1;
			var PartResponse = Client.uploadPart(new UploadPartRequest().withBucketName(BucketName).withKey(Key)
					.withUploadId(UploadID).withInputStream(CreateBody(Payload)).withPartNumber(PartNumber)
					.withPartSize(Payload.length()));
			Parts.add(new PartETag(PartNumber, PartResponse.getETag()));
			AllPayload += Payload;
		}

		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadID, Parts));

		var Response = Client.getObject(BucketName, Key);
		var Text = GetBody(Response.getObjectContent());
		assertEquals(AllPayload, Text);
	}

	@Test
	@Tag("Cancel")
	@Tag("KSAN")
	//@Tag("멀티파트 업로드하는 도중 중단 성공 확인")
	public void test_abort_multipart_upload() {
		var BucketName = GetNewBucket();
		var Key = "mymultipart";
		var Size = 10 * MainData.MB;
		var Client = GetClient();

		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key, UploadData.UploadId));

		var HeadResponse = Client.listObjectsV2(BucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(0, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(0, BytesUsed);
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("존재하지 않은 멀티파트 업로드 중단 실패 확인")
	public void test_abort_multipart_upload_not_found() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "mymultipart";
		Client.putObject(BucketName, Key, "");

		var e = assertThrows(AmazonServiceException.class,
				() -> Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key, "56788")));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchUpload, ErrorCode);
	}

	@Test
	@Tag("List")
	@Tag("KSAN")
	//@Tag("멀티파트 업로드 중인 목록 확인")
	public void test_list_multipart_upload() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "mymultipart";
		var Key2 = "mymultipart2";

		var UploadIDs = new ArrayList<>(Arrays.asList(
				new String[] { SetupMultipartUpload(Client, BucketName, Key, 5 * MainData.MB, null).UploadId,
						SetupMultipartUpload(Client, BucketName, Key, 6 * MainData.MB, null).UploadId,
						SetupMultipartUpload(Client, BucketName, Key2, 5 * MainData.MB, null).UploadId, }));

		var Response = Client.listMultipartUploads(new ListMultipartUploadsRequest(BucketName));
		var GetUploadIDs = new ArrayList<String>();

		for (var UploadData : Response.getMultipartUploads())
			GetUploadIDs.add(UploadData.getUploadId());

		for (var UploadID : UploadIDs)
			assertTrue(GetUploadIDs.contains(UploadID));

		Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key, UploadIDs.get(0)));
		Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key, UploadIDs.get(1)));
		Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key2, UploadIDs.get(2)));
	}

	@Test
	@Tag("ERROR")
	//@Tag("업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인")
	public void test_multipart_upload_missing_part() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "mymultipart";
		var Body = "test";

		var InitResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key));
		var UploadID = InitResponse.getUploadId();

		var Parts = new ArrayList<PartETag>();
		var PartResponse = Client
				.uploadPart(new UploadPartRequest().withBucketName(BucketName).withKey(Key).withUploadId(UploadID)
						.withInputStream(CreateBody(Body)).withPartNumber(1).withPartSize(Body.length()));
		Parts.add(new PartETag(9999, PartResponse.getETag()));

		var e = assertThrows(AmazonServiceException.class, () -> Client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadID, Parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidPart, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	//@Tag("잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인")
	public void test_multipart_upload_incorrect_etag() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "mymultipart";

		var InitResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key));
		var UploadID = InitResponse.getUploadId();

		var Parts = new ArrayList<PartETag>();
		Client.uploadPart(new UploadPartRequest().withBucketName(BucketName).withKey(Key).withUploadId(UploadID)
				.withInputStream(CreateBody("\\00")).withPartNumber(1).withPartSize("\\00".length()));
		Parts.add(new PartETag(1, "ffffffffffffffffffffffffffffffff"));

		var e = assertThrows(AmazonServiceException.class, () -> Client
				.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadID, Parts)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidPart, ErrorCode);
	}

	@Test
	@Tag("Overwrite")
	@Tag("KSAN")
	//@Tag("버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인")
	public void test_atomic_multipart_upload_write() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo";
		Client.putObject(BucketName, Key, "bar");

		var InitResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key));
		var UploadID = InitResponse.getUploadId();

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);

		Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key, UploadID));

		Response = Client.getObject(BucketName, Key);
		Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}
	
	@Test
    @Tag("List")
	@Tag("KSAN")
    //@Tag("멀티파트 업로드 목록 확인")
    public void test_multipart_upload_list()
    {
		var BucketName = GetNewBucket();
		var Key = "mymultipart";
		var ContentType = "text/bla";
		var Size = 50 * MainData.MB;
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata("x-amz-meta-foo", "bar");
		MetadataList.setContentType(ContentType);
		var Client = GetClient();

		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, MetadataList);

        var Response = Client.listParts(new ListPartsRequest(BucketName, Key, UploadData.UploadId));
        PartsETagCompare(UploadData.Parts, Response.getParts());
		
		Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key, UploadData.UploadId));
    }

	@Test
	@Tag("Cancel")
	@Tag("KSAN")
	//@Tag("멀티파트 업로드하는 도중 중단 성공 확인")
	public void test_abort_multipart_upload_list() {
		var BucketName = GetNewBucket();
		var Key = "mymultipart";
		var Size = 10 * MainData.MB;
		var Client = GetClient();

		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, null);
		Client.abortMultipartUpload(new AbortMultipartUploadRequest(BucketName, Key, UploadData.UploadId));

		var ListResponse = Client.listMultipartUploads(new ListMultipartUploadsRequest(BucketName));
		assertEquals(0, ListResponse.getMultipartUploads().size());
	}

	@Test
	@Tag("Copy")
	//@Tag("멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인")
	public void test_multipart_copy_many() {
		var BucketName = GetNewBucket();
		var SourceKey = "mymultipart";
		var Size = 10 * MainData.MB;
		var Client = GetClient();
		var Body = new StringBuilder();

		// 멀티파트 업로드
		var UploadData = SetupMultipartUpload(Client, BucketName, SourceKey, Size, null);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, SourceKey, UploadData.UploadId, UploadData.Parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.Body);
		CheckContentUsingRange(BucketName, SourceKey, Body.toString(), MainData.MB);
		
		// 멀티파트 카피
		var TargetKey1 = "mymultipart1";
		UploadData = MultipartCopy(BucketName, SourceKey, BucketName, TargetKey1, Size, Client, 0, null);
		// 추가파츠 업로드
		UploadData = MultipartUpload(Client, BucketName, TargetKey1, Size, UploadData);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, TargetKey1, UploadData.UploadId, UploadData.Parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.Body);
		CheckContentUsingRange(BucketName, TargetKey1, Body.toString(), MainData.MB);
		
		// 멀티파트 카피
		var TargetKey2 = "mymultipart2";
		UploadData = MultipartCopy(BucketName, TargetKey1, BucketName, TargetKey2, Size * 2, Client, 0, null);
		// 추가파츠 업로드
		UploadData = MultipartUpload(Client, BucketName, TargetKey2, Size, UploadData);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, TargetKey2, UploadData.UploadId, UploadData.Parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.Body);
		CheckContentUsingRange(BucketName, TargetKey2, Body.toString(), MainData.MB);
	}
}
