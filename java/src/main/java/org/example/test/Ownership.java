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
package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;

import com.google.gson.JsonObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;
import com.amazonaws.services.s3.model.ownership.OwnershipControls;
import com.amazonaws.services.s3.model.ownership.OwnershipControlsRule;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.Permission;

public class Ownership extends TestBase {

	@Test
	@Tag("Get")
	public void testGetBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 1, ObjectOwnership.BucketOwnerEnforced);

		client.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
	}

	@Test
	@Tag("Put")
	public void testCreateBucketWithOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 2, ObjectOwnership.BucketOwnerEnforced);

		var response = client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
		assertEquals(ObjectOwnership.BucketOwnerEnforced.toString(),
				response.getOwnershipControls().getRules().get(0).getOwnership());
	}

	@Test
	@Tag("Put")
	public void testChangeBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 3, ObjectOwnership.BucketOwnerEnforced);

		var response = client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
		assertEquals(ObjectOwnership.BucketOwnerEnforced.toString(),
				response.getOwnershipControls().getRules().get(0).getOwnership());

		client.setBucketOwnershipControls(bucketName, new OwnershipControls()
				.withRules(List.of(new OwnershipControlsRule().withOwnership(ObjectOwnership.ObjectWriter))));

		response = client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
		assertEquals(ObjectOwnership.ObjectWriter.toString(),
				response.getOwnershipControls().getRules().get(0).getOwnership());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucket(client, 4, ObjectOwnership.BucketOwnerEnforced);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipDenyObjectACL() {
		var client = getClient();
		var bucketName = createBucket(client, 5, ObjectOwnership.BucketOwnerEnforced);
		var key = "testBucketOwnershipDenyObjectACL";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata()));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectAcl(bucketName, key, CannedAccessControlList.AuthenticatedRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Check")
	public void testObjectOwnershipDenyChange() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 6);
		var key = "testObjectOwnershipDenyChange";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var publicClient = getPublicClient();
		publicClient.getObjectMetadata(bucketName, key);

		client.setBucketOwnershipControls(bucketName, new OwnershipControls()
				.withRules(List.of(new OwnershipControlsRule().withOwnership(ObjectOwnership.BucketOwnerEnforced))));

		// BucketOwnerEnforced 로 바꾸면 오브젝트 ACL 이 무시되어 퍼블릭 접근이 막힌다.
		// AWS 는 이미 맺은 연결에서는 변경 전 권한으로 계속 응답하므로 새 클라이언트로 확인한다.
		waitUntil(() -> status(() -> {
			var newPublicClient = getPublicClient();
			try {
				newPublicClient.getObjectMetadata(bucketName, key);
			} finally {
				newPublicClient.shutdown();
			}
		}) == HttpStatus.SC_FORBIDDEN);
	}

	@Test
	@Tag("Error")
	public void testObjectOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 7);
		var key = "testObjectOwnershipDenyACL";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var publicClient = getPublicClient();
		publicClient.getObjectMetadata(bucketName, key);

		client.setBucketOwnershipControls(bucketName, new OwnershipControls()
				.withRules(List.of(new OwnershipControlsRule().withOwnership(ObjectOwnership.BucketOwnerEnforced))));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectAcl(bucketName, key, CannedAccessControlList.Private));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.getErrorCode());
	}

	private static void putOwnership(AmazonS3 client, String bucketName, String ownership) {
		client.setBucketOwnershipControls(bucketName,
				new OwnershipControls().withRules(List.of(new OwnershipControlsRule().withOwnership(ownership))));
	}

	private static String getOwnership(AmazonS3 client, String bucketName) {
		var response = client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
		return response.getOwnershipControls().getRules().get(0).getOwnership();
	}

	private static String objectOwnerId(AmazonS3 client, String bucketName, String key) {
		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withPrefix(key).withFetchOwner(true));
		return response.getObjectSummaries().get(0).getOwner().getId();
	}

	/** 설정 변경이 반영될 때까지 check 가 true 를 돌려주기를 기다린다. */
	private void waitUntil(BooleanSupplier check) {
		var deadline = System.nanoTime() + 60_000_000_000L;
		while (true) {
			if (check.getAsBoolean())
				return;
			if (System.nanoTime() >= deadline)
				fail("설정 변경이 제한 시간 안에 반영되지 않았습니다");
			delay(2000);
		}
	}

	private static int status(Runnable func) {
		try {
			func.run();
			return HttpStatus.SC_OK;
		} catch (AmazonServiceException e) {
			return e.getStatusCode();
		}
	}

	/** ACL 로 alt 사용자에게 쓰기 권한을 준 ObjectWriter 버킷을 만든다. */
	private String createBucketAltWritable(AmazonS3 client, int testId) {
		var bucketName = createBucketCannedAcl(client, testId);
		client.setBucketAcl(bucketName,
				createAcl(config.mainUser.toOwner(), config.altUser.toGrantee(), Permission.Write));
		return bucketName;
	}

	private void checkOwnerFullControlOnly(AccessControlList acl) {
		var grants = acl.getGrantsAsList();
		assertEquals(1, grants.size());
		assertEquals(config.mainUser.id, grants.get(0).getGrantee().getIdentifier());
		assertEquals(Permission.FullControl, grants.get(0).getPermission());
	}

	@Test
	@Tag("Get")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): 새 버킷의 소유권 기본값은 ObjectWriter 다.
	// SCP 등 ACL 로 권한을 주는 기존 사용 방식을 유지하려고 AWS 의 2023년 기본값(BucketOwnerEnforced)을 따르지 않는다.
	public void testBucketOwnershipDefault() {
		var client = getClient();
		// createBucket(client, testId) 는 소유권을 지정하지 않고 버킷을 만든다
		var bucketName = createBucket(client, 8);
		// 소유권을 지정하지 않고 만든 버킷은 BucketOwnerEnforced 이다
		assertEquals("BucketOwnerEnforced", getOwnership(client, bucketName));
	}

	@Test
	@Tag("Put")
	public void testPutBucketOwnershipAllValues() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		for (var ownership : List.of("ObjectWriter", "BucketOwnerPreferred", "BucketOwnerEnforced")) {
			putOwnership(client, bucketName, ownership);
			assertEquals(ownership, getOwnership(client, bucketName));
		}
	}

	@Test
	@Tag("Error")
	public void testPutBucketOwnershipInvalid() {
		var client = getClient();
		var bucketName = createBucket(client, 10);
		var e = assertThrows(AmazonServiceException.class, () -> putOwnership(client, bucketName, "Invalid"));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Delete")
	public void testDeleteBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 11, ObjectOwnership.BucketOwnerPreferred);
		client.deleteBucketOwnershipControls(new DeleteBucketOwnershipControlsRequest().withBucketName(bucketName));
		var e = assertThrows(AmazonServiceException.class, () -> client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.OWNERSHIP_CONTROLS_NOT_FOUND_ERROR, e.getErrorCode());
		// 설정이 없어도 삭제는 성공한다
		client.deleteBucketOwnershipControls(new DeleteBucketOwnershipControlsRequest().withBucketName(bucketName));
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(12);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> putOwnership(client, bucketName, "ObjectWriter"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> client.deleteBucketOwnershipControls(
				new DeleteBucketOwnershipControlsRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Permission")
	public void testBucketOwnershipAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 13);

		var e = assertThrows(AmazonServiceException.class, () -> altClient
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> putOwnership(altClient, bucketName, "ObjectWriter"));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> altClient.deleteBucketOwnershipControls(
				new DeleteBucketOwnershipControlsRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testCreateBucketOwnershipEnforcedWithAcl() {
		var client = getClient();
		var bucketName = getNewBucketName(14);
		// BucketOwnerEnforced 버킷은 ACL 을 함께 지정해 만들 수 없다
		var e = assertThrows(AmazonServiceException.class,
				() -> client.createBucket(new CreateBucketRequest(bucketName)
						.withObjectOwnership(ObjectOwnership.BucketOwnerEnforced)
						.withCannedAcl(CannedAccessControlList.PublicRead)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_BUCKET_ACL_WITH_OBJECT_OWNERSHIP, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testCreateBucketInvalidOwnership() {
		var client = getClient();
		var bucketName = getNewBucketName(15);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.createBucket(new CreateBucketRequest(bucketName).withObjectOwnership("Invalid")));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipEnforcedWithAclGrants() {
		var client = getClient();
		var bucketName = createBucketAltWritable(client, 16);
		// 버킷 ACL 에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced 로 바꿀 수 없다
		var e = assertThrows(AmazonServiceException.class,
				() -> putOwnership(client, bucketName, "BucketOwnerEnforced"));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_BUCKET_ACL_WITH_OBJECT_OWNERSHIP, e.getErrorCode());

		// ACL 을 초기화하면 바꿀 수 있다
		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
		putOwnership(client, bucketName, "BucketOwnerEnforced");
		assertEquals("BucketOwnerEnforced", getOwnership(client, bucketName));
	}

	@Test
	@Tag("Owner")
	public void testObjectOwnerObjectWriter() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketAltWritable(client, 17);
		var key = "testObjectOwnerObjectWriter";

		// ObjectWriter: 올린 사용자가 오브젝트 소유자가 되고, 버킷 소유자도 읽을 수 없다
		altClient.putObject(bucketName, key, key);
		assertEquals(config.altUser.id, objectOwnerId(client, bucketName, key));
		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		// bucket-owner-full-control 을 줘도 소유자는 올린 사용자다
		var key2 = key + "2";
		altClient.putObject(new PutObjectRequest(bucketName, key2, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
		assertEquals(config.altUser.id, objectOwnerId(client, bucketName, key2));
		assertEquals(key, client.getObjectAsString(bucketName, key2));
	}

	@Test
	@Tag("Owner")
	public void testObjectOwnerBucketOwnerPreferred() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketAltWritable(client, 18);
		putOwnership(client, bucketName, "BucketOwnerPreferred");
		var key = "testObjectOwnerBucketOwnerPreferred";

		// BucketOwnerPreferred: ACL 없이 올리면 올린 사용자가 소유자다
		altClient.putObject(bucketName, key, key);
		assertEquals(config.altUser.id, objectOwnerId(client, bucketName, key));

		// bucket-owner-full-control 로 올리면 버킷 소유자가 소유자가 된다
		var key2 = key + "2";
		altClient.putObject(new PutObjectRequest(bucketName, key2, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
		assertEquals(config.mainUser.id, objectOwnerId(client, bucketName, key2));
	}

	@Test
	@Tag("Owner")
	public void testObjectOwnerBucketOwnerEnforced() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 19, ObjectOwnership.BucketOwnerEnforced);
		var key = "testObjectOwnerBucketOwnerEnforced";
		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));
		var principal = new JsonObject();
		principal.addProperty("CanonicalUser", config.altUser.id);
		client.setBucketPolicy(bucketName, makeJsonPolicy("s3:PutObject", resource, principal, null).toString());

		// BucketOwnerEnforced: 누가 올려도 버킷 소유자가 소유자다
		waitUntil(() -> status(() -> altClient.putObject(bucketName, key, key)) == HttpStatus.SC_OK);
		assertEquals(config.mainUser.id, objectOwnerId(client, bucketName, key));

		// bucket-owner-full-control 은 허용되고 그 밖의 ACL 은 거부된다
		altClient.putObject(new PutObjectRequest(bucketName, key + "2", createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
		var e = assertThrows(AmazonServiceException.class,
				() -> altClient
						.putObject(new PutObjectRequest(bucketName, key + "3", createBody(key), new ObjectMetadata())
								.withCannedAcl(CannedAccessControlList.Private)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipEnforcedAcl() {
		var client = getClient();
		var bucketName = createBucket(client, 20, ObjectOwnership.BucketOwnerEnforced);
		var key = "testBucketOwnershipEnforcedAcl";
		client.putObject(bucketName, key, key);

		// ACL 조회는 가능하고 소유자의 FULL_CONTROL 만 나온다
		checkOwnerFullControlOnly(client.getBucketAcl(bucketName));
		checkOwnerFullControlOnly(client.getObjectAcl(bucketName, key));

		// ACL 변경은 거부된다
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.Private));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> client.putObject(new PutObjectRequest(bucketName, key + "2", createBody("a"), new ObjectMetadata())
						.withCannedAcl(CannedAccessControlList.PublicRead)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.getErrorCode());

		// 소유자의 bucket-owner-full-control 은 허용된다
		client.putObject(new PutObjectRequest(bucketName, key + "3", createBody("a"), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
	}
}
