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
package org.example.testV2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.function.BooleanSupplier;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.example.Utility.Utils;

import com.google.gson.JsonObject;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Permission;

public class Ownership extends TestBase {

	@Test
	@Tag("Get")
	public void testGetBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 1, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		client.getBucketOwnershipControls(g -> g.bucket(bucketName));
	}

	@Test
	@Tag("Put")
	public void testCreateBucketWithOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 2, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		assertEquals(ObjectOwnership.BUCKET_OWNER_ENFORCED,
				response.ownershipControls().rules().get(0).objectOwnership());
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Put")
	public void testChangeBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 3, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		assertEquals(ObjectOwnership.BUCKET_OWNER_ENFORCED,
				response.ownershipControls().rules().get(0).objectOwnership());

		client.putBucketOwnershipControls(p -> p.bucket(bucketName).ownershipControls(c -> c.rules(r -> r
				.objectOwnership(ObjectOwnership.BUCKET_OWNER_PREFERRED))));

		response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		assertEquals(ObjectOwnership.BUCKET_OWNER_PREFERRED,
				response.ownershipControls().rules().get(0).objectOwnership());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucket(client, 4, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		assertEquals(ObjectOwnership.BUCKET_OWNER_ENFORCED,
				response.ownershipControls().rules().get(0).objectOwnership());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipDenyObjectACL() {
		var client = getClient();
		var bucketName = createBucket(client, 5, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var key = "testBucketOwnershipDenyObjectACL";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Check")
	public void testObjectOwnershipDenyChange() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 6);
		var key = "testObjectOwnershipDenyChange";

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));

		var publicClient = getPublicClient();
		publicClient.headObject(h -> h.bucket(bucketName).key(key));

		client.putBucketOwnershipControls(p -> p.bucket(bucketName).ownershipControls(c -> c.rules(r -> r
				.objectOwnership(ObjectOwnership.BUCKET_OWNER_ENFORCED))));

		// BucketOwnerEnforced 로 바꾸면 오브젝트 ACL 이 무시되어 퍼블릭 접근이 막힌다.
		// AWS 는 이미 맺은 연결에서는 변경 전 권한으로 계속 응답하므로 새 클라이언트로 확인한다.
		waitUntil(() -> status(() -> {
			try (var newPublicClient = getPublicClient()) {
				newPublicClient.headObject(h -> h.bucket(bucketName).key(key));
			}
		}) == HttpStatus.SC_FORBIDDEN);
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Error")
	public void testObjectOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 7);
		var key = "testObjectOwnershipDenyACL";

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));

		client.putBucketOwnershipControls(p -> p.bucket(bucketName).ownershipControls(c -> c.rules(r -> r
				.objectOwnership(ObjectOwnership.BUCKET_OWNER_ENFORCED))));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.awsErrorDetails().errorCode());
	}

	@SuppressWarnings("unchecked")
	private static void putOwnership(S3Client client, String bucketName, String ownership) {
		client.putBucketOwnershipControls(
				p -> p.bucket(bucketName).ownershipControls(c -> c.rules(r -> r.objectOwnership(ownership))));
	}

	private static String getOwnership(S3Client client, String bucketName) {
		var response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		return response.ownershipControls().rules().get(0).objectOwnershipAsString();
	}

	private static String objectOwnerId(S3Client client, String bucketName, String key) {
		var response = client.listObjectsV2(l -> l.bucket(bucketName).prefix(key).fetchOwner(true));
		return response.contents().get(0).owner().id();
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
		} catch (AwsServiceException e) {
			return e.statusCode();
		}
	}

	/** ACL 로 alt 사용자에게 쓰기 권한을 준 ObjectWriter 버킷을 만든다. */
	private String createBucketAltWritable(S3Client client, int testId) {
		var bucketName = createBucketCannedAcl(client, testId);
		client.putBucketAcl(p -> p.bucket(bucketName).grantFullControl("id=" + config.mainUser.id)
				.grantWrite("id=" + config.altUser.id));
		return bucketName;
	}

	private void checkOwnerFullControlOnly(List<Grant> grants) {
		assertEquals(1, grants.size());
		assertEquals(config.mainUser.id, grants.get(0).grantee().id());
		assertEquals(Permission.FULL_CONTROL, grants.get(0).permission());
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
		var e = assertThrows(AwsServiceException.class, () -> putOwnership(client, bucketName, "Invalid"));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Delete")
	public void testDeleteBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, 11, ObjectOwnership.BUCKET_OWNER_PREFERRED);
		client.deleteBucketOwnershipControls(d -> d.bucket(bucketName));
		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketOwnershipControls(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.OWNERSHIP_CONTROLS_NOT_FOUND_ERROR, e.awsErrorDetails().errorCode());
		// 설정이 없어도 삭제는 성공한다
		client.deleteBucketOwnershipControls(d -> d.bucket(bucketName));
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(12);

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketOwnershipControls(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> putOwnership(client, bucketName, "ObjectWriter"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.deleteBucketOwnershipControls(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Permission")
	public void testBucketOwnershipAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 13);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getBucketOwnershipControls(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> putOwnership(altClient, bucketName, "ObjectWriter"));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.deleteBucketOwnershipControls(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testCreateBucketOwnershipEnforcedWithAcl() {
		var client = getClient();
		var bucketName = getNewBucketName(14);
		// BucketOwnerEnforced 버킷은 ACL 을 함께 지정해 만들 수 없다
		var e = assertThrows(AwsServiceException.class, () -> client.createBucket(c -> c.bucket(bucketName)
				.objectOwnership(ObjectOwnership.BUCKET_OWNER_ENFORCED).acl(BucketCannedACL.PUBLIC_READ)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_BUCKET_ACL_WITH_OBJECT_OWNERSHIP, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testCreateBucketInvalidOwnership() {
		var client = getClient();
		var bucketName = getNewBucketName(15);
		var e = assertThrows(AwsServiceException.class,
				() -> client.createBucket(c -> c.bucket(bucketName).objectOwnership("Invalid")));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipEnforcedWithAclGrants() {
		var client = getClient();
		var bucketName = createBucketAltWritable(client, 16);
		// 버킷 ACL 에 소유자 외의 권한이 남아 있으면 BucketOwnerEnforced 로 바꿀 수 없다
		var e = assertThrows(AwsServiceException.class,
				() -> putOwnership(client, bucketName, "BucketOwnerEnforced"));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_BUCKET_ACL_WITH_OBJECT_OWNERSHIP, e.awsErrorDetails().errorCode());

		// ACL 을 초기화하면 바꿀 수 있다
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));
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
		altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		assertEquals(config.altUser.id, objectOwnerId(client, bucketName, key));
		var e = assertThrows(AwsServiceException.class, () -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		// bucket-owner-full-control 을 줘도 소유자는 올린 사용자다
		var key2 = key + "2";
		altClient.putObject(p -> p.bucket(bucketName).key(key2).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));
		assertEquals(config.altUser.id, objectOwnerId(client, bucketName, key2));
		assertEquals(key, getBody(client.getObject(g -> g.bucket(bucketName).key(key2))));
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
		altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		assertEquals(config.altUser.id, objectOwnerId(client, bucketName, key));

		// bucket-owner-full-control 로 올리면 버킷 소유자가 소유자가 된다
		var key2 = key + "2";
		altClient.putObject(p -> p.bucket(bucketName).key(key2).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));
		assertEquals(config.mainUser.id, objectOwnerId(client, bucketName, key2));
	}

	@Test
	@Tag("Owner")
	public void testObjectOwnerBucketOwnerEnforced() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 19, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var key = "testObjectOwnerBucketOwnerEnforced";
		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));
		var principal = new JsonObject();
		principal.addProperty("CanonicalUser", config.altUser.id);
		var policyDocument = makeJsonPolicy("s3:PutObject", resource, principal, null);
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		// BucketOwnerEnforced: 누가 올려도 버킷 소유자가 소유자다
		waitUntil(() -> status(() -> altClient.putObject(p -> p.bucket(bucketName).key(key),
				RequestBody.fromString(key))) == HttpStatus.SC_OK);
		assertEquals(config.mainUser.id, objectOwnerId(client, bucketName, key));

		// bucket-owner-full-control 은 허용되고 그 밖의 ACL 은 거부된다
		altClient.putObject(
				p -> p.bucket(bucketName).key(key + "2").acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));
		var e = assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key + "3").acl(ObjectCannedACL.PRIVATE),
						RequestBody.fromString(key)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipEnforcedAcl() {
		var client = getClient();
		var bucketName = createBucket(client, 20, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var key = "testBucketOwnershipEnforcedAcl";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		// ACL 조회는 가능하고 소유자의 FULL_CONTROL 만 나온다
		checkOwnerFullControlOnly(client.getBucketAcl(g -> g.bucket(bucketName)).grants());
		checkOwnerFullControlOnly(client.getObjectAcl(g -> g.bucket(bucketName).key(key)).grants());

		// ACL 변경은 거부된다
		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key + "2").acl(ObjectCannedACL.PUBLIC_READ),
						RequestBody.fromString("a")));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.awsErrorDetails().errorCode());

		// 소유자의 bucket-owner-full-control 은 허용된다
		client.putObject(p -> p.bucket(bucketName).key(key + "3").acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString("a"));
	}
}
