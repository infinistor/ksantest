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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockRequest;
import com.amazonaws.services.s3.model.GetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusRequest;

public class Access extends TestBase {
	@Test
	@Tag("Denied")
	public void testBlockPublicAclAndPolicy() {
		var client = getClient();
		var bucketName = createBucket(client, 1, ObjectOwnership.ObjectWriter);

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName));
		assertEquals(accessConf.getBlockPublicAcls(),
				response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(accessConf.getBlockPublicPolicy(),
				response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.PublicReadWrite));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicAcls() {
		var key = "testBlockPublicAcls";
		var client = getClient();
		var bucketName = createBucket(client, 2, ObjectOwnership.ObjectWriter);

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(false).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName));
		assertEquals(accessConf.getBlockPublicAcls(),
				response.getPublicAccessBlockConfiguration().getBlockPublicAcls());

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(
				new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
						.withCannedAcl(CannedAccessControlList.PublicRead)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> client.putObject(
				new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
						.withCannedAcl(CannedAccessControlList.PublicReadWrite)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> client.putObject(
				new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
						.withCannedAcl(CannedAccessControlList.AuthenticatedRead)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicPolicy() {
		var client = getClient();
		var bucketName = createBucket(client, 3, ObjectOwnership.ObjectWriter);

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(false).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));
		var policyDocument = makeJsonPolicy("s3:GetObject", resource, null, null);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketPolicy(bucketName, policyDocument.toString()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Check")
	public void testDeletePublicBlock() {
		var client = getClient();
		var bucketName = createBucket(client, 4, ObjectOwnership.ObjectWriter);

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true)
				.withIgnorePublicAcls(true).withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest()
				.withBucketName(bucketName).withPublicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName));
		assertEquals(accessConf.getBlockPublicAcls(),
				response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(accessConf.getBlockPublicPolicy(),
				response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());
		assertEquals(accessConf.getIgnorePublicAcls(),
				response.getPublicAccessBlockConfiguration().getIgnorePublicAcls());
		assertEquals(accessConf.getRestrictPublicBuckets(),
				response.getPublicAccessBlockConfiguration().getRestrictPublicBuckets());

		client.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(bucketName));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Denied")
	public void testIgnorePublicAcls() {
		var key = "testIgnorePublicAcls";
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 5, ObjectOwnership.ObjectWriter, null);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));
		var response = altClient.getObject(bucketName, key);
		assertEquals(key, getBody(response.getObjectContent()));

		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(new PublicAccessBlockConfiguration()
						.withBlockPublicAcls(false).withIgnorePublicAcls(true)
						.withBlockPublicPolicy(false).withRestrictPublicBuckets(false)));
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var publicClient = getPublicClient();
		var e = assertThrows(AmazonServiceException.class, () -> publicClient.listObjects(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> publicClient.getObject(bucketName, key));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Check")
	public void testPutPublicBlock() {
		var client = getClient();
		var bucketName = createBucket(client, 6, ObjectOwnership.ObjectWriter);

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true)
				.withIgnorePublicAcls(true).withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName));
		assertEquals(accessConf.getBlockPublicAcls(),
				response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(accessConf.getBlockPublicPolicy(),
				response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());
		assertEquals(accessConf.getIgnorePublicAcls(),
				response.getPublicAccessBlockConfiguration().getIgnorePublicAcls());
		assertEquals(accessConf.getRestrictPublicBuckets(),
				response.getPublicAccessBlockConfiguration().getRestrictPublicBuckets());
		client.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(bucketName));
	}

	private static void putPublicBlock(AmazonS3 client, String bucketName, boolean blockPublicAcls,
			boolean ignorePublicAcls, boolean blockPublicPolicy, boolean restrictPublicBuckets) {
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(new PublicAccessBlockConfiguration()
						.withBlockPublicAcls(blockPublicAcls).withIgnorePublicAcls(ignorePublicAcls)
						.withBlockPublicPolicy(blockPublicPolicy).withRestrictPublicBuckets(restrictPublicBuckets)));
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

	private static int getObjectStatus(AmazonS3 client, String bucketName, String key) {
		try {
			client.getObjectAsString(bucketName, key);
			return HttpStatus.SC_OK;
		} catch (AmazonServiceException e) {
			return e.getStatusCode();
		}
	}

	/**
	 * 새 연결로 오브젝트를 조회한다.
	 * AWS 는 유지 중인 연결(keep-alive)에 이전 인가 결과를 계속 돌려주는 경우가 있어,
	 * 설정 변경이 반영됐는지는 매번 새 클라이언트로 확인한다.
	 */
	private static int getObjectStatusNewConnection(Supplier<AmazonS3> clientFactory, String bucketName, String key) {
		var client = clientFactory.get();
		try {
			return getObjectStatus(client, bucketName, key);
		} finally {
			client.shutdown();
		}
	}

	private JsonObject altUserPrincipal() {
		var principal = new JsonObject();
		principal.addProperty("CanonicalUser", config.altUser.id);
		return principal;
	}

	@Test
	@Tag("Check")
	public void testPublicBlockDefault() {
		var client = getClient();
		// createBucket(client, testId) 는 퍼블릭 액세스 차단 설정을 건드리지 않고 기본값 그대로 버킷을 만든다
		var bucketName = createBucket(client, 7);
		// 새 버킷은 네 가지 차단이 모두 켜진 상태로 만들어진다
		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName))
				.getPublicAccessBlockConfiguration();
		assertTrue(response.getBlockPublicAcls());
		assertTrue(response.getIgnorePublicAcls());
		assertTrue(response.getBlockPublicPolicy());
		assertTrue(response.getRestrictPublicBuckets());
	}

	@Test
	@Tag("Check")
	public void testPutPublicBlockPartial() {
		var client = getClient();
		var bucketName = createBucket(client, 8);
		// 지정하지 않은 항목은 false 로 저장된다
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(new PublicAccessBlockConfiguration().withBlockPublicAcls(true)));
		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName))
				.getPublicAccessBlockConfiguration();
		assertTrue(response.getBlockPublicAcls());
		assertFalse(response.getIgnorePublicAcls());
		assertFalse(response.getBlockPublicPolicy());
		assertFalse(response.getRestrictPublicBuckets());
	}

	@Test
	@Tag("Check")
	public void testDeletePublicBlockNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		client.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(bucketName));
		var e = assertThrows(AmazonServiceException.class,
				() -> client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION, e.getErrorCode());
		// 설정이 없어도 삭제는 성공한다
		client.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(bucketName));
	}

	@Test
	@Tag("Error")
	public void testPublicBlockBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(10);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> putPublicBlock(client, bucketName, false, false, false, false));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> client
				.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Permission")
	public void testPublicBlockAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 11);
		putPublicBlock(client, bucketName, true, false, false, false);

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> putPublicBlock(altClient, bucketName, false, false, false, false));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> altClient
				.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicAclsAllowPrivate() {
		var key = "testBlockPublicAclsAllowPrivate";
		var client = getClient();
		var bucketName = createBucket(client, 12, ObjectOwnership.ObjectWriter);
		putPublicBlock(client, bucketName, true, false, false, false);
		client.putObject(bucketName, key, key);

		// 퍼블릭 ACL 은 오브젝트 ACL 변경에서도 거부된다
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectAcl(bucketName, key, CannedAccessControlList.PublicRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		// 퍼블릭이 아닌 ACL 은 허용된다
		client.setObjectAcl(bucketName, key, CannedAccessControlList.Private);
		client.putObject(new PutObjectRequest(bucketName, key + "2", createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.Private));
		client.setBucketAcl(bucketName, CannedAccessControlList.Private);
	}

	@Test
	@Tag("Policy")
	public void testBucketPolicyStatus() {
		var client = getClient();
		var bucketName = createBucket(client, 13);
		putPublicBlock(client, bucketName, false, false, false, false);
		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET_POLICY, e.getErrorCode());

		// 모든 사용자에게 여는 정책은 퍼블릭으로 판정된다
		client.setBucketPolicy(bucketName, makeJsonPolicy("s3:GetObject", resource, null, null).toString());
		assertTrue(client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName))
				.getPolicyStatus().getIsPublic());

		// 특정 사용자에게만 여는 정책은 퍼블릭이 아니다
		client.setBucketPolicy(bucketName,
				makeJsonPolicy("s3:GetObject", resource, altUserPrincipal(), null).toString());
		assertFalse(client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName))
				.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Policy")
	public void testBlockPublicPolicyAllowNonPublic() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		putPublicBlock(client, bucketName, false, false, true, false);
		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));

		// 퍼블릭 정책은 거부되지만 특정 사용자에게만 여는 정책은 허용된다
		var publicPolicy = makeJsonPolicy("s3:GetObject", resource, null, null);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketPolicy(bucketName, publicPolicy.toString()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.setBucketPolicy(bucketName,
				makeJsonPolicy("s3:GetObject", resource, altUserPrincipal(), null).toString());
		assertTrue(StringUtils.isNotBlank(client.getBucketPolicy(bucketName).getPolicyText()));
	}

	@Test
	@Tag("Denied")
	public void testRestrictPublicBuckets() {
		var key = "testRestrictPublicBuckets";
		var client = getClient();
		var altClient = getAltClient();
		var publicClient = getPublicClient();
		var bucketName = createBucket(client, 15);
		putPublicBlock(client, bucketName, false, false, false, false);
		client.putObject(bucketName, key, key);
		// 익명 사용자가 버킷의 모든 객체를 읽을 수 있도록 정책을 건다
		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));
		client.setBucketPolicy(bucketName, makeJsonPolicy("s3:GetObject", resource, null, null).toString());

		// 퍼블릭 정책으로 누구나 읽을 수 있다
		waitUntil(() -> getObjectStatus(publicClient, bucketName, key) == HttpStatus.SC_OK);
		waitUntil(() -> getObjectStatus(altClient, bucketName, key) == HttpStatus.SC_OK);

		// RestrictPublicBuckets 를 켜면 퍼블릭 정책이 있어도 버킷 소유자만 접근할 수 있다
		putPublicBlock(client, bucketName, false, false, false, true);
		waitUntil(() -> getObjectStatusNewConnection(this::getPublicClient, bucketName, key) == HttpStatus.SC_FORBIDDEN);
		waitUntil(() -> getObjectStatusNewConnection(this::getAltClient, bucketName, key) == HttpStatus.SC_FORBIDDEN);
		assertEquals(HttpStatus.SC_OK, getObjectStatus(client, bucketName, key));
	}
}