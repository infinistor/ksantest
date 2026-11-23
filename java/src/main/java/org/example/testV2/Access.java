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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.model.PublicAccessBlockConfiguration;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;

public class Access extends TestBase {
	@Test
	@Tag("Denied")
	public void testBlockPublicAclAndPolicy() {
		var client = getClient();
		var bucketName = createBucket(client, 1, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(false)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName));
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());

		BucketCannedACL[] publicAcls = {
				BucketCannedACL.PUBLIC_READ,
				BucketCannedACL.PUBLIC_READ_WRITE,
				BucketCannedACL.AUTHENTICATED_READ
		};

		for (var acl : publicAcls) {
			var e = assertThrows(AwsServiceException.class,
					() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(acl)));
			assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
			assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
		}
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicAcls() {
		var key = "testBlockPublicAcls";
		var client = getClient();
		var bucketName = createBucket(client, 2, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(false)
				.blockPublicPolicy(false).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName));
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());

		ObjectCannedACL[] publicAcls = {
				ObjectCannedACL.PUBLIC_READ,
				ObjectCannedACL.PUBLIC_READ_WRITE,
				ObjectCannedACL.AUTHENTICATED_READ
		};

		for (ObjectCannedACL acl : publicAcls) {
			var e = assertThrows(AwsServiceException.class,
					() -> client.putObject(p -> p.bucket(bucketName).key(key).acl(acl),
							RequestBody.fromString(key)));
			assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
			assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
		}
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicPolicy() {
		var client = getClient();
		var bucketName = createBucket(client, 3, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(false).ignorePublicAcls(false)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));
		var policyDocument = makeJsonPolicy("s3:GetObject", resource, null, null);
		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testDeletePublicBlock() {
		var client = getClient();
		var bucketName = createBucket(client, 4, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true)
				.ignorePublicAcls(true).blockPublicPolicy(true).restrictPublicBuckets(false).build();
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName));
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());
		assertEquals(accessConf.ignorePublicAcls(), response.publicAccessBlockConfiguration().ignorePublicAcls());
		assertEquals(accessConf.restrictPublicBuckets(),
				response.publicAccessBlockConfiguration().restrictPublicBuckets());

		client.deletePublicAccessBlock(d -> d.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class, () -> client.getPublicAccessBlock(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Denied")
	public void testIgnorePublicAcls() {
		var key = "testIgnorePublicAcls";
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(client, 5);

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));
		var response = altClient.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(key, getBody(response));

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(a -> a
				.blockPublicAcls(false).ignorePublicAcls(true).blockPublicPolicy(false).restrictPublicBuckets(false)));

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));

		var publicClient = getPublicClient();
		var e = assertThrows(AwsServiceException.class, () -> publicClient.listObjects(l -> l.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
		e = assertThrows(AwsServiceException.class,
				() -> publicClient.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testPutPublicBlock() {
		var client = getClient();
		var bucketName = createBucket(client, 6, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true)
				.ignorePublicAcls(true).blockPublicPolicy(true).restrictPublicBuckets(false).build();
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName));
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());
		assertEquals(accessConf.ignorePublicAcls(), response.publicAccessBlockConfiguration().ignorePublicAcls());
		assertEquals(accessConf.restrictPublicBuckets(),
				response.publicAccessBlockConfiguration().restrictPublicBuckets());
		client.deletePublicAccessBlock(d -> d.bucket(bucketName));
	}

	private static void putPublicBlock(S3Client client, String bucketName, boolean blockPublicAcls,
			boolean ignorePublicAcls, boolean blockPublicPolicy, boolean restrictPublicBuckets) {
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(c -> c
				.blockPublicAcls(blockPublicAcls).ignorePublicAcls(ignorePublicAcls)
				.blockPublicPolicy(blockPublicPolicy).restrictPublicBuckets(restrictPublicBuckets)));
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

	private static int getObjectStatus(S3Client client, String bucketName, String key) {
		try {
			client.getObject(g -> g.bucket(bucketName).key(key), ResponseTransformer.toBytes());
			return HttpStatus.SC_OK;
		} catch (AwsServiceException e) {
			return e.statusCode();
		}
	}

	/**
	 * 새 연결로 오브젝트를 조회한다.
	 * AWS 는 유지 중인 연결(keep-alive)에 이전 인가 결과를 계속 돌려주는 경우가 있어,
	 * 설정 변경이 반영됐는지는 매번 새 클라이언트로 확인한다.
	 */
	private static int getObjectStatusNewConnection(Supplier<S3Client> clientFactory, String bucketName, String key) {
		try (var client = clientFactory.get()) {
			return getObjectStatus(client, bucketName, key);
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
		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName)).publicAccessBlockConfiguration();
		assertTrue(response.blockPublicAcls());
		assertTrue(response.ignorePublicAcls());
		assertTrue(response.blockPublicPolicy());
		assertTrue(response.restrictPublicBuckets());
	}

	@Test
	@Tag("Check")
	public void testPutPublicBlockPartial() {
		var client = getClient();
		var bucketName = createBucket(client, 8);
		// 지정하지 않은 항목은 false 로 저장된다
		client.putPublicAccessBlock(
				p -> p.bucket(bucketName).publicAccessBlockConfiguration(c -> c.blockPublicAcls(true)));
		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName)).publicAccessBlockConfiguration();
		assertTrue(response.blockPublicAcls());
		assertFalse(response.ignorePublicAcls());
		assertFalse(response.blockPublicPolicy());
		assertFalse(response.restrictPublicBuckets());
	}

	@Test
	@Tag("Check")
	public void testDeletePublicBlockNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		client.deletePublicAccessBlock(d -> d.bucket(bucketName));
		var e = assertThrows(AwsServiceException.class, () -> client.getPublicAccessBlock(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_PUBLIC_ACCESS_BLOCK_CONFIGURATION, e.awsErrorDetails().errorCode());
		// 설정이 없어도 삭제는 성공한다
		client.deletePublicAccessBlock(d -> d.bucket(bucketName));
	}

	@Test
	@Tag("Error")
	public void testPublicBlockBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(10);

		var e = assertThrows(AwsServiceException.class, () -> client.getPublicAccessBlock(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> putPublicBlock(client, bucketName, false, false, false, false));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> client.deletePublicAccessBlock(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Permission")
	public void testPublicBlockAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 11);
		putPublicBlock(client, bucketName, true, false, false, false);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getPublicAccessBlock(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> putPublicBlock(altClient, bucketName, false, false, false, false));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.deletePublicAccessBlock(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicAclsAllowPrivate() {
		var key = "testBlockPublicAclsAllowPrivate";
		var client = getClient();
		var bucketName = createBucket(client, 12, ObjectOwnership.OBJECT_WRITER);
		putPublicBlock(client, bucketName, true, false, false, false);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		// 퍼블릭 ACL 은 오브젝트 ACL 변경에서도 거부된다
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		// 퍼블릭이 아닌 ACL 은 허용된다
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE));
		client.putObject(p -> p.bucket(bucketName).key(key + "2").acl(ObjectCannedACL.PRIVATE),
				RequestBody.fromString(key));
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PRIVATE));
	}

	@Test
	@Tag("Policy")
	public void testBucketPolicyStatus() {
		var client = getClient();
		var bucketName = createBucket(client, 13);
		putPublicBlock(client, bucketName, false, false, false, false);
		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketPolicyStatus(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET_POLICY, e.awsErrorDetails().errorCode());

		// 모든 사용자에게 여는 정책은 퍼블릭으로 판정된다
		var publicPolicy = makeJsonPolicy("s3:GetObject", resource, null, null);
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(publicPolicy.toString()));
		assertTrue(client.getBucketPolicyStatus(g -> g.bucket(bucketName)).policyStatus().isPublic());

		// 특정 사용자에게만 여는 정책은 퍼블릭이 아니다
		var userPolicy = makeJsonPolicy("s3:GetObject", resource, altUserPrincipal(), null);
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(userPolicy.toString()));
		assertFalse(client.getBucketPolicyStatus(g -> g.bucket(bucketName)).policyStatus().isPublic());
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
		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketPolicy(p -> p.bucket(bucketName).policy(publicPolicy.toString())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		var userPolicy = makeJsonPolicy("s3:GetObject", resource, altUserPrincipal(), null);
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(userPolicy.toString()));
		assertTrue(StringUtils.isNotBlank(client.getBucketPolicy(g -> g.bucket(bucketName)).policy()));
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
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		setPublicReadPolicy(client, bucketName);

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
