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

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PublicAccessBlockConfiguration;

import org.junit.jupiter.api.Tag;

public class Access extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Access Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Access End");
	}

	@Test
	@Tag("Check")
	// 버킷의 접근권한 블록 설정 확인
	public void testPutPublicBlock() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(true)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf).build());

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName).build());
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());
		assertEquals(accessConf.ignorePublicAcls(), response.publicAccessBlockConfiguration().ignorePublicAcls());
		assertEquals(accessConf.restrictPublicBuckets(),
				response.publicAccessBlockConfiguration().restrictPublicBuckets());
	}

	@Test
	@Tag("Denied")
	// 버킷의 접근권한 블록을 설정한뒤 acl로 버킷의 권한정보를 덮어씌우기 실패 확인
	public void testBlockPublicPutBucketAcls() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(false)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf).build());

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName).build());
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ).build()));
		var statusCode = e.statusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE).build()));
		statusCode = e.statusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.AUTHENTICATED_READ).build()));
		statusCode = e.statusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("Denied")
	// 버킷의 접근권한 블록에서 acl권한 설정금지로 설정한뒤 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인
	public void testBlockPublicObjectCannedAcls() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(false)
				.blockPublicPolicy(false).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf).build());

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName).build());
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());

		var e = assertThrows(AwsServiceException.class, () -> client.putObject(
				p -> p.bucket(bucketName).key("foo1").acl(ObjectCannedACL.PUBLIC_READ).build(),
				RequestBody.fromString("foo1")));
		var statusCode = e.statusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AwsServiceException.class,
				() -> client.putObject(
						p -> p.bucket(bucketName).key("foo2").acl(ObjectCannedACL.PUBLIC_READ_WRITE).build(),
						RequestBody.fromString("foo2")));
		statusCode = e.statusCode();
		assertEquals(403, statusCode);
		e = assertThrows(AwsServiceException.class,
				() -> client.putObject(
						p -> p.bucket(bucketName).key("foo3").acl(ObjectCannedACL.AUTHENTICATED_READ).build(),
						RequestBody.fromString("foo3")));
		statusCode = e.statusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("Denied")
	// 버킷의 접근권한블록으로 권한 설정을 할 수 없도록 막은 뒤 버킷의 정책을 추가하려고 할때 실패 확인
	public void testBlockPublicPolicy() {
		var bucketName = getNewBucket();
		var client = getClient();
		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(false).ignorePublicAcls(false)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf).build());

		var resource = makeArnResource(String.format("%s/*", bucketName));
		var policyDocument = makeJsonPolicy("s3:GetObject", resource, null, null);
		assertThrows(AwsServiceException.class,
				() -> client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()).build()));
	}

	@Test
	@Tag("Denied")
	// 버킷의 접근권한블록으로 개인버킷처럼 설정한뒤 버킷의acl권한을 public-read로 변경해도 적용되지 않음을 확인
	public void testIgnorePublicAcls() {
		var bucketName = getNewBucket();
		var client = getClient();
		var altClient = getAltClient();

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ).build());
		altClient.listObjects(l -> l.bucket(bucketName).build());

		client.putObject(
				p -> p.bucket(bucketName).key("key1").acl(ObjectCannedACL.PUBLIC_READ).build(),
				RequestBody.fromString("abcde"));
		var response = altClient.getObject(g -> g.bucket(bucketName).key("key1").build());
		assertEquals("abcde", getBody(response));

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(false).ignorePublicAcls(true)
				.blockPublicPolicy(false).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf).build());

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ).build());

		var publicClient = getPublicClient();
		assertThrows(AwsServiceException.class,
				() -> publicClient.listObjects(l -> l.bucket(bucketName).build()));
		assertThrows(AwsServiceException.class,
				() -> publicClient.getObject(g -> g.bucket(bucketName).key("key1").build()));
	}

	@Test
	@Tag("Check")
	// 버킷의 접근권한 블록 삭제 확인
	public void testDeletePublicBlock() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(true)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf).build());

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName).build());
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());
		assertEquals(accessConf.ignorePublicAcls(), response.publicAccessBlockConfiguration().ignorePublicAcls());
		assertEquals(accessConf.restrictPublicBuckets(),
				response.publicAccessBlockConfiguration().restrictPublicBuckets());

		client.deletePublicAccessBlock(d -> d.bucket(bucketName).build());

		assertThrows(SdkClientException.class,
				() -> client.getPublicAccessBlock(g -> g.bucket(bucketName).build()));
	}
}