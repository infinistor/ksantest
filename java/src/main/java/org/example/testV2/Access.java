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

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.model.PublicAccessBlockConfiguration;

public class Access extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Access V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Access V2 End");
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicAclAndPolicy() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(false)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName));
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.AUTHENTICATED_READ)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicAcls() {
		var key = "testBlockPublicAcls";
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true).ignorePublicAcls(false)
				.blockPublicPolicy(false).restrictPublicBuckets(false).build();

		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName));
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());

		var e = assertThrows(AwsServiceException.class, () -> client.putObject(
				p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putObject(
						p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ_WRITE),
						RequestBody.fromString(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putObject(
						p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.AUTHENTICATED_READ),
						RequestBody.fromString(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Denied")
	public void testBlockPublicPolicy() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(false).ignorePublicAcls(false)
				.blockPublicPolicy(true).restrictPublicBuckets(false).build();
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var resource = makeArnResource(String.format("%s/*", bucketName));
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
		var bucketName = createBucket(client, ObjectOwnership.OBJECT_WRITER);

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
		var bucketName = createBucketCannedAcl(client);

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
		var bucketName = createBucket(client, ObjectOwnership.OBJECT_WRITER);

		var accessConf = PublicAccessBlockConfiguration.builder().blockPublicAcls(true)
				.ignorePublicAcls(true).blockPublicPolicy(true).restrictPublicBuckets(false).build();
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(g -> g.bucket(bucketName));
		assertEquals(accessConf.blockPublicAcls(), response.publicAccessBlockConfiguration().blockPublicAcls());
		assertEquals(accessConf.blockPublicPolicy(), response.publicAccessBlockConfiguration().blockPublicPolicy());
		assertEquals(accessConf.ignorePublicAcls(), response.publicAccessBlockConfiguration().ignorePublicAcls());
		assertEquals(accessConf.restrictPublicBuckets(),
				response.publicAccessBlockConfiguration().restrictPublicBuckets());
	}
}