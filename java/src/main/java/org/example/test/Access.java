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

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockRequest;
import com.amazonaws.services.s3.model.GetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;

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
	@Tag("Denied")
	public void testBlockPublicAclAndPolicy() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter);

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
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter);

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
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter);

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(false).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var resource = makeArnResource(String.format("%s/*", bucketName));
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
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter);

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
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, null);

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
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter);

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
	}
}