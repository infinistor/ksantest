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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockRequest;
import com.amazonaws.services.s3.model.GetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;

public class Access extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("Access Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Access End");
	}

	@Test
	@Tag("Check")
	// 버킷의 접근권한 블록 설정 확인
	public void test_put_public_block() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(true)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
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

	@Test
	@Tag("Denied")
	// 버킷의 접근권한 블록을 설정한뒤 acl로 버킷의 권한정보를 덮어씌우기 실패 확인
	public void test_block_public_put_bucket_acls() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName));
		assertEquals(accessConf.getBlockPublicAcls(),
				response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(accessConf.getBlockPublicPolicy(),
				response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());

		AmazonServiceException e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.PublicReadWrite));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("Denied")
	// 버킷의 접근권한 블록에서 acl권한 설정금지로 설정한뒤 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인
	public void test_block_public_object_canned_acls() {
		var bucketName = getNewBucket();
		var client = getClient();
		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(4);

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(false).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName));
		assertEquals(accessConf.getBlockPublicAcls(),
				response.getPublicAccessBlockConfiguration().getBlockPublicAcls());

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(
				new PutObjectRequest(bucketName, "foo1", createBody("foo1"), metadata)
						.withCannedAcl(CannedAccessControlList.PublicRead)));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> client.putObject(
				new PutObjectRequest(bucketName, "foo2", createBody("foo2"), metadata)
						.withCannedAcl(CannedAccessControlList.PublicReadWrite)));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> client.putObject(
				new PutObjectRequest(bucketName, "foo3", createBody("foo3"), metadata)
						.withCannedAcl(CannedAccessControlList.AuthenticatedRead)));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("Denied")
	// 버킷의 접근권한블록으로 권한 설정을 할 수 없도록 막은 뒤 버킷의 정책을 추가하려고 할때 실패 확인
	public void test_block_public_policy() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(false).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var resource = makeArnResource(String.format("%s/*", bucketName));
		var policyDocument = makeJsonPolicy("s3:GetObject", resource, null, null);
		assertThrows(AmazonServiceException.class, () -> client.setBucketPolicy(bucketName, policyDocument.toString()));
	}

	@Test
	@Tag("Denied")
	// 버킷의 접근권한블록으로 개인버킷처럼 설정한뒤 버킷의acl권한을 public-read로 변경해도 적용되지 않음을 확인
	public void test_ignore_public_acls() {
		var bucketName = getNewBucket();
		var client = getClient();
		var altClient = getAltClient();

		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
		altClient.listObjects(bucketName);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(5);

		client.putObject(new PutObjectRequest(bucketName, "key1", createBody("abcde"), metadata)
				.withCannedAcl(CannedAccessControlList.PublicRead));
		var response = altClient.getObject(bucketName, "key1");
		assertEquals("abcde", getBody(response.getObjectContent()));

		var accessConf = new PublicAccessBlockConfiguration()
				.withBlockPublicAcls(false)
				.withIgnorePublicAcls(true)
				.withBlockPublicPolicy(false)
				.withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var publicClient = getPublicClient();
		assertThrows(AmazonServiceException.class, () -> publicClient.listObjects(bucketName));
		assertThrows(AmazonServiceException.class, () -> publicClient.getObject(bucketName, "key1"));
	}

	@Test
	@Tag("Check")
	// 버킷의 접근권한 블록 삭제 확인
	public void test_delete_public_block() {
		var bucketName = getNewBucket();
		var client = getClient();

		var accessConf = new PublicAccessBlockConfiguration()
				.withBlockPublicAcls(true)
				.withIgnorePublicAcls(true)
				.withBlockPublicPolicy(true)
				.withRestrictPublicBuckets(false);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest()
				.withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(accessConf));

		var response = client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName));
		assertEquals(accessConf.getBlockPublicAcls(), response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(accessConf.getBlockPublicPolicy(), response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());
		assertEquals(accessConf.getIgnorePublicAcls(), response.getPublicAccessBlockConfiguration().getIgnorePublicAcls());
		assertEquals(accessConf.getRestrictPublicBuckets(), response.getPublicAccessBlockConfiguration().getRestrictPublicBuckets());

		client.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(bucketName));

		assertThrows(SdkClientException.class, () -> client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(bucketName)));
	}
}