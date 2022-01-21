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
import org.junit.jupiter.api.DisplayName;
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

public class Access extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Access Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Access End");
	}

	@Test
	@DisplayName("test_put_public_block")
	@Tag("Check")
	//@Tag("버킷의 접근권한 블록 설정 확인")
	public void test_put_public_block() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var AccessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(true)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		Client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(BucketName)
				.withPublicAccessBlockConfiguration(AccessConf));

		var Response = Client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(BucketName));
		assertEquals(AccessConf.getBlockPublicAcls(),
				Response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(AccessConf.getBlockPublicPolicy(),
				Response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());
		assertEquals(AccessConf.getIgnorePublicAcls(),
				Response.getPublicAccessBlockConfiguration().getIgnorePublicAcls());
		assertEquals(AccessConf.getRestrictPublicBuckets(),
				Response.getPublicAccessBlockConfiguration().getRestrictPublicBuckets());
	}

	@Test
	@DisplayName("test_block_public_put_bucket_acls")
	@Tag("Denied")
	//@Tag("버킷의 접근권한 블록을 설정한뒤 acl로 버킷의 권한정보를 덮어씌우기 실패 확인")
	public void test_block_public_put_bucket_acls() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var AccessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		Client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(BucketName)
				.withPublicAccessBlockConfiguration(AccessConf));

		var Response = Client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(BucketName));
		assertEquals(AccessConf.getBlockPublicAcls(),
				Response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(AccessConf.getBlockPublicPolicy(),
				Response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());

		AmazonServiceException e = assertThrows(AmazonServiceException.class,
				() -> Client.setBucketAcl(BucketName, CannedAccessControlList.PublicRead));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> Client.setBucketAcl(BucketName, CannedAccessControlList.PublicReadWrite));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> Client.setBucketAcl(BucketName, CannedAccessControlList.AuthenticatedRead));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@DisplayName("test_block_public_object_canned_acls")
	@Tag("Denied")
	//@Tag("버킷의 접근권한 블록에서 acl권한 설정금지로 설정한뒤 오브젝트에 acl정보를 추가한뒤 업로드 실패 확인")
	public void test_block_public_object_canned_acls() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(4);

		var AccessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(false).withRestrictPublicBuckets(false);
		Client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(BucketName)
				.withPublicAccessBlockConfiguration(AccessConf));

		var Response = Client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(BucketName));
		assertEquals(AccessConf.getBlockPublicAcls(),
				Response.getPublicAccessBlockConfiguration().getBlockPublicAcls());

		var e = assertThrows(AmazonServiceException.class, () -> Client.putObject(
				new PutObjectRequest(BucketName, "foo1", CreateBody("foo1"), Metadata).withCannedAcl(CannedAccessControlList.PublicRead)));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class, () -> Client.putObject(
				new PutObjectRequest(BucketName, "foo2", CreateBody("foo2"), Metadata).withCannedAcl(CannedAccessControlList.PublicReadWrite)));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class, () -> Client.putObject(
				new PutObjectRequest(BucketName, "foo3", CreateBody("foo3"), Metadata).withCannedAcl(CannedAccessControlList.AuthenticatedRead)));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@DisplayName("test_block_public_policy")
	@Tag("Denied")
	//@Tag("버킷의 접근권한블록으로 권한 설정을 할 수 없도록 막은 뒤 버킷의 정책을 추가하려고 할때 실패 확인")
	public void test_block_public_policy() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var AccessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(false).withIgnorePublicAcls(false)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		Client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(BucketName)
				.withPublicAccessBlockConfiguration(AccessConf));

		var Resource = MakeArnResource(String.format("%s/*", BucketName));
		var PolicyDocument = MakeJsonPolicy("s3:GetObject", Resource, null, null);
		assertThrows(AmazonServiceException.class, () -> Client.setBucketPolicy(BucketName, PolicyDocument.toString()));
	}

	@Test
	@DisplayName("test_ignore_public_acls")
	@Tag("Denied")
	//@Tag("버킷의 접근권한블록으로 개인버킷처럼 설정한뒤 버킷의acl권한을 public-read로 변경해도 적용되지 않음을 확인")
	public void test_ignore_public_acls() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var AltClient = GetAltClient();

		Client.setBucketAcl(BucketName, CannedAccessControlList.PublicRead);
		AltClient.listObjects(BucketName);

		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(5);

		Client.putObject(new PutObjectRequest(BucketName, "key1", CreateBody("abcde"), Metadata).withCannedAcl(CannedAccessControlList.PublicRead));
		var Response = AltClient.getObject(BucketName, "key1");
		assertEquals("abcde", GetBody(Response.getObjectContent()));

		var AccessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(false).withIgnorePublicAcls(true)
				.withBlockPublicPolicy(false).withRestrictPublicBuckets(false);
		Client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(BucketName)
				.withPublicAccessBlockConfiguration(AccessConf));
		Client.setBucketAcl(BucketName, CannedAccessControlList.PublicRead);

		var UnauthenticatedClient = GetUnauthenticatedClient();
		assertThrows(AmazonServiceException.class, () -> UnauthenticatedClient.listObjects(BucketName));
		assertThrows(AmazonServiceException.class, () -> UnauthenticatedClient.getObject(BucketName, "key1"));
	}

	@Test
	@DisplayName("test_delete_public_block")
	@Tag("Check")
	//@Tag("버킷의 접근권한 블록 삭제 확인")
	public void test_delete_public_block() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var AccessConf = new PublicAccessBlockConfiguration().withBlockPublicAcls(true).withIgnorePublicAcls(true)
				.withBlockPublicPolicy(true).withRestrictPublicBuckets(false);
		Client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(BucketName)
				.withPublicAccessBlockConfiguration(AccessConf));

		var Response = Client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(BucketName));
		assertEquals(AccessConf.getBlockPublicAcls(),
				Response.getPublicAccessBlockConfiguration().getBlockPublicAcls());
		assertEquals(AccessConf.getBlockPublicPolicy(),
				Response.getPublicAccessBlockConfiguration().getBlockPublicPolicy());
		assertEquals(AccessConf.getIgnorePublicAcls(),
				Response.getPublicAccessBlockConfiguration().getIgnorePublicAcls());
		assertEquals(AccessConf.getRestrictPublicBuckets(),
				Response.getPublicAccessBlockConfiguration().getRestrictPublicBuckets());

		Client.deletePublicAccessBlock(new DeletePublicAccessBlockRequest().withBucketName(BucketName));

		assertThrows(SdkClientException.class, () ->Client.getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(BucketName)));
	}
}