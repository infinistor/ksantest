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

public class Ownership extends TestBase {

	@Test
	@Tag("Get")
	public void testGetBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		client.getBucketOwnershipControls(g -> g.bucket(bucketName));
	}

	@Test
	@Tag("Put")
	public void testCreateBucketWithOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		assertEquals(ObjectOwnership.BUCKET_OWNER_ENFORCED,
				response.ownershipControls().rules().get(0).objectOwnership());
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Put")
	public void testChangeBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
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
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
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
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
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
		var bucketName = createBucketCannedAcl(client);
		var key = "testObjectOwnershipDenyChange";

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));

		var publicClient = getPublicClient();
		publicClient.headObject(h -> h.bucket(bucketName).key(key));

		client.putBucketOwnershipControls(p -> p.bucket(bucketName).ownershipControls(c -> c.rules(r -> r
				.objectOwnership(ObjectOwnership.BUCKET_OWNER_ENFORCED))));

		publicClient.headObject(h -> h.bucket(bucketName).key(key));
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Error")
	public void testObjectOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
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
}
