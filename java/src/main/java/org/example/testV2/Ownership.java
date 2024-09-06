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
	// 버킷의 오너십 조회 확인
	public void testGetBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		assertEquals(ObjectOwnership.BUCKET_OWNER_ENFORCED,
				response.ownershipControls().rules().get(0).objectOwnership());
	}

	@Test
	@Tag("Put")
	// 버킷을 생성할때 오너십 설정 확인
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
	// 버킷의 오너십 변경 확인
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
	// 오너십 설정된 버킷에서 버킷 ACL 설정이 실패하는지 확인
	public void testBucketOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var response = client.getBucketOwnershipControls(g -> g.bucket(bucketName));
		assertEquals(ObjectOwnership.BUCKET_OWNER_ENFORCED,
				response.ownershipControls().rules().get(0).objectOwnership());

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ)));
		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 오너십 설정된 버킷에서 오브젝트 ACL 설정이 실패하는지 확인
	public void testBucketOwnershipDenyObjectACL() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		var key = "testBucketOwnershipDenyObjectACL";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ)));
		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Check")
	// ACL 설정된 오브젝트에 오너십을 변경해도 접근 가능한지 확인
	public void testObjectOwnershipDenyChange() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
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
	// ACL 설정된 오브젝트에 오너십을 변경할경우 ACL 설정이 실패하는지 확인
	public void testObjectOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectOwnershipDenyACL";

		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(key));

		client.putBucketOwnershipControls(p -> p.bucket(bucketName).ownershipControls(c -> c.rules(r -> r
				.objectOwnership(ObjectOwnership.BUCKET_OWNER_ENFORCED))));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PRIVATE)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	// OwnershipPreferred 설정한 오너십이 정상적으로 적용되는지 확인
	public void testObjectOwnershipPreferred() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_PREFERRED);
		var key = "testObjectOwnershipPreferred";

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE));

		var altClient = getAltClient();
		altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		assertEquals(config.altUser.id, response.owner().id());
		assertEquals(config.altUser.id, response.grants().get(0).grantee().id());
	}

	@Test
	@Tag("Check")
	// OwnershipPreferred 설정한 오너십에 bucket-owner-full-control 설정이 정상적으로 적용되는지 확인
	public void testObjectOwnershipPreferredWithBucketOwnerFullControl() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_PREFERRED);
		var key = "testObjectOwnershipPreferredWithBucketOwnerFullControl";

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE));

		var altClient = getAltClient();
		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL),
				RequestBody.fromString(key));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		assertEquals(1, response.grants().size());
		assertEquals(config.mainUser.id, response.owner().id());
		assertEquals(config.mainUser.id, response.grants().get(0).grantee().id());
	}

	@Test
	@Tag("Check")
	// OwnershipPreferred 설정한 오너십에 bucket-owner-read 설정이 정상적으로 적용되는지 확인
	public void testObjectOwnershipPreferredWithBucketOwnerRead() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BUCKET_OWNER_PREFERRED);
		var key = "testObjectOwnershipPreferredWithBucketOwnerRead";

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE));

		var altClient = getAltClient();
		altClient.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.BUCKET_OWNER_READ),
				RequestBody.fromString(key));

		var response = altClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		assertEquals(config.altUser.id, response.owner().id());
		assertEquals(config.mainUser.id, response.grants().get(0).grantee().id());
	}
}
