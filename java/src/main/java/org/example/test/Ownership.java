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

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;
import com.amazonaws.services.s3.model.ownership.OwnershipControls;
import com.amazonaws.services.s3.model.ownership.OwnershipControlsRule;

public class Ownership extends TestBase {

	@Test
	@Tag("Get")
	public void testGetBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BucketOwnerEnforced);

		client.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
	}

	@Test
	@Tag("Put")
	public void testCreateBucketWithOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BucketOwnerEnforced);

		var response = client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
		assertEquals(ObjectOwnership.BucketOwnerEnforced.toString(),
				response.getOwnershipControls().getRules().get(0).getOwnership());
	}

	@Test
	@Tag("Put")
	public void testChangeBucketOwnership() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BucketOwnerEnforced);

		var response = client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
		assertEquals(ObjectOwnership.BucketOwnerEnforced.toString(),
				response.getOwnershipControls().getRules().get(0).getOwnership());

		client.setBucketOwnershipControls(bucketName, new OwnershipControls()
				.withRules(List.of(new OwnershipControlsRule().withOwnership(ObjectOwnership.ObjectWriter))));

		response = client
				.getBucketOwnershipControls(new GetBucketOwnershipControlsRequest().withBucketName(bucketName));
		assertEquals(ObjectOwnership.ObjectWriter.toString(),
				response.getOwnershipControls().getRules().get(0).getOwnership());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BucketOwnerEnforced);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testBucketOwnershipDenyObjectACL() {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.BucketOwnerEnforced);
		var key = "testBucketOwnershipDenyObjectACL";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata()));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectAcl(bucketName, key, CannedAccessControlList.AuthenticatedRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("Check")
	public void testObjectOwnershipDenyChange() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectOwnershipDenyChange";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var publicClient = getPublicClient();
		publicClient.getObjectMetadata(bucketName, key);

		client.setBucketOwnershipControls(bucketName, new OwnershipControls()
				.withRules(List.of(new OwnershipControlsRule().withOwnership(ObjectOwnership.BucketOwnerEnforced))));

		publicClient.getObjectMetadata(bucketName, key);
	}

	@Test
	@Tag("Error")
	public void testObjectOwnershipDenyACL() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "testObjectOwnershipDenyACL";

		client.putObject(new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));

		var publicClient = getPublicClient();
		publicClient.getObjectMetadata(bucketName, key);

		client.setBucketOwnershipControls(bucketName, new OwnershipControls()
				.withRules(List.of(new OwnershipControlsRule().withOwnership(ObjectOwnership.BucketOwnerEnforced))));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectAcl(bucketName, key, CannedAccessControlList.Private));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED, e.getErrorCode());
	}
}
