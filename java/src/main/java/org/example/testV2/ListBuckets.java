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

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;

public class ListBuckets extends TestBase {
	@Test
	@Tag("Get")
	public void testBucketsCreateThenList() {
		var client = getClient();
		var bucketNames = new ArrayList<String>();
		for (int i = 0; i < 5; i++) {
			var bucketName = createBucket(client, 1);
			bucketNames.add(bucketName);
		}

		var response = client.listBuckets();
		var bucketList = getBucketList(response);

		for (var bucketName : bucketNames) {
			if (!bucketList.contains(bucketName))
				fail(String.format("S3 implementation's GET on Service did not return bucket we created: %s",
						bucketName));
		}
	}

	@Test
	@Tag("ERROR")
	public void testListBucketsInvalidAuth() {
		var badAuthClient = getBadAuthClient(null, null);

		var e = assertThrows(AwsServiceException.class, badAuthClient::listBuckets);
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.INVALID_ACCESS_KEY_ID, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testListBucketsBadAuth() {
		var mainAccessKey = config.mainUser.accessKey;
		var badAuthClient = getBadAuthClient(mainAccessKey, null);

		var e = assertThrows(AwsServiceException.class, badAuthClient::listBuckets);
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.SIGNATURE_DOES_NOT_MATCH, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Metadata")
	public void testHeadBucket() {
		var client = getClient();
		var bucketName = createBucket(client, 4);

		var response = client.headBucket(h -> h.bucket(bucketName));
		assertNotNull(response);
	}

	@Test
	@Tag("Prefix")
	public void testListBucketsPrefix() {
		var client = getClient();
		var prefix = "1111-my-test";
		var bucketName = Utils.getNewBucketName(prefix, Utils.toSuiteId("ListBuckets"), 5);
		client.createBucket(b -> b.bucket(bucketName));

		for (int i = 0; i < 5; i++) {
			createBucket(client, 5);
		}

		var response = client.listBuckets(l -> l.prefix(prefix));
		var bucketList = getBucketList(response);
		assertEquals(1, bucketList.size());
		assertEquals(bucketName, bucketList.get(0));
		client.deleteBucket(b -> b.bucket(bucketName));
	}

	@Test
	@Tag("MaxBuckets")
	public void testListBucketsMaxBuckets() {
		var client = getClient();
		for (int i = 0; i < 5; i++) {
			createBucket(client, 6);
		}

		// {prefix}{suite}-{testId}- 로 한정해 병렬 실행 시 다른 테스트 버킷과 섞이지 않게 함
		var prefix = getPrefix() + getSuiteId() + "-6-";

		// 전체 버킷 리스트를 먼저 가져옴
		var fullResponse = client.listBuckets(l -> l.prefix(prefix));
		var fullBucketList = getBucketList(fullResponse);
		fullBucketList.sort(String::compareTo);

		// maxBuckets로 제한해서 가져온 결과가 전체 리스트의 앞부분과 일치하는지 확인
		var response = client.listBuckets(l -> l.prefix(prefix).maxBuckets(2));
		var bucketList = getBucketList(response);
		assertEquals(2, bucketList.size());
		assertEquals(fullBucketList.subList(0, 2), bucketList);
	}

	@Test
	@Tag("ContinuationToken")
	public void testListBucketsContinuationToken() {
		var client = getClient();
		for (int i = 0; i < 5; i++) {
			createBucket(client, 7);
		}

		// {prefix}{suite}-{testId}- 로 한정해 병렬 실행 시 다른 테스트 버킷과 섞이지 않게 함
		var prefix = getPrefix() + getSuiteId() + "-7-";

		// 전체 버킷 리스트를 먼저 가져옴
		var fullResponse = client.listBuckets(l -> l.prefix(prefix));
		var fullBucketList = getBucketList(fullResponse);
		fullBucketList.sort(String::compareTo);

		// 첫 번째 페이지: maxBuckets=2
		var response = client.listBuckets(l -> l.prefix(prefix).maxBuckets(2));
		var bucketList = getBucketList(response);
		assertEquals(2, bucketList.size());
		assertEquals(fullBucketList.subList(0, 2), bucketList);

		// 두 번째 페이지: continuationToken 사용
		var response2 = client
				.listBuckets(l -> l.prefix(prefix).maxBuckets(2).continuationToken(response.continuationToken()));
		var bucketList2 = getBucketList(response2);
		assertEquals(2, bucketList2.size());
		assertEquals(fullBucketList.subList(2, 4), bucketList2);
	}

	/** HEAD 요청은 응답 본문이 없어 에러 코드 대신 상태 코드만 확인한다. */
	private static int httpStatus(Runnable func) {
		var e = assertThrows(AwsServiceException.class, func::run);
		return e.statusCode();
	}

	@Test
	@Tag("Metadata")
	public void testHeadBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(8);
		assertEquals(HttpStatus.SC_NOT_FOUND, httpStatus(() -> client.headBucket(h -> h.bucket(bucketName))));
	}

	@Test
	@Tag("Permission")
	public void testHeadBucketAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 9);
		assertEquals(HttpStatus.SC_FORBIDDEN, httpStatus(() -> altClient.headBucket(h -> h.bucket(bucketName))));
	}

	@Test
	@Tag("Prefix")
	public void testListBucketsPrefixNotMatch() {
		var client = getClient();
		createBucket(client, 11);
		var response = client.listBuckets(l -> l.prefix("zzzz-no-such-bucket-prefix-" + getSuiteId()));
		assertEquals(0, response.buckets().size());
		assertNull(response.continuationToken());
	}

	@Test
	@Tag("ERROR")
	public void testListBucketsMaxBucketsInvalid() {
		var client = getClient();
		// max-buckets 는 1 이상 10000 이하만 허용된다
		var e = assertThrows(AwsServiceException.class, () -> client.listBuckets(l -> l.maxBuckets(10001)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ContinuationToken")
	public void testListBucketsPaging() {
		var client = getClient();
		for (int i = 0; i < 5; i++) {
			createBucket(client, 14);
		}
		var prefix = getPrefix() + getSuiteId() + "-14-";
		var fullBucketList = getBucketList(client.listBuckets(l -> l.prefix(prefix)));
		assertEquals(5, fullBucketList.size());

		// 토큰이 없어질 때까지 끝까지 넘기면 전체 목록과 순서까지 같다
		var pagedBucketList = new ArrayList<String>();
		var pageCount = 0;
		String token = null;
		while (true) {
			var request = ListBucketsRequest.builder().prefix(prefix).maxBuckets(2);
			if (StringUtils.isNotEmpty(token))
				request.continuationToken(token);
			var response = client.listBuckets(request.build());
			var page = getBucketList(response);
			assertTrue(page.size() <= 2);
			pagedBucketList.addAll(page);
			pageCount++;
			token = response.continuationToken();
			if (StringUtils.isEmpty(token))
				break;
		}
		assertEquals(3, pageCount);
		assertEquals(fullBucketList, pagedBucketList);
		var sortedBucketList = new ArrayList<>(pagedBucketList);
		sortedBucketList.sort(String::compareTo);
		assertEquals(sortedBucketList, pagedBucketList);
	}

	@Test
	@Tag("Get")
	public void testListBucketsOwner() {
		var client = getClient();
		var bucketName = createBucket(client, 15);
		var response = client.listBuckets(l -> l.prefix(bucketName));
		assertEquals(1, response.buckets().size());
		assertEquals(bucketName, response.buckets().get(0).name());
		assertNotNull(response.buckets().get(0).creationDate());
		assertEquals(config.mainUser.id, response.owner().id());
	}

	@Test
	@Tag("Permission")
	public void testListBucketsAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 16);
		// 다른 사용자의 목록에는 내 버킷이 나오지 않는다
		var response = altClient.listBuckets(l -> l.prefix(bucketName));
		assertEquals(0, response.buckets().size());
	}
}
