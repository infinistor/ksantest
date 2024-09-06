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

import java.util.ArrayList;

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

public class ListBuckets extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("ListBuckets V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("ListBuckets V2 End");
	}

	@Test
	@Tag("Get")
	public void testBucketsCreateThenList()
	{
		var client = getClient();
		var bucketNames = new ArrayList<String>();
		for (int i = 0; i < 5; i++)
		{
			var bucketName = createBucket(client);
			bucketNames.add(bucketName);
		}

		var response = client.listBuckets();
		var bucketList = getBucketList(response);

		for (var bucketName : bucketNames)
		{
			if(!bucketList.contains(bucketName))
				fail(String.format("S3 implementation's GET on Service did not return bucket we created: %s", bucketName));
		}
	}

	@Test
	@Tag("ERROR")
	public void testListBucketsInvalidAuth()
	{
		var badAuthClient = getBadAuthClient(null, null);

		var e = assertThrows(AwsServiceException.class, badAuthClient::listBuckets);
		
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
		assertEquals(403, statusCode);
		assertEquals(MainData.INVALID_ACCESS_KEY_ID, errorCode);
	}

	@Test
	@Tag("ERROR")
	public void testListBucketsBadAuth()
	{
		var mainAccessKey = config.mainUser.accessKey;
		var badAuthClient = getBadAuthClient(mainAccessKey, null);

		var e = assertThrows(AwsServiceException.class, badAuthClient::listBuckets);
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
		assertEquals(403, statusCode);
		assertEquals(MainData.SIGNATURE_DOES_NOT_MATCH, errorCode);
	}
	
	@Test
	@Tag("Metadata")
	public void testHeadBucket()
	{
		var client = getClient();
		var bucketName = createBucket(client);
		
		var response = client.headBucket(h -> h.bucket(bucketName));
		assertNotNull(response);
	}
}
