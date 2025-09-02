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

import java.util.List;
import java.util.Random;

import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Localtest extends TestBase {
	private static final Random random = new Random();

	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Localtest Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Localtest End");
	}

	@Test
	@Tag("Get")
	public void testBucketInfo() {
		// var client = getClient();
		// var bucketName = createBucket(client);
		// var keys = List.of("test1", "test2", "test3", "test4", "test5");
		// var totalSize = 0;

		// // 다양한 오브젝트 업로드
		// for (var key : keys) {
		// 	var size = random.nextInt(1024 * 1024 * 1);
		// 	var body = Utils.randomTextToLong(size);
		// 	client.putObject(bucketName, key, body);
		// 	totalSize += size;
		// }

		// // 버킷 정보 조회
		// var portal = new Portal(client);
		// var bucketInfo = portal.getBucketInfo(bucketName);
		// assertEquals(keys.size(), bucketInfo.fileCount);
		// assertEquals(totalSize, bucketInfo.totalSize);
	}
}
