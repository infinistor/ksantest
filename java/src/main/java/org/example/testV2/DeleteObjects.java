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

import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

public class DeleteObjects extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("DeleteObjects V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("DeleteObjects V2 End");
	}

	@Test
	@Tag("ListObject")
	public void testMultiObjectDelete() {
		var keyNames = List.of("key0", "key1", "key2");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));

		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());

		delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));
		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("ListObjectsV2")
	public void testMultiObjectV2Delete() {
		var keyNames = List.of("key0", "key1", "key2");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));

		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());

		delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));
		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("Versioning")
	public void testMultiObjectDeleteVersions() {
		var keyNames = List.of("key0", "key1", "key2");
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		for (var Key : keyNames)
			createMultipleVersions(client, bucketName, Key, 3, false);

		var listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));

		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());

		delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));
		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("quiet")
	public void testMultiObjectDeleteQuiet() {
		var keyNames = List.of("key0", "key1", "key2");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client
				.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList).quiet(true)));

		assertEquals(0, delResponse.deleted().size());

		listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("Directory")
	public void testDirectoryDelete() {
		var keyNames = List.of("a/b/", "a/b/c/d/obj1", "a/b/c/d/obj2", "1/2/", "1/2/3/4/obj1", "q/w/e/r/obj");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(keyNames.size(), listResponse.contents().size());

		client.deleteObject(d -> d.bucket(bucketName).key("a/b/"));
		client.deleteObject(d -> d.bucket(bucketName).key("1/2/"));
		client.deleteObject(d -> d.bucket(bucketName).key("q/w/"));

		listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(4, listResponse.contents().size());

		client.deleteObject(d -> d.bucket(bucketName).key("a/b/"));
		client.deleteObject(d -> d.bucket(bucketName).key("1/2/"));
		client.deleteObject(d -> d.bucket(bucketName).key("q/w/"));

		listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(4, listResponse.contents().size());
	}

	@Test
	@Tag("versioning")
	public void testDirectoryDeleteVersions() {
		var keyNames = List.of("a/", "a/obj1", "a/obj2", "b/", "b/obj1");
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		for (var Key : keyNames)
			createMultipleVersions(client, bucketName, Key, 3, false);

		var listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(keyNames.size(), listResponse.contents().size());

		var versResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(15, versResponse.versions().size());

		client.deleteObject(d -> d.bucket(bucketName).key("a/"));

		listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(4, listResponse.contents().size());

		versResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(15, versResponse.versions().size());
		assertEquals(1, versResponse.deleteMarkers().size());
		
		var deleteList = List.of("a/obj1", "a/obj2");
		var objectList = getKeyVersions(deleteList);
		
		var delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));
		assertEquals(2, delResponse.deleted().size());
		
		versResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(15, versResponse.versions().size());
		assertEquals(3, versResponse.deleteMarkers().size());

	}
}
