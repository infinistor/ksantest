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

import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

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

	@Test
	@Tag("DeleteObjects")
	public void testDeleteObjects() {
		var client = getClient();
		var bucketName = createBucket(client);

		var keyCount = 100;
		var keyNames = new ArrayList<String>();

		// 100개의 오브젝트 생성
		for (var i = 0; i < keyCount; i++) {
			var key = String.format("key-%03d", i);

			keyNames.add(key);
			client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		}

		// 100개의 오브젝트가 있는지 확인
		var listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(keyCount, listResponse.contents().size());

		// 모두 삭제
		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(objectList)));
		assertEquals(keyCount, delResponse.deleted().size());

		// 0개의 오브젝트가 있는지 확인
		listResponse = client.listObjects(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());

		// 모두 올바르게 삭제되었는지 확인
		for (var key : keyNames) {
			var e = assertThrows(AwsServiceException.class, () -> client.getObject(g -> g.bucket(bucketName).key(key)));
			assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		}
	}

	@Test
	@Tag("versioning")
	public void testDeleteObjectsWithVersioning() {
		var client = getClient();
		var bucketName = createBucket(client);
		var methodName = "testDeleteObjectsWithVersioning";
		var keyNames = List.of(
				methodName + "-0",
				methodName + "-1",
				methodName + "-2",
				methodName + "-3",
				methodName + "-4");

		// 1. 버킷 생성 및 버저닝 설정
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 2. 오브젝트 5개 업로드 * 2개 버전
		for (var key : keyNames) {
			createMultipleVersions(client, bucketName, key, 2, false);
		}

		// 2-1. 각 오브젝트의 nonCurrent version(첫 번째 버전) 수집
		var initialVersResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		var initialVersions = initialVersResponse.versions();
		var nonCurrentVersions = new ArrayList<ObjectIdentifier>();

		// 각 key별로 가장 오래된 버전(nonCurrent) 찾기
		for (var key : keyNames) {
			var keyVersions = new ArrayList<software.amazon.awssdk.services.s3.model.ObjectVersion>();
			for (var version : initialVersions) {
				if (version.key().equals(key)) {
					keyVersions.add(version);
				}
			}
			// 가장 오래된 버전(첫 번째 버전) 추가
			if (!keyVersions.isEmpty()) {
				var oldestVersion = keyVersions.get(keyVersions.size() - 1); // 가장 오래된 버전
				nonCurrentVersions.add(ObjectIdentifier.builder()
						.key(oldestVersion.key())
						.versionId(oldestVersion.versionId())
						.build());
			}
		}

		// 3. 버전 정보를 추가하지 않고 오브젝트 삭제 + nonCurrent version 삭제 (섞여있는 경우)
		var objectList = getKeyVersions(keyNames);
		var mixedDeleteList = new ArrayList<ObjectIdentifier>();
		mixedDeleteList.addAll(objectList); // 버전 정보 없는 삭제 (deleteMarker 생성)
		mixedDeleteList.addAll(nonCurrentVersions); // 버전 정보 포함 삭제 (nonCurrent version 삭제)

		var delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(mixedDeleteList)));
		assertEquals(keyNames.size() + nonCurrentVersions.size(), delResponse.deleted().size());

		// 4. deleteMarker 갯수가 5개, version object 갯수가 5개인지 확인
		var versResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		var deleteMarkers = versResponse.deleteMarkers();
		var remainingVersions = versResponse.versions();
		assertEquals(5, deleteMarkers.size());
		assertEquals(5, remainingVersions.size()); // 10개에서 5개 nonCurrent 삭제되어 5개 남음

		// 5. deleteMarker와 버전정보를 포함한 오브젝트 삭제(DeleteObjects)
		var finalVersions = versResponse.versions();
		var finalDeleteMarkers = versResponse.deleteMarkers();
		var deleteList = new ArrayList<ObjectIdentifier>();

		// 모든 버전을 삭제 리스트에 추가
		for (var version : finalVersions) {
			deleteList.add(ObjectIdentifier.builder()
					.key(version.key())
					.versionId(version.versionId())
					.build());
		}

		// 모든 deleteMarker를 삭제 리스트에 추가
		for (var deleteMarker : finalDeleteMarkers) {
			deleteList.add(ObjectIdentifier.builder()
					.key(deleteMarker.key())
					.versionId(deleteMarker.versionId())
					.build());
		}

		delResponse = client.deleteObjects(d -> d.bucket(bucketName).delete(o -> o.objects(deleteList)));
		assertEquals(finalVersions.size() + finalDeleteMarkers.size(), delResponse.deleted().size());

		// 6. listVersions로 완전히 삭제됨을 확인
		versResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, versResponse.versions().size());
		assertEquals(0, versResponse.deleteMarkers().size());
	}
}
