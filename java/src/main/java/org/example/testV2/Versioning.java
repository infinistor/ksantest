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
import java.util.Collections;
import java.util.List;
import java.util.HashMap;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Permission;

public class Versioning extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Versioning V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Versioning V2 End");
	}

	@Test
	@Tag("Check")
	public void testVersioningBucketCreateSuspend() {
		var bucketName = createBucket();

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.SUSPENDED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.SUSPENDED);
	}

	@Test
	@Tag("Object")
	public void testVersioningObjCreateReadRemove() {
		var client = getClient();
		var bucketName = createBucket(client);
		client.putBucketVersioning(
				p -> p.bucket(bucketName).versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
		var key = "obj";
		var numVersions = 5;

		doTestCreateRemoveVersions(client, bucketName, key, numVersions, 0, 0);
		doTestCreateRemoveVersions(client, bucketName, key, numVersions, 4, -1);
	}

	@Test
	@Disabled("JAVA에서는 DeleteObject API를 이용하여 오브젝트를 삭제할 경우 반환값이 없어 삭제된 오브젝트의 버전 정보를 받을 수 없음으로 테스트 불가")
	@Tag("Object")
	public void testVersioningObjCreateReadRemoveHead() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putBucketVersioning(
				p -> p.bucket(bucketName).versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
		var key = "obj";
		var numVersions = 5;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var removedVersionID = versionIds.get(0);
		versionIds.remove(0);
		contents.remove(0);
		numVersions--;

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(removedVersionID));

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(contents.get(contents.size() - 1), body);

		client.deleteObject(d -> d.bucket(bucketName).key(key));

		var deleteMarkerVersionId = getResponse.response().versionId();
		versionIds.add(deleteMarkerVersionId);

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(numVersions, listResponse.versions().size());
		assertEquals(1, listResponse.deleteMarkers().size());

		assertEquals(deleteMarkerVersionId, listResponse.deleteMarkers().get(0).versionId());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjPlainNullVersionRemoval() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var content = "foo data";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId("null"));

		var e = assertThrows(AwsServiceException.class, () -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.versions().size());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjPlainNullVersionOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var content = "foo zzz";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var content2 = "zzz";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content2));
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals(content2, body);

		var versionId = response.response().versionId();
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId));
		response = client.getObject(g -> g.bucket(bucketName).key(key));
		body = getBody(response);
		assertEquals(content, body);

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId("null"));

		var e = assertThrows(AwsServiceException.class, () -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.versions().size());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjPlainNullVersionOverwriteSuspended() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "foo";
		var content = "foo zzz";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.SUSPENDED);

		var content2 = "zzz";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content2));
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals(content2, body);

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(1, listResponse.versions().size());

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId("null"));

		var e = assertThrows(AwsServiceException.class, () -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjSuspendVersions() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var key = "obj";
		var numVersions = 5;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.SUSPENDED);
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);

		overwriteSuspendedVersioningObj(client, bucketName, key, versionIds, contents, "null content 1");
		overwriteSuspendedVersioningObj(client, bucketName, key, versionIds, contents, "null content 2");
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);
		overwriteSuspendedVersioningObj(client, bucketName, key, versionIds, contents, "null content 3");
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		createMultipleVersions(client, bucketName, key, 3, versionIds, contents, true);
		numVersions += 3;

		for (int i = 0; i < numVersions; i++)
			removeObjVersion(client, bucketName, key, versionIds, contents, 0);

		assertEquals(0, versionIds.size());
		assertEquals(0, contents.size());

	}

	@Test
	@Tag("Object")
	public void testVersioningObjCreateVersionsRemoveAll() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "obj";
		var numVersions = 10;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		for (int i = 0; i < numVersions; i++)
			removeObjVersion(client, bucketName, key, versionIds, contents, 0);

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, response.versions().size());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjCreateVersionsRemoveSpecialNames() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var keys = List.of("_", ":", " ");
		var numVersions = 10;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		for (var key : keys) {
			createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

			for (int i = 0; i < numVersions; i++)
				removeObjVersion(client, bucketName, key, versionIds, contents, 0);

			var response = client.listObjectVersions(l -> l.bucket(bucketName));
			assertEquals(0, response.versions().size());
		}
	}

	@Test
	@Tag("Multipart")
	public void testVersioningObjCreateOverwriteMultipart() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "obj";
		var numVersions = 3;
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++)
			contents.add(doTestMultipartUploadContents(bucketName, key, 3));

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		for (var version : response.versions())
			versionIds.add(version.versionId());

		Collections.reverse(versionIds);
		checkObjVersions(client, bucketName, key, versionIds, contents);

		for (int i = 0; i < numVersions; i++)
			removeObjVersion(client, bucketName, key, versionIds, contents, 0);

		response = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, response.versions().size());
	}

	@Test
	@Tag("Check")
	public void testVersioningObjListMarker() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key1 = "obj";
		var key2 = "obj-1";
		var numVersions = 5;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		var versionIds2 = new ArrayList<String>();
		var contents2 = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(p -> p.bucket(bucketName).key(key1).build(), RequestBody.fromString(body));
			var versionId = response.versionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(p -> p.bucket(bucketName).key(key2).build(), RequestBody.fromString(body));
			var versionId = response.versionId();

			contents2.add(body);
			versionIds2.add(versionId);
		}

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = reverseVersions(listResponse.versions());

		int index = 0;
		for (int i = 0; i < 5; i++, index++) {
			var version = versions.get(i);
			assertEquals(version.versionId(), versionIds2.get(i));
			assertEquals(version.key(), key2);
			checkObjContent(client, bucketName, key2, version.versionId(), contents2.get(i));
		}

		for (int i = 0; i < 5; i++, index++) {
			var version = versions.get(index);
			assertEquals(version.versionId(), versionIds.get(i));
			assertEquals(version.key(), key1);
			checkObjContent(client, bucketName, key1, version.versionId(), contents.get(i));
		}
	}

	@Test
	@Tag("Copy")
	public void testVersioningCopyObjVersion() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "obj";
		var numVersions = 3;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		for (int i = 0; i < numVersions; i++) {
			var index = i;
			var newKeyName = String.format("key_%s", index);
			client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(key).destinationBucket(bucketName)
					.destinationKey(newKeyName)
					.sourceVersionId(versionIds.get(index)));
			var getResponse = client.getObject(g -> g.bucket(bucketName).key(newKeyName));
			var content = getBody(getResponse);
			assertEquals(contents.get(index), content);
		}

		var anotherBucketName = createBucket(client);

		for (int i = 0; i < numVersions; i++) {
			var index = i;
			var newKeyName = String.format("key_%s", index);
			client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(key).destinationBucket(anotherBucketName)
					.destinationKey(newKeyName)
					.sourceVersionId(versionIds.get(index)));
			var getResponse = client.getObject(g -> g.bucket(anotherBucketName).key(newKeyName));
			var content = getBody(getResponse);
			assertEquals(contents.get(index), content);
		}

		var newKeyName2 = "newKey";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(key).destinationBucket(anotherBucketName)
				.destinationKey(newKeyName2));

		var response = client.getObject(g -> g.bucket(anotherBucketName).key(newKeyName2));
		var body = getBody(response);
		assertEquals(body, contents.get(contents.size() - 1));
	}

	@Test
	@Tag("Delete")
	public void testVersioningMultiObjectDelete() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "key";
		var numVersions = 2;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = reverseVersions(listResponse.versions());

		for (var version : versions)
			client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(version.versionId()));

		listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.versions().size());

		for (var version : versions)
			client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(version.versionId()));

		listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.versions().size());
	}

	@Test
	@Tag("DeleteMarker")
	public void testVersioningMultiObjectDeleteWithMarker() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "key";
		var numVersions = 2;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		client.deleteObject(d -> d.bucket(bucketName).key(key));
		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = response.versions();
		var deleteMarkers = response.deleteMarkers();

		versionIds.add(deleteMarkers.get(0).versionId());
		assertEquals(3, versionIds.size());
		assertEquals(1, deleteMarkers.size());

		for (var version : versions)
			client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(version.versionId()));

		for (var DeleteMarker : deleteMarkers)
			client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(DeleteMarker.versionId()));

		response = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, response.versions().size());
		assertEquals(0, response.deleteMarkers().size());

		for (var version : versions)
			client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(version.versionId()));

		for (var DeleteMarker : deleteMarkers)
			client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(DeleteMarker.versionId()));

		response = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, response.versions().size());
		assertEquals(0, response.deleteMarkers().size());
	}

	@Test
	@Tag("DeleteMarker")
	public void testVersioningMultiObjectDeleteWithMarkerCreate() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "key";

		client.deleteObject(d -> d.bucket(bucketName).key(key));

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		var deleteMarker = response.deleteMarkers();

		assertEquals(1, deleteMarker.size());
		assertEquals(key, deleteMarker.get(0).key());
	}

	@Test
	@Tag("ACL")
	public void testVersionedObjectAcl() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "xyz";
		var numVersions = 3;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var versionId = versionIds.get(1);

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId(versionId));

		var user = config.mainUser.toOwnerV2();
		assertEquals(user.id(), response.owner().id());

		var getGrants = response.grants();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(
				Grant.builder().grantee(config.mainUser.toGranteeV2()).permission(Permission.FULL_CONTROL).build());
		checkGrants(myGrants, getGrants);
	}

	@Test
	@Tag("ACL")
	public void testVersionedObjectAclNoVersionSpecified() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "xyz";
		var numVersions = 3;
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var acl = createPublicAcl();
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		checkAcl(acl, response);

		client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ));

		response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

		acl = createPublicAcl(Permission.READ);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Check")
	public void testVersionedConcurrentObjectCreateAndRemove() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "my_obj";
		var numVersions = 3;

		var allTasks = new ArrayList<Thread>();

		for (int i = 0; i < 3; i++) {
			var tList = doCreateVersionedObjConcurrent(client, bucketName, key, numVersions);
			allTasks.addAll(tList);

			var tList2 = doClearVersionedBucketConcurrent(client, bucketName);
			allTasks.addAll(tList2);
		}

		for (var mTask : allTasks) {
			try {
				mTask.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}

		var tList3 = doClearVersionedBucketConcurrent(client, bucketName);
		for (var mTask : tList3) {
			try {
				mTask.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, response.versions().size());
	}

	@Test
	@Tag("Check")
	public void testVersioningBucketAtomicUploadReturnVersionId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "bar";

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));
		var versionId = putResponse.versionId();

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = listResponse.versions();
		for (var version : versions)
			assertEquals(versionId, version.versionId());
	}

	@Test
	@Tag("MultiPart")
	public void testVersioningBucketMultipartUploadReturnVersionId() {
		var size = 50 * MainData.MB;

		var client = getClient();
		var bucketName = createBucket(client);
		var key = "bar";
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "baz");

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var uploadData = setupMultipartUpload(client, bucketName, key, size, 0, metadata);

		var compResponse = client.completeMultipartUpload(compRequest -> compRequest.bucket(bucketName).key(key)
				.uploadId(uploadData.uploadId).multipartUpload(p -> p.parts(uploadData.parts)));
		var versionId = compResponse.versionId();

		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = listResponse.versions();
		for (var version : versions)
			assertEquals(versionId, version.versionId());
	}

	@Test
	@Tag("metadata")
	public void testVersioningGetObjectHead() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "foo";
		var versions = new ArrayList<String>();

		for (int i = 1; i <= 5; i++) {
			var response = client.putObject(p -> p.bucket(bucketName).key(key).build(),
					RequestBody.fromString(Utils.randomTextToLong(i)));
			versions.add(response.versionId());
		}

		for (int i = 0; i < 5; i++) {
			var index = i;
			var response = client.headObject(h -> h.bucket(bucketName).key(key).versionId(versions.get(index)));
			assertEquals(index + 1L, response.contentLength());
		}
	}

	@Test
	@Tag("Delete")
	public void testVersioningLatest() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var key = "foo";
		var versions = new ArrayList<String>();

		for (int i = 1; i <= 5; i++) {
			var response = client.putObject(p -> p.bucket(bucketName).key(key).build(),
					RequestBody.fromString(Utils.randomTextToLong(i)));
			versions.add(0, response.versionId());
		}

		while (versions.size() > 1) {
			var deleteVersionId = versions.get(0);
			versions.remove(deleteVersionId);
			client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(deleteVersionId));

			var listVersion = versions.get(0);
			var response = client.headObject(h -> h.bucket(bucketName).key(key));
			assertEquals(listVersion, response.versionId());
		}
	}
}
