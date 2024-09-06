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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;

public class Versioning extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Versioning Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Versioning End");
	}

	@Test
	@Tag("Check")
	public void testVersioningBucketCreateSuspend() {
		var bucketName = createBucket();
		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
	}

	@Test
	@Tag("Object")
	public void testVersioningObjCreateReadRemove() {
		var client = getClient();
		var bucketName = createBucket(client);
		client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
				new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
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

		client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
				new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
		var key = "obj";
		var numVersions = 5;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var removedVersionID = versionIds.get(0);
		versionIds.remove(0);
		contents.remove(0);
		numVersions--;

		client.deleteVersion(bucketName, key, removedVersionID);

		var getResponse = client.getObject(bucketName, key);
		var body = getBody(getResponse.getObjectContent());
		assertEquals(contents.get(contents.size() - 1), body);

		client.deleteObject(bucketName, key);

		var deleteMarkerVersionId = getResponse.getObjectMetadata().getVersionId();
		versionIds.add(deleteMarkerVersionId);

		var listResponse = client.listVersions(bucketName, "");
		assertEquals(numVersions, getVersions(listResponse.getVersionSummaries()).size());
		assertEquals(1, getDeleteMarkers(listResponse.getVersionSummaries()).size());

		assertEquals(deleteMarkerVersionId, getDeleteMarkers(listResponse.getVersionSummaries()).get(0).getVersionId());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjPlainNullVersionRemoval() {
		var key = "foo";
		var content = "foo data";
		var client = getClient();
		var bucketName = createBucket(client);

		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		client.putObject(bucketName, key, content);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		client.deleteVersion(bucketName, key, "null");

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(404, statusCode);
		assertEquals(MainData.NO_SUCH_KEY, errorCode);

		var listResponse = client.listVersions(bucketName, "");
		assertEquals(0, listResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjPlainNullVersionOverwrite() {
		var key = "foo";
		var content = "foo zzz";
		var client = getClient();
		var bucketName = createBucket(client);

		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		client.putObject(bucketName, key, content);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var content2 = "zzz";
		client.putObject(bucketName, key, content2);
		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals(content2, body);

		var versionId = response.getObjectMetadata().getVersionId();
		client.deleteVersion(bucketName, key, versionId);
		response = client.getObject(bucketName, key);
		body = getBody(response.getObjectContent());
		assertEquals(content, body);

		client.deleteVersion(bucketName, key, "null");

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());

		var listResponse = client.listVersions(bucketName, "");
		assertEquals(0, listResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjPlainNullVersionOverwriteSuspended() {
		var key = "foo";
		var content = "foo zzz";
		var client = getClient();
		var bucketName = createBucket(client);

		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		client.putObject(bucketName, key, content);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);

		var content2 = "zzz";
		client.putObject(bucketName, key, content2);
		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals(content2, body);

		var listResponse = client.listVersions(bucketName, "");
		assertEquals(1, listResponse.getVersionSummaries().size());

		client.deleteVersion(bucketName, key, "null");

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjSuspendVersions() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var key = "obj";
		var numVersions = 5;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);

		overwriteSuspendedVersioningObj(client, bucketName, key, versionIds, contents, "null content 1");
		overwriteSuspendedVersioningObj(client, bucketName, key, versionIds, contents, "null content 2");
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);
		overwriteSuspendedVersioningObj(client, bucketName, key, versionIds, contents, "null content 3");
		deleteSuspendedVersioningObj(client, bucketName, key, versionIds, contents);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

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

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "obj";
		var numVersions = 10;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		for (int i = 0; i < numVersions; i++)
			removeObjVersion(client, bucketName, key, versionIds, contents, 0);

		var response = client.listVersions(bucketName, "");
		assertEquals(0, response.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	public void testVersioningObjCreateVersionsRemoveSpecialNames() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var keys = List.of("_", ":", " ");
		var numVersions = 10;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		for (var key : keys) {
			createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

			for (int i = 0; i < numVersions; i++)
				removeObjVersion(client, bucketName, key, versionIds, contents, 0);

			var response = client.listVersions(bucketName, "");
			assertEquals(0, response.getVersionSummaries().size());
		}
	}

	@Test
	@Tag("Multipart")
	public void testVersioningObjCreateOverwriteMultipart() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "obj";
		var numVersions = 3;
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++)
			contents.add(doTestMultipartUploadContents(bucketName, key, 3));

		var response = client.listVersions(bucketName, "");
		for (var version : response.getVersionSummaries())
			versionIds.add(version.getVersionId());

		Collections.reverse(versionIds);
		checkObjVersions(client, bucketName, key, versionIds, contents);

		for (int i = 0; i < numVersions; i++)
			removeObjVersion(client, bucketName, key, versionIds, contents, 0);

		response = client.listVersions(bucketName, "");
		assertEquals(0, response.getVersionSummaries().size());
	}

	@Test
	@Tag("Check")
	public void testVersioningObjListMarker() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key1 = "obj";
		var key2 = "obj-1";
		var numVersions = 5;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		var versionIds2 = new ArrayList<String>();
		var contents2 = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(bucketName, key1, body);
			var versionId = response.getVersionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(bucketName, key2, body);
			var versionId = response.getVersionId();

			contents2.add(body);
			versionIds2.add(versionId);
		}

		var listResponse = client.listVersions(bucketName, "");
		var versions = getVersions(listResponse.getVersionSummaries());
		Collections.reverse(versions);

		int index = 0;
		for (int i = 0; i < 5; i++, index++) {
			var version = versions.get(i);
			assertEquals(version.getVersionId(), versionIds2.get(i));
			assertEquals(version.getKey(), key2);
			checkObjContent(client, bucketName, key2, version.getVersionId(), contents2.get(i));
		}

		for (int i = 0; i < 5; i++, index++) {
			var version = versions.get(index);
			assertEquals(version.getVersionId(), versionIds.get(i));
			assertEquals(version.getKey(), key1);
			checkObjContent(client, bucketName, key1, version.getVersionId(), contents.get(i));
		}
	}

	@Test
	@Tag("Copy")
	public void testVersioningCopyObjVersion() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "obj";
		var numVersions = 3;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		for (int i = 0; i < numVersions; i++) {
			var newKeyName = String.format("key_%s", i);
			client.copyObject(new CopyObjectRequest(bucketName, key, bucketName, newKeyName)
					.withSourceVersionId(versionIds.get(i)));
			var getResponse = client.getObject(bucketName, newKeyName);
			var content = getBody(getResponse.getObjectContent());
			assertEquals(contents.get(i), content);
		}

		var anotherBucketName = createBucket(client);

		for (int i = 0; i < numVersions; i++) {
			var newKeyName = String.format("key_%s", i);
			client.copyObject(new CopyObjectRequest(bucketName, key, bucketName, newKeyName)
					.withSourceVersionId(versionIds.get(i)));
			var getResponse = client.getObject(bucketName, newKeyName);
			var content = getBody(getResponse.getObjectContent());
			assertEquals(contents.get(i), content);
		}

		var newKeyName2 = "newKey";
		client.copyObject(bucketName, key, anotherBucketName, newKeyName2);

		var response = client.getObject(anotherBucketName, newKeyName2);
		var body = getBody(response.getObjectContent());
		assertEquals(body, contents.get(contents.size() - 1));
	}

	@Test
	@Tag("Delete")
	public void testVersioningMultiObjectDelete() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "key";
		var numVersions = 2;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var listResponse = client.listVersions(bucketName, "");
		var versions = getVersions(listResponse.getVersionSummaries());
		Collections.reverse(versions);

		for (var version : versions)
			client.deleteVersion(bucketName, key, version.getVersionId());

		listResponse = client.listVersions(bucketName, "");
		assertEquals(0, listResponse.getVersionSummaries().size());

		for (var version : versions)
			client.deleteVersion(bucketName, key, version.getVersionId());

		listResponse = client.listVersions(bucketName, "");
		assertEquals(0, listResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("DeleteMarker")
	public void testVersioningMultiObjectDeleteWithMarker() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "key";
		var numVersions = 2;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		client.deleteObject(bucketName, key);
		var response = client.listVersions(bucketName, "");
		var versions = getVersions(response.getVersionSummaries());
		var deleteMarkers = getDeleteMarkers(response.getVersionSummaries());

		versionIds.add(deleteMarkers.get(0).getVersionId());
		assertEquals(3, versionIds.size());
		assertEquals(1, deleteMarkers.size());

		for (var version : versions)
			client.deleteVersion(bucketName, key, version.getVersionId());

		for (var DeleteMarker : deleteMarkers)
			client.deleteVersion(bucketName, key, DeleteMarker.getVersionId());

		response = client.listVersions(bucketName, "");
		assertEquals(0, getVersions(response.getVersionSummaries()).size());
		assertEquals(0, getDeleteMarkers(response.getVersionSummaries()).size());

		for (var version : versions)
			client.deleteVersion(bucketName, key, version.getVersionId());

		for (var DeleteMarker : deleteMarkers)
			client.deleteVersion(bucketName, key, DeleteMarker.getVersionId());

		response = client.listVersions(bucketName, "");
		assertEquals(0, getVersions(response.getVersionSummaries()).size());
		assertEquals(0, getDeleteMarkers(response.getVersionSummaries()).size());
	}

	@Test
	@Tag("DeleteMarker")
	public void testVersioningMultiObjectDeleteWithMarkerCreate() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "key";

		client.deleteObject(bucketName, key);

		var response = client.listVersions(bucketName, "");
		var deleteMarker = getDeleteMarkers(response.getVersionSummaries());

		assertEquals(1, deleteMarker.size());
		assertEquals(key, deleteMarker.get(0).getKey());
	}

	@Test
	@Tag("ACL")
	public void testVersionedObjectAcl() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "xyz";
		var numVersions = 3;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var versionId = versionIds.get(1);

		var response = client.getObjectAcl(bucketName, key, versionId);

		var user = new CanonicalGrantee(config.mainUser.id);
		user.setDisplayName(config.mainUser.displayName);

		assertEquals(user.getIdentifier(), response.getOwner().getId());

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<>(getGrants));
	}

	@Test
	@Tag("ACL")
	public void testVersionedObjectAclNoVersionSpecified() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "xyz";
		var numVersions = 3;
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var acl = createPublicAcl();
		var response = client.getObjectAcl(bucketName, key);

		checkAcl(acl, response);

		client.setObjectAcl(bucketName, key, CannedAccessControlList.PublicRead);

		response = client.getObjectAcl(bucketName, key);
		
		acl = createPublicAcl(Permission.Read);
		checkAcl(acl, response);
	}

	@Test
	@Tag("Check")
	public void testVersionedConcurrentObjectCreateAndRemove() {
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

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

		var response = client.listVersions(bucketName, "");
		assertEquals(0, response.getVersionSummaries().size());
	}

	@Test
	@Tag("Check")
	public void testVersioningBucketAtomicUploadReturnVersionId() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "bar";

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var putResponse = client.putObject(bucketName, key, "");
		var versionId = putResponse.getVersionId();

		var listResponse = client.listVersions(bucketName, "");
		var versions = getVersions(listResponse.getVersionSummaries());
		for (var version : versions)
			assertEquals(versionId, version.getVersionId());

		bucketName = createBucket(client);
		key = "baz";
		putResponse = client.putObject(bucketName, key, "");
		assertNull(putResponse.getVersionId());

		bucketName = createBucket(client);
		key = "baz";
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
		putResponse = client.putObject(bucketName, key, "");
		assertNull(putResponse.getVersionId());
	}

	@Test
	@Tag("MultiPart")
	public void testVersioningBucketMultipartUploadReturnVersionId() {
		var contentType = "text/bla";
		var size = 50 * MainData.MB;

		var client = getClient();
		var bucketName = createBucket(client);
		var key = "bar";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("foo", "baz");
		metadata.setContentType(contentType);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);

		var compResponse = client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		var versionId = compResponse.getVersionId();

		var listResponse = client.listVersions(bucketName, "");
		var versions = getVersions(listResponse.getVersionSummaries());
		for (var version : versions)
			assertEquals(versionId, version.getVersionId());

		bucketName = createBucket(client);
		key = "baz";

		uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);
		compResponse = client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		assertNull(compResponse.getVersionId());

		bucketName = createBucket(client);
		key = "foo";

		uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
		compResponse = client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		assertNull(compResponse.getVersionId());
	}

	@Test
	@Tag("metadata")
	public void testVersioningGetObjectHead() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "foo";
		var versions = new ArrayList<String>();

		for (int i = 1; i <= 5; i++) {
			var response = client.putObject(bucketName, key, Utils.randomTextToLong(i));
			versions.add(response.getVersionId());
		}

		for (int i = 0; i < 5; i++) {
			var response = client
					.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key, versions.get(i)));
			assertEquals(i + 1L, response.getContentLength());
		}
	}

	@Test
	@Tag("Delete")
	public void testVersioningLatest() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "foo";
		var versions = new ArrayList<String>();

		for (int i = 1; i <= 5; i++) {
			var response = client.putObject(bucketName, key, Utils.randomTextToLong(i));
			versions.add(0, response.getVersionId());
		}

		while (versions.size() > 1) {
			var deleteVersionId = versions.get(0);
			versions.remove(deleteVersionId);
			client.deleteVersion(bucketName, key, deleteVersionId);

			var listVersion = versions.get(0);
			var response = client.getObjectMetadata(bucketName, key);
			assertEquals(listVersion, response.getVersionId());
		}
	}
}
