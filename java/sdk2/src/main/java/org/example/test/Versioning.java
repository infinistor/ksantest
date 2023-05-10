/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
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
import java.util.Arrays;
import java.util.Collections;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;

public class Versioning extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("Versioning Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Versioning End");
	}

	@Test
	@Tag("Check")
	// 버킷의 버저닝 옵션 변경 가능 확인
	public void test_versioning_bucket_create_suspend() {
		var bucketName = getNewBucket();
		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
	}

	@Test
	@Tag("Object")
	// 버저닝 오브젝트의 생성/읽기/삭제 확인
	public void test_versioning_obj_create_read_remove() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	// 버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인
	public void test_versioning_obj_create_read_remove_head() {
		var bucketName = getNewBucket();

		var client = getClient();
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
		assertEquals(numVersions, GetVersions(listResponse.getVersionSummaries()).size());
		assertEquals(1, GetDeleteMarkers(listResponse.getVersionSummaries()).size());

		assertEquals(deleteMarkerVersionId, GetDeleteMarkers(listResponse.getVersionSummaries()).get(0).getVersionId());
	}

	@Test
	@Tag("Object")
	// 버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인
	public void test_versioning_obj_plain_null_version_removal() {
		var bucketName = getNewBucket();
		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		var client = getClient();
		var key = "foo";
		var content = "foo data";
		client.putObject(bucketName, key, content);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		client.deleteVersion(bucketName, key, "null");

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(404, statusCode);
		assertEquals(MainData.NoSuchKey, errorCode);

		var listResponse = client.listVersions(bucketName, "");
		assertEquals(0, listResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	// [버킷에 버저닝 설정이 되어있는 상태] null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인
	public void test_versioning_obj_plain_null_version_overwrite() {
		var bucketName = getNewBucket();
		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		var client = getClient();
		var key = "foo";
		var content = "foo zzz";
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
		assertEquals(MainData.NoSuchKey, e.getErrorCode());

		var listResponse = client.listVersions(bucketName, "");
		assertEquals(0, listResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	// [버킷에 버저닝 설정이 되어있지만 중단된 상태일때] null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인
	public void test_versioning_obj_plain_null_version_overwrite_suspended() {
		var bucketName = getNewBucket();
		checkVersioning(bucketName, BucketVersioningConfiguration.OFF);

		var client = getClient();
		var key = "foo";
		var content = "foo zzz";
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
		assertEquals(MainData.NoSuchKey, e.getErrorCode());
	}

	@Test
	@Tag("Object")
	// 버전관리를 일시중단했을때 올바르게 동작하는지 확인
	public void test_versioning_obj_suspend_versions() {
		var bucketName = getNewBucket();
		var client = getClient();

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
	// 오브젝트하나의 여러버전을 모두 삭제 가능한지 확인
	public void test_versioning_obj_create_versions_remove_all() {
		var bucketName = getNewBucket();
		var client = getClient();

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
	// 이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인
	public void test_versioning_obj_create_versions_remove_special_names() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var Keys = new ArrayList<>(Arrays.asList(new String[] { "_testobj", "_", ":", " " }));
		var numVersions = 10;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		for (var key : Keys) {
			createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

			for (int i = 0; i < numVersions; i++)
				removeObjVersion(client, bucketName, key, versionIds, contents, 0);

			var response = client.listVersions(bucketName, "");
			assertEquals(0, response.getVersionSummaries().size());
		}
	}

	@Test
	@Tag("Multipart")
	// 오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인
	public void test_versioning_obj_create_overwrite_multipart() {
		var bucketName = getNewBucket();
		var client = getClient();

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
	// 오브젝트의 해당 버전 정보가 올바른지 확인
	public void test_versioning_obj_list_marker() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "obj";
		var Key2 = "obj-1";
		var numVersions = 5;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		var versionIds2 = new ArrayList<String>();
		var contents2 = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(bucketName, key, body);
			var versionId = response.getVersionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(bucketName, Key2, body);
			var versionId = response.getVersionId();

			contents2.add(body);
			versionIds2.add(versionId);
		}

		var listResponse = client.listVersions(bucketName, "");
		var versions = GetVersions(listResponse.getVersionSummaries());
		Collections.reverse(versions);

		int index = 0;
		for (int i = 0; i < 5; i++, index++) {
			var version = versions.get(i);
			assertEquals(version.getVersionId(), versionIds2.get(i));
			assertEquals(version.getKey(), Key2);
			checkObjContent(client, bucketName, Key2, version.getVersionId(), contents2.get(i));
		}

		for (int i = 0; i < 5; i++, index++) {
			var version = versions.get(index);
			assertEquals(version.getVersionId(), versionIds.get(i));
			assertEquals(version.getKey(), key);
			checkObjContent(client, bucketName, key, version.getVersionId(), contents.get(i));
		}
	}

	@Test
	@Tag("Copy")
	// 오브젝트의 버전별 복사가 가능한지 화인
	public void test_versioning_copy_obj_version() {
		var bucketName = getNewBucket();
		var client = getClient();

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

		var anotherBucketName = getNewBucket();

		for (int i = 0; i < numVersions; i++) {
			var newKeyName = String.format("key_%s", i);
			client.copyObject(new CopyObjectRequest(bucketName, key, bucketName, newKeyName)
					.withSourceVersionId(versionIds.get(i)));
			var getResponse = client.getObject(bucketName, newKeyName);
			var content = getBody(getResponse.getObjectContent());
			assertEquals(contents.get(i), content);
		}

		var newKeyName2 = "new_key";
		client.copyObject(bucketName, key, anotherBucketName, newKeyName2);

		var response = client.getObject(anotherBucketName, newKeyName2);
		var body = getBody(response.getObjectContent());
		assertEquals(body, contents.get(contents.size() - 1));
	}

	@Test
	@Tag("Delete")
	// 버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인
	public void test_versioning_multi_object_delete() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "key";
		var numVersions = 2;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var listResponse = client.listVersions(bucketName, "");
		var versions = GetVersions(listResponse.getVersionSummaries());
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
	// 버전이 여러개인 오브젝트에 대한 삭제마커가 올바르게 동작하는지 확인
	public void test_versioning_multi_object_delete_with_marker() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "key";
		var numVersions = 2;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		client.deleteObject(bucketName, key);
		var response = client.listVersions(bucketName, "");
		var versions = GetVersions(response.getVersionSummaries());
		var deleteMarkers = GetDeleteMarkers(response.getVersionSummaries());

		versionIds.add(deleteMarkers.get(0).getVersionId());
		assertEquals(3, versionIds.size());
		assertEquals(1, deleteMarkers.size());

		for (var version : versions)
			client.deleteVersion(bucketName, key, version.getVersionId());

		for (var DeleteMarker : deleteMarkers)
			client.deleteVersion(bucketName, key, DeleteMarker.getVersionId());

		response = client.listVersions(bucketName, "");
		assertEquals(0, GetVersions(response.getVersionSummaries()).size());
		assertEquals(0, GetDeleteMarkers(response.getVersionSummaries()).size());

		for (var version : versions)
			client.deleteVersion(bucketName, key, version.getVersionId());

		for (var DeleteMarker : deleteMarkers)
			client.deleteVersion(bucketName, key, DeleteMarker.getVersionId());

		response = client.listVersions(bucketName, "");
		assertEquals(0, GetVersions(response.getVersionSummaries()).size());
		assertEquals(0, GetDeleteMarkers(response.getVersionSummaries()).size());
	}

	@Test
	@Tag("DeleteMarker")
	// 존재하지않는 오브젝트를 삭제할경우 삭제마커가 생성되는지 확인
	public void test_versioning_multi_object_delete_with_marker_create() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "key";

		client.deleteObject(bucketName, key);
		// var DeleteMarkerVersionId = DelResponse.getVersionId();

		var response = client.listVersions(bucketName, "");
		var DeleteMarker = GetDeleteMarkers(response.getVersionSummaries());

		assertEquals(1, DeleteMarker.size());
		// assertEquals(DeleteMarkerVersionId, DeleteMarker[0].getVersionId());
		assertEquals(key, DeleteMarker.get(0).getKey());
	}

	@Test
	@Tag("ACL")
	// 오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인
	public void test_versioned_object_acl() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "xyz";
		var numVersions = 3;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var versionId = versionIds.get(1);

		var response = client.getObjectAcl(bucketName, key, versionId);

		var user = new CanonicalGrantee(config.mainUser.userId);
		user.setDisplayName(config.mainUser.displayName);

		if (!StringUtils.isBlank(config.URL))
			assertEquals(user.getDisplayName(), response.getOwner().getDisplayName());
		assertEquals(user.getIdentifier(), response.getOwner().getId());

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<Grant>(getGrants));
	}

	@Test
	@Tag("ACL")
	// 버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인
	public void test_versioned_object_acl_no_version_specified() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "xyz";
		var numVersions = 3;

		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);

		var getResponse = client.getObject(bucketName, key);
		var versionId = getResponse.getObjectMetadata().getVersionId();

		var response = client.getObjectAcl(bucketName, key, versionId);

		var user = new CanonicalGrantee(config.mainUser.userId);
		user.setDisplayName(config.mainUser.displayName);

		if (!StringUtils.isBlank(config.URL))
			assertEquals(user.getDisplayName(), response.getOwner().getDisplayName());
		assertEquals(user.getIdentifier(), response.getOwner().getId());

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		checkGrants(myGrants, new ArrayList<Grant>(getGrants));

		client.setObjectAcl(bucketName, key, CannedAccessControlList.PublicRead);

		response = client.getObjectAcl(bucketName, key, versionId);
		getGrants = response.getGrantsAsList();

		myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(user, Permission.FullControl));
		myGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		checkGrants(myGrants, new ArrayList<Grant>(getGrants));
	}

	@Test
	@Tag("Check")
	// 오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인
	public void test_versioned_concurrent_object_create_and_remove() {
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		var key = "myobj";
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
			}
		}

		var tList3 = doClearVersionedBucketConcurrent(client, bucketName);
		for (var mTask : tList3) {
			try {
				mTask.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		var response = client.listVersions(bucketName, "");
		assertEquals(0, response.getVersionSummaries().size());
	}

	@Test
	@Tag("Check")
	// 버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인
	public void test_versioning_bucket_atomic_upload_return_version_id() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "bar";

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var putResponse = client.putObject(bucketName, key, "");
		var versionId = putResponse.getVersionId();

		var listResponse = client.listVersions(bucketName, "");
		var versions = GetVersions(listResponse.getVersionSummaries());
		for (var version : versions)
			assertEquals(versionId, version.getVersionId());

		bucketName = getNewBucket();
		key = "baz";
		putResponse = client.putObject(bucketName, key, "");
		assertNull(putResponse.getVersionId());

		bucketName = getNewBucket();
		key = "baz";
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
		putResponse = client.putObject(bucketName, key, "");
		assertNull(putResponse.getVersionId());
	}

	@Test
	@Tag("MultiPart")
	// 버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인
	public void test_versioning_bucket_multipart_upload_return_version_id() {
		var contentType = "text/bla";
		var size = 50 * MainData.MB;

		var bucketName = getNewBucket();
		var client = getClient();
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
		var versions = GetVersions(listResponse.getVersionSummaries());
		for (var version : versions)
			assertEquals(versionId, version.getVersionId());

		bucketName = getNewBucket();
		key = "baz";

		uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);
		compResponse = client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		assertNull(compResponse.getVersionId());

		bucketName = getNewBucket();
		key = "foo";

		uploadData = setupMultipartUpload(client, bucketName, key, size, metadata);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.SUSPENDED);
		compResponse = client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));
		assertNull(compResponse.getVersionId());
	}

	@Test
	@Tag("metadata")
	// 업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인
	public void test_versioning_get_object_head() {
		var bucketName = getNewBucket();
		var client = getClient();
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
			assertEquals(i + 1, response.getContentLength());
		}
	}

	@Test
	@Tag("Delete")
	// 버전이 여러개인 오브젝트의 최신 버전을 삭제 했을때 이전버전이 최신버전으로 변경되는지 확인
	public void test_versioning_latest() {
		var bucketName = getNewBucket();
		var client = getClient();
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
