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
import java.util.List;

import org.example.s3tests.MainData;
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

public class Versioning extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Versioning Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Versioning End");
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	// @Tag("버킷의 버저닝 옵션 변경 가능 확인")
	public void test_versioning_bucket_create_suspend() {
		var BucketName = GetNewBucket();
		CheckVersioning(BucketName, BucketVersioningConfiguration.OFF);

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.SUSPENDED);
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.SUSPENDED);
	}

	@Test
	@Tag("Object")
	@Tag("KSAN")
	// @Tag("버저닝 오브젝트의 생성/읽기/삭제 확인")
	public void test_versioning_obj_create_read_remove() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		Client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(BucketName,
				new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
		var Key = "testobj";
		var NumVersions = 5;

		DoTestCreateRemoveVersions(Client, BucketName, Key, NumVersions, 0, 0);
		DoTestCreateRemoveVersions(Client, BucketName, Key, NumVersions, 4, -1);
	}

	@Test
	@Disabled("JAVA에서는 DeleteObject API를 이용하여 오브젝트를 삭제할 경우 반환값이 없어 삭제된 오브젝트의 버전 정보를 받을 수 없음으로 테스트 불가")
	@Tag("Object")
	// @Tag("버저닝 오브젝트의 해더 정보를 사용하여 읽기/쓰기/삭제확인")
	public void test_versioning_obj_create_read_remove_head() {
		var BucketName = GetNewBucket();

		var Client = GetClient();
		Client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(BucketName,
				new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
		var Key = "testobj";
		var NumVersions = 5;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		var RemovedVersionID = VersionIDs.get(0);
		VersionIDs.remove(0);
		Contents.remove(0);
		NumVersions--;

		Client.deleteVersion(BucketName, Key, RemovedVersionID);
		
		var GetResponse = Client.getObject(BucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals(Contents.get(Contents.size() - 1), Body);

		Client.deleteObject(BucketName, Key);

		var DeleteMarkerVersionID = GetResponse.getObjectMetadata().getVersionId();
		VersionIDs.add(DeleteMarkerVersionID);

		var ListResponse = Client.listVersions(BucketName, "");
		assertEquals(NumVersions, GetVersions(ListResponse.getVersionSummaries()).size());
		assertEquals(1, GetDeleteMarkers(ListResponse.getVersionSummaries()).size());
		for (var Item : ListResponse.getVersionSummaries())
		{
			System.out.format("%s, %b\n", Item.getVersionId(), Item.isDeleteMarker());
		}
		System.out.format("\n");
		
		assertEquals(DeleteMarkerVersionID, GetDeleteMarkers(ListResponse.getVersionSummaries()).get(0).getVersionId());
	}

	@Test
	@Tag("Object")
	// @Tag("버킷에 버저닝 설정을 할 경우 소급적용되지 않음을 확인")
	public void test_versioning_obj_plain_null_version_removal() {
		var BucketName = GetNewBucket();
		CheckVersioning(BucketName, BucketVersioningConfiguration.OFF);

		var Client = GetClient();
		var Key = "testobjfoo";
		var Content = "fooz";
		Client.putObject(BucketName, Key, Content);

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		Client.deleteVersion(BucketName, Key, "null");

		var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(BucketName, Key));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchKey, ErrorCode);

		var ListResponse = Client.listVersions(BucketName, "");
		assertEquals(0, ListResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	//@Tag("[버킷에 버저닝 설정이 되어있는 상태] null 버전 오브젝트를 덮어쓰기 할경우 버전 정보가 추가됨을 확인")
	public void test_versioning_obj_plain_null_version_overwrite() {
		var BucketName = GetNewBucket();
		CheckVersioning(BucketName, BucketVersioningConfiguration.OFF);

		var Client = GetClient();
		var Key = "testobjfoo";
		var Content = "fooz";
		Client.putObject(BucketName, Key, Content);

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Content2 = "zzz";
		Client.putObject(BucketName, Key, Content2);
		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Content2, Body);

		var VersionID = Response.getObjectMetadata().getVersionId();
		Client.deleteVersion(BucketName, Key, VersionID);
		Response = Client.getObject(BucketName, Key);
		Body = GetBody(Response.getObjectContent());
		assertEquals(Content, Body);

		Client.deleteVersion(BucketName, Key, "null");

		var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(BucketName, Key));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchKey, ErrorCode);

		var ListResponse = Client.listVersions(BucketName, "");
		assertEquals(0, ListResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	//@Tag("[버킷에 버저닝 설정이 되어있지만 중단된 상태일때] null 버전 오브젝트를 덮어쓰기 할경우 버전정보가 추가되지 않음을 확인")
	public void test_versioning_obj_plain_null_version_overwrite_suspended() {
		var BucketName = GetNewBucket();
		CheckVersioning(BucketName, BucketVersioningConfiguration.OFF);

		var Client = GetClient();
		var Key = "testobjfoo";
		var Content = "fooz";
		Client.putObject(BucketName, Key, Content);

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.SUSPENDED);

		var Content2 = "zzz";
		Client.putObject(BucketName, Key, Content2);
		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Content2, Body);

		var ListResponse = Client.listVersions(BucketName, "");
		assertEquals(1, ListResponse.getVersionSummaries().size());

		Client.deleteVersion(BucketName, Key, "null");

		var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(BucketName, Key));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchKey, ErrorCode);
	}

	@Test
	@Tag("Object")
	//@Tag("버전관리를 일시중단했을때 올바르게 동작하는지 확인")
	public void test_versioning_obj_suspend_versions() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		var Key = "testobj";
		var NumVersions = 5;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.SUSPENDED);
		DeleteSuspendedVersioningObj(Client, BucketName, Key, VersionIDs, Contents);
		DeleteSuspendedVersioningObj(Client, BucketName, Key, VersionIDs, Contents);

		OverwriteSuspendedVersioningObj(Client, BucketName, Key, VersionIDs, Contents, "null content 1");
		OverwriteSuspendedVersioningObj(Client, BucketName, Key, VersionIDs, Contents, "null content 2");
		DeleteSuspendedVersioningObj(Client, BucketName, Key, VersionIDs, Contents);
		OverwriteSuspendedVersioningObj(Client, BucketName, Key, VersionIDs, Contents, "null content 3");
		DeleteSuspendedVersioningObj(Client, BucketName, Key, VersionIDs, Contents);

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		CreateMultipleVersions(Client, BucketName, Key, 3, VersionIDs, Contents, true);
		NumVersions += 3;

		for (int i = 0; i < NumVersions; i++)
			RemoveObjVersion(Client, BucketName, Key, VersionIDs, Contents, 0);

		assertEquals(0, VersionIDs.size());
		assertEquals(0, Contents.size());

	}

	@Test
	@Tag("Object")
	//@Tag("오브젝트하나의 여러버전을 모두 삭제 가능한지 확인")
	public void test_versioning_obj_create_versions_remove_all() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "testobj";
		var NumVersions = 10;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		for (int i = 0; i < NumVersions; i++)
			RemoveObjVersion(Client, BucketName, Key, VersionIDs, Contents, 0);

		var Response = Client.listVersions(BucketName, "");
		assertEquals(0, Response.getVersionSummaries().size());
	}

	@Test
	@Tag("Object")
	//@Tag("이름에 특수문자가 들어간 오브젝트에 대해 버전관리가 올바르게 동작하는지 확인")
	public void test_versioning_obj_create_versions_remove_special_names() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Keys = new ArrayList<>(Arrays.asList(new String[] { "_testobj", "_", ":", " " }));
		var NumVersions = 10;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		for (var Key : Keys) {
			CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

			for (int i = 0; i < NumVersions; i++)
				RemoveObjVersion(Client, BucketName, Key, VersionIDs, Contents, 0);

			var Response = Client.listVersions(BucketName, "");
			assertEquals(0, Response.getVersionSummaries().size());
		}
	}

	public void ListTest(List<String> Temp) {
		if (Temp == null)
			Temp = new ArrayList<String>();
		Temp.add(Integer.toString(Temp.size()));
	}

	@Test
	@Tag("Multipart")
	//// @Tag("오브젝트를 멀티파트 업로드하였을 경우 버전관리가 올바르게 동작하는지 확인")
	public void test_versioning_obj_create_overwrite_multipart() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "testobj";
		var NumVersions = 3;
		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();

		for (int i = 0; i < NumVersions; i++)
			Contents.add(DoTestMultipartUploadContents(BucketName, Key, 3));

		var Response = Client.listVersions(BucketName, "");
		for (var Version : Response.getVersionSummaries())
			VersionIDs.add(Version.getVersionId());

		Collections.reverse(VersionIDs);
		CheckObjVersions(Client, BucketName, Key, VersionIDs, Contents);

		for (int i = 0; i < NumVersions; i++)
			RemoveObjVersion(Client, BucketName, Key, VersionIDs, Contents, 0);

		Response = Client.listVersions(BucketName, "");
		assertEquals(0, Response.getVersionSummaries().size());
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	//// @Tag("오브젝트의 해당 버전 정보가 올바른지 확인")
	public void test_versioning_obj_list_marker() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "testobj";
		var Key2 = "testobj-1";
		var NumVersions = 5;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		var VersionIDs2 = new ArrayList<String>();
		var Contents2 = new ArrayList<String>();

		for (int i = 0; i < NumVersions; i++) {
			var Body = String.format("content-%s", i);
			var Response = Client.putObject(BucketName, Key, Body);
			var VersionID = Response.getVersionId();

			Contents.add(Body);
			VersionIDs.add(VersionID);
		}

		for (int i = 0; i < NumVersions; i++) {
			var Body = String.format("content-%s", i);
			var Response = Client.putObject(BucketName, Key2, Body);
			var VersionID = Response.getVersionId();

			Contents2.add(Body);
			VersionIDs2.add(VersionID);
		}

		var ListResponse = Client.listVersions(BucketName, "");
		var Versions = GetVersions(ListResponse.getVersionSummaries());
		Collections.reverse(Versions);

		int index = 0;
		for (int i = 0; i < 5; i++, index++) {
			var Version = Versions.get(i);
			assertEquals(Version.getVersionId(), VersionIDs2.get(i));
			assertEquals(Version.getKey(), Key2);
			CheckObjContent(Client, BucketName, Key2, Version.getVersionId(), Contents2.get(i));
		}

		for (int i = 0; i < 5; i++, index++) {
			var Version = Versions.get(index);
			assertEquals(Version.getVersionId(), VersionIDs.get(i));
			assertEquals(Version.getKey(), Key);
			CheckObjContent(Client, BucketName, Key, Version.getVersionId(), Contents.get(i));
		}
	}

	@Test
	@Tag("Copy")
	//// @Tag("오브젝트의 버전별 복사가 가능한지 화인")
	public void test_versioning_copy_obj_version() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "testobj";
		var NumVersions = 3;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		for (int i = 0; i < NumVersions; i++) {
			var NewKeyName = String.format("key_%s", i);
			Client.copyObject(new CopyObjectRequest(BucketName, Key, BucketName, NewKeyName)
					.withSourceVersionId(VersionIDs.get(i)));
			var GetResponse = Client.getObject(BucketName, NewKeyName);
			var Content = GetBody(GetResponse.getObjectContent());
			assertEquals(Contents.get(i), Content);
		}

		var AnotherBucketName = GetNewBucket();

		for (int i = 0; i < NumVersions; i++) {
			var NewKeyName = String.format("key_%s", i);
			Client.copyObject(new CopyObjectRequest(BucketName, Key, BucketName, NewKeyName)
					.withSourceVersionId(VersionIDs.get(i)));
			var GetResponse = Client.getObject(BucketName, NewKeyName);
			var Content = GetBody(GetResponse.getObjectContent());
			assertEquals(Contents.get(i), Content);
		}

		var NewKeyName2 = "new_key";
		Client.copyObject(BucketName, Key, AnotherBucketName, NewKeyName2);

		var Response = Client.getObject(AnotherBucketName, NewKeyName2);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Body, Contents.get(Contents.size() - 1));
	}

	@Test
	@Tag("Delete")
	//// @Tag("버전이 여러개인 오브젝트에 대한 삭제가 올바르게 동작하는지 확인")
	public void test_versioning_multi_object_delete() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "key";
		var NumVersions = 2;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		var ListResponse = Client.listVersions(BucketName, "");
		var Versions = GetVersions(ListResponse.getVersionSummaries());
		Collections.reverse(Versions);

		for (var Version : Versions)
			Client.deleteVersion(BucketName, Key, Version.getVersionId());

		ListResponse = Client.listVersions(BucketName, "");
		assertEquals(0, ListResponse.getVersionSummaries().size());

		for (var Version : Versions)
			Client.deleteVersion(BucketName, Key, Version.getVersionId());

		ListResponse = Client.listVersions(BucketName, "");
		assertEquals(0, ListResponse.getVersionSummaries().size());
	}

	@Test
	@Tag("DeleteMarker")
	//// @Tag("버전이 여러개인 오브젝트에 대한 삭제마커가 올바르게 동작하는지 확인")
	public void test_versioning_multi_object_delete_with_marker() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "key";
		var NumVersions = 2;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		Client.deleteObject(BucketName, Key);
		var Response = Client.listVersions(BucketName, "");
		var Versions = GetVersions(Response.getVersionSummaries());
		var DeleteMarkers = GetDeleteMarkers(Response.getVersionSummaries());

		VersionIDs.add(DeleteMarkers.get(0).getVersionId());
		assertEquals(3, VersionIDs.size());
		assertEquals(1, DeleteMarkers.size());

		for (var Version : Versions)
			Client.deleteVersion(BucketName, Key, Version.getVersionId());

		for (var DeleteMarker : DeleteMarkers)
			Client.deleteVersion(BucketName, Key, DeleteMarker.getVersionId());

		Response = Client.listVersions(BucketName, "");
		assertEquals(0, GetVersions(Response.getVersionSummaries()).size());
		assertEquals(0, GetDeleteMarkers(Response.getVersionSummaries()).size());

		for (var Version : Versions)
			Client.deleteVersion(BucketName, Key, Version.getVersionId());

		for (var DeleteMarker : DeleteMarkers)
			Client.deleteVersion(BucketName, Key, DeleteMarker.getVersionId());

		Response = Client.listVersions(BucketName, "");
		assertEquals(0, GetVersions(Response.getVersionSummaries()).size());
		assertEquals(0, GetDeleteMarkers(Response.getVersionSummaries()).size());
	}

	@Test
	@Tag("DeleteMarker")
	//// @Tag("존재하지않는 오브젝트를 삭제할경우 삭제마커가 생성되는지 확인")
	public void test_versioning_multi_object_delete_with_marker_create() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "key";

		Client.deleteObject(BucketName, Key);
		// var DeleteMarkerVersionId = DelResponse.getVersionId();

		var Response = Client.listVersions(BucketName, "");
		var DeleteMarker = GetDeleteMarkers(Response.getVersionSummaries());

		assertEquals(1, DeleteMarker.size());
		// assertEquals(DeleteMarkerVersionId, DeleteMarker[0].getVersionId());
		assertEquals(Key, DeleteMarker.get(0).getKey());
	}

	@Test
	@Tag("ACL")
	//// @Tag("오브젝트 버전의 acl이 올바르게 관리되고 있는지 확인")
	public void test_versioned_object_acl() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "xyz";
		var NumVersions = 3;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		var VersionID = VersionIDs.get(1);

		var Response = Client.getObjectAcl(BucketName, Key, VersionID);

		var User = new CanonicalGrantee(Config.MainUser.UserID);
		User.setDisplayName(Config.MainUser.DisplayName);

		if (!StringUtils.isBlank(Config.URL))
			assertEquals(User.getDisplayName(), Response.getOwner().getDisplayName());
		assertEquals(User.getIdentifier(), Response.getOwner().getId());

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("ACL")
	//@Tag("버전정보를 입력하지 않고 오브젝트의 acl정보를 수정할 경우 가장 최신 버전에 반영되는지 확인")
	public void test_versioned_object_acl_no_version_specified() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "xyz";
		var NumVersions = 3;

		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, NumVersions, VersionIDs, Contents, true);

		var GetResponse = Client.getObject(BucketName, Key);
		var VersionID = GetResponse.getObjectMetadata().getVersionId();

		var Response = Client.getObjectAcl(BucketName, Key, VersionID);

		var User = new CanonicalGrantee(Config.MainUser.UserID);
		User.setDisplayName(Config.MainUser.DisplayName);

		if (!StringUtils.isBlank(Config.URL))
			assertEquals(User.getDisplayName(), Response.getOwner().getDisplayName());
		assertEquals(User.getIdentifier(), Response.getOwner().getId());

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));

		Client.setObjectAcl(BucketName, Key, CannedAccessControlList.PublicRead);

		Response = Client.getObjectAcl(BucketName, Key, VersionID);
		GetGrants = Response.getGrantsAsList();

		MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, Permission.FullControl));
		MyGrants.add(new Grant(GroupGrantee.AllUsers, Permission.Read));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	//// @Tag("오브젝트 버전을 추가/삭제를 여러번 했을 경우 올바르게 동작하는지 확인")
	public void test_versioned_concurrent_object_create_and_remove() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var Key = "myobj";
		var NumVersions = 3;

		var AllTasks = new ArrayList<Thread>();

		for (int i = 0; i < 3; i++) {
			var TList = DoCreateVersionedObjConcurrent(Client, BucketName, Key, NumVersions);
			AllTasks.addAll(TList);

			var TList2 = DoClearVersionedBucketConcurrent(Client, BucketName);
			AllTasks.addAll(TList2);
		}

		for (var mTask : AllTasks)
		{
			try {
				mTask.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		var TList3 = DoClearVersionedBucketConcurrent(Client, BucketName);
		for (var mTask : TList3)
		{
			try {
				mTask.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		var Response = Client.listVersions(BucketName, "");
		assertEquals(0, Response.getVersionSummaries().size());
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	//@Tag("버킷의 버저닝 설정이 업로드시 올바르게 동작하는지 확인")
	public void test_versioning_bucket_atomic_upload_return_version_id() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "bar";

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		var PutResponse = Client.putObject(BucketName, Key, "");
		var VersionID = PutResponse.getVersionId();

		var ListResponse = Client.listVersions(BucketName, "");
		var Versions = GetVersions(ListResponse.getVersionSummaries());
		for (var Version : Versions)
			assertEquals(VersionID, Version.getVersionId());

		BucketName = GetNewBucket();
		Key = "baz";
		PutResponse = Client.putObject(BucketName, Key, "");
		assertNull(PutResponse.getVersionId());

		BucketName = GetNewBucket();
		Key = "baz";
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.SUSPENDED);
		PutResponse = Client.putObject(BucketName, Key, "");
		assertNull(PutResponse.getVersionId());
	}

	@Test
	@Tag("MultiPart")
	@Tag("KSAN")
	//@Tag("버킷의 버저닝 설정이 멀티파트 업로드시 올바르게 동작하는지 확인")
	public void test_versioning_bucket_multipart_upload_return_version_id() {
		var ContentType = "text/bla";
		var Size = 50 * MainData.MB;

		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "bar";
		var Metadata = new ObjectMetadata();
		Metadata.addUserMetadata("foo", "baz");
		Metadata.setContentType(ContentType);

		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, Metadata);

		var CompResponse = Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));
		var VersionID = CompResponse.getVersionId();

		var ListResponse = Client.listVersions(BucketName, "");
		var Versions = GetVersions(ListResponse.getVersionSummaries());
		for (var Version : Versions)
			assertEquals(VersionID, Version.getVersionId());

		BucketName = GetNewBucket();
		Key = "baz";

		UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, Metadata);
		CompResponse = Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));
		assertNull(CompResponse.getVersionId());

		BucketName = GetNewBucket();
		Key = "foo";

		UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, Metadata);
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.SUSPENDED);
		CompResponse = Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));
		assertNull(CompResponse.getVersionId());
	}
	
	@Test
	@Tag("Metadata")
	// @Tag("업로드한 오브젝트의 버전별 헤더 정보가 올바른지 확인")
	public void test_versioning_get_object_head()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);

		var KeyName = "foo";

		for (int i = 1; i <= 5; i++)
			Client.putObject(BucketName, KeyName, RandomTextToLong(i));

		var VersionResponse = Client.listVersions(BucketName, "");
		var VersionList = GetVersionIDs(VersionResponse.getVersionSummaries());
		Collections.sort(VersionList);
		for (int i = 0; i < 5; i++)
		{
			var Response = Client.getObjectMetadata(new GetObjectMetadataRequest(BucketName, KeyName, VersionList.get(i)));
			assertEquals(i + 1, Response.getContentLength());
		}
	}
}
