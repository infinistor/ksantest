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

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;

public class DeleteObjects extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("DeleteObjects Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("DeleteObjects End");
	}

	@Test
	@Tag("ListObject")
	//버킷에 존재하는 오브젝트 여러개를 한번에 삭제
	public void testMultiObjectDelete()
	{
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "key0", "key1", "key2" }));
		var bucketName = createObjects(KeyNames);
		var client = getClient();

		var ListResponse = client.listObjects(bucketName);
		assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

		var ObjectList = getKeyVersions(KeyNames);
		var DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList));

		assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

		ListResponse = client.listObjects(bucketName);
		assertEquals(0, ListResponse.getObjectSummaries().size());

		DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList));
		assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

		ListResponse = client.listObjects(bucketName);
		assertEquals(0, ListResponse.getObjectSummaries().size());
	}

	@Test
	@Tag("ListObjectsV2")
	//버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)
	public void testMultiObjectv2Delete()
	{
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "key0", "key1", "key2" }));
		var bucketName = createObjects(KeyNames);
		var client = getClient();

		var ListResponse = client.listObjectsV2(bucketName);
		assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

		var ObjectList = getKeyVersions(KeyNames);
		var DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList));

		assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

		ListResponse = client.listObjectsV2(bucketName);
		assertEquals(0, ListResponse.getObjectSummaries().size());

		DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList));
		assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

		ListResponse = client.listObjectsV2(bucketName);
		assertEquals(0, ListResponse.getObjectSummaries().size());
	}

	@Test
	@Tag("Versioning")
	// 버킷에 존재하는 버저닝 오브젝트 여러개를 한번에 삭제
	public void testMultiObjectDeleteVersions()
	{
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "key0", "key1", "key2" }));
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		for (var Key : KeyNames)
			createMultipleVersions(client, bucketName, Key, 3, false);

		var ListResponse = client.listObjectsV2(bucketName);
		assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

		var ObjectList = getKeyVersions(KeyNames);
		var DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList));

		assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

		ListResponse = client.listObjectsV2(bucketName);
		assertEquals(0, ListResponse.getObjectSummaries().size());

		DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList));
		assertEquals(KeyNames.size(), DelResponse.getDeletedObjects().size());

		ListResponse = client.listObjectsV2(bucketName);
		assertEquals(0, ListResponse.getObjectSummaries().size());
	}

	@Test
	@Tag("quiet")
	//quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제
	public void testMultiObjectDeleteQuiet()
	{
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "key0", "key1", "key2" }));
		var bucketName = createObjects(KeyNames);
		var client = getClient();

		var ListResponse = client.listObjects(bucketName);
		assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

		var ObjectList = getKeyVersions(KeyNames);
		var DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList).withQuiet(true));

		assertEquals(0, DelResponse.getDeletedObjects().size());

		ListResponse = client.listObjects(bucketName);
		assertEquals(0, ListResponse.getObjectSummaries().size());
	}

	@Test
	@Tag("Directory")
	//업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
	public void testDirectoryDelete()
	{
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "a/b/", "a/b/c/d/obj1", "a/b/c/d/obj2", "1/2/", "1/2/3/4/obj1", "q/w/e/r/obj" }));
		var bucketName = createObjectsToBody(KeyNames, "");
		var client = getClient();

		var ListResponse = client.listObjects(bucketName);
		assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());

		client.deleteObject(bucketName, "a/b/");
		client.deleteObject(bucketName, "1/2/");
		client.deleteObject(bucketName, "q/w/");

		ListResponse = client.listObjects(bucketName);
		assertEquals(4, ListResponse.getObjectSummaries().size());

		client.deleteObject(bucketName, "a/b/");
		client.deleteObject(bucketName, "1/2/");
		client.deleteObject(bucketName, "q/w/");

		ListResponse = client.listObjects(bucketName);
		assertEquals(4, ListResponse.getObjectSummaries().size());
	}

	@Test
	@Tag("versioning")
	//버저닝 된 버킷에 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
	public void testDirectoryDeleteVersions()
	{
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "a/", "a/obj1", "a/obj2", "b/", "b/obj1" }));
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		for (var Key : KeyNames)
			createMultipleVersion(client, bucketName, Key, 3, false, "");

		var ListResponse = client.listObjects(bucketName);
		assertEquals(KeyNames.size(), ListResponse.getObjectSummaries().size());
		
		var VersResponse = client.listVersions(bucketName, "");
		assertEquals(15, VersResponse.getVersionSummaries().size());

		client.deleteObject(bucketName, "a/");

		ListResponse = client.listObjects(bucketName);
		assertEquals(4, ListResponse.getObjectSummaries().size());

		VersResponse = client.listVersions(bucketName, "");
		assertEquals(16, VersResponse.getVersionSummaries().size());

		var DeleteList = new ArrayList<>(Arrays.asList(new String[] {"a/obj1", "a/obj2" }));
		var ObjectList = getKeyVersions(DeleteList);

		var DelResponse = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(ObjectList));
		assertEquals(2, DelResponse.getDeletedObjects().size());
		
		VersResponse = client.listVersions(bucketName, "");
		assertEquals(18, VersResponse.getVersionSummaries().size());
	}
}
