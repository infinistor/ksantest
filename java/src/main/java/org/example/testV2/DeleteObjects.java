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
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;


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
		var keyNames = List.of("key0", "key1", "key2");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).build()).build());

		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(0, listResponse.contents().size());

		delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).build()).build());
		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("ListObjectsV2")
	//버킷에 존재하는 오브젝트 여러개를 한번에 삭제(ListObjectsV2)
	public void testMultiObjectv2Delete()
	{
		var keyNames = List.of( "key0", "key1", "key2");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var listResponse = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).build()).build());

		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
		assertEquals(0, listResponse.contents().size());

		delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).build()).build());
		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("Versioning")
	// 버킷에 존재하는 버저닝 오브젝트 여러개를 한번에 삭제
	public void testMultiObjectDeleteVersions()
	{
		var keyNames = List.of("key0", "key1", "key2");
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		for (var Key : keyNames)
			createMultipleVersions(client, bucketName, Key, 3, false);

		var listResponse = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).build()).build());

		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
		assertEquals(0, listResponse.contents().size());

		delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).build()).build());
		assertEquals(keyNames.size(), delResponse.deleted().size());

		listResponse = client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("quiet")
	//quiet옵션을 설정한 상태에서 버킷에 존재하는 오브젝트 여러개를 한번에 삭제
	public void testMultiObjectDeleteQuiet()
	{
		var keyNames = List.of("key0", "key1", "key2");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(keyNames.size(), listResponse.contents().size());

		var objectList = getKeyVersions(keyNames);
		var delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).quiet(true).build()).build());

		assertEquals(0, delResponse.deleted().size());

		listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(0, listResponse.contents().size());
	}

	@Test
	@Tag("Directory")
	//업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
	public void testDirectoryDelete()
	{
		var keyNames = List.of("a/b/", "a/b/c/d/obj1", "a/b/c/d/obj2", "1/2/", "1/2/3/4/obj1", "q/w/e/r/obj");
		var bucketName = createObjectsToBody(keyNames, "");
		var client = getClient();

		var listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(keyNames.size(), listResponse.contents().size());

		client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key("a/b/").build());
		client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key("1/2/").build());
		client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key("q/w/").build());

		listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(4, listResponse.contents().size());

		client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key("a/b/").build());
		client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key("1/2/").build());
		client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key("q/w/").build());

		listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(4, listResponse.contents().size());
	}

	@Test
	@Tag("versioning")
	//버저닝 된 버킷에 업로드한 디렉토리를 삭제해도 해당 디렉토리에 오브젝트가 보이는지 확인
	public void testDirectoryDeleteVersions()
	{
		var keyNames = List.of("a/", "a/obj1", "a/obj2", "b/", "b/obj1");
		var bucketName = getNewBucket();
		var client = getClient();

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		for (var Key : keyNames)
			createMultipleVersions(client, bucketName, Key, 3, false);

		var listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(keyNames.size(), listResponse.contents().size());
		
		var versResponse = client.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucketName).build());
		assertEquals(15, versResponse.versions().size());

		client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key("a/").build());

		listResponse = client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build());
		assertEquals(4, listResponse.contents().size());

		versResponse = client.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucketName).build());
		assertEquals(16, versResponse.versions().size());

		var deleteList = List.of("a/obj1", "a/obj2");
		var objectList = getKeyVersions(deleteList);

		var delResponse = client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(objectList).build()).build());
		assertEquals(2, delResponse.deleted().size());
		
		versResponse = client.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucketName).build());
		assertEquals(18, versResponse.versions().size());
	}
}
