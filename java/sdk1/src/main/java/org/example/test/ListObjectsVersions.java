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
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.Data.ObjectData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.ListVersionsRequest;

public class ListObjectsVersions extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("ListObjectsVersions Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("ListObjectsVersions End");
	}

	@Test
	@Tag("Metadata")
	//Version정보를 가질 수 있는 버킷에서 ListObjectsVersions로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	public void test_bucket_list_return_data_versioning() {
		var bucketName = getNewBucket();
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo" }));
		bucketName = createObjects(KeyNames, bucketName);

		var client = getClient();
		var Data = new ArrayList<ObjectData>();

		for (var Key : KeyNames) {
			var ObjResponse = client.getObjectMetadata(bucketName, Key);
			var ACLResponse = client.getObjectAcl(bucketName, Key);

			Data.add(new ObjectData().withKey(Key).withDisplayName(ACLResponse.getOwner().getDisplayName())
					.withID(ACLResponse.getOwner().getId()).withETag(ObjResponse.getETag())
					.withLastModified(ObjResponse.getLastModified()).withContentLength(ObjResponse.getContentLength())
					.withVersionId(ObjResponse.getVersionId()));
		}

		var Response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
		var ObjList = Response.getVersionSummaries();

		for (var Object : ObjList) {
			var KeyName = Object.getKey();
			var KeyData = GetObjectToKey(KeyName, Data);

			assertNotNull(KeyData);
			assertEquals(KeyData.eTag, Object.getETag());
			assertEquals(KeyData.contentLength, Object.getSize());
			assertEquals(KeyData.displayName, Object.getOwner().getDisplayName());
			assertEquals(KeyData.id, Object.getOwner().getId());
			assertEquals(KeyData.versionId, Object.getVersionId());
			assertEquals(KeyData.lastModified, Object.getLastModified());
		}
	}
	
	@Test
	@Tag("Filtering")
	// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	public void test_versioning_bucket_list_filtering_all() {
		var keyNames = new ArrayList<>(Arrays.asList(new String[] { "test1/f1", "test2/f2", "test3", "test4/f3", "test_f4" }));
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var marker = "test3";
		var Delimiter = "/";
		var MaxKeys = 3;

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(Delimiter).withMaxResults(MaxKeys));
		assertEquals(Delimiter, response.getDelimiter());
		assertEquals(MaxKeys, response.getMaxKeys());
		assertEquals(marker, response.getNextKeyMarker());

		var keys = GetKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "test3" })), keys);
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "test1/", "test2/" })), prefixes);

		response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(Delimiter).withMaxResults(MaxKeys).withKeyMarker(marker));
		assertEquals(Delimiter, response.getDelimiter());
		assertEquals(MaxKeys, response.getMaxKeys());
		assertEquals(false, response.isTruncated());
	}
}
