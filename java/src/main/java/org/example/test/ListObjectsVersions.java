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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.s3tests.ObjectData;
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
	@Tag("KSAN")
	//Version정보를 가질 수 있는 버킷에서 ListObjectsVersions로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	public void test_bucket_list_return_data_versioning() {
		var BucketName = GetNewBucket();
		CheckConfigureVersioningRetry(BucketName, BucketVersioningConfiguration.ENABLED);
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo" }));
		BucketName = CreateObjects(KeyNames, BucketName);

		var Client = GetClient();
		var Data = new ArrayList<ObjectData>();

		for (var Key : KeyNames) {
			var ObjResponse = Client.getObjectMetadata(BucketName, Key);
			var ACLResponse = Client.getObjectAcl(BucketName, Key);

			Data.add(new ObjectData().withKey(Key).withDisplayName(ACLResponse.getOwner().getDisplayName())
					.withID(ACLResponse.getOwner().getId()).withETag(ObjResponse.getETag())
					.withLastModified(ObjResponse.getLastModified()).withContentLength(ObjResponse.getContentLength())
					.withVersionId(ObjResponse.getVersionId()));
		}

		var Response = Client.listVersions(new ListVersionsRequest().withBucketName(BucketName));
		var ObjList = Response.getVersionSummaries();

		for (var Object : ObjList) {
			var KeyName = Object.getKey();
			var KeyData = GetObjectToKey(KeyName, Data);

			assertNotNull(KeyData);
			assertEquals(KeyData.ETag, Object.getETag());
			assertEquals(KeyData.ContentLength, Object.getSize());
			assertEquals(KeyData.DisplayName, Object.getOwner().getDisplayName());
			assertEquals(KeyData.ID, Object.getOwner().getId());
			assertEquals(KeyData.VersionId, Object.getVersionId());
			assertEquals(KeyData.LastModified, Object.getLastModified());
		}
	}
}
