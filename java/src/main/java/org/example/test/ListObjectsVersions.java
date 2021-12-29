package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.s3tests.ObjectData;
import org.junit.jupiter.api.DisplayName;
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
	@DisplayName("test_bucket_list_return_data_versioning")
	@Tag("Metadata")
	@Tag("KSAN")
	//@Tag("Version정보를 가질 수 있는 버킷에서 ListObjectsVersions로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인")
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
