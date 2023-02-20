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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

import org.example.Data.FormFile;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.auth.AWS2SignerBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.TagSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Taggings extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("Taggings Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Taggings End");
	}

	@Test
	@Tag("Check")
	// 버킷에 사용자 추가 태그값을 설정할경우 성공확인
	public void test_set_tagging() {
		var bucketName = getNewBucket();
		var client = getClient();

		var TagSets = new ArrayList<TagSet>();
		var Tag1 = new TagSet();
		Tag1.setTag("Hello", "World");
		TagSets.add(Tag1);
		var Tags = new BucketTaggingConfiguration();
		Tags.setTagSets(TagSets);

		var Response = client.getBucketTaggingConfiguration(bucketName);
		assertNull(Response);

		client.setBucketTaggingConfiguration(bucketName, Tags);

		Response = client.getBucketTaggingConfiguration(bucketName);
		assertEquals(1, Response.getAllTagSets().size());
		BucketTaggingCompare(Tags.getAllTagSets(), Response.getAllTagSets());
		client.deleteBucketTaggingConfiguration(bucketName);

		Response = client.getBucketTaggingConfiguration(bucketName);
		assertNull(Response);

	}

	@Test
	@Tag("Check")
	// 오브젝트에 태그 설정이 올바르게 적용되는지 확인
	public void test_get_obj_tagging() {
		var Key = "testputtags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createSimpleTagSet(2));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet));

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagSet.getTagSet(), Response.getTagSet());
	}

	@Test
	@Tag("Check")
	// 오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인
	public void test_get_obj_head_tagging() {
		var Key = "testputtags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();
		var Count = 2;

		var InputTagSet = new ObjectTagging(createSimpleTagSet(Count));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet));

		var Response = client.getObjectMetadata(bucketName, Key);
		assertEquals(Integer.toString(Count), Response.getRawMetadataValue("x-amz-tagging-count"));
	}

	@Test
	@Tag("Max")
	// 추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)
	public void test_put_max_tags() {
		var Key = "testputmaxtags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createSimpleTagSet(10));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet));

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagSet.getTagSet(), Response.getTagSet());
	}

	@Test
	@Tag("Overflow")
	// 추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인
	public void test_put_excess_tags() {
		var Key = "testputmaxtags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createSimpleTagSet(11));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.BadRequest, ErrorCode);

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		assertEquals(0, Response.getTagSet().size());
	}

	@Test
	@Tag("Max")
	// 태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인
	public void test_put_max_kvsize_tags() {
		var Key = "testputmaxkeysize";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createDetailTagSet(10, 128, 256));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet));

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagSet.getTagSet(), Response.getTagSet());
	}

	@Test
	@Tag("Overflow")
	// 태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인
	public void test_put_excess_key_tags() {
		var Key = "testputexcesskeytags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createDetailTagSet(10, 129, 256));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidTag, ErrorCode);

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		assertEquals(0, Response.getTagSet().size());
	}

	@Test
	@Tag("Overflow")
	// 태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인
	public void test_put_excess_val_tags() {
		var Key = "testputexcesskeytags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createDetailTagSet(10, 128, 259));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidTag, ErrorCode);

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		assertEquals(0, Response.getTagSet().size());
	}

	@Test
	@Tag("Overwrite")
	// 오브젝트의 태그목록을 덮어쓰기 가능한지 확인
	public void test_put_modify_tags() {
		var Key = "testputmodifytags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createSimpleTagSet(2));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet));

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagSet.getTagSet(), Response.getTagSet());

		var InputTagSet2 = new ObjectTagging(createDetailTagSet(1, 128, 256));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet2));

		Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagSet2.getTagSet(), Response.getTagSet());
	}

	@Test
	@Tag("Delete")
	// 오브젝트의 태그를 삭제 가능한지 확인
	public void test_put_delete_tags() {
		var Key = "testputmodifytags";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var InputTagSet = new ObjectTagging(createSimpleTagSet(2));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagSet));

		var Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagSet.getTagSet(), Response.getTagSet());

		client.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, Key));

		Response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		assertEquals(0, Response.getTagSet().size());
	}

	@Test
	@Tag("PutObject")
	// 헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인
	public void test_put_obj_with_tags() {
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "testtagobj1";
		var Data = Utils.randomTextToLong(100);

		var TagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("bar", ""));
		TagSets.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));

		var Headers = new ObjectMetadata();
		Headers.setHeader("x-amz-tagging", "foo=bar&bar");

		client.putObject(bucketName, Key, createBody(Data), Headers);
		var Response = client.getObject(bucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);

		var GetResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(TagSets, GetResponse.getTagSet());
	}

	@Test
	@Tag("Post")
	// 로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인
	public void test_post_object_tags_authenticated_request() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = getNewBucket();
		var client = getClient();
		var ContentType = "text/plain";
		var Key = "foo.txt";

		var TagSets = createSimpleTagSet(2);
		var XmlInputTagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", bucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$tagging");
		starts3.add("");
		Conditions.add(starts3);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(Policy, config.mainUser.secretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put("key", Key);
		Payload.put("AWSAccessKeyId", config.mainUser.accessKey);
		Payload.put("acl", "private");
		Payload.put("signature", Signature);
		Payload.put("policy", Policy);
		Payload.put("tagging", XmlInputTagset);
		Payload.put("x-ignore-foo", "bar");
		Payload.put("Content-Type", ContentType);

		var Result = NetUtils.postUpload(getURL(bucketName), Payload, FileData);
		assertEquals(204, Result.statusCode, Result.getErrorCode());

		var Response = client.getObject(bucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);

		var GetResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(TagSets, GetResponse.getTagSet());
	}
}
