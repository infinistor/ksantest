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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.net.MalformedURLException;
import java.util.Base64;
import java.util.HashMap;

import org.example.Data.FormFile;
import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.auth.AWS2SignerBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.Tagging;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@SuppressWarnings("unchecked")
public class Taggings extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Tagging V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Tagging V2 End");
	}

	@Test
	@Tag("Check")
	public void testSetTagging() {
		var client = getClient();
		var bucketName = createBucket(client);

		var tagConfig = Tagging.builder()
				.tagSet(t -> t.key("Hello").value("World").build())
				.build();

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketTagging(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_TAG_SET, e.awsErrorDetails().errorCode());

		client.putBucketTagging(p -> p.bucket(bucketName).tagging(tagConfig));

		var response = client.getBucketTagging(g -> g.bucket(bucketName));
		assertEquals(1, response.tagSet().size());
		tagCompare(tagConfig.tagSet(), response.tagSet());
		client.deleteBucketTagging(d -> d.bucket(bucketName));

		e = assertThrows(AwsServiceException.class, () -> client.getBucketTagging(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_TAG_SET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testGetObjTagging() {
		var key = "obj";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(2)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());
	}

	@Disabled("Java JDK V2에서는 HeadObject로 태그정보를 확인할 수 없음")
	@Test
	@Tag("Check")
	public void testGetObjHeadTagging() {
		var key = "obj";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);
		var count = 2;

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(count)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.headObject(g -> g.bucket(bucketName).key(key));
		assertEquals(count, response.metadata().get("x-amz-tag-count"));
	}

	@Test
	@Tag("Max")
	public void testPutMaxTags() {
		var key = "obj";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(10)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessTags() {
		var key = "test put max tags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(11)).build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.BAD_REQUEST, e.awsErrorDetails().errorCode());

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(0, response.tagSet().size());
	}

	@Test
	@Tag("Max")
	public void testPutMaxSizeTags() {
		var key = "test put max key size";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeDetailTagSet(10, 128, 256)).build();
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessKeyTags() {
		var key = "test put excess key tags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeDetailTagSet(10, 129, 256)).build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_TAG, e.awsErrorDetails().errorCode());

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(0, response.tagSet().size());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessValTags() {
		var key = "test put excess value tags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeDetailTagSet(10, 128, 259)).build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_TAG, e.awsErrorDetails().errorCode());

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(0, response.tagSet().size());
	}

	@Test
	@Tag("Overwrite")
	public void testPutModifyTags() {
		var key = "test put modify tags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(2)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());

		var inputTagSet2 = Tagging.builder().tagSet(makeDetailTagSet(1, 128, 128)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet2));

		response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet2.tagSet(), response.tagSet());
	}

	@Test
	@Tag("Delete")
	public void testPutDeleteTags() {
		var key = "test delete tags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(2)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());

		client.deleteObjectTagging(d -> d.bucket(bucketName).key(key));

		response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(0, response.tagSet().size());
	}

	@Test
	@Tag("PutObject")
	public void testPutObjWithTags() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "test tag obj1";
		var data = Utils.randomTextToLong(100);

		var tags = Tagging.builder().tagSet(
				t -> t.key("bar").value("").build(),
				t -> t.key("foo").value("bar").build()).build();

		var headers = new HashMap<String, String>();
		headers.put("x-amz-tagging", "foo=bar&bar");

		client.putObject(p -> p.bucket(bucketName).key(key).tagging(tags).metadata(headers).build(),
				RequestBody.fromString(data));
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals(data, body);

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(tags.tagSet(), getResponse.tagSet());
	}

	@Test
	@Tag("Post")
	public void testPostObjectTagsAuthenticatedRequest() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var client = getClient();
		var bucketName = createBucket(client);
		var contentType = "text/plain";
		var key = "foo.txt";

		var tags = makeSimpleTagSet(2);
		var xmlInputTagSet = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		conditions.add(starts1);

		var acl = new JsonObject();
		acl.addProperty("acl", "private");
		conditions.add(acl);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(contentType);
		conditions.add(starts2);

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$tagging");
		starts3.add("");
		conditions.add(starts3);

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("AWSAccessKeyId", config.mainUser.accessKey);
		payload.put("acl", "private");
		payload.put("signature", signature);
		payload.put("policy", policy);
		payload.put("tagging", xmlInputTagSet);
		payload.put("x-ignore-foo", "bar");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(tags, getResponse.tagSet());
	}

	@Test
	@Tag("Check")
	public void testGetObjNonTagging() {
		var key = "obj";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key).tagging(Tagging.builder().build()),
				RequestBody.fromString(""));
	}
}
