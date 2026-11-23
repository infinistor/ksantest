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

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.example.Data.FormFile;
import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.TagSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Taggings extends TestBase {
	@Test
	@Tag("Check")
	public void testSetTagging() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var tags = new ArrayList<TagSet>();
		var tag = new TagSet();
		tag.setTag("Hello", "World");
		tags.add(tag);
		var tagConfig = new BucketTaggingConfiguration();
		tagConfig.setTagSets(tags);

		var response = client.getBucketTaggingConfiguration(bucketName);
		assertNull(response);

		client.setBucketTaggingConfiguration(bucketName, tagConfig);

		response = client.getBucketTaggingConfiguration(bucketName);
		assertEquals(1, response.getAllTagSets().size());
		bucketTaggingCompare(tagConfig.getAllTagSets(), response.getAllTagSets());
		client.deleteBucketTaggingConfiguration(bucketName);

		response = client.getBucketTaggingConfiguration(bucketName);
		assertNull(response);
	}

	@Test
	@Tag("Check")
	public void testGetObjTagging() {
		var key = "testGetObjTagging";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 2, key, 0);

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(2));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), response.getTagSet());
	}

	@Test
	@Tag("Check")
	public void testGetObjHeadTagging() {
		var key = "testGetObjHeadTagging";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 3, key, 0);
		var count = 2;

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(count));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(Integer.toString(count), response.getRawMetadataValue("x-amz-tagging-count"));
	}

	@Test
	@Tag("Max")
	public void testPutMaxTags() {
		var key = "testPutMaxTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 4, key, 0);

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(10));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), response.getTagSet());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessTags() {
		var key = "testPutExcessTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 5, key, 0);

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(11));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.BAD_REQUEST, e.getErrorCode());

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		assertEquals(0, response.getTagSet().size());
	}

	@Test
	@Tag("Max")
	public void testPutMaxSizeTags() {
		var key = "testPutMaxSizeTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 6, key, 0);

		var inputTagSet = new ObjectTagging(makeDetailTagSet(10, 128, 256));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), response.getTagSet());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessKeyTags() {
		var key = "testPutExcessKeyTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 7, key, 0);

		var inputTagSet = new ObjectTagging(makeDetailTagSet(10, 129, 256));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_TAG, e.getErrorCode());

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		assertEquals(0, response.getTagSet().size());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessValTags() {
		var key = "testPutExcessValTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 8, key, 0);

		var inputTagSet = new ObjectTagging(makeDetailTagSet(10, 128, 259));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_TAG, e.getErrorCode());

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		assertEquals(0, response.getTagSet().size());
	}

	@Test
	@Tag("Overwrite")
	public void testPutModifyTags() {
		var key = "testPutModifyTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 9, key, 0);

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(2));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), response.getTagSet());

		var inputTagSet2 = new ObjectTagging(makeDetailTagSet(1, 128, 128));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet2));

		response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet2.getTagSet(), response.getTagSet());
	}

	@Test
	@Tag("Delete")
	public void testPutDeleteTags() {
		var key = "testPutDeleteTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 10, key, 0);

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(2));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), response.getTagSet());

		client.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, key));

		response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		assertEquals(0, response.getTagSet().size());
	}

	@Test
	@Tag("PutObject")
	public void testPutObjWithTags() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		var key = "testPutObjWithTags";
		var data = Utils.randomTextToLong(100);

		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		tags.add(new com.amazonaws.services.s3.model.Tag("bar", ""));
		tags.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));

		var headers = new ObjectMetadata();
		headers.setHeader("x-amz-tagging", "foo=bar&bar");

		client.putObject(bucketName, key, createBody(data), headers);
		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals(data, body);

		var getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(tags, getResponse.getTagSet());
	}

	@Test
	@Tag("Post")
	public void testPostObjectTagsAuthenticatedRequest() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		var contentType = "text/plain";
		var key = "testPostObjectTagsAuthenticatedRequest";

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
		starts1.add("testPostObjectTagsAuthenticatedRequest");
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

		var auth = createSigV4Post();
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		payload.put("tagging", xmlInputTagSet);
		payload.put("x-ignore-foo", "bar");
		payload.put("Content-Type", contentType);
		auth.putFormFields(payload, signature);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);

		var getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(tags, getResponse.getTagSet());
	}

	@Test
	@Tag("Check")
	public void testGetObjNonTagging() {
		var key = "testGetObjNonTagging";
		var client = getClient();
		var bucketName = createBucket(client, 13);

		var putObjectRequest = new PutObjectRequest(bucketName, key, createBody("00"), new ObjectMetadata());
		putObjectRequest.setTagging(new ObjectTagging(new ArrayList<>()));
		client.putObject(putObjectRequest);
	}

	private static com.amazonaws.services.s3.model.Tag tag(String key, String value) {
		return new com.amazonaws.services.s3.model.Tag(key, value);
	}

	private static ObjectTagging tagging(com.amazonaws.services.s3.model.Tag... tags) {
		return new ObjectTagging(List.of(tags));
	}

	private static BucketTaggingConfiguration bucketTagging(List<com.amazonaws.services.s3.model.Tag> tags) {
		var tagSet = new TagSet();
		for (var item : tags)
			tagSet.setTag(item.getKey(), item.getValue());
		return new BucketTaggingConfiguration().withTagSets(tagSet);
	}

	private static List<com.amazonaws.services.s3.model.Tag> toTags(BucketTaggingConfiguration config) {
		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		for (var item : config.getTagSet().getAllTags().entrySet())
			tags.add(tag(item.getKey(), item.getValue()));
		return tags;
	}

	private static void assertError(Executable executable, int statusCode, String errorCode) {
		var e = assertThrows(AmazonServiceException.class, executable);
		assertEquals(statusCode, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());
	}

	private static void assertObjectNotExist(AmazonS3 client, String bucketName, String key) {
		var e = assertThrows(AmazonServiceException.class, () -> client.getObjectMetadata(bucketName, key));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
	}

	private static PutObjectRequest putRequest(String bucketName, String key, ObjectTagging tagging) {
		return new PutObjectRequest(bucketName, key, createBody(""), new ObjectMetadata()).withTagging(tagging);
	}

	@Test
	@Tag("Max")
	public void testBucketTaggingMax() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		// 버킷 태그는 50개까지 넣을 수 있다
		var tagSet = makeSimpleTagSet(50);
		client.setBucketTaggingConfiguration(bucketName, bucketTagging(tagSet));
		tagCompare(tagSet, toTags(client.getBucketTaggingConfiguration(bucketName)));

		assertError(() -> client.setBucketTaggingConfiguration(bucketName, bucketTagging(makeSimpleTagSet(51))),
				HttpStatus.SC_BAD_REQUEST, MainData.BAD_REQUEST);
		assertEquals(50, toTags(client.getBucketTaggingConfiguration(bucketName)).size());
	}

	@Test
	@Tag("ERROR")
	public void testBucketTaggingInvalidTag() {
		var client = getClient();
		var bucketName = createBucket(client, 18);
		var invalidTags = List.of(
				tag("aws:key", "value"), // aws: 접두어는 시스템 태그 전용
				tag("k".repeat(129), "value"), // 키 128자 초과
				tag("key", "v".repeat(257)), // 값 256자 초과
				tag("key?<>", "value")); // 허용하지 않는 문자

		for (var invalidTag : invalidTags) {
			assertError(() -> client.setBucketTaggingConfiguration(bucketName, bucketTagging(List.of(invalidTag))),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		}
		// SDK v1 은 NoSuchTagSet(404) 을 null 로 돌려준다
		assertNull(client.getBucketTaggingConfiguration(bucketName));
	}

	@Test
	@Tag("Check")
	public void testBucketTaggingEmptyValue() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var tags = List.of(tag("key", ""));

		client.setBucketTaggingConfiguration(bucketName, bucketTagging(tags));
		tagCompare(tags, toTags(client.getBucketTaggingConfiguration(bucketName)));
	}

	@Test
	@Tag("Delete")
	public void testBucketTaggingEmptySet() {
		var client = getClient();
		var bucketName = createBucket(client, 20);
		client.setBucketTaggingConfiguration(bucketName, bucketTagging(List.of(tag("key", "value"))));

		// 빈 태그 목록을 넣으면 태그가 지워진다
		client.setBucketTaggingConfiguration(bucketName, bucketTagging(List.of()));
		assertNull(client.getBucketTaggingConfiguration(bucketName));

		// 태그가 없어도 삭제는 성공한다
		client.deleteBucketTaggingConfiguration(bucketName);
	}

	@Test
	@Tag("ERROR")
	public void testBucketTaggingBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(21);

		// SDK v1 의 조회는 404 를 null 로 바꾸므로 설정·삭제만 확인한다
		assertError(() -> client.setBucketTaggingConfiguration(bucketName,
				bucketTagging(List.of(tag("key", "value")))), HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.deleteBucketTaggingConfiguration(bucketName), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_BUCKET);
	}

	@Test
	@Tag("ERROR")
	public void testObjTaggingDuplicateKey() {
		var key = "testObjTaggingDuplicateKey";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 22, key, 10);

		assertError(() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key,
				tagging(tag("key", "value1"), tag("key", "value2")))),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		assertEquals(0, client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet().size());
	}

	@Test
	@Tag("ERROR")
	public void testObjTaggingInvalidTag() {
		var key = "testObjTaggingInvalidTag";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 23, key, 10);
		var invalidTags = List.of(
				tag("aws:key", "value"), // aws: 접두어는 시스템 태그 전용
				tag("key?<>", "value")); // 허용하지 않는 문자

		for (var invalidTag : invalidTags) {
			assertError(() -> client.setObjectTagging(
					new SetObjectTaggingRequest(bucketName, key, tagging(invalidTag))),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		}
		assertEquals(0, client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet().size());
	}

	@Test
	@Tag("Check")
	public void testObjTaggingSpecialCharacters() {
		var key = "testObjTaggingSpecialCharacters";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 24, key, 10);

		// 허용 문자(영숫자, 공백, + - = . _ : / @)와 빈 값은 그대로 저장된다
		var tagging = tagging(tag("a+b-=._:/@ c", "v+-=._:/@ d"), tag("empty", ""));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, tagging));
		tagCompare(tagging.getTagSet(),
				client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet());
	}

	@Test
	@Tag("Delete")
	public void testObjTaggingEmptySet() {
		var key = "testObjTaggingEmptySet";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 25, key, 10);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, tagging(tag("key", "value"))));

		// 빈 태그 목록을 넣으면 태그가 지워진다
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, tagging()));
		assertEquals(0, client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet().size());

		// 태그가 없어도 삭제는 성공한다
		client.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, key));
	}

	@Test
	@Tag("ERROR")
	public void testObjTaggingKeyNotExist() {
		var key = "testObjTaggingKeyNotExist";
		var client = getClient();
		var bucketName = createBucket(client, 26);

		assertError(() -> client.setObjectTagging(
				new SetObjectTaggingRequest(bucketName, key, tagging(tag("key", "value")))),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY);
		assertError(() -> client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY);
		assertError(() -> client.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, key)),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY);
	}

	@Test
	@Tag("Overflow")
	public void testPutObjWithExcessTags() {
		var key = "testPutObjWithExcessTags";
		var client = getClient();
		var bucketName = createBucket(client, 27);

		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		for (int i = 0; i < 11; i++)
			tags.add(tag("key" + i, "value" + i));

		assertError(() -> client.putObject(putRequest(bucketName, key, new ObjectTagging(tags))),
				HttpStatus.SC_BAD_REQUEST, MainData.BAD_REQUEST);
		assertObjectNotExist(client, bucketName, key);
	}

	@Test
	@Tag("ERROR")
	public void testPutObjWithInvalidTags() {
		var key = "testPutObjWithInvalidTags";
		var client = getClient();
		var bucketName = createBucket(client, 28);

		// 헤더로 준 태그의 키가 중복되면 InvalidArgument
		assertError(() -> client.putObject(
				putRequest(bucketName, key, tagging(tag("key", "value1"), tag("key", "value2")))),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		var invalidTags = List.of(tag("aws:key", "value"), tag("key?", "value"), tag("k".repeat(129), "value"));
		for (var invalidTag : invalidTags) {
			assertError(() -> client.putObject(putRequest(bucketName, key, tagging(invalidTag))),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		}
		assertObjectNotExist(client, bucketName, key);
	}

	@Test
	@Tag("Version")
	public void testObjTaggingVersion() {
		var key = "testObjTaggingVersion";
		var client = getClient();
		var bucketName = createBucket(client, 29);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var version1 = client.putObject(bucketName, key, "1").getVersionId();
		var version2 = client.putObject(bucketName, key, "2").getVersionId();
		var tagging = tagging(tag("version", "1"));

		// 버전을 지정하면 그 버전의 태그만 바뀐다
		var putResponse = client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, version1, tagging));
		assertEquals(version1, putResponse.getVersionId());

		var getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key, version1));
		assertEquals(version1, getResponse.getVersionId());
		tagCompare(tagging.getTagSet(), getResponse.getTagSet());

		// 버전을 지정하지 않으면 최신 버전이 대상이다
		getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		assertEquals(version2, getResponse.getVersionId());
		assertEquals(0, getResponse.getTagSet().size());

		var deleteResponse = client
				.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, key).withVersionId(version1));
		assertEquals(version1, deleteResponse.getVersionId());
		assertEquals(0, client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key, version1)).getTagSet()
				.size());
	}

	@Test
	@Tag("Version")
	public void testObjTaggingInvalidVersion() {
		var key = "testObjTaggingInvalidVersion";
		var client = getClient();
		var bucketName = createBucket(client, 30);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		client.putObject(bucketName, key, "1");

		assertError(() -> client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key, "invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		assertError(() -> client.setObjectTagging(
				new SetObjectTaggingRequest(bucketName, key, "invalid", tagging(tag("key", "value")))),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		assertError(() -> client.deleteObjectTagging(
				new DeleteObjectTaggingRequest(bucketName, key).withVersionId("invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("Version")
	public void testObjTaggingDeleteMarker() {
		var key = "testObjTaggingDeleteMarker";
		var client = getClient();
		var bucketName = createBucket(client, 31);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var versionId = client.putObject(bucketName, key, "1").getVersionId();
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, tagging(tag("key", "value"))));
		client.deleteObject(bucketName, key);
		var deleteMarker = client.listVersions(bucketName, key).getVersionSummaries().stream()
				.filter(v -> v.isDeleteMarker()).findFirst().orElseThrow().getVersionId();
		var tagging = tagging(tag("key", "value2"));

		// 최신 버전이 삭제 마커이면 버전을 지정하든 않든 태그 요청은 405 (versionId 가 null 이면 지정하지 않은 요청)
		for (var target : Arrays.asList(null, deleteMarker)) {
			assertError(() -> client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key, target)),
					HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED);
			assertError(() -> client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, target, tagging)),
					HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED);
			assertError(() -> client.deleteObjectTagging(
					new DeleteObjectTaggingRequest(bucketName, key).withVersionId(target)),
					HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED);
		}

		// 삭제 마커 아래의 실제 버전은 버전을 지정하면 그대로 조회된다
		var response = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key, versionId));
		tagCompare(List.of(tag("key", "value")), response.getTagSet());
	}
}
