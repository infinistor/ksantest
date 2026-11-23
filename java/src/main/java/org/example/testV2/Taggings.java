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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.Tagging;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@SuppressWarnings("unchecked")
public class Taggings extends TestBase {
	@Test
	@Tag("Check")
	public void testSetTagging() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

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
		var key = "testGetObjTagging";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 2, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(2)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());
	}

	@Test
	@Tag("Check")
	public void testGetObjHeadTagging() {
		var key = "testGetObjHeadTagging";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 3, key, 0);
		var count = 2;

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(count)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.headObject(g -> g.bucket(bucketName).key(key));
		assertEquals(count, response.tagCount());
	}

	@Test
	@Tag("Max")
	public void testPutMaxTags() {
		var key = "testPutMaxTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 4, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(10)).build();

		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessTags() {
		var key = "testPutExcessTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 5, key, 0);

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
		var key = "testPutMaxSizeTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 6, key, 0);

		var inputTagSet = Tagging.builder().tagSet(makeDetailTagSet(10, 128, 256)).build();
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), response.tagSet());
	}

	@Test
	@Tag("Overflow")
	public void testPutExcessKeyTags() {
		var key = "testPutExcessKeyTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 7, key, 0);

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
		var key = "testPutExcessValTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 8, key, 0);

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
		var key = "testPutModifyTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 9, key, 0);

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
		var key = "testPutDeleteTags";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 10, key, 0);

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
		var bucketName = createBucket(client, 11);
		var key = "testPutObjWithTags";
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

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(tags, getResponse.tagSet());
	}

	@Test
	@Tag("Check")
	public void testGetObjNonTagging() {
		var key = "testGetObjNonTagging";
		var client = getClient();
		var bucketName = createBucket(client, 13);

		client.putObject(p -> p.bucket(bucketName).key(key).tagging(Tagging.builder().build()),
				RequestBody.fromString(""));
	}

	@Test
	@Tag("PutObject")
	public void testPutObjWithTagsEncoded() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		var key = "testPutObjWithTagsEncoded";

		// 공백, '/', '+', '='는 x-amz-tagging 헤더에서 URL 인코딩되어 전송되므로 서버가 디코딩해서 저장해야 한다
		var tags = Tagging.builder().tagSet(
				t -> t.key("k 1").value("a/b+c d").build(),
				t -> t.key("k2").value("x=y").build()).build();

		client.putObject(p -> p.bucket(bucketName).key(key).tagging(tags), RequestBody.fromString(key));

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(tags.tagSet(), getResponse.tagSet());
	}

	@Test
	@Tag("Multipart")
	public void testMultipartUploadWithTagsEncoded() {
		var client = getClient();
		var bucketName = createBucket(client, 15);
		var key = "testMultipartUploadWithTagsEncoded";

		var tags = Tagging.builder().tagSet(
				t -> t.key("k 1").value("a/b+c d").build(),
				t -> t.key("k2").value("x=y").build()).build();

		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(key).tagging(tags)).uploadId();
		var partResponse = client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
				RequestBody.fromString(key));
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadId)
				.multipartUpload(m -> m.parts(p -> p.partNumber(1).eTag(partResponse.eTag()))));

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(tags.tagSet(), getResponse.tagSet());
	}

	private static software.amazon.awssdk.services.s3.model.Tag tag(String key, String value) {
		return software.amazon.awssdk.services.s3.model.Tag.builder().key(key).value(value).build();
	}

	private static Tagging tagging(software.amazon.awssdk.services.s3.model.Tag... tags) {
		return Tagging.builder().tagSet(List.of(tags)).build();
	}

	private static void assertError(Executable executable, int statusCode, String errorCode) {
		var e = assertThrows(AwsServiceException.class, executable);
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
	}

	private static void assertObjectNotExist(S3Client client, String bucketName, String key) {
		var e = assertThrows(AwsServiceException.class, () -> client.headObject(h -> h.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
	}

	@Test
	@Tag("Max")
	public void testBucketTaggingMax() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		// 버킷 태그는 50개까지 넣을 수 있다
		var tagSet = makeSimpleTagSet(50);
		client.putBucketTagging(p -> p.bucket(bucketName).tagging(t -> t.tagSet(tagSet)));
		tagCompare(tagSet, client.getBucketTagging(g -> g.bucket(bucketName)).tagSet());

		assertError(() -> client.putBucketTagging(
				p -> p.bucket(bucketName).tagging(t -> t.tagSet(makeSimpleTagSet(51)))),
				HttpStatus.SC_BAD_REQUEST, MainData.BAD_REQUEST);
		assertEquals(50, client.getBucketTagging(g -> g.bucket(bucketName)).tagSet().size());
	}

	@Test
	@Tag("ERROR")
	public void testBucketTaggingDuplicateKey() {
		var client = getClient();
		var bucketName = createBucket(client, 17);

		assertError(() -> client.putBucketTagging(
				p -> p.bucket(bucketName).tagging(tagging(tag("key", "value1"), tag("key", "value2")))),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
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
			assertError(() -> client.putBucketTagging(p -> p.bucket(bucketName).tagging(tagging(invalidTag))),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		}
		assertError(() -> client.getBucketTagging(g -> g.bucket(bucketName)), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_TAG_SET);
	}

	@Test
	@Tag("Check")
	public void testBucketTaggingEmptyValue() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var tagging = tagging(tag("key", ""));

		client.putBucketTagging(p -> p.bucket(bucketName).tagging(tagging));
		tagCompare(tagging.tagSet(), client.getBucketTagging(g -> g.bucket(bucketName)).tagSet());
	}

	@Test
	@Tag("Delete")
	public void testBucketTaggingEmptySet() {
		var client = getClient();
		var bucketName = createBucket(client, 20);
		client.putBucketTagging(p -> p.bucket(bucketName).tagging(tagging(tag("key", "value"))));

		// 빈 태그 목록을 넣으면 태그가 지워진다
		client.putBucketTagging(p -> p.bucket(bucketName).tagging(tagging()));
		assertError(() -> client.getBucketTagging(g -> g.bucket(bucketName)), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_TAG_SET);

		// 태그가 없어도 삭제는 성공한다
		var response = client.deleteBucketTagging(d -> d.bucket(bucketName));
		assertEquals(HttpStatus.SC_NO_CONTENT, response.sdkHttpResponse().statusCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketTaggingBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(21);

		assertError(() -> client.getBucketTagging(g -> g.bucket(bucketName)), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_BUCKET);
		assertError(() -> client.putBucketTagging(
				p -> p.bucket(bucketName).tagging(tagging(tag("key", "value")))),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.deleteBucketTagging(d -> d.bucket(bucketName)), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_BUCKET);
	}

	@Test
	@Tag("ERROR")
	public void testObjTaggingDuplicateKey() {
		var key = "testObjTaggingDuplicateKey";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 22, key, 10);

		assertError(() -> client.putObjectTagging(p -> p.bucket(bucketName).key(key)
				.tagging(tagging(tag("key", "value1"), tag("key", "value2")))),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		assertEquals(0, client.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet().size());
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
			assertError(() -> client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging(invalidTag))),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		}
		assertEquals(0, client.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet().size());
	}

	@Test
	@Tag("Check")
	public void testObjTaggingSpecialCharacters() {
		var key = "testObjTaggingSpecialCharacters";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 24, key, 10);

		// 허용 문자(영숫자, 공백, + - = . _ : / @)와 빈 값은 그대로 저장된다
		var tagging = tagging(tag("a+b-=._:/@ c", "v+-=._:/@ d"), tag("empty", ""));
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging));
		tagCompare(tagging.tagSet(), client.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet());
	}

	@Test
	@Tag("Delete")
	public void testObjTaggingEmptySet() {
		var key = "testObjTaggingEmptySet";
		var client = getClient();
		var bucketName = createKeyWithRandomContent(client, 25, key, 10);
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging(tag("key", "value"))));

		// 빈 태그 목록을 넣으면 태그가 지워진다
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging()));
		assertEquals(0, client.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet().size());

		// 태그가 없어도 삭제는 성공한다
		var response = client.deleteObjectTagging(d -> d.bucket(bucketName).key(key));
		assertEquals(HttpStatus.SC_NO_CONTENT, response.sdkHttpResponse().statusCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjTaggingKeyNotExist() {
		var key = "testObjTaggingKeyNotExist";
		var client = getClient();
		var bucketName = createBucket(client, 26);

		assertError(() -> client.putObjectTagging(
				p -> p.bucket(bucketName).key(key).tagging(tagging(tag("key", "value")))),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY);
		assertError(() -> client.getObjectTagging(g -> g.bucket(bucketName).key(key)), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_KEY);
		assertError(() -> client.deleteObjectTagging(d -> d.bucket(bucketName).key(key)), HttpStatus.SC_NOT_FOUND,
				MainData.NO_SUCH_KEY);
	}

	@Test
	@Tag("Overflow")
	public void testPutObjWithExcessTags() {
		var key = "testPutObjWithExcessTags";
		var client = getClient();
		var bucketName = createBucket(client, 27);

		var pairs = new ArrayList<String>();
		for (int i = 0; i < 11; i++)
			pairs.add("key" + i + "=value" + i);
		var tagging = String.join("&", pairs);

		assertError(() -> client.putObject(p -> p.bucket(bucketName).key(key).tagging(tagging),
				RequestBody.fromString("")), HttpStatus.SC_BAD_REQUEST, MainData.BAD_REQUEST);
		assertObjectNotExist(client, bucketName, key);
	}

	@Test
	@Tag("ERROR")
	public void testPutObjWithInvalidTags() {
		var key = "testPutObjWithInvalidTags";
		var client = getClient();
		var bucketName = createBucket(client, 28);

		// 헤더로 준 태그의 키가 중복되면 InvalidArgument
		assertError(() -> client.putObject(p -> p.bucket(bucketName).key(key).tagging("key=value1&key=value2"),
				RequestBody.fromString("")), HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		for (var tagging : List.of("aws:key=value", "key%3F=value", "k".repeat(129) + "=value")) {
			assertError(() -> client.putObject(p -> p.bucket(bucketName).key(key).tagging(tagging),
					RequestBody.fromString("")), HttpStatus.SC_BAD_REQUEST, MainData.INVALID_TAG);
		}
		assertObjectNotExist(client, bucketName, key);
	}

	@Test
	@Tag("Version")
	public void testObjTaggingVersion() {
		var key = "testObjTaggingVersion";
		var client = getClient();
		var bucketName = createBucket(client, 29);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var version1 = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("1")).versionId();
		var version2 = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("2")).versionId();
		var tagging = tagging(tag("version", "1"));

		// 버전을 지정하면 그 버전의 태그만 바뀐다
		var putResponse = client
				.putObjectTagging(p -> p.bucket(bucketName).key(key).versionId(version1).tagging(tagging));
		assertEquals(version1, putResponse.versionId());

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key).versionId(version1));
		assertEquals(version1, getResponse.versionId());
		tagCompare(tagging.tagSet(), getResponse.tagSet());

		// 버전을 지정하지 않으면 최신 버전이 대상이다
		getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(version2, getResponse.versionId());
		assertEquals(0, getResponse.tagSet().size());

		var deleteResponse = client.deleteObjectTagging(d -> d.bucket(bucketName).key(key).versionId(version1));
		assertEquals(version1, deleteResponse.versionId());
		assertEquals(0,
				client.getObjectTagging(g -> g.bucket(bucketName).key(key).versionId(version1)).tagSet().size());
	}

	@Test
	@Tag("Version")
	public void testObjTaggingInvalidVersion() {
		var key = "testObjTaggingInvalidVersion";
		var client = getClient();
		var bucketName = createBucket(client, 30);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("1"));

		assertError(() -> client.getObjectTagging(g -> g.bucket(bucketName).key(key).versionId("invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		assertError(() -> client.putObjectTagging(p -> p.bucket(bucketName).key(key).versionId("invalid")
				.tagging(tagging(tag("key", "value")))), HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		assertError(() -> client.deleteObjectTagging(d -> d.bucket(bucketName).key(key).versionId("invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("Version")
	public void testObjTaggingDeleteMarker() {
		var key = "testObjTaggingDeleteMarker";
		var client = getClient();
		var bucketName = createBucket(client, 31);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("1")).versionId();
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging(tag("key", "value"))));
		var deleteMarker = client.deleteObject(d -> d.bucket(bucketName).key(key)).versionId();
		var tagging = tagging(tag("key", "value2"));

		// 최신 버전이 삭제 마커이면 버전을 지정하든 않든 태그 요청은 405 (versionId 가 null 이면 지정하지 않은 요청)
		for (var target : Arrays.asList(null, deleteMarker)) {
			assertError(() -> client.getObjectTagging(g -> g.bucket(bucketName).key(key).versionId(target)),
					HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED);
			assertError(() -> client.putObjectTagging(
					p -> p.bucket(bucketName).key(key).versionId(target).tagging(tagging)),
					HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED);
			assertError(() -> client.deleteObjectTagging(d -> d.bucket(bucketName).key(key).versionId(target)),
					HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED);
		}

		// 삭제 마커 아래의 실제 버전은 버전을 지정하면 그대로 조회된다
		var response = client.getObjectTagging(g -> g.bucket(bucketName).key(key).versionId(versionId));
		tagCompare(List.of(tag("key", "value")), response.tagSet());
	}
}
