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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;

import org.example.Data.FormFile;
import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.auth.AWS2SignerBase;
import org.example.auth.AWS4SignerBase;
import org.example.auth.AWS4SignerForAuthorizationHeader;
import org.example.auth.AWS4SignerForChunkedUpload;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.utils.BinaryUtils;

public class Post extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Post V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Post V2 End");
	}

	@Test
	@Tag("Upload")
	public void testPostObjectAnonymousRequest() throws MalformedURLException {
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, BucketCannedACL.PUBLIC_READ_WRITE);

		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "public-read");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectAuthenticatedRequest() throws MalformedURLException {
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucket(client);

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
		payload.put("Content-Length", String.format("%d", fileData.body.length()));
		payload.put("Content-Type", contentType);
		payload.put("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectAuthenticatedNoContentType() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, BucketCannedACL.PUBLIC_READ_WRITE);

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

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

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

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectAuthenticatedRequestBadAccessKey() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, BucketCannedACL.PUBLIC_READ_WRITE);

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

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("AWSAccessKeyId", "foo");
		payload.put("acl", "private");
		payload.put("signature", signature);
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("StatusCode")
	public void testPostObjectSetSuccessCode() throws MalformedURLException {
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, BucketCannedACL.PUBLIC_READ_WRITE);

		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "public-read");
		payload.put("Content-Type", contentType);
		payload.put("successActionStatus", "201");

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_CREATED, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("StatusCode")
	public void testPostObjectSetInvalidSuccessCode() throws MalformedURLException {
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, BucketCannedACL.PUBLIC_READ_WRITE);

		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "public-read");
		payload.put("Content-Type", contentType);
		payload.put("successActionStatus", HttpStatus.SC_NOT_FOUND + "");

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectUploadLargerThanChunk() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var key = "foo.txt";
		var contentType = "text/plain";
		var size = 5 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		var client = getClient();
		var bucketName = createBucket(client);

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
		contentLengthRange.add(size);
		conditions.add(contentLengthRange);

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var fileData = new FormFile(key, contentType, data);
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("AWSAccessKeyId", config.mainUser.accessKey);
		payload.put("acl", "private");
		payload.put("signature", signature);
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectSetKeyFromFilename() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var contentType = "text/plain";
		var key = "foo.txt";

		var client = getClient();
		var bucketName = createBucket(client);

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectIgnoredHeader() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "foo.txt";
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
		payload.put("x-ignore-foo", "bar");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("Upload")
	public void testPostObjectCaseInsensitiveConditionFields() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "foo.txt";
		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bUcKeT", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("StArTs-WiTh");
		starts1.add("$KeY");
		starts1.add("foo");
		conditions.add(starts1);

		var acl = new JsonObject();
		acl.addProperty("AcL", "private");
		conditions.add(acl);

		var starts2 = new JsonArray();
		starts2.add("StArTs-WiTh");
		starts2.add("$CoNtEnT-TyPe");
		starts2.add(contentType);
		conditions.add(starts2);

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("kEy", key);
		payload.put("AWSAccessKeyId", config.mainUser.accessKey);
		payload.put("aCl", "private");
		payload.put("signature", signature);
		payload.put("pOLICy", policy);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("Upload")
	public void testPostObjectEscapedFieldValues() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var key = "\\$foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucket(client);

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectSuccessRedirectAction() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, BucketCannedACL.PUBLIC_READ_WRITE);

		var redirectURL = createURL(bucketName);

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

		var starts3 = new JsonArray();
		starts3.add("eq");
		starts3.add("$successActionRedirect");
		starts3.add(redirectURL.toString());
		conditions.add(starts3);

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

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
		payload.put("Content-Type", contentType);
		payload.put("successActionRedirect", redirectURL.toString());

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_OK, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(String.format("%s?bucket=%s&key=%s&etag=%s%s%s", redirectURL, bucketName, key, "%22",
				response.response().eTag().replace("\"", ""), "%22"), result.URL);
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidSignature() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";
		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		payload.put("signature", signature.substring(0, signature.length() - 1));
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidAccessKey() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("AWSAccessKeyId", config.mainUser.accessKey.substring(0, config.mainUser.accessKey.length() - 1));
		payload.put("acl", "private");
		payload.put("signature", signature);
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidDateFormat() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100).replace("T", " "));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectNoKeySpecified() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

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

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var fileData = new FormFile("", contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("AWSAccessKeyId", config.mainUser.accessKey);
		payload.put("acl", "private");
		payload.put("signature", signature);
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingSignature() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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

		policyDocument.add("conditions", conditions);

		var bytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(bytesJsonPolicyDocument);

		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("AWSAccessKeyId", config.mainUser.accessKey);
		payload.put("acl", "private");
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingPolicyCondition() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.message);
	}

	@Test
	@Tag("Metadata")
	public void testPostObjectUserSpecifiedHeader() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var key = "foo.txt";
		var contentType = "text/plain";

		var client = getClient();
		var bucketName = createBucket(client);

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
		starts3.add("$x-amz-meta-foo");
		starts3.add("bar");
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
		payload.put("x-amz-meta-foo", "bar-clamp");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("bar-clamp", response.response().metadata().get(("foo")));
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectRequestMissingPolicySpecifiedField() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		starts3.add("$x-amz-meta-foo");
		starts3.add("bar");
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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectConditionIsCaseSensitive() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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

		policyDocument.add("CONDITIONS", conditions);

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectExpiresIsCaseSensitive() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("EXPIRATION", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectExpiredPolicy() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(-100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidRequestFieldValue() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";
		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		conditions.add(starts1);

		var acl = new JsonObject();
		acl.addProperty("acl", "private");
		conditions.add(acl);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(contentType);
		conditions.add(starts2);

		var starts3 = new JsonArray();
		starts3.add("eq");
		starts3.add("$x-amz-meta-foo");
		starts3.add("");
		conditions.add(starts3);

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

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
		payload.put("x-amz-meta-foo", "bar-clamp");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingExpiresCondition() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";
		var policyDocument = new JsonObject();

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		payload.put("Content-Type", contentType);
		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingConditionsList() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "foo.txt";
		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

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
		payload.put("Content-Type", contentType);
		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectUploadSizeLimitExceeded() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		contentLengthRange.add(0);
		conditions.add(contentLengthRange);

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingContentLengthArgument() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		conditions.add(contentLengthRange);

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidContentLengthArgument() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		contentLengthRange.add(-1);
		contentLengthRange.add(0);
		conditions.add(contentLengthRange);

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectUploadSizeBelowMinimum() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		contentLengthRange.add(512);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectEmptyConditions() throws MalformedURLException {
		assumeFalse(config.isAWS());
		var bucketName = createBucket();

		var contentType = "text/plain";
		var key = "foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));
		policyDocument.add("conditions", new JsonArray());

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("PresignedURL")
	public void testPresignedUrlPutGet() {
		var bucketName = createBucket();
		var client = getS3Presigner();
		var key = "foo";

		var putURL = client.presignPutObject(
				s -> s.signatureDuration(Duration.ofMinutes(10)).putObjectRequest(p -> p.bucket(bucketName).key(key)));
		var putResponse = putObject(putURL.url(), key);
		assertEquals(HttpStatus.SC_OK, putResponse.getStatusLine().getStatusCode());

		var getURL = client.presignGetObject(
				s -> s.signatureDuration(Duration.ofMinutes(10)).getObjectRequest(p -> p.bucket(bucketName).key(key)));
		var getResponse = getObject(getURL.url());
		assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());

	}

	@Test
	@Tag("signV4")
	public void testPutObjectV4() throws MalformedURLException {
		var bucketName = createBucket();
		var key = "foo";
		var endPoint = createURL(bucketName, key);
		var size = 100;
		var content = Utils.randomTextToLong(size);

		byte[] contentHash = AWS4SignerBase.hash(content);
		String contentHashString = BinaryUtils.toHex(contentHash);

		var headers = new HashMap<String, String>();
		headers.put("x-amz-content-sha256", contentHashString);
		headers.put("x-amz-decoded-content-length", "" + content.length());

		var signer = new AWS4SignerForAuthorizationHeader(endPoint, "PUT", "s3", config.regionName);

		var authorization = signer.computeSignature(headers, null, contentHashString, config.mainUser.accessKey,
				config.mainUser.secretKey);
		headers.put("Authorization", authorization);

		var result = NetUtils.putUpload(endPoint, "PUT", headers, content);

		assertEquals(HttpStatus.SC_OK, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("signV4")
	public void testPutObjectChunkedV4() throws MalformedURLException {
		var bucketName = createBucket();
		var key = "foo";
		var endPoint = createURL(bucketName, key);
		var size = 100;
		var content = Utils.randomTextToLong(size);

		var headers = new HashMap<String, String>();
		headers.put("x-amz-content-sha256", AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256);
		headers.put("content-encoding", "" + "aws-chunked");
		headers.put("x-amz-decoded-content-length", "" + content.length());

		var signer = new AWS4SignerForChunkedUpload(endPoint, "PUT", "s3", config.regionName);

		long totalLength = AWS4SignerForChunkedUpload.calculateChunkedContentLength(content.length(),
				NetUtils.USER_DATE_BLOCK_SIZE);
		headers.put("content-length", "" + totalLength);

		String authorization = signer.computeSignature(headers, null, AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256,
				config.mainUser.accessKey, config.mainUser.secretKey);
		headers.put("Authorization", authorization);

		var result = NetUtils.putUploadChunked(endPoint, "PUT", headers, signer, content);
		assertEquals(HttpStatus.SC_OK, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("signV4")
	public void testGetObjectV4() throws MalformedURLException {
		var key = "foo";
		var httpMethod = "GET";
		var size = 100;
		var client = getClient();
		var bucketName = createBucket(client);
		var endPoint = createURL(bucketName, key);
		var content = Utils.randomTextToLong(size);

		client.putObject(g -> g.bucket(bucketName).key(key), RequestBody.fromString(content));

		var headers = new HashMap<String, String>();
		headers.put("x-amz-content-sha256", AWS4SignerBase.EMPTY_BODY_SHA256);

		var signer = new AWS4SignerForChunkedUpload(endPoint, httpMethod, "s3", config.regionName);

		String authorization = signer.computeSignature(headers, null, AWS4SignerBase.EMPTY_BODY_SHA256,
				config.mainUser.accessKey, config.mainUser.secretKey);
		headers.put("Authorization", authorization);

		var result = NetUtils.putUpload(endPoint, httpMethod, headers, null);
		assertEquals(HttpStatus.SC_OK, result.statusCode, result.getErrorCode());
		assertEquals(size, result.GetContent().length());
		assertEquals(content, result.GetContent());
		assertTrue(content.equals(result.GetContent()), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectWrongBucket() throws MalformedURLException {
		var bucketName = createBucket();
		var badBucketName = getNewBucketName();

		var contentType = "text/plain";
		var key = "\\$foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
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
		contentLengthRange.add(512);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

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
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(badBucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NOT_FOUND, result.statusCode, result.getErrorCode());
	}
}
