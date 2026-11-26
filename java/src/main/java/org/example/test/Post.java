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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.MalformedURLException;
import java.util.HashMap;

import org.example.Data.FormFile;
import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.auth.AWS4SignerBase;
import org.example.auth.AWS4SignerForAuthorizationHeader;
import org.example.auth.AWS4SignerForChunkedUpload;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;
import com.amazonaws.util.BinaryUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.example.Data.MyResult;
import org.example.Data.UserData;
import com.google.gson.JsonElement;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GroupGrantee;

public class Post extends TestBase {
	@Test
	@Tag("Upload")
	public void testPostObjectAnonymousRequest() throws MalformedURLException {
		var key = "foo.txt";
		var client = getClient();
		var bucketName = createBucket(client, 1, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		var contentType = "text/plain";
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "public-read");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectAuthenticatedRequest() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 2);
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
		payload.put("Content-Type", contentType);
		auth.putFormFields(payload, signature);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectAuthenticatedNoContentType() throws MalformedURLException {
		var key = "foo.txt";
		var contentType = "text/plain";
		var client = getClient();
		var bucketName = createBucket(client, 3, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

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
		auth.putFormFields(payload, signature);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());
		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectAuthenticatedRequestBadAccessKey() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 4, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

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

		var auth = createSigV4Post("foo");
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("StatusCode")
	public void testPostObjectSetSuccessCode() throws MalformedURLException {
		var key = "foo.txt";
		var client = getClient();
		var bucketName = createBucket(client, 5, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		var contentType = "text/plain";
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "public-read");
		payload.put("Content-Type", contentType);
		payload.put("success_action_status", "201");

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_CREATED, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("StatusCode")
	public void testPostObjectSetInvalidSuccessCode() throws MalformedURLException {
		var key = "foo.txt";
		var contentType = "text/plain";
		var client = getClient();
		var bucketName = createBucket(client, 6, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "public-read");
		payload.put("Content-Type", contentType);
		payload.put("success_action_status", HttpStatus.SC_NOT_FOUND + "");

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectUploadLargerThanChunk() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		var contentType = "text/plain";
		var key = "foo.txt";
		var size = 5 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

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

		var auth = createSigV4Post();
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, data);
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectSetKeyFromFilename() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 8);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectIgnoredHeader() throws MalformedURLException {
		var bucketName = createBucket(9);

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
		auth.putFormFields(payload, signature);
		payload.put("x-ignore-foo", "bar");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("Upload")
	public void testPostObjectCaseInsensitiveConditionFields() throws MalformedURLException {
		var bucketName = createBucket(10);

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

		var auth = createSigV4Post();
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("kEy", key);
		payload.put("aCl", "private");
		payload.put("pOLICy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("Upload")
	public void testPostObjectEscapedFieldValues() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 11);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Upload")
	public void testPostObjectSuccessRedirectAction() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 12, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		var contentType = "text/plain";
		var key = "foo.txt";
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
		starts3.add("$success_action_redirect");
		starts3.add(redirectURL.toString());
		conditions.add(starts3);

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);
		payload.put("success_action_redirect", redirectURL.toString());

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_OK, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		assertEquals(String.format("%s?bucket=%s&key=%s&etag=%s%s%s", redirectURL, bucketName, key, "%22",
				response.getObjectMetadata().getETag().replace("\"", ""), "%22"), result.URL);
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidSignature() throws MalformedURLException {
		var bucketName = createBucket(13);

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
		auth.putFormFields(payload, signature.substring(0, signature.length() - 1));
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidAccessKey() throws MalformedURLException {
		var bucketName = createBucket(14);

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

		var auth = createSigV4Post(config.mainUser.accessKey.substring(0, config.mainUser.accessKey.length() - 1));
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidDateFormat() throws MalformedURLException {
		var bucketName = createBucket(15);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectNoKeySpecified() throws MalformedURLException {
		var bucketName = createBucket(16);

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

		var auth = createSigV4Post();
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile("", contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingSignature() throws MalformedURLException {
		var bucketName = createBucket(17);

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

		var auth = createSigV4Post();
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingPolicyCondition() throws MalformedURLException {
		var bucketName = createBucket(18);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.message);
	}

	@Test
	@Tag("Metadata")
	public void testPostObjectUserSpecifiedHeader() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 19);

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

		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$x-amz-meta-foo");
		starts3.add("bar");
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
		auth.putFormFields(payload, signature);
		payload.put("x-amz-meta-foo", "bar-clamp");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.getErrorCode());

		var response = client.getObject(bucketName, key);
		assertEquals("bar-clamp", response.getObjectMetadata().getUserMetadata().get(("foo")));
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectRequestMissingPolicySpecifiedField() throws MalformedURLException {
		var bucketName = createBucket(20);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectConditionIsCaseSensitive() throws MalformedURLException {
		var bucketName = createBucket(21);

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

		var auth = createSigV4Post();
		auth.addConditions(conditions);
		policyDocument.add("CONDITIONS", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectExpiresIsCaseSensitive() throws MalformedURLException {
		var bucketName = createBucket(22);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectExpiredPolicy() throws MalformedURLException {
		var bucketName = createBucket(23);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidRequestFieldValue() throws MalformedURLException {
		var bucketName = createBucket(24);

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
		auth.putFormFields(payload, signature);
		payload.put("x-amz-meta-foo", "bar-clamp");
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_FORBIDDEN, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingExpiresCondition() throws MalformedURLException {
		var bucketName = createBucket(25);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);
		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingConditionsList() throws MalformedURLException {
		var bucketName = createBucket(26);

		var contentType = "text/plain";
		var key = "foo.txt";
		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var auth = createSigV4Post();
		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);
		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectUploadSizeLimitExceeded() throws MalformedURLException {
		var bucketName = createBucket(27);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectMissingContentLengthArgument() throws MalformedURLException {
		var bucketName = createBucket(28);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectInvalidContentLengthArgument() throws MalformedURLException {
		var bucketName = createBucket(29);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectUploadSizeBelowMinimum() throws MalformedURLException {
		var bucketName = createBucket(30);

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
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectEmptyConditions() throws MalformedURLException {
		var bucketName = createBucket(31);

		var contentType = "text/plain";
		var key = "foo.txt";

		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));
		policyDocument.add("conditions", new JsonArray());

		var auth = createSigV4Post();
		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("acl", "private");
		payload.put("policy", policy);
		auth.putFormFields(payload, signature);
		payload.put("Content-Type", contentType);

		var result = NetUtils.postUpload(createURL(bucketName), payload, fileData);
		assertEquals(HttpStatus.SC_BAD_REQUEST, result.statusCode, result.getErrorCode());
	}

	@Test
	@Tag("PresignedURL")
	public void testPresignedUrlPutGet() {
		var client = getClient();
		var bucketName = createBucket(client, 32);
		var key = "foo";

		var putURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var putResponse = putObject(putURL, key);
		assertEquals(HttpStatus.SC_OK, putResponse.getStatusLine().getStatusCode());

		var getURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var getResponse = getObject(getURL);
		assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("PresignedURL")
	public void testPresignedUrlPutGetV4() {
		var bucketName = getNewBucketName(33);
		var client = getClientV4(true, true);
		var key = "foo";

		client.createBucket(bucketName);

		var putURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var putResponse = putObject(putURL, key);
		assertEquals(HttpStatus.SC_OK, putResponse.getStatusLine().getStatusCode());

		var getURL = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var getResponse = getObject(getURL);
		assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("signV4")
	public void testPutObjectV4() throws MalformedURLException {
		var bucketName = createBucket(34);
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
		var bucketName = createBucket(35);
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
		var client = getClient();
		var bucketName = createBucket(client, 36);
		var key = "foo";
		var endPoint = createURL(bucketName, key);
		var httpMethod = "GET";
		var size = 100;
		var content = Utils.randomTextToLong(size);

		client.putObject(bucketName, key, content);

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
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectWrongBucket() throws MalformedURLException {
		var bucketName = getNewBucketNameOnly(37);
		var badBucketName = getNewBucketNameOnly(37);

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

		var auth = createSigV4Post();
		auth.addConditions(conditions);
		policyDocument.add("conditions", conditions);

		var policy = encodePostPolicy(policyDocument);
		var signature = auth.sign(policy);
		var fileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put("key", key);
		payload.put("bucket", bucketName);
		payload.put("acl", "private");
		payload.put("policy", policy);
		payload.put("Content-Type", contentType);
		auth.putFormFields(payload, signature);

		var result = NetUtils.postUpload(createURL(badBucketName), payload, fileData);
		assertEquals(HttpStatus.SC_NOT_FOUND, result.statusCode, result.getErrorCode());
	}

	// region SigV4 POST (AWS 실측 기준)

	private static Map<String, String> formFields(String... pairs) {
		var fields = new LinkedHashMap<String, String>();
		for (int i = 0; i < pairs.length; i += 2)
			fields.put(pairs[i], pairs[i + 1]);
		return fields;
	}

	private static JsonObject exact(String name, String value) {
		var condition = new JsonObject();
		condition.addProperty(name, value);
		return condition;
	}

	private static JsonArray rule(Object... values) {
		var condition = new JsonArray();
		for (var value : values) {
			if (value instanceof Number)
				condition.add((Number) value);
			else
				condition.add((String) value);
		}
		return condition;
	}

	private static JsonArray conditions(JsonElement... items) {
		var conditions = new JsonArray();
		for (var item : items)
			conditions.add(item);
		return conditions;
	}

	/** key 를 뺀 폼 필드를 값이 같아야 하는 조건으로 바꾼다. */
	private static JsonArray exactConditions(Map<String, String> fields) {
		var conditions = new JsonArray();
		for (var field : fields.entrySet()) {
			if (!field.getKey().equals("key"))
				conditions.add(exact(field.getKey(), field.getValue()));
		}
		return conditions;
	}

	private static JsonArray plus(JsonArray base, JsonElement... items) {
		var conditions = base.deepCopy();
		for (var item : items)
			conditions.add(item);
		return conditions;
	}

	private static JsonObject policyDocument(String expiration, JsonArray conditions) {
		var document = new JsonObject();
		if (expiration != null)
			document.addProperty("expiration", expiration);
		if (conditions != null)
			document.add("conditions", conditions);
		return document;
	}

	/**
	 * SigV4 로 서명한 폼 업로드. 날짜·자격 증명은 정책과 폼에 같은 값을 쓴다.
	 * policy, rawPolicy 가 모두 null 이면 bucket, key(아무 값), x-amz-* 조건에 conditions 를 더한 정책을 만든다.
	 * policy 는 기본 조건 목록을 받아 정책 문서를 돌려준다. rawPolicy 는 그대로 policy 필드에 넣는다.
	 * mutate 는 서명까지 끝난 폼 필드를 고친다.
	 */
	private MyResult postForm(String bucketName, Map<String, String> fields, JsonArray conditions, String body,
			UserData user, Function<JsonArray, JsonObject> policy, String rawPolicy,
			Consumer<Map<String, String>> mutate) throws MalformedURLException {
		if (user == null)
			user = config.mainUser;
		var auth = new SigV4Post(user.accessKey, user.secretKey, config.regionName);
		var baseConditions = conditions(exact("bucket", bucketName), rule("starts-with", "$key", ""));
		auth.addConditions(baseConditions);

		var encoded = rawPolicy;
		if (encoded == null) {
			JsonObject document;
			if (policy == null) {
				if (conditions != null)
					baseConditions.addAll(conditions);
				document = policyDocument(getTimeToAddMinutes(100), baseConditions);
			} else {
				document = policy.apply(baseConditions);
			}
			encoded = encodePostPolicy(document);
		}

		// 폼 필드는 넣은 순서대로 보낸다
		var payload = new LinkedHashMap<String, String>(fields);
		payload.put("policy", encoded);
		auth.putFormFields(payload, auth.sign(encoded));
		if (mutate != null)
			mutate.accept(payload);
		return NetUtils.postUpload(createURL(bucketName), payload, new FormFile("foo.txt", "text/plain", body));
	}

	private MyResult postForm(String bucketName, Map<String, String> fields) throws MalformedURLException {
		return postForm(bucketName, fields, null, "bar", null, null, null, null);
	}

	private MyResult postForm(String bucketName, Map<String, String> fields, JsonArray conditions)
			throws MalformedURLException {
		return postForm(bucketName, fields, conditions, "bar", null, null, null, null);
	}

	private MyResult postFormBody(String bucketName, Map<String, String> fields, String body)
			throws MalformedURLException {
		return postForm(bucketName, fields, null, body, null, null, null, null);
	}

	private MyResult postFormPolicy(String bucketName, Map<String, String> fields,
			Function<JsonArray, JsonObject> policy) throws MalformedURLException {
		return postForm(bucketName, fields, null, "bar", null, policy, null, null);
	}

	private MyResult postFormRawPolicy(String bucketName, Map<String, String> fields, String rawPolicy)
			throws MalformedURLException {
		return postForm(bucketName, fields, null, "bar", null, null, rawPolicy, null);
	}

	private MyResult postFormMutated(String bucketName, Map<String, String> fields,
			Consumer<Map<String, String>> mutate) throws MalformedURLException {
		return postForm(bucketName, fields, null, "bar", null, null, null, mutate);
	}

	/** 에러 본문의 Code 를 꺼낸다. */
	private static String postErrorCode(MyResult result) {
		int start = result.message.indexOf("<Code>");
		int end = result.message.indexOf("</Code>");
		return start != -1 && end != -1 ? result.message.substring(start + 6, end) : "";
	}

	private static void assertPostError(MyResult result, int statusCode, String errorCode) {
		assertEquals(statusCode, result.statusCode, result.message);
		assertEquals(errorCode, postErrorCode(result), result.message);
	}

	private static void assertPostSuccess(MyResult result) {
		assertEquals(HttpStatus.SC_NO_CONTENT, result.statusCode, result.message);
	}

	private static int keyCount(AmazonS3 client, String bucketName) {
		return client.listObjectsV2(bucketName).getKeyCount();
	}

	private static String sha256Base64(String value) {
		try {
			var digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
			return Base64.getEncoder().encodeToString(digest);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	@Test
	@Tag("Upload")
	public void testPostObjectV4StoredFields() throws MalformedURLException {
		var key = "testPostObjectV4StoredFields";
		var client = getClient();
		var bucketName = createBucket(client, 37);
		var fields = formFields("key", key, "Content-Type", "image/png", "Cache-Control", "max-age=60",
				"Content-Disposition", "attachment; filename=a.txt", "Content-Encoding", "identity", "x-amz-meta-foo",
				"bar", "x-amz-server-side-encryption", "AES256", "x-amz-website-redirect-location", "/other.txt",
				"tagging", "<Tagging><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag></TagSet></Tagging>");
		assertPostSuccess(postForm(bucketName, fields, exactConditions(fields)));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(3L, response.getContentLength());
		assertEquals("image/png", response.getContentType());
		assertEquals("max-age=60", response.getCacheControl());
		assertEquals("attachment; filename=a.txt", response.getContentDisposition());
		assertEquals("identity", response.getContentEncoding());
		assertEquals(Map.of("foo", "bar"), response.getUserMetadata());
		assertEquals("AES256", response.getSSEAlgorithm());
		assertEquals("/other.txt", response.getRawMetadataValue("x-amz-website-redirect-location"));
		assertEquals(List.of(new com.amazonaws.services.s3.model.Tag("k1", "v1")),
				client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet());
	}

	@Test
	@Tag("Upload")
	public void testPostObjectV4Overwrite() throws MalformedURLException {
		var key = "testPostObjectV4Overwrite";
		var client = getClient();
		var bucketName = createBucket(client, 38);
		assertPostSuccess(postFormBody(bucketName, formFields("key", key), "first"));
		assertPostSuccess(postFormBody(bucketName, formFields("key", key), "second"));
		assertEquals("second", getBody(client.getObject(bucketName, key).getObjectContent()));
		// 빈 파일도 올릴 수 있다
		assertPostSuccess(postFormBody(bucketName, formFields("key", "empty"), ""));
		assertEquals(0L, client.getObjectMetadata(bucketName, "empty").getContentLength());
	}

	@Test
	@Tag("Versioning")
	public void testPostObjectV4Versioning() throws MalformedURLException {
		var key = "testPostObjectV4Versioning";
		var client = getClient();
		var bucketName = createBucket(client, 39);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		assertPostSuccess(postFormBody(bucketName, formFields("key", key), "first"));
		assertPostSuccess(postFormBody(bucketName, formFields("key", key), "second"));
		var versions = client.listVersions(bucketName, "").getVersionSummaries();
		assertEquals(2, versions.size());
		assertEquals("second", getBody(client.getObject(bucketName, key).getObjectContent()));
	}

	@Test
	@Tag("Policy")
	public void testPostObjectV4ConditionMatched() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 40);
		// 접두어가 맞는 키
		assertPostSuccess(postForm(bucketName, formFields("key", "user/a.txt"),
				conditions(rule("starts-with", "$key", "user/"))));
		// 빈 접두어는 아무 값이나 받는다
		assertPostSuccess(postForm(bucketName, formFields("key", "any", "x-amz-meta-foo", "anything"),
				conditions(rule("starts-with", "$x-amz-meta-foo", ""))));
		// 조건의 필드 이름은 대소문자를 구분하지 않는다
		assertPostSuccess(postForm(bucketName, formFields("key", "case", "x-amz-meta-foo", "value"),
				conditions(rule("starts-with", "$X-Amz-Meta-Foo", ""))));
		// x-ignore- 로 시작하는 필드는 조건이 없어도 된다
		assertPostSuccess(postForm(bucketName, formFields("key", "ignore", "x-ignore-foo", "bar")));
		assertEquals(4, keyCount(client, bucketName));
	}

	@Test
	@Tag("Policy")
	public void testPostObjectV4ConditionFailed() throws MalformedURLException {
		var key = "testPostObjectV4ConditionFailed";
		var client = getClient();
		var bucketName = createBucket(client, 41);
		// (폼 필드, 추가 조건)
		var fields = new ArrayList<Map<String, String>>();
		var extras = new ArrayList<JsonArray>();
		fields.add(formFields("key", "other/a.txt"));
		extras.add(conditions(rule("starts-with", "$key", "user/")));
		fields.add(formFields("key", key, "Content-Type", "text/html"));
		extras.add(conditions(rule("eq", "$Content-Type", "text/plain")));
		fields.add(formFields("key", key, "x-amz-meta-foo", "2"));
		extras.add(conditions(exact("x-amz-meta-foo", "1")));
		// eq 는 값의 대소문자를 구분한다
		fields.add(formFields("key", key, "x-amz-meta-foo", "ABC"));
		extras.add(conditions(rule("eq", "$x-amz-meta-foo", "abc")));
		// 정책에 있는 필드가 폼에 없다
		fields.add(formFields("key", key));
		extras.add(conditions(exact("x-amz-meta-foo", "bar")));
		// 폼에 있는 필드가 정책에 없다
		fields.add(formFields("key", key, "x-amz-meta-foo", "bar"));
		extras.add(conditions());
		for (int i = 0; i < fields.size(); i++)
			assertPostError(postForm(bucketName, fields.get(i), extras.get(i)), HttpStatus.SC_FORBIDDEN,
					MainData.ACCESS_DENIED);
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("Policy")
	public void testPostObjectV4RequiredConditions() throws MalformedURLException {
		var key = "testPostObjectV4RequiredConditions";
		var client = getClient();
		var bucketName = createBucket(client, 42);
		var expiration = getTimeToAddMinutes(100);

		// bucket, key, x-amz-algorithm, x-amz-credential, x-amz-date 조건은 모두 있어야 한다
		for (int i = 0; i < 5; i++) {
			var index = i;
			assertPostError(postFormPolicy(bucketName, formFields("key", key), base -> {
				var conditions = base.deepCopy();
				conditions.remove(index);
				return policyDocument(expiration, conditions);
			}), HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		}
		// 다른 버킷 이름을 적은 정책
		assertPostError(postFormPolicy(bucketName, formFields("key", key), base -> {
			var conditions = base.deepCopy();
			conditions.set(0, exact("bucket", "other-bucket-name"));
			return policyDocument(expiration, conditions);
		}), HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("Policy")
	public void testPostObjectV4ExpiredPolicy() throws MalformedURLException {
		var key = "testPostObjectV4ExpiredPolicy";
		var client = getClient();
		var bucketName = createBucket(client, 43);
		assertPostError(
				postFormPolicy(bucketName, formFields("key", key),
						base -> policyDocument(getTimeToAddMinutes(-10), base)),
				HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("Policy")
	public void testPostObjectV4InvalidPolicyDocument() throws MalformedURLException {
		var key = "testPostObjectV4InvalidPolicyDocument";
		var client = getClient();
		var bucketName = createBucket(client, 44);
		var expiration = getTimeToAddMinutes(100);

		var rawPolicies = List.of("!!!not-base64!!!",
				Base64.getEncoder().encodeToString("not json".getBytes(StandardCharsets.UTF_8)));
		for (var rawPolicy : rawPolicies)
			assertPostError(postFormRawPolicy(bucketName, formFields("key", key), rawPolicy),
					HttpStatus.SC_BAD_REQUEST, MainData.INVALID_POLICY_DOCUMENT);

		var policies = new ArrayList<Function<JsonArray, JsonObject>>();
		// expiration 누락
		policies.add(base -> policyDocument(null, base));
		// conditions 누락
		policies.add(base -> policyDocument(expiration, null));
		policies.add(base -> policyDocument(expiration, conditions()));
		// ISO8601 이 아니다
		policies.add(base -> policyDocument("2030-01-01 00:00:00", base));
		policies.add(base -> {
			var document = policyDocument(expiration, base);
			document.addProperty("unknown", "value");
			return document;
		});
		// 이름은 소문자여야 한다
		policies.add(base -> {
			var document = new JsonObject();
			document.addProperty("EXPIRATION", expiration);
			document.add("CONDITIONS", base);
			return document;
		});
		// 조건 형식이 잘못됐다
		policies.add(base -> policyDocument(expiration, plus(base, rule("ends-with", "$key", "txt"))));
		policies.add(base -> policyDocument(expiration, plus(base, rule("eq", "$key"))));
		policies.add(base -> policyDocument(expiration, plus(base, rule("content-length-range", 0))));
		for (var policy : policies)
			assertPostError(postFormPolicy(bucketName, formFields("key", key), policy), HttpStatus.SC_BAD_REQUEST,
					MainData.INVALID_POLICY_DOCUMENT);
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("Policy")
	public void testPostObjectV4ContentLengthRange() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 45);
		// 본문은 3바이트. 범위는 양 끝을 포함한다
		assertPostSuccess(
				postForm(bucketName, formFields("key", "max"), conditions(rule("content-length-range", 0, 3))));
		assertPostSuccess(
				postForm(bucketName, formFields("key", "min"), conditions(rule("content-length-range", 3, 10))));
		assertPostError(
				postForm(bucketName, formFields("key", "large"), conditions(rule("content-length-range", 0, 2))),
				HttpStatus.SC_BAD_REQUEST, MainData.ENTITY_TOO_LARGE);
		assertPostError(
				postForm(bucketName, formFields("key", "small"), conditions(rule("content-length-range", 4, 10))),
				HttpStatus.SC_BAD_REQUEST, MainData.ENTITY_TOO_SMALL);
		assertEquals(List.of("max", "min"), getKeys(client.listObjectsV2(bucketName).getObjectSummaries()));
	}

	@Test
	@Tag("Auth")
	public void testPostObjectV4InvalidSignature() throws MalformedURLException {
		var key = "testPostObjectV4InvalidSignature";
		var client = getClient();
		var bucketName = createBucket(client, 46);

		assertPostError(
				postFormMutated(bucketName, formFields("key", key),
						payload -> payload.put("x-amz-signature", "0".repeat(64))),
				HttpStatus.SC_FORBIDDEN, MainData.SIGNATURE_DOES_NOT_MATCH);
		assertPostError(postFormMutated(bucketName, formFields("key", key), payload -> {
			var scope = payload.get("x-amz-credential").split("/", 2)[1];
			payload.put("x-amz-credential", "AKIAXXXXXXXXXXXXXXXX/" + scope);
		}), HttpStatus.SC_FORBIDDEN, MainData.INVALID_ACCESS_KEY_ID);
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("Auth")
	public void testPostObjectV4InvalidAuthField() throws MalformedURLException {
		var key = "testPostObjectV4InvalidAuthField";
		var client = getClient();
		var bucketName = createBucket(client, 47);

		var mutations = new ArrayList<Consumer<Map<String, String>>>();
		// 서명 필드가 하나라도 없다
		mutations.add(payload -> payload.remove("x-amz-signature"));
		mutations.add(payload -> payload.remove("x-amz-date"));
		mutations.add(payload -> payload.remove("x-amz-algorithm"));
		mutations.add(payload -> payload.remove("x-amz-credential"));
		// 값이 잘못됐다
		mutations.add(payload -> payload.put("x-amz-algorithm", "AWS4-HMAC-SHA1"));
		mutations.add(payload -> payload.put("x-amz-date", "2030-01-01"));
		mutations.add(payload -> payload.put("x-amz-credential", "malformed"));
		mutations.add(payload -> payload.put("x-amz-credential",
				payload.get("x-amz-credential").replace("/s3/aws4_request", "/ec2/aws4_request")));
		for (var mutate : mutations)
			assertPostError(postFormMutated(bucketName, formFields("key", key), mutate), HttpStatus.SC_BAD_REQUEST,
					MainData.INVALID_ARGUMENT);
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("Access")
	public void testPostObjectV4AccessDenied() throws MalformedURLException {
		var key = "testPostObjectV4AccessDenied";
		var client = getClient();
		var bucketName = createBucket(client, 48);
		// 쓰기 권한이 없는 사용자가 서명한 폼
		assertPostError(postForm(bucketName, formFields("key", key), null, "bar", config.altUser, null, null, null),
				HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		// 서명 없는 폼
		var result = NetUtils.postUpload(createURL(bucketName), formFields("key", key),
				new FormFile("foo.txt", "text/plain", "bar"));
		assertPostError(result, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectV4InvalidKey() throws MalformedURLException {
		var client = getClient();
		var bucketName = createBucket(client, 49);
		assertPostError(postForm(bucketName, formFields()), HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT);
		assertPostError(postForm(bucketName, formFields("key", "")), HttpStatus.SC_BAD_REQUEST,
				MainData.INVALID_ARGUMENT);
		// 키는 1024바이트까지
		assertPostError(postForm(bucketName, formFields("key", "k".repeat(1025))), HttpStatus.SC_BAD_REQUEST,
				MainData.KEY_TOO_LONG);
		assertPostSuccess(postForm(bucketName, formFields("key", "k".repeat(1024))));
		assertEquals(1, keyCount(client, bucketName));
	}

	@Test
	@Tag("ERROR")
	public void testPostObjectV4InvalidFieldValue() throws MalformedURLException {
		var key = "testPostObjectV4InvalidFieldValue";
		var client = getClient();
		var bucketName = createBucket(client, 50);
		var duplicatedTag = "<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag>"
				+ "<Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>";
		// (필드 이름, 값, 에러 코드)
		var cases = List.of(
				List.of("acl", "invalid", MainData.INVALID_ARGUMENT),
				List.of("x-amz-server-side-encryption", "AES128", MainData.INVALID_ARGUMENT),
				List.of("tagging", "<Tagging>", MainData.MALFORMED_XML),
				List.of("tagging", duplicatedTag, MainData.INVALID_TAG),
				List.of("x-amz-website-redirect-location", "invalid", MainData.INVALID_REDIRECT_LOCATION),
				List.of("x-amz-meta-big", "v".repeat(2100), MainData.METADATA_TOO_LARGE));
		for (var item : cases) {
			var name = item.get(0);
			var result = postForm(bucketName, formFields("key", key, name, item.get(1)),
					conditions(rule("starts-with", "$" + name, "")));
			assertPostError(result, HttpStatus.SC_BAD_REQUEST, item.get(2));
		}
		assertEquals(0, keyCount(client, bucketName));
	}

	@Test
	@Tag("Checksum")
	public void testPostObjectV4ContentMd5() throws MalformedURLException {
		var key = "testPostObjectV4ContentMd5";
		var client = getClient();
		var bucketName = createBucket(client, 51);

		for (var md5 : List.of(Utils.getMD5("other"), "invalid")) {
			var result = postForm(bucketName, formFields("key", key, "Content-MD5", md5),
					conditions(exact("Content-MD5", md5)));
			assertPostError(result, HttpStatus.SC_BAD_REQUEST,
					md5.equals("invalid") ? MainData.INVALID_DIGEST : MainData.BAD_DIGEST);
		}
		assertEquals(0, keyCount(client, bucketName));
		var md5 = Utils.getMD5("bar");
		assertPostSuccess(postForm(bucketName, formFields("key", key, "Content-MD5", md5),
				conditions(exact("Content-MD5", md5))));
		assertEquals("bar", getBody(client.getObject(bucketName, key).getObjectContent()));
	}

	@Test
	@Tag("Checksum")
	public void testPostObjectV4Checksum() throws MalformedURLException {
		var key = "testPostObjectV4Checksum";
		var client = getClient();
		var bucketName = createBucket(client, 52);

		var badFields = formFields("key", key, "x-amz-checksum-algorithm", "SHA256", "x-amz-checksum-sha256",
				sha256Base64("other"));
		assertPostError(postForm(bucketName, badFields, exactConditions(badFields)), HttpStatus.SC_BAD_REQUEST,
				MainData.BAD_DIGEST);
		assertEquals(0, keyCount(client, bucketName));
		var fields = formFields("key", key, "x-amz-checksum-algorithm", "SHA256", "x-amz-checksum-sha256",
				sha256Base64("bar"));
		assertPostSuccess(postForm(bucketName, fields, exactConditions(fields)));
		// SDK v1 에는 ChecksumMode 가 없어 헤더를 직접 붙인다
		var request = new GetObjectMetadataRequest(bucketName, key);
		request.putCustomRequestHeader("x-amz-checksum-mode", "ENABLED");
		var response = client.getObjectMetadata(request);
		assertEquals(sha256Base64("bar"), response.getRawMetadataValue("x-amz-checksum-sha256"));
	}

	@Test
	@Tag("ACL")
	public void testPostObjectV4Acl() throws MalformedURLException {
		var client = getClient();
		// ACL 을 쓰는 버킷: acl 필드가 오브젝트 ACL 이 된다
		var bucketName = createBucketCannedAcl(client, 53);
		assertPostSuccess(postForm(bucketName, formFields("key", "public", "acl", "public-read"),
				conditions(exact("acl", "public-read"))));
		var grants = new ArrayList<String>();
		for (var grant : client.getObjectAcl(bucketName, "public").getGrantsAsList())
			grants.add((grant.getGrantee() instanceof GroupGrantee ? grant.getGrantee().getIdentifier() : "") + " "
					+ grant.getPermission().toString());
		Collections.sort(grants);
		assertEquals(List.of(" FULL_CONTROL", MainData.ALL_USERS + " READ"), grants);

		// ACL 을 끈 버킷: 소유자 전권을 뜻하는 값만 받는다
		var enforced = createBucket(client, 53, ObjectOwnership.BucketOwnerEnforced);
		for (var acl : List.of("private", "bucket-owner-full-control"))
			assertPostSuccess(
					postForm(enforced, formFields("key", acl, "acl", acl), conditions(exact("acl", acl))));
		assertPostError(
				postForm(enforced, formFields("key", "public", "acl", "public-read"),
						conditions(exact("acl", "public-read"))),
				HttpStatus.SC_BAD_REQUEST, MainData.ACCESS_CONTROL_LIST_NOT_SUPPORTED);
		assertEquals(2, keyCount(client, enforced));
	}

	// endregion
}
