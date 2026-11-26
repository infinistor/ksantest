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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.MetadataDirective;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.amazonaws.services.s3.AmazonS3;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.function.Executable;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.TagSet;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;

public class Policy extends TestBase {
	@Test
	@Tag("Check")
	public void testBucketPolicy() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 1);
		var key = "testBucketPolicy";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);

		statement.addProperty(MainData.POLICY_PRINCIPAL, "*");
		statement.addProperty(MainData.POLICY_ACTION, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.POLICY_RESOURCE, resources);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var response = altClient.listObjects(bucketName);
		assertEquals(1, response.getObjectSummaries().size());

		var getResponse = client.getBucketPolicy(bucketName);
		assertEquals(policyDocument.toString(), getResponse.getPolicyText());
	}

	@Test
	@Tag("Check")
	public void testBucketV2Policy() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 2);
		var key = "testBucketV2Policy";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);
		statement.addProperty(MainData.POLICY_PRINCIPAL, "*");
		statement.addProperty(MainData.POLICY_ACTION, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.POLICY_RESOURCE, resources);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var response = altClient.listObjectsV2(bucketName);
		assertEquals(1, response.getObjectSummaries().size());

		var getResponse = client.getBucketPolicy(bucketName);
		assertEquals(policyDocument.toString(), getResponse.getPolicyText());
	}

	@Test
	@Tag("Priority")
	public void testBucketPolicyAcl() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 3);
		var key = "testBucketPolicyAcl";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_DENY);

		var principal = new JsonObject();
		principal.addProperty("AWS", "*");
		statement.add(MainData.POLICY_PRINCIPAL, principal);

		statement.addProperty(MainData.POLICY_ACTION, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.POLICY_RESOURCE, resources);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead);
		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Priority")
	public void testBucketV2PolicyAcl() {

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 4);
		var key = "testBucketV2PolicyAcl";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_DENY);

		var principal = new JsonObject();
		principal.addProperty("AWS", "*");
		statement.add(MainData.POLICY_PRINCIPAL, principal);

		statement.addProperty(MainData.POLICY_ACTION, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.POLICY_RESOURCE, resources);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead);
		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var e = assertThrows(AmazonServiceException.class, () -> altClient.listObjectsV2(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Tagging")
	public void testGetTagsAclPublic() {
		var key = "testGetTagsAclPublic";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 5);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, key));
		var policyDocument = makeJsonPolicy("s3:GetObjectTagging", resource, null, null);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(10));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var altClient = getAltClient();

		var getResponse = altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), getResponse.getTagSet());
	}

	@Test
	@Tag("Tagging")
	public void testPutTagsAclPublic() {
		var key = "testPutTagsAclPublic";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 6);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, key));
		var policyDocument = makeJsonPolicy("s3:PutObjectTagging", resource, null, null);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(10));
		var altClient = getAltClient();
		altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), getResponse.getTagSet());
	}

	@Test
	@Tag("Tagging")
	public void testDeleteTagsObjPublic() {
		var key = "testDeleteTagsObjPublic";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 7);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, key));
		var policyDocument = makeJsonPolicy("s3:DeleteObjectTagging", resource, null, null);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var inputTagSet = new ObjectTagging(makeSimpleTagSet(10));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var altClient = getAltClient();
		altClient.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, key));

		var getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		assertEquals(0, getResponse.getTagSet().size());
	}

	@Test
	@Tag("TagOptions")
	public void testBucketPolicyGetObjExistingTag() {
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 8);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", resource, null, tagConditional);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		tags.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		tags.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var inputTagSet = new ObjectTagging(tags);

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag, inputTagSet));

		tags.clear();
		tags.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		inputTagSet.setTagSet(tags);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privateTag, inputTagSet));

		tags.clear();
		tags.add(new com.amazonaws.services.s3.model.Tag("security1", "public"));
		inputTagSet.setTagSet(tags);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidTag, inputTagSet));

		var altClient = getAltClient();
		var getResponse = altClient.getObject(bucketName, publicTag);
		assertNotNull(getResponse);

		var e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, privateTag));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());

		e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, invalidTag));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	@Test
	@Tag("TagOptions")
	public void testBucketPolicyGetObjTaggingExistingTag() {
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 9);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObjectTagging", resource, null, tagConditional);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		tags.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		tags.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var inputTagSet = new ObjectTagging(tags);

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag, inputTagSet));

		tags.clear();
		tags.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		inputTagSet.setTagSet(tags);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privateTag, inputTagSet));

		tags.clear();
		tags.add(new com.amazonaws.services.s3.model.Tag("security1", "public"));
		inputTagSet.setTagSet(tags);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidTag, inputTagSet));

		var altClient = getAltClient();
		var getResponse = altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, publicTag));
		assertNotNull(getResponse);

		var e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, publicTag));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privateTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	@Test
	@Tag("TagOptions")
	public void testBucketPolicyPutObjTaggingExistingTag() {
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 10);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:PutObjectTagging", resource, null, tagConditional);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag,
				new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security", "public"),
						new com.amazonaws.services.s3.model.Tag("foo", "bar")))));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privateTag,
				new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security", "private")))));

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidTag,
				new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security1", "public")))));

		var testTagSet = new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security", "public"),
				new com.amazonaws.services.s3.model.Tag("foo", "bar")));

		var altClient = getAltClient();
		altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag, testTagSet));

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, privateTag, testTagSet)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());

		altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag,
				new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security", "private")))));

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag,
						new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security", "public"),
								new com.amazonaws.services.s3.model.Tag("foo", "bar"))))));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	@Test
	@Tag("PathOptions")
	public void testBucketPolicyPutObjCopySource() {
		var publicFoo = "public/foo";
		var publicBar = "public/bar";
		var privateFoo = "private/foo";
		var client = getClient();
		var altClient = getAltClient();
		var sourceBucketName = createBucketCannedAcl(client, 11);
		var targetBucketName = createBucketCannedAcl(client, 11);

		createObjects(client, sourceBucketName, List.of(publicFoo, publicBar, privateFoo));

		var sourceResource = Utils.makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.setBucketPolicy(sourceBucketName, policyDocument.toString());

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-copy-source", String.format("/%s/public/*", sourceBucketName));
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", targetBucketName, "*"));
		var policyDocument2 = makeJsonPolicy("s3:PutObject", resource, null, tagConditional);
		client.setBucketPolicy(targetBucketName, policyDocument2.toString());

		var newFoo = "newFoo";
		altClient.copyObject(sourceBucketName, publicFoo, targetBucketName, newFoo);

		var response = altClient.getObject(targetBucketName, newFoo);
		var body = getBody(response.getObjectContent());
		assertEquals(publicFoo, body);

		var newFoo2 = "newFoo2";
		altClient.copyObject(sourceBucketName, publicBar, targetBucketName, newFoo2);

		response = altClient.getObject(targetBucketName, newFoo2);
		body = getBody(response.getObjectContent());
		assertEquals(publicBar, body);

		assertThrows(AmazonServiceException.class,
				() -> altClient.copyObject(sourceBucketName, privateFoo, targetBucketName, newFoo2));
	}

	@Test
	@Tag("MetadataOptions")
	public void testBucketPolicyPutObjCopySourceMeta() {
		var publicFoo = "public/foo";
		var publicBar = "public/bar";
		var client = getClient();
		var sourceBucketName = createBucketCannedAcl(client, 12);
		createObjects(client, sourceBucketName, List.of(publicFoo, publicBar));

		var sourceResource = Utils.makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.setBucketPolicy(sourceBucketName, policyDocument.toString());

		var targetBucketName = createBucketCannedAcl(client, 12);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-metadata-directive", "COPY");
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", targetBucketName, "*"));
		policyDocument = makeJsonPolicy("s3:PutObject", resource, null, s3Conditional);
		client.setBucketPolicy(targetBucketName, policyDocument.toString());

		var altClient = getAltClient();
		var newFoo = "newFoo";
		altClient.copyObject(new CopyObjectRequest(sourceBucketName, publicFoo, targetBucketName, newFoo)
				.withMetadataDirective(MetadataDirective.COPY));

		var response = altClient.getObject(targetBucketName, newFoo);
		var body = getBody(response.getObjectContent());
		assertEquals(publicFoo, body);

		var newFoo2 = "newFoo2";
		assertThrows(AmazonServiceException.class,
				() -> altClient.copyObject(sourceBucketName, publicBar, targetBucketName, newFoo2));

		assertThrows(AmazonServiceException.class,
				() -> altClient.copyObject(new CopyObjectRequest(sourceBucketName, publicBar, targetBucketName, newFoo2)
						.withMetadataDirective(MetadataDirective.REPLACE)));
	}

	@Test
	@Tag("ACLOptions")
	public void testBucketPolicyPutObjAcl() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 13);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-acl", "public*");
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, "*"));
		var s1 = makeJsonStatement("s3:PutObject", resource, null, null, null);
		var s2 = makeJsonStatement("s3:PutObject", resource, MainData.POLICY_EFFECT_DENY, null, tagConditional);
		var policyDocument = makeJsonPolicy(s1, s2);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var key1 = "private-key";

		altClient.putObject(bucketName, key1, key1);

		var key2 = "public-key";
		var headers = new ObjectMetadata();
		headers.setHeader("x-amz-acl", "public-read");
		headers.setContentType("text/plain");
		headers.setContentLength(key2.length());

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.putObject(bucketName, key2, createBody(key2), headers));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	@Test
	@Tag("GrantOptions")
	public void testBucketPolicyPutObjGrant() {
		var client = getClient();
		var bucketName1 = createBucketCannedAcl(client, 14);
		var bucketName2 = createBucketCannedAcl(client, 14);

		var mainUserId = config.mainUser.id;
		var altUserId = config.altUser.id;

		var ownerId = "id=" + mainUserId;

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-grant-full-control", ownerId);
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName1, "*"));
		var policyDocument = makeJsonPolicy("s3:PutObject", resource, null, s3Conditional);

		var resource2 = Utils.makeArnResource(String.format("%s/%s", bucketName2, "*"));
		var policyDocument2 = makeJsonPolicy("s3:PutObject", resource2, null, null);

		client.setBucketPolicy(bucketName1, policyDocument.toString());
		client.setBucketPolicy(bucketName2, policyDocument2.toString());

		var altClient = getAltClient();
		var key1 = "key1";

		var headers = new ObjectMetadata();
		headers.setHeader("x-amz-grant-full-control", ownerId);
		headers.setContentType("text/plain");
		headers.setContentLength(key1.length());

		altClient.putObject(bucketName1, key1, createBody(key1), headers);

		var key2 = "key2";
		altClient.putObject(bucketName2, key2, key2);

		var acl1Response = client.getObjectAcl(bucketName1, key1);

		assertThrows(AmazonServiceException.class, () -> client.getObjectAcl(bucketName2, key2));

		var acl2Response = altClient.getObjectAcl(bucketName2, key2);

		assertEquals(mainUserId, acl1Response.getGrantsAsList().get(0).getGrantee().getIdentifier());
		assertEquals(altUserId, acl2Response.getGrantsAsList().get(0).getGrantee().getIdentifier());
	}

	@Test
	@Tag("TagOptions")
	public void testBucketPolicyGetObjAclExistingTag() {
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 15);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObjectAcl", resource, null, tagConditional);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		tags.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		tags.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var inputTagSet = new ObjectTagging(tags);

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag, inputTagSet));

		tags.clear();
		tags.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		inputTagSet.setTagSet(tags);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privateTag, inputTagSet));

		tags.clear();
		tags.add(new com.amazonaws.services.s3.model.Tag("security1", "public"));
		inputTagSet.setTagSet(tags);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidTag, inputTagSet));

		var altClient = getAltClient();
		var aclResponse = altClient.getObjectAcl(bucketName, publicTag);
		assertNotNull(aclResponse);

		var e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, publicTag));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privateTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithAllUser() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 16);

		assertThrows(AmazonServiceException.class,
				() -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));

		var resource1 = MainData.POLICY_RESOURCE_PREFIX + bucketName;
		var resource2 = MainData.POLICY_RESOURCE_PREFIX + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);

		var principal = new JsonObject();
		principal.addProperty("AWS", "*");
		statement.add(MainData.POLICY_PRINCIPAL, principal);

		statement.addProperty(MainData.POLICY_ACTION, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.POLICY_RESOURCE, resources);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertTrue(response.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithSpecificUserAccess() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 17);

		assertThrows(AmazonServiceException.class,
				() -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));

		var resource1 = MainData.POLICY_RESOURCE_PREFIX + bucketName;
		var resource2 = MainData.POLICY_RESOURCE_PREFIX + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);

		var principal = new JsonObject();
		principal.addProperty("CanonicalUser", config.mainUser.id);
		statement.add(MainData.POLICY_PRINCIPAL, principal);

		statement.addProperty(MainData.POLICY_ACTION, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.POLICY_RESOURCE, resources);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertFalse(response.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithWideIPRange() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 18);

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();
		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);

		var principal = new JsonObject();
		principal.addProperty("AWS", "*");
		statement.add(MainData.POLICY_PRINCIPAL, principal);

		statement.addProperty(MainData.POLICY_ACTION, "s3:GetObject");

		var resources = new JsonArray();
		resources.add(Utils.makeArnResource(String.format("%s/*", bucketName)));
		statement.add(MainData.POLICY_RESOURCE, resources);

		var ipAddress = new JsonObject();
		ipAddress.addProperty("aws:SourceIp", "0.0.0.0/1");
		var condition = new JsonObject();
		condition.add("IpAddress", ipAddress);
		statement.add(MainData.POLICY_CONDITION, condition);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertTrue(response.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithIPRange() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 19);

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();
		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);

		var principal = new JsonObject();
		principal.addProperty("AWS", "*");
		statement.add(MainData.POLICY_PRINCIPAL, principal);

		statement.addProperty(MainData.POLICY_ACTION, "s3:GetObject");

		var resources = new JsonArray();
		resources.add(Utils.makeArnResource(String.format("%s/*", bucketName)));
		statement.add(MainData.POLICY_RESOURCE, resources);

		var ipAddress = new JsonObject();
		ipAddress.addProperty("aws:SourceIp", "127.0.0.1/32");
		var condition = new JsonObject();
		condition.add("IpAddress", ipAddress);
		statement.add(MainData.POLICY_CONDITION, condition);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertFalse(response.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithTimeCondition() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 20);

		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));
		
		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();

		// 매우 제한적인 시간대에만 허용하는 정책 (1분)
		var allowStatement = new JsonObject();
		allowStatement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);
		var allowPrincipal = new JsonObject();
		allowPrincipal.addProperty("AWS", "*");
		allowStatement.add(MainData.POLICY_PRINCIPAL, allowPrincipal);
		allowStatement.addProperty(MainData.POLICY_ACTION, "s3:GetObject");
		allowStatement.addProperty(MainData.POLICY_RESOURCE, resource);

		// 매우 제한적인 시간 조건 추가 (1초)
		var condition = new JsonObject();
		var now = java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC);

		var startTimeCondition = new JsonObject();
		startTimeCondition.addProperty("aws:CurrentTime", now.plusMinutes(10).toString());
		condition.add("DateGreaterThan", startTimeCondition);
		
		var endTimeCondition = new JsonObject();
		endTimeCondition.addProperty("aws:CurrentTime", now.plusMinutes(10).plusSeconds(1).toString());
		condition.add("DateLessThan", endTimeCondition);
		
		allowStatement.add(MainData.POLICY_CONDITION, condition);
		statements.add(allowStatement);

		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertTrue(response.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithTagCondition() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 21);

		var resource = Utils.makeArnResource(String.format("%s/*", bucketName));
		
		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();
		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);
		
		var principal = new JsonObject();
		principal.addProperty("AWS", "*");
		statement.add(MainData.POLICY_PRINCIPAL, principal);
		
		statement.addProperty(MainData.POLICY_ACTION, "s3:GetObject");
		statement.addProperty(MainData.POLICY_RESOURCE, resource);

		// 태그 조건 추가
		var tagCondition = new JsonObject();
		tagCondition.addProperty("s3:ExistingObjectTag/access", "restricted");
		var condition = new JsonObject();
		condition.add("StringEquals", tagCondition);
		statement.add(MainData.POLICY_CONDITION, condition);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertTrue(response.getPolicyStatus().getIsPublic());
	}

	// region 버킷 정책 API 검증
	/** 모든 주체의 GetObject 를 거부하는 문장. Deny 는 s3:GetObject 로만 한정한다(버킷 정리가 막히지 않게). */
	static JsonObject denyStatement(String bucketName) {
		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_DENY);
		statement.addProperty(MainData.POLICY_PRINCIPAL, "*");
		statement.addProperty(MainData.POLICY_ACTION, "s3:GetObject");
		statement.addProperty(MainData.POLICY_RESOURCE, MainData.POLICY_RESOURCE_PREFIX + bucketName + "/*");
		return statement;
	}

	static String policyDocumentOf(String version, JsonObject... statements) {
		var list = new JsonArray();
		for (var statement : statements)
			list.add(statement);
		var document = new JsonObject();
		document.addProperty(MainData.POLICY_VERSION, version);
		document.add(MainData.POLICY_STATEMENT, list);
		return document.toString();
	}

	static String policyDocumentOf(JsonObject... statements) {
		return policyDocumentOf(MainData.POLICY_VERSION_DATE, statements);
	}

	static JsonObject conditionOf(String operator, String conditionKey, String value) {
		var values = new JsonObject();
		values.addProperty(conditionKey, value);
		var condition = new JsonObject();
		condition.add(operator, values);
		return condition;
	}

	static void assertMalformedPolicy(AmazonS3 client, String bucketName, String policy) {
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_POLICY,
				() -> client.setBucketPolicy(bucketName, policy));
	}

	@Test
	@Tag("Get")
	public void testBucketPolicyGetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 22);

		// SDK V1 의 getBucketPolicy 는 NoSuchBucketPolicy 를 삼키고 빈 정책을 돌려준다
		assertNull(client.getBucketPolicy(bucketName).getPolicyText());
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET_POLICY,
				() -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));
	}

	@Test
	@Tag("Delete")
	public void testBucketPolicyDeleteNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 23);

		// 정책이 없어도 삭제는 성공한다
		client.deleteBucketPolicy(bucketName);
	}

	@Test
	@Tag("Check")
	public void testBucketPolicyPutGetDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 24);
		var statement = denyStatement(bucketName);
		statement.addProperty("Sid", "deny-get");
		var policy = policyDocumentOf(statement);
		client.setBucketPolicy(bucketName, policy);

		assertEquals(JsonParser.parseString(policy),
				JsonParser.parseString(client.getBucketPolicy(bucketName).getPolicyText()));
		assertFalse(client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName))
				.getPolicyStatus().getIsPublic());

		client.deleteBucketPolicy(bucketName);
		// SDK V1 의 getBucketPolicy 는 NoSuchBucketPolicy 를 삼키고 빈 정책을 돌려준다
		assertNull(client.getBucketPolicy(bucketName).getPolicyText());
	}

	@Test
	@Tag("ERROR")
	public void testBucketPolicyBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(25);
		var policy = policyDocumentOf(denyStatement(bucketName));

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET, () -> client.getBucketPolicy(bucketName));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.setBucketPolicy(bucketName, policy));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET, () -> client.deleteBucketPolicy(bucketName));
	}

	@Test
	@Tag("Access")
	public void testBucketPolicyAccessDenied() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 26);
		var policy = policyDocumentOf(denyStatement(bucketName));
		client.setBucketPolicy(bucketName, policy);

		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, () -> altClient.getBucketPolicy(bucketName));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.setBucketPolicy(bucketName, policy));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.deleteBucketPolicy(bucketName));
		// 정책은 그대로 남아 있다
		assertEquals(JsonParser.parseString(policy),
				JsonParser.parseString(client.getBucketPolicy(bucketName).getPolicyText()));
	}

	@Test
	@Tag("ERROR")
	public void testBucketPolicyInvalidJson() {
		var client = getClient();
		var bucketName = createBucket(client, 27);

		assertMalformedPolicy(client, bucketName, "not json");
		// Statement 가 비어 있다
		assertMalformedPolicy(client, bucketName, policyDocumentOf());
		assertMalformedPolicy(client, bucketName, policyDocumentOf("2020-01-01", denyStatement(bucketName)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketPolicyInvalidElement() {
		var client = getClient();
		var bucketName = createBucket(client, 28);

		var invalidEffect = denyStatement(bucketName);
		invalidEffect.addProperty(MainData.POLICY_EFFECT, "Maybe");

		var invalidAction = denyStatement(bucketName);
		invalidAction.addProperty(MainData.POLICY_ACTION, "s3:Invalid");

		var invalidPrincipal = denyStatement(bucketName);
		var principal = new JsonObject();
		principal.addProperty("AWS", "arn:aws:iam::000000000000:user/not-exist");
		invalidPrincipal.add(MainData.POLICY_PRINCIPAL, principal);

		// 없는 조건 키
		var invalidConditionKey = denyStatement(bucketName);
		invalidConditionKey.add(MainData.POLICY_CONDITION, conditionOf("StringEquals", "s3:invalid", "value"));

		// 없는 조건 연산자
		var invalidOperator = denyStatement(bucketName);
		invalidOperator.add(MainData.POLICY_CONDITION, conditionOf("InvalidEquals", "aws:SourceIp", "1.1.1.1"));

		for (var statement : List.of(invalidEffect, invalidAction, invalidPrincipal, invalidConditionKey,
				invalidOperator))
			assertMalformedPolicy(client, bucketName, policyDocumentOf(statement));

		// 정책이 저장되지 않았다(SDK V1 은 NoSuchBucketPolicy 를 빈 정책으로 돌려준다)
		assertNull(client.getBucketPolicy(bucketName).getPolicyText());
	}

	@Test
	@Tag("ERROR")
	public void testBucketPolicyInvalidResource() {
		var client = getClient();
		var bucketName = createBucket(client, 29);

		// 다른 버킷의 리소스
		var otherBucket = denyStatement(bucketName);
		otherBucket.addProperty(MainData.POLICY_RESOURCE, "arn:aws:s3:::other-bucket-name/*");
		assertMalformedPolicy(client, bucketName, policyDocumentOf(otherBucket));

		// 버킷 대상 액션에 오브젝트 리소스
		var bucketAction = denyStatement(bucketName);
		bucketAction.addProperty(MainData.POLICY_ACTION, "s3:ListBucket");
		assertMalformedPolicy(client, bucketName, policyDocumentOf(bucketAction));

		// Resource, Principal 누락
		for (var missing : List.of(MainData.POLICY_RESOURCE, MainData.POLICY_PRINCIPAL)) {
			var statement = denyStatement(bucketName);
			statement.remove(missing);
			assertMalformedPolicy(client, bucketName, policyDocumentOf(statement));
		}
	}

	@Test
	@Tag("Max")
	public void testBucketPolicyTooLarge() {
		var client = getClient();
		var bucketName = createBucket(client, 30);

		// 정책 문서는 20KB 까지
		var statements = new JsonObject[70];
		for (int i = 0; i < statements.length; i++) {
			statements[i] = denyStatement(bucketName);
			statements[i].addProperty("Sid", "statement" + i);
			statements[i].add(MainData.POLICY_CONDITION,
					conditionOf("StringEquals", "aws:Referer", "x".repeat(180)));
		}
		var policy = policyDocumentOf(statements);
		assertTrue(policy.length() > 20480);
		assertMalformedPolicy(client, bucketName, policy);
	}
	// endregion

	// region 정책 효과 (AWS 실측 기준)
	// 정책을 바꾼 뒤에는 새 클라이언트로 확인한다(같은 연결은 바뀌기 전 권한으로 답할 수 있다).
	// Deny 는 정리에 필요 없는 액션(GetObject, PutObject 등)으로만 건다.

	static JsonElement jsonOf(Object value) {
		if (value instanceof JsonElement element)
			return element;
		if (value instanceof List<?> list) {
			var array = new JsonArray();
			for (var item : list)
				array.add(jsonOf(item));
			return array;
		}
		return new JsonPrimitive(value.toString());
	}

	/** 항목을 바꾼 객체를 돌려준다. value 가 null 이면 그 항목을 뺀다(NotAction 등으로 바꿀 때). */
	static JsonObject with(JsonObject object, String name, Object value) {
		if (value == null)
			object.remove(name);
		else
			object.add(name, jsonOf(value));
		return object;
	}

	static JsonObject jsonObjectOf(String name, Object value) {
		return with(new JsonObject(), name, value);
	}

	static String arnOf(String resource) {
		return MainData.POLICY_RESOURCE_PREFIX + resource;
	}

	JsonObject altPrincipal() {
		return jsonObjectOf("CanonicalUser", config.altUser.id);
	}

	/** 주체가 서브 사용자인 문장을 만든다. action, resource 는 문자열이나 목록이고, null 이면 넣지 않는다. */
	JsonObject statementOf(String effect, Object action, Object resource) {
		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, effect);
		statement.add(MainData.POLICY_PRINCIPAL, altPrincipal());
		with(statement, MainData.POLICY_ACTION, action);
		with(statement, MainData.POLICY_RESOURCE, resource);
		return statement;
	}

	static void putPolicy(AmazonS3 client, String bucketName, JsonObject... statements) {
		client.setBucketPolicy(bucketName, policyDocumentOf(statements));
	}

	static void assertDenied(Executable executable) {
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, executable);
	}

	/** 본문이 문자열인 업로드 요청. 태그·암호화 헤더를 덧붙일 때 쓴다. */
	static PutObjectRequest putRequestOf(String bucketName, String key, String body, String sseAlgorithm) {
		var bytes = body.getBytes(StandardCharsets.UTF_8);
		var metadata = new ObjectMetadata();
		metadata.setContentLength(bytes.length);
		if (sseAlgorithm != null)
			metadata.setSSEAlgorithm(sseAlgorithm);
		return new PutObjectRequest(bucketName, key, new ByteArrayInputStream(bytes), metadata);
	}

	static ObjectTagging taggingOf(String... keyValues) {
		var tags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		for (int i = 0; i < keyValues.length; i += 2)
			tags.add(new com.amazonaws.services.s3.model.Tag(keyValues[i], keyValues[i + 1]));
		return new ObjectTagging(tags);
	}

	static UploadPartRequest partRequestOf(String bucketName, String key, String uploadId, int partNumber) {
		return new UploadPartRequest().withBucketName(bucketName).withKey(key).withUploadId(uploadId)
				.withPartNumber(partNumber).withInputStream(new ByteArrayInputStream(new byte[] { 'x' }))
				.withPartSize(1);
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyAllowUser() {
		var key = "testBucketPolicyAllowUser";
		var client = getClient();
		var bucketName = createBucket(client, 31);
		client.putObject(bucketName, key, "content");
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")));
		assertFalse(client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName))
				.getPolicyStatus().getIsPublic());

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		assertEquals(7, altClient.getObjectMetadata(bucketName, key).getContentLength());
		// 허용한 액션 말고는 그대로 거부된다
		assertDenied(() -> altClient.putObject(bucketName, "new", "x"));
		assertDenied(() -> altClient.listObjectsV2(bucketName));
		assertDenied(() -> altClient.getObjectAcl(bucketName, key));
		assertDenied(() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)));
		assertDenied(() -> altClient.deleteObject(bucketName, key));
		// 목록 권한이 없으면 없는 키도 403 이다
		assertDenied(() -> altClient.getObject(bucketName, "not-exist"));
		// 정책에 없는 주체
		assertDenied(() -> getPublicClient().getObject(bucketName, key));
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyDenyOverridesAllow() {
		var client = getClient();
		var bucketName = createBucket(client, 32);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		putPolicy(client, bucketName,
				statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")),
				statementOf("Deny", "s3:GetObject", arnOf(bucketName + "/private/*")));

		var altClient = getAltClient();
		assertEquals("public/a", getBody(altClient.getObject(bucketName, "public/a").getObjectContent()));
		assertDenied(() -> altClient.getObject(bucketName, "private/a"));
		// Deny 의 주체가 아닌 소유자는 읽을 수 있다
		assertEquals("private/a", getBody(getClient().getObject(bucketName, "private/a").getObjectContent()));
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyDenyOwner() {
		var key = "testBucketPolicyDenyOwner";
		var client = getClient();
		var bucketName = createBucket(client, 33);
		client.putObject(bucketName, key, "content");
		putPolicy(client, bucketName, with(statementOf("Deny", "s3:GetObject", arnOf(bucketName + "/*")),
				MainData.POLICY_PRINCIPAL, "*"));

		// 명시적 Deny 는 버킷 소유자에게도 적용된다
		var owner = getClient();
		assertDenied(() -> owner.getObject(bucketName, key));
		assertS3Error(HttpStatus.SC_FORBIDDEN, null, () -> owner.getObjectMetadata(bucketName, key));
		assertDenied(() -> owner.copyObject(bucketName, key, bucketName, "copy"));
		// 거부하지 않은 액션은 그대로 된다
		owner.putObject(bucketName, "new", "x");
		assertTrue(owner.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet().isEmpty());
	}

	@Test
	@Tag("NotAction")
	public void testBucketPolicyNotActionDeny() {
		var key = "testBucketPolicyNotActionDeny";
		var client = getClient();
		var bucketName = createBucket(client, 34);
		client.putObject(bucketName, key, "content");
		var resources = List.of(arnOf(bucketName), arnOf(bucketName + "/*"));
		putPolicy(client, bucketName,
				statementOf("Allow", "s3:*", resources),
				// GetObject, ListBucket 이 아닌 모든 액션을 거부
				with(statementOf("Deny", null, resources), "NotAction", List.of("s3:GetObject", "s3:ListBucket")));

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		assertEquals(1, altClient.listObjectsV2(bucketName).getKeyCount());
		assertDenied(() -> altClient.putObject(bucketName, "new", "x"));
		assertDenied(() -> altClient.deleteObject(bucketName, key));
		assertDenied(() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)));
	}

	@Test
	@Tag("NotAction")
	public void testBucketPolicyNotActionAllow() {
		var key = "testBucketPolicyNotActionAllow";
		var client = getClient();
		var bucketName = createBucket(client, 35);
		client.putObject(bucketName, key, "content");
		// PutObject, DeleteObject 가 아닌 모든 오브젝트 액션을 허용
		putPolicy(client, bucketName, with(statementOf("Allow", null, arnOf(bucketName + "/*")),
				"NotAction", List.of("s3:PutObject", "s3:DeleteObject")));

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		assertTrue(altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet().isEmpty());
		assertDenied(() -> altClient.putObject(bucketName, "new", "x"));
		assertDenied(() -> altClient.deleteObject(bucketName, key));
	}

	@Test
	@Tag("NotResource")
	public void testBucketPolicyNotResourceAllow() {
		var client = getClient();
		var bucketName = createBucket(client, 36);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObject", null),
				"NotResource", arnOf(bucketName + "/private/*")));

		var altClient = getAltClient();
		assertEquals("public/a", getBody(altClient.getObject(bucketName, "public/a").getObjectContent()));
		assertDenied(() -> altClient.getObject(bucketName, "private/a"));
	}

	@Test
	@Tag("NotResource")
	public void testBucketPolicyNotResourceDeny() {
		var client = getClient();
		var bucketName = createBucket(client, 37);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		putPolicy(client, bucketName,
				statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")),
				// public/ 아래가 아닌 것은 모두 거부
				with(statementOf("Deny", "s3:GetObject", null), "NotResource", arnOf(bucketName + "/public/*")));

		var altClient = getAltClient();
		assertEquals("public/a", getBody(altClient.getObject(bucketName, "public/a").getObjectContent()));
		assertDenied(() -> altClient.getObject(bucketName, "private/a"));
	}

	@Test
	@Tag("NotPrincipal")
	public void testBucketPolicyNotPrincipal() {
		var key = "testBucketPolicyNotPrincipal";
		var client = getClient();
		var bucketName = createBucket(client, 38);
		client.putObject(bucketName, key, "content");
		var resource = arnOf(bucketName + "/*");
		// Allow 에는 NotPrincipal 을 쓸 수 없다
		var allowNotPrincipal = with(statementOf("Allow", "s3:GetObject", resource), MainData.POLICY_PRINCIPAL, null);
		with(allowNotPrincipal, MainData.POLICY_NOT_PRINCIPAL, altPrincipal());
		assertMalformedPolicy(client, bucketName, policyDocumentOf(allowNotPrincipal));

		// 서브 사용자가 아닌 모든 주체의 GetObject 를 거부(소유자 포함)
		var deny = with(statementOf("Deny", "s3:GetObject", resource), MainData.POLICY_PRINCIPAL, null);
		with(deny, MainData.POLICY_NOT_PRINCIPAL, altPrincipal());
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", resource), deny);

		assertEquals("content", getBody(getAltClient().getObject(bucketName, key).getObjectContent()));
		assertDenied(() -> getClient().getObject(bucketName, key));
	}

	@Test
	@Tag("Wildcard")
	public void testBucketPolicyResourceWildcard() {
		var client = getClient();
		var bucketName = createBucket(client, 39);
		createObjects(client, bucketName,
				List.of("a1/file1.txt", "a1/file10.txt", "b1/file1.txt", "a1/sub/file2.txt", "a1/file1.TXT"));
		// * 는 0자 이상(/ 포함), ? 는 정확히 한 글자, 대소문자를 구분한다
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/a*/file?.txt")));

		var altClient = getAltClient();
		for (var key : List.of("a1/file1.txt", "a1/sub/file2.txt"))
			assertEquals(key, getBody(altClient.getObject(bucketName, key).getObjectContent()));
		for (var key : List.of("a1/file10.txt", "b1/file1.txt", "a1/file1.TXT"))
			assertDenied(() -> altClient.getObject(bucketName, key));
	}

	@Test
	@Tag("Wildcard")
	public void testBucketPolicyActionWildcard() {
		var key = "testBucketPolicyActionWildcard";
		var client = getClient();
		var bucketName = createBucket(client, 40);
		client.putObject(bucketName, key, "content");
		putPolicy(client, bucketName,
				statementOf("Allow", "s3:Get*", List.of(arnOf(bucketName), arnOf(bucketName + "/*"))));

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		assertEquals(1, altClient.getObjectAcl(bucketName, key).getGrantsAsList().size());
		assertTrue(altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key)).getTagSet().isEmpty());
		assertEquals(BucketVersioningConfiguration.OFF,
				altClient.getBucketVersioningConfiguration(bucketName).getStatus());
		altClient.getBucketLocation(bucketName);
		altClient.getBucketAcl(bucketName);
		// Get 으로 시작하지 않는 액션
		assertDenied(() -> altClient.listObjectsV2(bucketName));
		assertDenied(() -> altClient.putObject(bucketName, "new", "x"));
	}

	@Test
	@Tag("Wildcard")
	public void testBucketPolicyActionCaseInsensitive() {
		var key = "testBucketPolicyActionCaseInsensitive";
		var client = getClient();
		var bucketName = createBucket(client, 41);
		client.putObject(bucketName, key, "content");
		// 액션 이름은 대소문자를 구분하지 않는다
		putPolicy(client, bucketName, statementOf("Allow", "s3:getobject", arnOf(bucketName + "/*")));
		assertEquals("content", getBody(getAltClient().getObject(bucketName, key).getObjectContent()));
	}

	@Test
	@Tag("Resource")
	public void testBucketPolicyListBucketResource() {
		var key = "testBucketPolicyListBucketResource";
		var client = getClient();
		var bucketName = createBucket(client, 42);
		client.putObject(bucketName, key, "content");
		putPolicy(client, bucketName, statementOf("Allow", "s3:ListBucket", arnOf(bucketName)));

		var altClient = getAltClient();
		assertEquals(1, altClient.listObjectsV2(bucketName).getKeyCount());
		assertEquals(1, altClient.listObjects(bucketName).getObjectSummaries().size());
		altClient.headBucket(new HeadBucketRequest(bucketName));
		// ListBucket 만으로는 읽을 수 없고, 버전·멀티파트 목록은 다른 액션이다
		assertDenied(() -> altClient.getObject(bucketName, key));
		assertDenied(() -> altClient.listVersions(new ListVersionsRequest().withBucketName(bucketName)));
		assertDenied(() -> altClient.listMultipartUploads(new ListMultipartUploadsRequest(bucketName)));
		// 목록 권한이 있으면 없는 키는 404 다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> altClient.getObject(bucketName, "not-exist"));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyListPrefixCondition() {
		var client = getClient();
		var bucketName = createBucket(client, 43);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:ListBucket", arnOf(bucketName)),
				MainData.POLICY_CONDITION, conditionOf("StringEquals", "s3:prefix", "public/")));

		var altClient = getAltClient();
		assertEquals(1, altClient.listObjectsV2(bucketName, "public/").getKeyCount());
		assertEquals(1, altClient.listObjects(bucketName, "public/").getObjectSummaries().size());
		assertDenied(() -> altClient.listObjectsV2(bucketName));
		assertDenied(() -> altClient.listObjectsV2(bucketName, "public/a"));
		assertDenied(() -> altClient.listObjectsV2(bucketName, "private/"));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyListConditionKeys() {
		var client = getClient();
		var bucketName = createBucket(client, 44);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		// 조건 연산자가 여럿이면 모두 맞아야 한다
		var condition = conditionOf("StringLike", "s3:prefix", "public/*");
		with(condition, "StringEquals", jsonObjectOf("s3:delimiter", "/"));
		with(condition, "NumericLessThanEquals", jsonObjectOf("s3:max-keys", "10"));
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:ListBucket", arnOf(bucketName)),
				MainData.POLICY_CONDITION, condition));

		var altClient = getAltClient();
		var response = altClient.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName)
				.withPrefix("public/").withDelimiter("/").withMaxKeys(10));
		assertEquals(1, response.getKeyCount());
		assertDenied(() -> altClient.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withPrefix("public/").withMaxKeys(10)));
		assertDenied(() -> altClient.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName)
				.withPrefix("public/").withDelimiter("/").withMaxKeys(11)));
		assertDenied(() -> altClient.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withPrefix("public/").withDelimiter("/")));
		assertDenied(() -> altClient.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName)
				.withPrefix("private/").withDelimiter("/").withMaxKeys(10)));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyListDenyPrefix() {
		var client = getClient();
		var bucketName = createBucket(client, 45);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		var resource = arnOf(bucketName);
		putPolicy(client, bucketName,
				statementOf("Allow", "s3:ListBucket", resource),
				with(statementOf("Deny", "s3:ListBucket", resource),
						MainData.POLICY_CONDITION, conditionOf("StringLike", "s3:prefix", "private/*")));

		var altClient = getAltClient();
		assertDenied(() -> altClient.listObjectsV2(bucketName, "private/"));
		// 조건은 요청의 prefix 값만 본다. 응답에 private/ 아래 키가 들어 있어도 막지 않는다
		assertEquals(2, altClient.listObjectsV2(bucketName).getKeyCount());
		assertEquals(1, altClient.listObjectsV2(bucketName, "priv").getKeyCount());
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyListMaxKeys() {
		var client = getClient();
		var bucketName = createBucket(client, 46);
		createObjects(client, bucketName, List.of("a"));
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:ListBucket", arnOf(bucketName)),
				MainData.POLICY_CONDITION, conditionOf("NumericLessThanEquals", "s3:max-keys", "10")));

		var altClient = getAltClient();
		assertEquals(1, altClient
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(10)).getKeyCount());
		assertEquals(1, altClient.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(5))
				.getObjectSummaries().size());
		assertDenied(() -> altClient
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(11)));
		// max-keys 를 보내지 않으면 조건 키가 없어 맞지 않는다
		assertDenied(() -> altClient.listObjectsV2(bucketName));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicySecureTransport() {
		var key = "testBucketPolicySecureTransport";
		var client = getClient();
		var bucketName = createBucket(client, 47);
		client.putObject(bucketName, key, "content");
		var deny = with(statementOf("Deny", "s3:GetObject", arnOf(bucketName + "/*")), MainData.POLICY_PRINCIPAL, "*");
		with(deny, MainData.POLICY_CONDITION, conditionOf("Bool", "aws:SecureTransport", "false"));
		putPolicy(client, bucketName, deny);

		var httpClient = createClient(false, config.mainUser, true, true, config.getSignatureVersion());
		var httpsClient = getClientHttps();
		assertEquals("content", getBody(httpsClient.getObject(bucketName, key).getObjectContent()));
		assertDenied(() -> httpClient.getObject(bucketName, key));
		// 거부한 액션이 아니면 http 로도 된다
		httpClient.putObject(bucketName, "new", "x");
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicySourceIp() {
		var key = "testBucketPolicySourceIp";
		var client = getClient();
		// 조건과, 서브 사용자의 GetObject 가 되는지. 192.0.2.0/24 는 문서용 대역이라 요청 주소가 될 수 없다
		var conditions = List.of(
				conditionOf("IpAddress", "aws:SourceIp", "0.0.0.0/0"),
				conditionOf("IpAddress", "aws:SourceIp", "192.0.2.0/24"),
				conditionOf("NotIpAddress", "aws:SourceIp", "192.0.2.0/24"),
				conditionOf("NotIpAddress", "aws:SourceIp", "0.0.0.0/0"),
				jsonObjectOf("IpAddress", jsonObjectOf("aws:SourceIp", List.of("192.0.2.0/24", "0.0.0.0/0"))));
		var allowed = List.of(true, false, true, false, true);
		for (int i = 0; i < conditions.size(); i++) {
			var bucketName = createBucket(client, 48);
			client.putObject(bucketName, key, "content");
			putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")),
					MainData.POLICY_CONDITION, conditions.get(i)));
			var altClient = getAltClient();
			if (allowed.get(i))
				assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
			else
				assertDenied(() -> altClient.getObject(bucketName, key));
		}

		var bucketName = createBucket(client, 48);
		assertMalformedPolicy(client, bucketName,
				policyDocumentOf(with(statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")),
						MainData.POLICY_CONDITION, conditionOf("IpAddress", "aws:SourceIp", "not-an-ip"))));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyDenyUnencryptedPut() {
		var key = "testBucketPolicyDenyUnencryptedPut";
		var client = getClient();
		var bucketName = createBucket(client, 49);
		var deny = with(statementOf("Deny", "s3:PutObject", arnOf(bucketName + "/*")), MainData.POLICY_PRINCIPAL, "*");
		with(deny, MainData.POLICY_CONDITION,
				conditionOf("StringNotEquals", "s3:x-amz-server-side-encryption", "AES256"));
		putPolicy(client, bucketName, deny);

		var owner = getClient();
		// 헤더가 없으면 값이 AES256 이 아닌 것으로 본다
		assertDenied(() -> owner.putObject(bucketName, key, "content"));
		owner.putObject(putRequestOf(bucketName, key, "content", ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION));
		// 멀티파트 시작과 복사도 PutObject 액션이다
		assertDenied(() -> owner.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "multi")));
		assertDenied(() -> owner.copyObject(bucketName, key, bucketName, "copy"));
		var metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		owner.copyObject(new CopyObjectRequest(bucketName, key, bucketName, "copy").withNewObjectMetadata(metadata));
		assertEquals("content", getBody(owner.getObject(bucketName, "copy").getObjectContent()));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyNullCondition() {
		var key = "testBucketPolicyNullCondition";
		var client = getClient();
		var bucketName = createBucket(client, 50);
		// 암호화 헤더가 없는(Null) 업로드를 거부
		var deny = with(statementOf("Deny", "s3:PutObject", arnOf(bucketName + "/*")), MainData.POLICY_PRINCIPAL, "*");
		with(deny, MainData.POLICY_CONDITION, conditionOf("Null", "s3:x-amz-server-side-encryption", "true"));
		putPolicy(client, bucketName, deny);

		var owner = getClient();
		assertDenied(() -> owner.putObject(bucketName, key, "content"));
		owner.putObject(putRequestOf(bucketName, key, "content", ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION));
		assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION,
				owner.getObjectMetadata(bucketName, key).getSSEAlgorithm());
	}

	@Test
	@Tag("Version")
	public void testBucketPolicyGetObjectVersionAction() {
		var key = "testBucketPolicyGetObjectVersionAction";
		var client = getClient();
		var bucketName = createBucket(client, 51);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var version1 = client.putObject(bucketName, key, "1").getVersionId();
		var version2 = client.putObject(bucketName, key, "2").getVersionId();
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		assertEquals("2", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		// 버전을 지정한 조회는 s3:GetObjectVersion 이 필요하다(최신 버전이어도)
		for (var versionId : List.of(version1, version2))
			assertDenied(() -> altClient.getObject(new GetObjectRequest(bucketName, key, versionId)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, null,
				() -> altClient.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key, version1)));
	}

	@Test
	@Tag("Version")
	public void testBucketPolicyVersionIdCondition() {
		var key = "testBucketPolicyVersionIdCondition";
		var client = getClient();
		var bucketName = createBucket(client, 52);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var version1 = client.putObject(bucketName, key, "1").getVersionId();
		var version2 = client.putObject(bucketName, key, "2").getVersionId();
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObjectVersion", arnOf(bucketName + "/*")),
				MainData.POLICY_CONDITION, conditionOf("StringEquals", "s3:VersionId", version1)));

		var altClient = getAltClient();
		assertEquals("1",
				getBody(altClient.getObject(new GetObjectRequest(bucketName, key, version1)).getObjectContent()));
		assertDenied(() -> altClient.getObject(new GetObjectRequest(bucketName, key, version2)));
		// 버전을 지정하지 않은 조회는 s3:GetObject 가 필요하다
		assertDenied(() -> altClient.getObject(bucketName, key));
	}

	@Test
	@Tag("Version")
	public void testBucketPolicyDeleteObjectVersionAction() {
		var key = "testBucketPolicyDeleteObjectVersionAction";
		var client = getClient();
		var bucketName = createBucket(client, 53);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var versionId = client.putObject(bucketName, key, "1").getVersionId();
		putPolicy(client, bucketName, statementOf("Allow", "s3:DeleteObject", arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		// 버전을 지정한 삭제는 s3:DeleteObjectVersion 이 필요하다
		assertDenied(() -> altClient.deleteVersion(bucketName, key, versionId));
		// SDK V1 은 일부라도 실패하면 MultiObjectDeleteException 을 던진다
		var e = assertThrows(MultiObjectDeleteException.class, () -> altClient.deleteObjects(
				new DeleteObjectsRequest(bucketName).withKeys(List.of(new KeyVersion(key, versionId)))));
		assertTrue(e.getDeletedObjects().isEmpty());
		assertEquals(MainData.ACCESS_DENIED, e.getErrors().get(0).getCode());
		// 버전을 지정하지 않은 삭제(삭제 마커)는 된다. SDK V1 의 deleteObject 는 응답이 없어 버전 목록으로 확인한다
		altClient.deleteObject(bucketName, key);
		var versions = client.listVersions(new ListVersionsRequest().withBucketName(bucketName))
				.getVersionSummaries();
		assertTrue(versions.stream().anyMatch(v -> v.isLatest() && v.isDeleteMarker()));
		assertEquals("1",
				getBody(client.getObject(new GetObjectRequest(bucketName, key, versionId)).getObjectContent()));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyRequestObjectTagKeys() {
		var client = getClient();
		var bucketName = createBucket(client, 54, ObjectOwnership.BucketOwnerEnforced);
		// 요청의 태그 키가 모두 project, team 안에 있어야 한다
		putPolicy(client, bucketName,
				with(statementOf("Allow", List.of("s3:PutObject", "s3:PutObjectTagging"), arnOf(bucketName + "/*")),
						MainData.POLICY_CONDITION, jsonObjectOf("ForAllValues:StringEquals",
								jsonObjectOf("s3:RequestObjectTagKeys", List.of("project", "team")))));

		var altClient = getAltClient();
		altClient.putObject(putRequestOf(bucketName, "allowed", "x", null).withTagging(taggingOf("project", "a")));
		// 태그가 없으면 ForAllValues 는 참이다
		altClient.putObject(bucketName, "no-tag", "x");
		assertDenied(() -> altClient.putObject(
				putRequestOf(bucketName, "denied", "x", null).withTagging(taggingOf("project", "a", "owner", "b"))));
		var tagSet = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, "allowed")).getTagSet();
		assertEquals(1, tagSet.size());
		assertEquals("project", tagSet.get(0).getKey());
		assertEquals("a", tagSet.get(0).getValue());
	}

	@Test
	@Tag("Action")
	public void testBucketPolicyPutObjectAction() {
		var key = "testBucketPolicyPutObjectAction";
		var client = getClient();
		var bucketName = createBucket(client, 55, ObjectOwnership.BucketOwnerEnforced);
		client.putObject(bucketName, key, "content");
		putPolicy(client, bucketName, statementOf("Allow", "s3:PutObject", arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		altClient.putObject(bucketName, "new", "x");
		// 멀티파트 시작·파트 업로드는 PutObject 로, 시작한 사용자는 파트 목록·중단도 할 수 있다
		var uploadId = altClient.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, "multi"))
				.getUploadId();
		altClient.uploadPart(partRequestOf(bucketName, "multi", uploadId, 1));
		assertEquals(1, altClient.listParts(new ListPartsRequest(bucketName, "multi", uploadId)).getParts().size());
		altClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, "multi", uploadId));

		// 태그를 붙이려면 PutObjectTagging, 복사하려면 원본의 GetObject 가 필요하다
		assertDenied(() -> altClient
				.putObject(putRequestOf(bucketName, "tagged", "x", null).withTagging(taggingOf("a", "b"))));
		assertDenied(() -> altClient.copyObject(bucketName, key, bucketName, "copy"));
		assertDenied(() -> altClient.listMultipartUploads(new ListMultipartUploadsRequest(bucketName)));
		assertDenied(() -> altClient.getObject(bucketName, "new"));
		assertDenied(() -> altClient.deleteObject(bucketName, "new"));
	}

	@Test
	@Tag("Action")
	public void testBucketPolicyMultipartListActions() {
		var key = "testBucketPolicyMultipartListActions";
		var client = getClient();
		var bucketName = createBucket(client, 56);
		var uploadId = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key))
				.getUploadId();
		client.uploadPart(partRequestOf(bucketName, key, uploadId, 1));
		putPolicy(client, bucketName,
				statementOf("Allow", List.of("s3:ListMultipartUploadParts", "s3:ListBucketMultipartUploads"),
						List.of(arnOf(bucketName), arnOf(bucketName + "/*"))));

		var altClient = getAltClient();
		assertEquals(1, altClient.listParts(new ListPartsRequest(bucketName, key, uploadId)).getParts().size());
		assertEquals(1, altClient.listMultipartUploads(new ListMultipartUploadsRequest(bucketName))
				.getMultipartUploads().size());
		assertDenied(
				() -> altClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId)));
		assertDenied(() -> altClient.uploadPart(partRequestOf(bucketName, key, uploadId, 2)));
	}

	@Test
	@Tag("Action")
	public void testBucketPolicyBucketConfigActions() {
		var client = getClient();
		var bucketName = createBucket(client, 57);
		var actions = List.of("s3:GetBucketVersioning", "s3:GetBucketTagging", "s3:PutBucketTagging",
				"s3:GetBucketCORS", "s3:PutBucketCORS", "s3:GetBucketAcl", "s3:GetBucketLocation",
				"s3:ListBucketVersions");
		putPolicy(client, bucketName, statementOf("Allow", actions, arnOf(bucketName)));

		var altClient = getAltClient();
		assertEquals(BucketVersioningConfiguration.OFF,
				altClient.getBucketVersioningConfiguration(bucketName).getStatus());
		altClient.getBucketAcl(bucketName);
		altClient.getBucketLocation(bucketName);
		assertTrue(altClient.listVersions(new ListVersionsRequest().withBucketName(bucketName)).getVersionSummaries()
				.isEmpty());
		// 권한은 있고 설정이 없으면 403 이 아니라 404 다(SDK V1 은 404 를 삼키고 null 을 돌려준다)
		assertNull(altClient.getBucketTaggingConfiguration(bucketName));
		assertNull(altClient.getBucketCrossOriginConfiguration(bucketName));
		// 설정 삭제는 Put 액션으로 판정한다
		altClient.setBucketTaggingConfiguration(bucketName,
				new BucketTaggingConfiguration(List.of(new TagSet(Map.of("a", "b")))));
		assertEquals(Map.of("a", "b"), client.getBucketTaggingConfiguration(bucketName).getTagSet().getAllTags());
		altClient.deleteBucketTaggingConfiguration(bucketName);
		altClient.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(List.of(
				new CORSRule().withAllowedMethods(List.of(CORSRule.AllowedMethods.GET)).withAllowedOrigins(List.of("*")))));
		altClient.deleteBucketCrossOriginConfiguration(bucketName);

		// 허용하지 않은 액션
		assertDenied(() -> altClient.listObjectsV2(bucketName));
		assertDenied(() -> altClient.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(
				bucketName, new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED))));
		assertDenied(() -> altClient.getBucketLifecycleConfiguration(bucketName));
		assertDenied(() -> altClient.deleteBucket(bucketName));
	}

	@Test
	@Tag("Form")
	public void testBucketPolicyStatementForms() {
		var key = "testBucketPolicyStatementForms";
		var client = getClient();
		var bucketName = createBucket(client, 58);
		client.putObject(bucketName, key, "content");
		var resource = arnOf(bucketName + "/*");
		var statement = statementOf("Allow", "s3:GetObject", resource);

		// Statement 는 배열이 아니라 객체 하나여도 된다
		var single = with(jsonObjectOf(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE),
				MainData.POLICY_STATEMENT, statement);
		client.setBucketPolicy(bucketName, single.toString());
		assertEquals("content", getBody(getAltClient().getObject(bucketName, key).getObjectContent()));

		// Version 을 빼면 2008-10-17 로 저장된다
		var noVersion = jsonObjectOf(MainData.POLICY_STATEMENT, List.of(statement));
		client.setBucketPolicy(bucketName, noVersion.toString());
		assertEquals("2008-10-17", JsonParser.parseString(client.getBucketPolicy(bucketName).getPolicyText())
				.getAsJsonObject().get(MainData.POLICY_VERSION).getAsString());
		client.setBucketPolicy(bucketName, policyDocumentOf("2008-10-17", statement));

		// Id 와 Sid 는 그대로 돌려준다. Sid 가 겹쳐도 받는다
		var policy = jsonObjectOf(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);
		with(policy, "Id", "policy-id");
		with(policy, MainData.POLICY_STATEMENT, List.of(
				with(statementOf("Allow", "s3:GetObject", resource), "Sid", "same"),
				with(statementOf("Allow", "s3:GetObjectTagging", resource), "Sid", "same")));
		client.setBucketPolicy(bucketName, policy.toString());
		var actual = JsonParser.parseString(client.getBucketPolicy(bucketName).getPolicyText()).getAsJsonObject();
		assertEquals("policy-id", actual.get("Id").getAsString());
		var actualStatements = actual.getAsJsonArray(MainData.POLICY_STATEMENT);
		var expectedActions = List.of("s3:GetObject", "s3:GetObjectTagging");
		assertEquals(expectedActions.size(), actualStatements.size());
		for (int i = 0; i < actualStatements.size(); i++) {
			var actualStatement = actualStatements.get(i).getAsJsonObject();
			assertEquals("same", actualStatement.get("Sid").getAsString());
			assertEquals(expectedActions.get(i), actualStatement.get(MainData.POLICY_ACTION).getAsString());
		}

		// 주체를 여럿 적을 수 있다
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObject", resource), MainData.POLICY_PRINCIPAL,
				jsonObjectOf("CanonicalUser", List.of(config.altUser.id, config.mainUser.id))));
		assertEquals("content", getBody(getAltClient().getObject(bucketName, key).getObjectContent()));
	}

	@Test
	@Tag("ERROR")
	public void testBucketPolicyInvalidStatement() {
		var client = getClient();
		var bucketName = createBucket(client, 59);
		var resource = arnOf(bucketName + "/*");
		var invalidStatements = List.of(
				// 같은 뜻의 항목을 함께 쓴다
				with(statementOf("Allow", "s3:GetObject", resource), "NotAction", "s3:PutObject"),
				with(statementOf("Allow", "s3:GetObject", resource), "NotResource", arnOf(bucketName + "/a/*")),
				with(statementOf("Deny", "s3:GetObject", resource), MainData.POLICY_NOT_PRINCIPAL, altPrincipal()),
				// 필수 항목 누락
				statementOf("Allow", null, resource),
				with(statementOf("Allow", "s3:GetObject", resource), MainData.POLICY_EFFECT, null),
				new JsonObject(),
				// 값이 잘못됐다
				with(statementOf("Allow", "s3:GetObject", resource), "Unknown", "value"),
				statementOf("Allow", List.of(), resource),
				statementOf("Allow", "ec2:RunInstances", resource),
				statementOf("Allow", "s3:GetObject", "*"),
				with(statementOf("Allow", "s3:GetObject", resource), MainData.POLICY_PRINCIPAL,
						jsonObjectOf("CanonicalUser", "0".repeat(64))));
		for (var statement : invalidStatements)
			assertMalformedPolicy(client, bucketName, policyDocumentOf(statement));
		// 최상위가 객체가 아니다
		assertMalformedPolicy(client, bucketName, "[]");
		// 정책이 저장되지 않았다(SDK V1 은 NoSuchBucketPolicy 를 빈 정책으로 돌려준다)
		assertNull(client.getBucketPolicy(bucketName).getPolicyText());
	}

	@Test
	@Tag("Delete")
	public void testBucketPolicyDeleteRevokes() {
		var key = "testBucketPolicyDeleteRevokes";
		var client = getClient();
		var bucketName = createBucket(client, 60);
		client.putObject(bucketName, key, "content");
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")));
		assertEquals("content", getBody(getAltClient().getObject(bucketName, key).getObjectContent()));

		client.deleteBucketPolicy(bucketName);
		assertDenied(() -> getAltClient().getObject(bucketName, key));
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyOtherUserWrite() {
		var key = "testBucketPolicyOtherUserWrite";
		var client = getClient();
		var bucketName = createBucket(client, 61, ObjectOwnership.BucketOwnerEnforced);
		putPolicy(client, bucketName, statementOf("Allow",
				List.of("s3:PutObject", "s3:GetObject", "s3:DeleteObject"), arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		altClient.putObject(bucketName, key, "content");
		// 다른 사용자가 올린 오브젝트도 버킷 소유자의 것이다
		assertEquals("content", getBody(client.getObject(bucketName, key).getObjectContent()));
		assertEquals("content", getBody(altClient.getObject(bucketName, key).getObjectContent()));
		// 실패한 항목이 있으면 SDK V1 은 예외를 던지므로, 정상 반환이 곧 Errors 없음이다
		var response = altClient.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(key, "not-exist"));
		assertEquals(List.of("not-exist", key),
				response.getDeletedObjects().stream().map(d -> d.getKey()).sorted().toList());
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY, () -> client.getObject(bucketName, key));
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyAnonymousPrefix() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 62);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/public/*")),
				MainData.POLICY_PRINCIPAL, "*"));
		assertTrue(client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName))
				.getPolicyStatus().getIsPublic());

		var publicClient = getPublicClient();
		assertEquals("public/a", getBody(publicClient.getObject(bucketName, "public/a").getObjectContent()));
		assertDenied(() -> publicClient.getObject(bucketName, "private/a"));
		assertDenied(() -> publicClient.putObject(bucketName, "public/b", "x"));
		assertDenied(() -> publicClient.listObjectsV2(bucketName));
	}
	// endregion
}
