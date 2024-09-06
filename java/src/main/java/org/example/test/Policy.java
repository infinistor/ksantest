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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Policy extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Policy Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Policy End");
	}

	@Test
	@Tag("Check")
	public void testBucketPolicy() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "asdf";
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
		var bucketName = createBucketCannedACL(client);
		var key = "asdf";
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
		var bucketName = createBucketCannedACL(client);
		var key = "asdf";
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
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);
		assertEquals(MainData.ACCESS_DENIED, errorCode);

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Priority")
	public void testBucketV2PolicyAcl() {

		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		var key = "asdf";
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
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);
		assertEquals(MainData.ACCESS_DENIED, errorCode);

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Tagging")
	public void testGetTagsAclPublic() {
		var key = "acl";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
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
		var key = "acl";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
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
		var key = "acl";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
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
		var bucketName = createBucketCannedACL(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, invalidTag));
		statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);
	}

	@Test
	@Tag("TagOptions")
	public void testBucketPolicyGetObjTaggingExistingTag() {
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privateTag)));
		statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidTag)));
		statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);
	}

	@Test
	@Tag("TagOptions")
	public void testBucketPolicyPutObjTaggingExistingTag() {
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);

		altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag,
				new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security", "private")))));

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag,
						new ObjectTagging(List.of(new com.amazonaws.services.s3.model.Tag("security", "public"),
								new com.amazonaws.services.s3.model.Tag("foo", "bar"))))));
		statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);
	}

	@Test
	@Tag("PathOptions")
	public void testBucketPolicyPutObjCopySource() {
		var publicFoo = "public/foo";
		var publicBar = "public/bar";
		var privateFoo = "private/foo";
		var client = getClient();
		var altClient = getAltClient();
		var sourceBucketName = createBucketCannedACL(client);
		var targetBucketName = createBucketCannedACL(client);

		createObjects(client, sourceBucketName, List.of(publicFoo, publicBar, privateFoo));

		var sourceResource = makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.setBucketPolicy(sourceBucketName, policyDocument.toString());

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-copy-source", String.format("/%s/public/*", sourceBucketName));
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = makeArnResource(String.format("%s/%s", targetBucketName, "*"));
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
		var sourceBucketName = createBucketCannedACL(client);
		createObjects(client, sourceBucketName, List.of(publicFoo, publicBar));

		var sourceResource = makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.setBucketPolicy(sourceBucketName, policyDocument.toString());

		var targetBucketName = createBucketCannedACL(client);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-metadata-directive", "COPY");
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", targetBucketName, "*"));
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
		var bucketName = createBucketCannedACL(client);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-acl", "public*");
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var bucketName1 = createBucketCannedACL(client);
		var bucketName2 = createBucketCannedACL(client);

		var mainUserId = config.mainUser.id;
		var altUserId = config.altUser.id;

		var ownerId = "id=" + mainUserId;

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-grant-full-control", ownerId);
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName1, "*"));
		var policyDocument = makeJsonPolicy("s3:PutObject", resource, null, s3Conditional);

		var resource2 = makeArnResource(String.format("%s/%s", bucketName2, "*"));
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
		var bucketName = createBucketCannedACL(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privateTag)));
		statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidTag)));
		statusCode = e.getStatusCode();
		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);
	}

	@Test
	@Tag("Status")
	public void testGetPublicPolicyAclBucketPolicyStatus() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

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
	public void testGetNonpublicPolicyAclBucketPolicyStatus() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);

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

		var ipAddress = new JsonObject();
		ipAddress.addProperty("aws:SourceIp", "10.0.0.0/32");
		var condition = new JsonObject();
		condition.add("IpAddress", ipAddress);
		statement.add(MainData.POLICY_CONDITION, condition);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertFalse(response.getPolicyStatus().getIsPublic());
	}
}
