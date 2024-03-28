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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

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

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Policy extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("Policy SDK V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("Policy SDK V2 End");
	}

	@Test
	@Tag("Check")
	// 버킷에 정책 설정이 올바르게 적용되는지 확인
	public void test_bucket_policy()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "asdf";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);

		statement.addProperty(MainData.PolicyPrincipal, "*");
		statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.PolicyResource, resources);

		statements.add(statement);
		policyDocument.add(MainData.PolicyStatement, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var response = altClient.listObjects(bucketName);
		assertEquals(1, response.getObjectSummaries().size());

		var getResponse = client.getBucketPolicy(bucketName);
		assertEquals(policyDocument.toString(), getResponse.getPolicyText());
	}

	@Test
	@Tag("Check")
	// 버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)
	public void test_bucket_v2_policy()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "asdf";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);
		statement.addProperty(MainData.PolicyPrincipal, "*");
		statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.PolicyResource, resources);

		statements.add(statement);
		policyDocument.add(MainData.PolicyStatement, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var response = altClient.listObjectsV2(bucketName);
		assertEquals(1, response.getObjectSummaries().size());

		var getResponse = client.getBucketPolicy(bucketName);
		assertEquals(policyDocument.toString(), getResponse.getPolicyText());
	}

	@Test
	@Tag("Priority")
	// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인
	public void test_bucket_policy_acl()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "asdf";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectDeny);

		var principal = new JsonObject();
		principal.addProperty( "AWS", "*");
		statement.add(MainData.PolicyPrincipal, principal);

		statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.PolicyResource, resources);

		statements.add(statement);
		policyDocument.add(MainData.PolicyStatement, statements);

		client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead);
		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, key));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(403, statusCode);
		assertEquals(MainData.AccessDenied, errorCode);

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Priority")
	// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)
	public void test_bucket_v2_policy_acl()
	{

		var bucketName = getNewBucket();
		var client = getClient();
		var key = "asdf";
		client.putObject(bucketName, key, key);

		var resource1 = "arn:aws:s3:::" + bucketName;
		var resource2 = "arn:aws:s3:::" + bucketName + "/*";


		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectDeny);

		var principal = new JsonObject();
		principal.addProperty( "AWS", "*");
		statement.add(MainData.PolicyPrincipal, principal);

		statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.PolicyResource, resources);

		statements.add(statement);
		policyDocument.add(MainData.PolicyStatement, statements);

		client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead);
		client.setBucketPolicy(bucketName, policyDocument.toString());

		var altClient = getAltClient();
		var e = assertThrows(AmazonServiceException.class, () -> altClient.listObjectsV2(bucketName));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(403, statusCode);
		assertEquals(MainData.AccessDenied, errorCode);

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_get_tags_acl_public()
	{
		var key = "acl";
		var bucketName = createKeyWithRandomContent(key, 0, null, null);
		var client = getClient();

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
		var policyDocument = makeJsonPolicy("s3:GetObjectTagging", resource, null, null);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var inputTagSet = new ObjectTagging(createSimpleTagSet(10));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var altClient = getAltClient();

		var getResponse = altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), getResponse.getTagSet());
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_put_tags_acl_public()
	{
		var key = "acl";
		var bucketName = createKeyWithRandomContent(key, 0, null, null);
		var client = getClient();

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
		var policyDocument = makeJsonPolicy("s3:PutObjectTagging", resource, null, null);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var inputTagSet = new ObjectTagging(createSimpleTagSet(10));
		var altClient = getAltClient();
		altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		tagCompare(inputTagSet.getTagSet(), getResponse.getTagSet());
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_delete_tags_obj_public()
	{
		var key = "acl";
		var bucketName = createKeyWithRandomContent(key, 0, null, null);
		var client = getClient();

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
		var policyDocument = makeJsonPolicy("s3:DeleteObjectTagging", resource, null, null);

		client.setBucketPolicy(bucketName, policyDocument.toString());

		var inputTagSet = new ObjectTagging(createSimpleTagSet(10));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, inputTagSet));

		var altClient = getAltClient();
		altClient.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, key));

		var getResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
		assertEquals(0, getResponse.getTagSet().size());
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_existing_tag()
	{
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publicTag, privateTag, invalidTag })));
		var client = getClient();

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
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, invalidTag));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_tagging_existing_tag()
	{
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publicTag, privateTag, invalidTag })));
		var client = getClient();

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
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privateTag)));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidTag)));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 PutObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_tagging_existing_tag()
	{
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publicTag, privateTag, invalidTag })));
		var client = getClient();

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:PutObjectTagging", resource, null, tagConditional);

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

		var testTags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		testTags.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		testTags.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var TestTagSet = new ObjectTagging(testTags);

		var altClient = getAltClient();
		altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag, TestTagSet));

		var e = assertThrows(AmazonServiceException.class, () -> altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, privateTag, TestTagSet)));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		testTags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		testTags.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		var TestTagSet2 = new ObjectTagging(testTags);

		altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag, TestTagSet2));

		testTags = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		testTags.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		testTags.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var TestTagSet3 = new ObjectTagging(testTags);

		e = assertThrows(AmazonServiceException.class, () -> altClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publicTag, TestTagSet3)));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("PathOptions")
	// [복사하려는 경로명이 'bucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_copy_source()
	{
		var public_foo = "public/foo";
		var public_bar = "public/bar";
		var private_foo = "private/foo";
		var sourceBucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { public_foo, public_bar, private_foo })));
		var client = getClient();

		var sourceResource = makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.setBucketPolicy(sourceBucketName, policyDocument.toString());

		var targetBucketName = getNewBucket();

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-copy-source", String.format("/%s/public/*", sourceBucketName));
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = makeArnResource(String.format("%s/%s", targetBucketName, "*"));
		policyDocument = makeJsonPolicy("s3:PutObject", resource, null, tagConditional);
		client.setBucketPolicy(targetBucketName, policyDocument.toString());

		var altClient = getAltClient();
		var new_foo = "new_foo";
		altClient.copyObject(sourceBucketName, public_foo, targetBucketName, new_foo);

		var response = altClient.getObject(targetBucketName, new_foo);
		var Body = getBody(response.getObjectContent());
		assertEquals(public_foo, Body);

		var new_foo2 = "new_foo2";
		altClient.copyObject(sourceBucketName, public_bar, targetBucketName, new_foo2);

		response = altClient.getObject(targetBucketName, new_foo2);
		Body = getBody(response.getObjectContent());
		assertEquals(public_bar, Body);

		 assertThrows(AmazonServiceException.class, () -> altClient.copyObject(sourceBucketName, private_foo, targetBucketName, new_foo2));
	}

	@Test
	@Tag("MetadataOptions")
	// [오브젝트의 메타데이터값이 'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_copy_source_meta()
	{
		var public_foo = "public/foo";
		var public_bar = "public/bar";
		var sourceBucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { public_foo, public_bar })));
		var client = getClient();

		var sourceResource = makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.setBucketPolicy(sourceBucketName, policyDocument.toString());

		var targetBucketName = getNewBucket();

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-metadata-directive", "COPY");
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", targetBucketName, "*"));
		policyDocument = makeJsonPolicy("s3:PutObject", resource, null, s3Conditional);
		client.setBucketPolicy(targetBucketName, policyDocument.toString());

		var altClient = getAltClient();
		var new_foo = "new_foo";
		altClient.copyObject(new CopyObjectRequest(sourceBucketName, public_foo, targetBucketName, new_foo).withMetadataDirective(MetadataDirective.COPY));

		var response = altClient.getObject(targetBucketName, new_foo);
		var Body = getBody(response.getObjectContent());
		assertEquals(public_foo, Body);

		var new_foo2 = "new_foo2";
		assertThrows(AmazonServiceException.class, () -> altClient.copyObject(sourceBucketName, public_bar, targetBucketName, new_foo2));

		assertThrows(AmazonServiceException.class, () -> altClient.copyObject(new CopyObjectRequest(sourceBucketName, public_bar, targetBucketName, new_foo2).withMetadataDirective(MetadataDirective.REPLACE)));
	}

	@Test
	@Tag("ACLOptions")
	// [PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음] 조건부 정책설정시 올바르게 동작하는지 확
	public void test_bucket_policy_put_obj_acl()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-acl", "public*");
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var s1 = makeJsonStatement("s3:PutObject", resource, null, null, null);
		var s2 = makeJsonStatement("s3:PutObject", resource, MainData.PolicyEffectDeny, null, tagConditional);
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

		var e = assertThrows(AmazonServiceException.class, () -> altClient.putObject(bucketName, key2, createBody(key2), headers));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("GrantOptions")
	// [오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_grant()
	{
		var bucketName = getNewBucket();
		var BucketName2 = getNewBucket();
		var client = getClient();

		var mainUserId = config.mainUser.userId;
		var altUserId = config.altUser.userId;

		var ownerId = "id=" + mainUserId;

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-grant-full-control", ownerId);
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:PutObject", resource, null, s3Conditional);

		var resource2 = makeArnResource(String.format("%s/%s", BucketName2, "*"));
		var policyDocument2 = makeJsonPolicy("s3:PutObject", resource2, null, null);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		client.setBucketPolicy(BucketName2, policyDocument2.toString());

		var altClient = getAltClient();
		var Key1 = "key1";

		var headers = new ObjectMetadata();
		headers.setHeader("x-amz-grant-full-control", ownerId);
		headers.setContentType("text/plain");
		headers.setContentLength(Key1.length());

		altClient.putObject(bucketName, Key1, createBody(Key1), headers);

		var Key2 = "key2";
		altClient.putObject(BucketName2, Key2, Key2);

		var acl1Response = client.getObjectAcl(bucketName, Key1);

		assertThrows(AmazonServiceException.class, () -> client.getObjectAcl(BucketName2, Key2));

		var acl2Response = altClient.getObjectAcl(BucketName2, Key2);

		assertEquals(mainUserId, acl1Response.getGrantsAsList().get(0).getGrantee().getIdentifier());
		assertEquals(altUserId, acl2Response.getGrantsAsList().get(0).getGrantee().getIdentifier());
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectACL허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_acl_existing_tag()
	{
		var publicTag = "publicTag";
		var privateTag = "privateTag";
		var invalidTag = "invalidTag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publicTag, privateTag, invalidTag })));
		var client = getClient();

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
		var ACLResponse = altClient.getObjectAcl(bucketName, publicTag);
		assertNotNull(ACLResponse);

		var e = assertThrows(AmazonServiceException.class, () -> altClient.getObject(bucketName, publicTag));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privateTag)));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);

		e = assertThrows(AmazonServiceException.class, () -> altClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidTag)));
		statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("Status")
	// [모든 사용자가 버킷에 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	public void test_get_public_policy_acl_bucket_policy_status()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		assertThrows(AmazonServiceException.class, () -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));

		var resource1 = MainData.PolicyResourcePrefix + bucketName;
		var resource2 = MainData.PolicyResourcePrefix + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);

		var principal = new JsonObject();
		principal.addProperty( "AWS", "*");
		statement.add(MainData.PolicyPrincipal, principal);

		statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.PolicyResource, resources);

		statements.add(statement);
		policyDocument.add(MainData.PolicyStatement, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertTrue(response.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Status")
	// [특정 ip로 접근했을때만 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	public void test_get_nonpublic_policy_acl_bucket_policy_status()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		assertThrows(AmazonServiceException.class, () -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));

		var resource1 = MainData.PolicyResourcePrefix + bucketName;
		var resource2 = MainData.PolicyResourcePrefix + bucketName + "/*";

		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var statements = new JsonArray();

		var statement = new JsonObject();
		statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);

		var principal = new JsonObject();
		principal.addProperty( "AWS", "*");
		statement.add(MainData.PolicyPrincipal, principal);

		statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var resources = new JsonArray();
		resources.add(resource1);
		resources.add(resource2);
		statement.add(MainData.PolicyResource, resources);

		var ipAddress = new JsonObject();
		ipAddress.addProperty("aws:SourceIp", "10.0.0.0/32");
		var condition = new JsonObject();
		condition.add("IpAddress", ipAddress);
		statement.add(MainData.PolicyCondition, condition);

		statements.add(statement);
		policyDocument.add(MainData.PolicyStatement, statements);

		client.setBucketPolicy(bucketName, policyDocument.toString());
		var response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertFalse(response.getPolicyStatus().getIsPublic());
	}
}
