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
	static public void BeforeAll()
	{
		System.out.println("Policy Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Policy End");
	}

	@Test
	@Tag("Check")
	// 버킷에 정책 설정이 올바르게 적용되는지 확인
	public void test_bucket_policy()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "asdf";
		client.putObject(bucketName, Key, Key);

		var Resource1 = "arn:aws:s3:::" + bucketName;
		var Resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var Statements = new JsonArray();

		var Statement = new JsonObject();
		Statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);

		var Principal = new JsonObject();
		Principal.addProperty( "AWS", "*");
		Statement.add(MainData.PolicyPrincipal, Principal);

		Statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var ResourceList = new JsonArray();
		ResourceList.add(Resource1);
		ResourceList.add(Resource2);
		Statement.add(MainData.PolicyResource, ResourceList);

		Statements.add(Statement);
		PolicyDocument.add(MainData.PolicyStatement, Statements);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var AltClient = getAltClient();
		var Response = AltClient.listObjects(bucketName);
		assertEquals(1, Response.getObjectSummaries().size());

		var GetResponse = client.getBucketPolicy(bucketName);
		assertEquals(PolicyDocument.toString(), GetResponse.getPolicyText());
	}

	@Test
	@Tag("Check")
	// 버킷에 정책 설정이 올바르게 적용되는지 확인(ListObjectsV2)
	public void test_bucketv2_policy()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "asdf";
		client.putObject(bucketName, Key, Key);

		var Resource1 = "arn:aws:s3:::" + bucketName;
		var Resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var Statements = new JsonArray();

		var Statement = new JsonObject();
		Statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);

		var Principal = new JsonObject();
		Principal.addProperty( "AWS", "*");
		Statement.add(MainData.PolicyPrincipal, Principal);

		Statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var ResourceList = new JsonArray();
		ResourceList.add(Resource1);
		ResourceList.add(Resource2);
		Statement.add(MainData.PolicyResource, ResourceList);

		Statements.add(Statement);
		PolicyDocument.add(MainData.PolicyStatement, Statements);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var AltClient = getAltClient();
		var Response = AltClient.listObjectsV2(bucketName);
		assertEquals(1, Response.getObjectSummaries().size());

		var GetResponse = client.getBucketPolicy(bucketName);
		assertEquals(PolicyDocument.toString(), GetResponse.getPolicyText());
	}

	@Test
	@Tag("Priority")
	// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인
	public void test_bucket_policy_acl()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "asdf";
		client.putObject(bucketName, Key, Key);

		var Resource1 = "arn:aws:s3:::" + bucketName;
		var Resource2 = "arn:aws:s3:::" + bucketName + "/*";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var Statements = new JsonArray();

		var Statement = new JsonObject();
		Statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectDeny);

		var Principal = new JsonObject();
		Principal.addProperty( "AWS", "*");
		Statement.add(MainData.PolicyPrincipal, Principal);

		Statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var ResourceList = new JsonArray();
		ResourceList.add(Resource1);
		ResourceList.add(Resource2);
		Statement.add(MainData.PolicyResource, ResourceList);

		Statements.add(Statement);
		PolicyDocument.add(MainData.PolicyStatement, Statements);

		client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead);
		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var AltClient = getAltClient();
		var e = assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, Key));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(403, StatusCode);
		assertEquals(MainData.AccessDenied, ErrorCode);

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Priority")
	// 버킷에 정책과 acl설정을 할 경우 정책 설정이 우선시됨을 확인(ListObjectsV2)
	public void test_bucketv2_policy_acl()
	{

		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "asdf";
		client.putObject(bucketName, Key, Key);

		var Resource1 = "arn:aws:s3:::" + bucketName;
		var Resource2 = "arn:aws:s3:::" + bucketName + "/*";


		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var Statements = new JsonArray();

		var Statement = new JsonObject();
		Statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectDeny);

		var Principal = new JsonObject();
		Principal.addProperty( "AWS", "*");
		Statement.add(MainData.PolicyPrincipal, Principal);

		Statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var ResourceList = new JsonArray();
		ResourceList.add(Resource1);
		ResourceList.add(Resource2);
		Statement.add(MainData.PolicyResource, ResourceList);

		Statements.add(Statement);
		PolicyDocument.add(MainData.PolicyStatement, Statements);

		client.setBucketAcl(bucketName, CannedAccessControlList.AuthenticatedRead);
		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var AltClient = getAltClient();
		var e = assertThrows(AmazonServiceException.class, () -> AltClient.listObjectsV2(bucketName));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(403, StatusCode);
		assertEquals(MainData.AccessDenied, ErrorCode);

		client.deleteBucketPolicy(bucketName);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	@Test
	@Tag("Taggings")
	// 정책설정으로 오브젝트의 태그목록 읽기를 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_get_tags_acl_public()
	{
		var Key = "testgettagsacl";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var Resource = makeArnResource(String.format("%s/%s", bucketName, Key));
		var PolicyDocument = MakeJsonPolicy("s3:GetObjectTagging", Resource, null, null);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var InputTagset = new ObjectTagging(createSimpleTagSet(10));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagset));

		var AltClient = getAltClient();

		var GetResponse = AltClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagset.getTagSet(), GetResponse.getTagSet());
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그 입력을 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_put_tags_acl_public()
	{
		var Key = "testputtagsacl";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var Resource = makeArnResource(String.format("%s/%s", bucketName, Key));
		var PolicyDocument = MakeJsonPolicy("s3:PutObjectTagging", Resource, null, null);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var InputTagset = new ObjectTagging(createSimpleTagSet(10));
		var AltClient = getAltClient();
		AltClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagset));

		var GetResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		TaggingCompare(InputTagset.getTagSet(), GetResponse.getTagSet());
	}

	@Test
	@Tag("Tagging")
	// 정책설정으로 오브젝트의 태그 삭제를 public-read로 설정했을때 올바르게 동작하는지 확인
	public void test_delete_tags_obj_public()
	{
		var Key = "testdeltagsacl";
		var bucketName = createKeyWithRandomContent(Key, 0, null, null);
		var client = getClient();

		var Resource = makeArnResource(String.format("%s/%s", bucketName, Key));
		var PolicyDocument = MakeJsonPolicy("s3:DeleteObjectTagging", Resource, null, null);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var InputTagset = new ObjectTagging(createSimpleTagSet(10));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, Key, InputTagset));

		var AltClient = getAltClient();
		AltClient.deleteObjectTagging(new DeleteObjectTaggingRequest(bucketName, Key));

		var GetResponse = client.getObjectTagging(new GetObjectTaggingRequest(bucketName, Key));
		assertEquals(0, GetResponse.getTagSet().size());
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_existing_tag()
	{
		var publictag = "publictag";
		var privatetag = "privatetag";
		var invalidtag = "invalidtag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publictag, privatetag, invalidtag })));
		var client = getClient();

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var TagConditional = new JsonObject();
		TagConditional.add("StringEquals", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var PolicyDocument = MakeJsonPolicy("s3:GetObject", Resource, null, TagConditional);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var TagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		TagSets.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var InputTagset = new ObjectTagging(TagSets);

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publictag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privatetag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security1", "public"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidtag, InputTagset));

		var AltClient = getAltClient();
		var GetResponse = AltClient.getObject(bucketName, publictag);
		assertNotNull(GetResponse);

		var e = assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, privatetag));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, invalidtag));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_tagging_existing_tag()
	{
		var publictag = "publictag";
		var privatetag = "privatetag";
		var invalidtag = "invalidtag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publictag, privatetag, invalidtag })));
		var client = getClient();

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var TagConditional = new JsonObject();
		TagConditional.add("StringEquals", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var PolicyDocument = MakeJsonPolicy("s3:GetObjectTagging", Resource, null, TagConditional);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var TagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		TagSets.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var InputTagset = new ObjectTagging(TagSets);

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publictag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privatetag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security1", "public"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidtag, InputTagset));

		var AltClient = getAltClient();
		var GetResponse = AltClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, publictag));
		assertNotNull(GetResponse);

		var e = assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, publictag));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class, () -> AltClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privatetag)));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class, () -> AltClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidtag)));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 PutObjectTagging허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_tagging_existing_tag()
	{
		var publictag = "publictag";
		var privatetag = "privatetag";
		var invalidtag = "invalidtag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publictag, privatetag, invalidtag })));
		var client = getClient();

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var TagConditional = new JsonObject();
		TagConditional.add("StringEquals", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var PolicyDocument = MakeJsonPolicy("s3:PutObjectTagging", Resource, null, TagConditional);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var TagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		TagSets.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var InputTagset = new ObjectTagging(TagSets);

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publictag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privatetag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security1", "public"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidtag, InputTagset));

		var TestTagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TestTagSets.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		TestTagSets.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var TestTagset = new ObjectTagging(TestTagSets);

		var AltClient = getAltClient();
		AltClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publictag, TestTagset));

		var e = assertThrows(AmazonServiceException.class, () -> AltClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, privatetag, TestTagset)));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		TestTagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TestTagSets.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		var TestTagset2 = new ObjectTagging(TestTagSets);

		AltClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publictag, TestTagset2));

		TestTagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TestTagSets.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		TestTagSets.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var TestTagset3 = new ObjectTagging(TestTagSets);

		e = assertThrows(AmazonServiceException.class, () -> AltClient.setObjectTagging(new SetObjectTaggingRequest(bucketName, publictag, TestTagset3)));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@Tag("PathOptions")
	// [복사하려는 경로명이 'bucketName/public/*'에 해당할 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_copy_source()
	{
		var public_foo = "public/foo";
		var public_bar = "public/bar";
		var private_foo = "private/foo";
		var SourceBucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { public_foo, public_bar, private_foo })));
		var client = getClient();

		var SourceResource = makeArnResource(String.format("%s/%s", SourceBucketName, "*"));
		var PolicyDocument = MakeJsonPolicy("s3:GetObject", SourceResource, null, null);
		client.setBucketPolicy(SourceBucketName, PolicyDocument.toString());

		var TargetBucketName = getNewBucket();

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:x-amz-copy-source", String.format("/%s/public/*", SourceBucketName));
		var TagConditional = new JsonObject();
		TagConditional.add("StringLike", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", TargetBucketName, "*"));
		PolicyDocument = MakeJsonPolicy("s3:PutObject", Resource, null, TagConditional);
		client.setBucketPolicy(TargetBucketName, PolicyDocument.toString());

		var AltClient = getAltClient();
		var new_foo = "new_foo";
		AltClient.copyObject(SourceBucketName, public_foo, TargetBucketName, new_foo);

		var Response = AltClient.getObject(TargetBucketName, new_foo);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(public_foo, Body);

		var new_foo2 = "new_foo2";
		AltClient.copyObject(SourceBucketName, public_bar, TargetBucketName, new_foo2);

		Response = AltClient.getObject(TargetBucketName, new_foo2);
		Body = GetBody(Response.getObjectContent());
		assertEquals(public_bar, Body);

		 assertThrows(AmazonServiceException.class, () -> AltClient.copyObject(SourceBucketName, private_foo, TargetBucketName, new_foo2));
	}

	@Test
	@Tag("MetadataOptions")
	// [오브젝트의 메타데이터값이 'x-amz-metadata-directive=COPY'일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_copy_source_meta()
	{
		var public_foo = "public/foo";
		var public_bar = "public/bar";
		var SourceBucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { public_foo, public_bar })));
		var client = getClient();

		var SourceResource = makeArnResource(String.format("%s/%s", SourceBucketName, "*"));
		var PolicyDocument = MakeJsonPolicy("s3:GetObject", SourceResource, null, null);
		client.setBucketPolicy(SourceBucketName, PolicyDocument.toString());

		var TargetBucketName = getNewBucket();

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:x-amz-metadata-directive", "COPY");
		var S3Conditional = new JsonObject();
		S3Conditional.add("StringEquals", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", TargetBucketName, "*"));
		PolicyDocument = MakeJsonPolicy("s3:PutObject", Resource, null, S3Conditional);
		client.setBucketPolicy(TargetBucketName, PolicyDocument.toString());

		var AltClient = getAltClient();
		var new_foo = "new_foo";
		AltClient.copyObject(new CopyObjectRequest(SourceBucketName, public_foo, TargetBucketName, new_foo).withMetadataDirective(MetadataDirective.COPY));

		var Response = AltClient.getObject(TargetBucketName, new_foo);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(public_foo, Body);

		var new_foo2 = "new_foo2";
		assertThrows(AmazonServiceException.class, () -> AltClient.copyObject(SourceBucketName, public_bar, TargetBucketName, new_foo2));

		assertThrows(AmazonServiceException.class, () -> AltClient.copyObject(new CopyObjectRequest(SourceBucketName, public_bar, TargetBucketName, new_foo2).withMetadataDirective(MetadataDirective.REPLACE)));
	}

	@Test
	@Tag("ACLOptions")
	// [PutObject는 모든유저에게 허용하지만 권한설정에 'public*'이 포함되면 업로드허용하지 않음] 조건부 정책설정시 올바르게 동작하는지 확
	public void test_bucket_policy_put_obj_acl()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:x-amz-acl", "public*");
		var TagConditional = new JsonObject();
		TagConditional.add("StringLike", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var s1 = makeJsonStatement("s3:PutObject", Resource, null, null, null);
		var s2 = makeJsonStatement("s3:PutObject", Resource, MainData.PolicyEffectDeny, null, TagConditional);
		var PolicyDocument = MakeJsonPolicy(s1, s2);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());

		var AltClient = getAltClient();
		var Key1 = "private-key";

		AltClient.putObject(bucketName, Key1, Key1);

		var Key2 = "public-key";
		var Headers = new ObjectMetadata();
		Headers.setHeader("x-amz-acl", "public-read");
		Headers.setContentType("text/plain");
		Headers.setContentLength(Key2.length());

		var e = assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, Key2, createBody(Key2), Headers));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@Tag("GrantOptions")
	// [오브젝트의 grant-full-control이 메인유저일 경우에만 모든유저에게 PutObject허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_put_obj_grant()
	{
		var bucketName = getNewBucket();
		var BucketName2 = getNewBucket();
		var client = getClient();

		var MainUserID = config.mainUser.userId;
		var AltUserID = config.altUser.userId;

		var OwnerID_str = "id=" + MainUserID;

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:x-amz-grant-full-control", OwnerID_str);
		var S3Conditional = new JsonObject();
		S3Conditional.add("StringEquals", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var PolicyDocument = MakeJsonPolicy("s3:PutObject", Resource, null, S3Conditional);

		var Resource2 = makeArnResource(String.format("%s/%s", BucketName2, "*"));
		var PolicyDocument2 = MakeJsonPolicy("s3:PutObject", Resource2, null, null);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());
		client.setBucketPolicy(BucketName2, PolicyDocument2.toString());

		var AltClient = getAltClient();
		var Key1 = "key1";

		var Headers = new ObjectMetadata();
		Headers.setHeader("x-amz-grant-full-control", OwnerID_str);
		Headers.setContentType("text/plain");
		Headers.setContentLength(Key1.length());

		AltClient.putObject(bucketName, Key1, createBody(Key1), Headers);

		var Key2 = "key2";
		AltClient.putObject(BucketName2, Key2, Key2);

		var Acl1Response = client.getObjectAcl(bucketName, Key1);

		assertThrows(AmazonServiceException.class, () -> client.getObjectAcl(BucketName2, Key2));

		var Acl2Response = AltClient.getObjectAcl(BucketName2, Key2);

		assertEquals(MainUserID, Acl1Response.getGrantsAsList().get(0).getGrantee().getIdentifier());
		assertEquals(AltUserID, Acl2Response.getGrantsAsList().get(0).getGrantee().getIdentifier());
	}

	@Test
	@Tag("TagOptions")
	// [오브젝트의 태그에 'security'키 이름이 존재하며 키값이 public 일때만 모든유저에게 GetObjectACL허용] 조건부 정책설정시 올바르게 동작하는지 확인
	public void test_bucket_policy_get_obj_acl_existing_tag()
	{
		var publictag = "publictag";
		var privatetag = "privatetag";
		var invalidtag = "invalidtag";
		var bucketName = createObjects(new ArrayList<>(Arrays.asList(new String[] { publictag, privatetag, invalidtag })));
		var client = getClient();

		var Conditional = new JsonObject();
		Conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var TagConditional = new JsonObject();
		TagConditional.add("StringEquals", Conditional);

		var Resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var PolicyDocument = MakeJsonPolicy("s3:GetObjectAcl", Resource, null, TagConditional);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());


		var TagSets = new ArrayList<com.amazonaws.services.s3.model.Tag>();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "public"));
		TagSets.add(new com.amazonaws.services.s3.model.Tag("foo", "bar"));
		var InputTagset = new ObjectTagging(TagSets);

		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, publictag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security", "private"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, privatetag, InputTagset));

		TagSets.clear();
		TagSets.add(new com.amazonaws.services.s3.model.Tag("security1", "public"));
		InputTagset.setTagSet(TagSets);
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, invalidtag, InputTagset));

		var AltClient = getAltClient();
		var ACLResponse = AltClient.getObjectAcl(bucketName, publictag);
		assertNotNull(ACLResponse);

		var e = assertThrows(AmazonServiceException.class, () -> AltClient.getObject(bucketName, publictag));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class, () -> AltClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, privatetag)));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);

		e = assertThrows(AmazonServiceException.class, () -> AltClient.getObjectTagging(new GetObjectTaggingRequest(bucketName, invalidtag)));
		StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@Tag("Status")
	// [모든 사용자가 버킷에 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	public void test_get_publicpolicy_acl_bucket_policy_status()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		assertThrows(AmazonServiceException.class, () -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));

		var Resource1 = MainData.PolicyResourcePrefix + bucketName;
		var Resource2 = MainData.PolicyResourcePrefix + bucketName + "/*";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var Statements = new JsonArray();

		var Statement = new JsonObject();
		Statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);

		var Principal = new JsonObject();
		Principal.addProperty( "AWS", "*");
		Statement.add(MainData.PolicyPrincipal, Principal);

		Statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var ResourceList = new JsonArray();
		ResourceList.add(Resource1);
		ResourceList.add(Resource2);
		Statement.add(MainData.PolicyResource, ResourceList);

		Statements.add(Statement);
		PolicyDocument.add(MainData.PolicyStatement, Statements);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());
		var Response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertTrue(Response.getPolicyStatus().getIsPublic());
	}

	@Test
	@Tag("Status")
	// [특정 ip로 접근했을때만 public-read권한을 가지는 정책] 버킷의 정책상태가 올바르게 변경되는지 확인
	public void test_get_nonpublicpolicy_acl_bucket_policy_status()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		assertThrows(AmazonServiceException.class, () -> client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName)));

		var Resource1 = MainData.PolicyResourcePrefix + bucketName;
		var Resource2 = MainData.PolicyResourcePrefix + bucketName + "/*";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var Statements = new JsonArray();

		var Statement = new JsonObject();
		Statement.addProperty(MainData.PolicyEffect, MainData.PolicyEffectAllow);

		var Principal = new JsonObject();
		Principal.addProperty( "AWS", "*");
		Statement.add(MainData.PolicyPrincipal, Principal);

		Statement.addProperty(MainData.PolicyAction, "s3:ListBucket");

		var ResourceList = new JsonArray();
		ResourceList.add(Resource1);
		ResourceList.add(Resource2);
		Statement.add(MainData.PolicyResource, ResourceList);

		var IpAddress = new JsonObject();
		IpAddress.addProperty("aws:SourceIp", "10.0.0.0/32");
		var Condition = new JsonObject();
		Condition.add("IpAddress", IpAddress);
		Statement.add(MainData.PolicyCondition, Condition);

		Statements.add(Statement);
		PolicyDocument.add(MainData.PolicyStatement, Statements);

		client.setBucketPolicy(bucketName, PolicyDocument.toString());
		var Response = client.getBucketPolicyStatus(new GetBucketPolicyStatusRequest().withBucketName(bucketName));
		assertFalse(Response.getPolicyStatus().getIsPublic());
	}
}
