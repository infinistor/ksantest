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

import java.util.List;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Tagging;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("unchecked")
public class Policy extends TestBase {
	@Test
	@Tag("Check")
	public void testBucketPolicy() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 1);
		var key = "testBucketPolicy";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var altClient = getAltClient();
		var response = altClient.listObjects(l -> l.bucket(bucketName));
		assertEquals(1, response.contents().size());

		var getResponse = client.getBucketPolicy(g -> g.bucket(bucketName));
		assertEquals(policyDocument.toString(), getResponse.policy());
	}

	@Test
	@Tag("Check")
	public void testBucketV2Policy() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 2);
		var key = "testBucketV2Policy";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var altClient = getAltClient();
		var response = altClient.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(1, response.contents().size());

		var getResponse = client.getBucketPolicy(g -> g.bucket(bucketName));
		assertEquals(policyDocument.toString(), getResponse.policy());
	}

	@Test
	@Tag("Priority")
	public void testBucketPolicyAcl() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 3);
		var key = "testBucketPolicyAcl";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

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

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.AUTHENTICATED_READ));
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var altClient = getAltClient();
		var e = assertThrows(AwsServiceException.class, () -> altClient.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.deleteBucketPolicy(d -> d.bucket(bucketName));
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));
	}

	@Test
	@Tag("Priority")
	public void testBucketV2PolicyAcl() {

		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 4);
		var key = "testBucketV2PolicyAcl";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

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

		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.AUTHENTICATED_READ));
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var altClient = getAltClient();
		var e = assertThrows(AwsServiceException.class, () -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.deleteBucketPolicy(d -> d.bucket(bucketName));
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(10)).build();
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var altClient = getAltClient();

		var getResponse = altClient.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), getResponse.tagSet());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(10)).build();
		var altClient = getAltClient();
		altClient.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		tagCompare(inputTagSet.tagSet(), getResponse.tagSet());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var inputTagSet = Tagging.builder().tagSet(makeSimpleTagSet(10)).build();
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(inputTagSet));

		var altClient = getAltClient();
		altClient.deleteObjectTagging(d -> d.bucket(bucketName).key(key));

		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(0, getResponse.tagSet().size());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		client.putObjectTagging(p -> p.bucket(bucketName).key(publicTag).tagging(t -> t.tagSet(
				software.amazon.awssdk.services.s3.model.Tag.builder().key("security").value("public").build(),
				software.amazon.awssdk.services.s3.model.Tag.builder().key("foo").value("bar").build())));

		client.putObjectTagging(p -> p.bucket(bucketName).key(privateTag).tagging(t -> t.tagSet(
				software.amazon.awssdk.services.s3.model.Tag.builder().key("security").value("private").build())));

		client.putObjectTagging(p -> p.bucket(bucketName).key(invalidTag).tagging(t -> t.tagSet(
				software.amazon.awssdk.services.s3.model.Tag.builder().key("security1").value("public").build())));

		var altClient = getAltClient();
		var getResponse = altClient.getObject(g -> g.bucket(bucketName).key(publicTag));
		assertNotNull(getResponse);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(privateTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(invalidTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		client.putObjectTagging(p -> p.bucket(bucketName).key(publicTag).tagging(t -> t.tagSet(
				tag1 -> tag1.key("security").value("public"), tag2 -> tag2.key("foo").value("bar"))));

		client.putObjectTagging(p -> p.bucket(bucketName).key(privateTag).tagging(t -> t.tagSet(
				tag -> tag.key("security").value("private").build())));

		client.putObjectTagging(p -> p.bucket(bucketName).key(invalidTag).tagging(t -> t.tagSet(
				tag -> tag.key("security1").value("public").build())));

		var altClient = getAltClient();
		var getResponse = altClient.getObjectTagging(g -> g.bucket(bucketName).key(publicTag));
		assertNotNull(getResponse);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(publicTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(privateTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(invalidTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		client.putObjectTagging(p -> p.bucket(bucketName).key(publicTag).tagging(
				t -> t.tagSet(tag1 -> tag1.key("security").value("public"), tag2 -> tag2.key("foo").value("bar"))));
		client.putObjectTagging(
				p -> p.bucket(bucketName).key(privateTag)
						.tagging(t -> t.tagSet(tag -> tag.key("security").value("private"))));

		client.putObjectTagging(p -> p.bucket(bucketName).key(invalidTag)
				.tagging(t -> t.tagSet(tag -> tag.key("security1").value("public"))));

		var testTagSet = Tagging.builder()
				.tagSet(tag1 -> tag1.key("security").value("public"), tag2 -> tag2.key("foo").value("bar")).build();

		var altClient = getAltClient();
		altClient.putObjectTagging(p -> p.bucket(bucketName).key(publicTag).tagging(testTagSet));

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.putObjectTagging(p -> p.bucket(bucketName).key(privateTag).tagging(testTagSet)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());

		altClient.putObjectTagging(p -> p.bucket(bucketName).key(publicTag)
				.tagging(t -> t.tagSet(tag -> tag.key("security").value("private").build())));

		e = assertThrows(AwsServiceException.class,
				() -> altClient.putObjectTagging(p -> p.bucket(bucketName).key(invalidTag).tagging(t -> t.tagSet(
						tag1 -> tag1.key("security").value("public"), tag2 -> tag2.key("foo").value("bar")))));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
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
		client.putBucketPolicy(p -> p.bucket(sourceBucketName).policy(policyDocument.toString()));

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-copy-source", String.format("%s/public/*", sourceBucketName));
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", targetBucketName, "*"));
		var policyDocument2 = makeJsonPolicy("s3:PutObject", resource, null, tagConditional);
		client.putBucketPolicy(p -> p.bucket(targetBucketName).policy(policyDocument2.toString()));

		var newFoo = "newFoo";
		altClient.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(publicFoo)
				.destinationBucket(targetBucketName).destinationKey(newFoo));

		var response = altClient.getObject(g -> g.bucket(targetBucketName).key(newFoo));
		var body = getBody(response);
		assertEquals(publicFoo, body);

		var newFoo2 = "newFoo2";
		altClient.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(publicBar)
				.destinationBucket(targetBucketName).destinationKey(newFoo2));

		response = altClient.getObject(g -> g.bucket(targetBucketName).key(newFoo2));
		body = getBody(response);
		assertEquals(publicBar, body);

		assertThrows(AwsServiceException.class,
				() -> altClient.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(privateFoo)
						.destinationBucket(targetBucketName).destinationKey(newFoo2)));
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
		client.putBucketPolicy(p -> p.bucket(sourceBucketName).policy(policyDocument.toString()));

		var targetBucketName = createBucketCannedAcl(client, 12);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-metadata-directive", "COPY");
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", targetBucketName, "*"));
		var policyDocument2 = makeJsonPolicy("s3:PutObject", resource, null, s3Conditional);
		client.putBucketPolicy(p -> p.bucket(targetBucketName).policy(policyDocument2.toString()));

		var altClient = getAltClient();
		var newFoo = "newFoo";
		altClient.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(publicFoo)
				.destinationBucket(targetBucketName).destinationKey(newFoo)
				.metadataDirective(MetadataDirective.COPY));

		var response = altClient.getObject(g -> g.bucket(targetBucketName).key(newFoo));
		var body = getBody(response);
		assertEquals(publicFoo, body);

		var newFoo2 = "newFoo2";
		assertThrows(AwsServiceException.class,
				() -> altClient.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(publicBar)
						.destinationBucket(targetBucketName).destinationKey(newFoo2)));

		assertThrows(AwsServiceException.class,
				() -> altClient.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(publicBar)
						.destinationBucket(targetBucketName).destinationKey(newFoo2)
						.metadataDirective(MetadataDirective.REPLACE)));
	}

	@Test
	@Tag("ACLOptions")
	public void testBucketPolicyPutObjAcl() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(client, 13);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-acl", "public*");
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = Utils.makeArnResource(String.format("%s/%s", bucketName, "*"));
		var s1 = makeJsonStatement("s3:PutObject", resource, null, null, null);
		var s2 = makeJsonStatement("s3:PutObject", resource, MainData.POLICY_EFFECT_DENY, null, tagConditional);
		var policyDocument = makeJsonPolicy(s1, s2);

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		var key1 = "private-key";
		altClient.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString(key1));

		var key2 = "public-key";
		var e = assertThrows(AwsServiceException.class,
				() -> altClient.putObject(p -> p.bucket(bucketName).key(key2).acl(ObjectCannedACL.PUBLIC_READ),
						RequestBody.fromString(key2)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
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

		client.putBucketPolicy(p -> p.bucket(bucketName1).policy(policyDocument.toString()));
		client.putBucketPolicy(p -> p.bucket(bucketName2).policy(policyDocument2.toString()));

		var altClient = getAltClient();
		var key1 = "key1";

		altClient.putObject(p -> p.bucket(bucketName1).key(key1).grantFullControl(ownerId),
				RequestBody.fromString(key1));

		var key2 = "key2";
		altClient.putObject(p -> p.bucket(bucketName2).key(key2), RequestBody.fromString(key2));

		var acl1Response = client.getObjectAcl(g -> g.bucket(bucketName1).key(key1));

		assertThrows(AwsServiceException.class, () -> client.getObjectAcl(g -> g.bucket(bucketName1).key(key2)));

		var acl2Response = altClient.getObjectAcl(g -> g.bucket(bucketName2).key(key2));

		assertEquals(mainUserId, acl1Response.grants().get(0).grantee().id());
		assertEquals(altUserId, acl2Response.grants().get(0).grantee().id());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		client.putObjectTagging(p -> p.bucket(bucketName).key(publicTag).tagging(t -> t.tagSet(
				software.amazon.awssdk.services.s3.model.Tag.builder().key("security").value("public").build(),
				software.amazon.awssdk.services.s3.model.Tag.builder().key("foo").value("bar").build())));

		client.putObjectTagging(p -> p.bucket(bucketName).key(privateTag).tagging(t -> t.tagSet(
				software.amazon.awssdk.services.s3.model.Tag.builder().key("security").value("private").build())));

		client.putObjectTagging(p -> p.bucket(bucketName).key(invalidTag).tagging(t -> t.tagSet(
				software.amazon.awssdk.services.s3.model.Tag.builder().key("security1").value("public").build())));

		var altClient = getAltClient();
		var aclResponse = altClient.getObjectAcl(g -> g.bucket(bucketName).key(publicTag));
		assertNotNull(aclResponse);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getObject(g -> g.bucket(bucketName).key(publicTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(privateTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(invalidTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithAllUser() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 16);

		assertThrows(AwsServiceException.class,
				() -> client.getBucketPolicyStatus(g -> g.bucket(bucketName)));

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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));
		var response = client.getBucketPolicyStatus(g -> g.bucket(bucketName));
		assertTrue(response.policyStatus().isPublic());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithSpecificUserAccess() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 17);

		assertThrows(AwsServiceException.class,
				() -> client.getBucketPolicyStatus(g -> g.bucket(bucketName)));

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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));
		var response = client.getBucketPolicyStatus(g -> g.bucket(bucketName));
		assertFalse(response.policyStatus().isPublic());
	}

	@Test
	@Tag("Status")
	public void testBucketPolicyStatusWithWideIPRange() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 18);

		// 너무 넓은 IP 범위를 가진 정책 (public으로 간주)
		var policyDocument = new JsonObject();
		policyDocument.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statements = new JsonArray();
		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, MainData.POLICY_EFFECT_ALLOW);
		var principal = new JsonObject();
		principal.addProperty("AWS", "*");
		statement.add(MainData.POLICY_PRINCIPAL, principal);
		statement.addProperty(MainData.POLICY_ACTION, "s3:GetObject");
		statement.addProperty(MainData.POLICY_RESOURCE, Utils.makeArnResource(String.format("%s/*", bucketName)));

		var ipAddress = new JsonObject();
		ipAddress.addProperty("aws:SourceIp", "0.0.0.0/1"); // 너무 넓은 IP 범위
		var condition = new JsonObject();
		condition.add("IpAddress", ipAddress);
		statement.add(MainData.POLICY_CONDITION, condition);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));
		var response = client.getBucketPolicyStatus(g -> g.bucket(bucketName));
		assertTrue(response.policyStatus().isPublic());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));
		var response = client.getBucketPolicyStatus(g -> g.bucket(bucketName));
		assertFalse(response.policyStatus().isPublic());
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

		// 매우 제한적인 시간대에만 허용하는 정책 (1초)
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));
		var response = client.getBucketPolicyStatus(g -> g.bucket(bucketName));
		assertTrue(response.policyStatus().isPublic());
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

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));
		var response = client.getBucketPolicyStatus(g -> g.bucket(bucketName));
		assertTrue(response.policyStatus().isPublic());
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

	static void assertMalformedPolicy(S3Client client, String bucketName, String policy) {
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_POLICY,
				() -> client.putBucketPolicy(p -> p.bucket(bucketName).policy(policy)));
	}

	@Test
	@Tag("Get")
	public void testBucketPolicyGetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 22);

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET_POLICY,
				() -> client.getBucketPolicy(g -> g.bucket(bucketName)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET_POLICY,
				() -> client.getBucketPolicyStatus(g -> g.bucket(bucketName)));
	}

	@Test
	@Tag("Delete")
	public void testBucketPolicyDeleteNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 23);

		// 정책이 없어도 삭제는 성공한다
		var response = client.deleteBucketPolicy(d -> d.bucket(bucketName));
		assertEquals(HttpStatus.SC_NO_CONTENT, response.sdkHttpResponse().statusCode());
	}

	@Test
	@Tag("Check")
	public void testBucketPolicyPutGetDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 24);
		var statement = denyStatement(bucketName);
		statement.addProperty("Sid", "deny-get");
		var policy = policyDocumentOf(statement);
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policy));

		assertEquals(JsonParser.parseString(policy),
				JsonParser.parseString(client.getBucketPolicy(g -> g.bucket(bucketName)).policy()));
		assertFalse(client.getBucketPolicyStatus(g -> g.bucket(bucketName)).policyStatus().isPublic());

		client.deleteBucketPolicy(d -> d.bucket(bucketName));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET_POLICY,
				() -> client.getBucketPolicy(g -> g.bucket(bucketName)));
	}

	@Test
	@Tag("ERROR")
	public void testBucketPolicyBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(25);
		var policy = policyDocumentOf(denyStatement(bucketName));

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.getBucketPolicy(g -> g.bucket(bucketName)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.putBucketPolicy(p -> p.bucket(bucketName).policy(policy)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.deleteBucketPolicy(d -> d.bucket(bucketName)));
	}

	@Test
	@Tag("Access")
	public void testBucketPolicyAccessDenied() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 26);
		var policy = policyDocumentOf(denyStatement(bucketName));
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policy));

		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.getBucketPolicy(g -> g.bucket(bucketName)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.putBucketPolicy(p -> p.bucket(bucketName).policy(policy)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
				() -> altClient.deleteBucketPolicy(d -> d.bucket(bucketName)));
		// 정책은 그대로 남아 있다
		assertEquals(JsonParser.parseString(policy),
				JsonParser.parseString(client.getBucketPolicy(g -> g.bucket(bucketName)).policy()));
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

		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET_POLICY,
				() -> client.getBucketPolicy(g -> g.bucket(bucketName)));
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
}
