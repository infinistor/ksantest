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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@SuppressWarnings("unchecked")
public class Policy extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Policy V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Policy V2 End");
	}

	@Test
	@Tag("Check")
	public void testBucketPolicy() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		var key = "asdf";
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
		var bucketName = createBucketCannedAcl(client);
		var key = "asdf";
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
		var bucketName = createBucketCannedAcl(client);
		var key = "asdf";
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
		var bucketName = createBucketCannedAcl(client);
		var key = "asdf";
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
		var key = "acl";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
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
		var key = "acl";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
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
		var key = "acl";
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		createKeyWithRandomContent(client, key, 0, bucketName);

		var resource = makeArnResource(String.format("%s/%s", bucketName, key));
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
		var bucketName = createBucketCannedAcl(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var bucketName = createBucketCannedAcl(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var bucketName = createBucketCannedAcl(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:PutObjectTagging", resource, null, tagConditional);

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));

		client.putObjectTagging(p -> p.bucket(bucketName).key(publicTag).tagging(
				t -> t.tagSet(tag1 -> tag1.key("security").value("public"), tag2 -> tag2.key("foo").value("bar"))));
		client.putObjectTagging(
				p -> p.bucket(bucketName).key(privateTag).tagging(t -> t.tagSet(tag -> tag.key("security").value("private"))));

		client.putObjectTagging(p -> p.bucket(bucketName).key(invalidTag)
				.tagging(t -> t.tagSet(tag -> tag.key("security1").value("public"))));

		var testTagSet = Tagging.builder()
				.tagSet(tag1 -> tag1.key("security").value("public"), tag2 -> tag2.key("foo").value("bar")).build();

		var altClient = getAltClient();
		altClient.putObjectTagging(p -> p.bucket(bucketName).key(publicTag).tagging(testTagSet));

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.putObjectTagging(p -> p.bucket(bucketName).key(privateTag).tagging(testTagSet)));

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
		var sourceBucketName = createBucketCannedAcl(client);
		var targetBucketName = createBucketCannedAcl(client);

		createObjects(client, sourceBucketName, List.of(publicFoo, publicBar, privateFoo));

		var sourceResource = makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.putBucketPolicy(p -> p.bucket(sourceBucketName).policy(policyDocument.toString()));

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-copy-source", String.format("%s/public/*", sourceBucketName));
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = makeArnResource(String.format("%s/%s", targetBucketName, "*"));
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
		var sourceBucketName = createBucketCannedAcl(client);
		createObjects(client, sourceBucketName, List.of(publicFoo, publicBar));

		var sourceResource = makeArnResource(String.format("%s/%s", sourceBucketName, "*"));
		var policyDocument = makeJsonPolicy("s3:GetObject", sourceResource, null, null);
		client.putBucketPolicy(p -> p.bucket(sourceBucketName).policy(policyDocument.toString()));

		var targetBucketName = createBucketCannedAcl(client);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-metadata-directive", "COPY");
		var s3Conditional = new JsonObject();
		s3Conditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", targetBucketName, "*"));
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
		var bucketName = createBucketCannedAcl(client);

		var conditional = new JsonObject();
		conditional.addProperty("s3:x-amz-acl", "public*");
		var tagConditional = new JsonObject();
		tagConditional.add("StringLike", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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
		var bucketName1 = createBucketCannedAcl(client);
		var bucketName2 = createBucketCannedAcl(client);

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

		client.putBucketPolicy(p -> p.bucket(bucketName1).policy(policyDocument.toString()));
		client.putBucketPolicy(p -> p.bucket(bucketName2).policy(policyDocument2.toString()));

		var altClient = getAltClient();
		var key1 = "key1";

		altClient.putObject(p -> p.bucket(bucketName1).key(key1).grantFullControl(ownerId), RequestBody.fromString(key1));

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
		var bucketName = createBucketCannedAcl(client);
		createObjects(client, bucketName, List.of(publicTag, privateTag, invalidTag));

		var conditional = new JsonObject();
		conditional.addProperty("s3:ExistingObjectTag/security", "public");
		var tagConditional = new JsonObject();
		tagConditional.add("StringEquals", conditional);

		var resource = makeArnResource(String.format("%s/%s", bucketName, "*"));
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

		e = assertThrows(AwsServiceException.class,
				() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(privateTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(invalidTag)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	@Test
	@Tag("Status")
	public void testGetPublicPolicyAclBucketPolicyStatus() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

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
	public void testGetNonpublicPolicyAclBucketPolicyStatus() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

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

		var ipAddress = new JsonObject();
		ipAddress.addProperty("aws:SourceIp", "10.0.0.0/32");
		var condition = new JsonObject();
		condition.add("IpAddress", ipAddress);
		statement.add(MainData.POLICY_CONDITION, condition);

		statements.add(statement);
		policyDocument.add(MainData.POLICY_STATEMENT, statements);

		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocument.toString()));
		var response = client.getBucketPolicyStatus(g -> g.bucket(bucketName));
		assertFalse(response.policyStatus().isPublic());
	}
}
