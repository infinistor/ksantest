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
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.function.Executable;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

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

	static void putPolicy(S3Client client, String bucketName, JsonObject... statements) {
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocumentOf(statements)));
	}

	static void assertDenied(Executable executable) {
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, executable);
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyAllowUser() {
		var key = "testBucketPolicyAllowUser";
		var client = getClient();
		var bucketName = createBucket(client, 31);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")));
		assertFalse(client.getBucketPolicyStatus(g -> g.bucket(bucketName)).policyStatus().isPublic());

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		assertEquals(7, altClient.headObject(h -> h.bucket(bucketName).key(key)).contentLength().intValue());
		// 허용한 액션 말고는 그대로 거부된다
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x")));
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.getObjectAcl(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key(key)));
		// 목록 권한이 없으면 없는 키도 403 이다
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key("not-exist")));
		// 정책에 없는 주체
		assertDenied(() -> getPublicClient().getObject(g -> g.bucket(bucketName).key(key)));
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
		assertEquals("public/a", getBody(altClient.getObject(g -> g.bucket(bucketName).key("public/a"))));
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key("private/a")));
		// Deny 의 주체가 아닌 소유자는 읽을 수 있다
		assertEquals("private/a", getBody(getClient().getObject(g -> g.bucket(bucketName).key("private/a"))));
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyDenyOwner() {
		var key = "testBucketPolicyDenyOwner";
		var client = getClient();
		var bucketName = createBucket(client, 33);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		putPolicy(client, bucketName, with(statementOf("Deny", "s3:GetObject", arnOf(bucketName + "/*")),
				MainData.POLICY_PRINCIPAL, "*"));

		// 명시적 Deny 는 버킷 소유자에게도 적용된다
		var owner = getClient();
		assertDenied(() -> owner.getObject(g -> g.bucket(bucketName).key(key)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, null, () -> owner.headObject(h -> h.bucket(bucketName).key(key)));
		assertDenied(() -> owner.copyObject(c -> c.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey("copy")));
		// 거부하지 않은 액션은 그대로 된다
		owner.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x"));
		assertTrue(owner.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet().isEmpty());
	}

	@Test
	@Tag("NotAction")
	public void testBucketPolicyNotActionDeny() {
		var key = "testBucketPolicyNotActionDeny";
		var client = getClient();
		var bucketName = createBucket(client, 34);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		var resources = List.of(arnOf(bucketName), arnOf(bucketName + "/*"));
		putPolicy(client, bucketName,
				statementOf("Allow", "s3:*", resources),
				// GetObject, ListBucket 이 아닌 모든 액션을 거부
				with(statementOf("Deny", null, resources), "NotAction", List.of("s3:GetObject", "s3:ListBucket")));

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		assertEquals(1, altClient.listObjectsV2(l -> l.bucket(bucketName)).keyCount().intValue());
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x")));
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.getObjectTagging(g -> g.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("NotAction")
	public void testBucketPolicyNotActionAllow() {
		var key = "testBucketPolicyNotActionAllow";
		var client = getClient();
		var bucketName = createBucket(client, 35);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		// PutObject, DeleteObject 가 아닌 모든 오브젝트 액션을 허용
		putPolicy(client, bucketName, with(statementOf("Allow", null, arnOf(bucketName + "/*")),
				"NotAction", List.of("s3:PutObject", "s3:DeleteObject")));

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		assertTrue(altClient.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet().isEmpty());
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x")));
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key(key)));
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
		assertEquals("public/a", getBody(altClient.getObject(g -> g.bucket(bucketName).key("public/a"))));
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key("private/a")));
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
		assertEquals("public/a", getBody(altClient.getObject(g -> g.bucket(bucketName).key("public/a"))));
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key("private/a")));
	}

	@Test
	@Tag("NotPrincipal")
	public void testBucketPolicyNotPrincipal() {
		var key = "testBucketPolicyNotPrincipal";
		var client = getClient();
		var bucketName = createBucket(client, 38);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		var resource = arnOf(bucketName + "/*");
		// Allow 에는 NotPrincipal 을 쓸 수 없다
		var allowNotPrincipal = with(statementOf("Allow", "s3:GetObject", resource), MainData.POLICY_PRINCIPAL, null);
		with(allowNotPrincipal, MainData.POLICY_NOT_PRINCIPAL, altPrincipal());
		assertMalformedPolicy(client, bucketName, policyDocumentOf(allowNotPrincipal));

		// 서브 사용자가 아닌 모든 주체의 GetObject 를 거부(소유자 포함)
		var deny = with(statementOf("Deny", "s3:GetObject", resource), MainData.POLICY_PRINCIPAL, null);
		with(deny, MainData.POLICY_NOT_PRINCIPAL, altPrincipal());
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", resource), deny);

		assertEquals("content", getBody(getAltClient().getObject(g -> g.bucket(bucketName).key(key))));
		assertDenied(() -> getClient().getObject(g -> g.bucket(bucketName).key(key)));
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
			assertEquals(key, getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		for (var key : List.of("a1/file10.txt", "b1/file1.txt", "a1/file1.TXT"))
			assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("Wildcard")
	public void testBucketPolicyActionWildcard() {
		var key = "testBucketPolicyActionWildcard";
		var client = getClient();
		var bucketName = createBucket(client, 40);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		putPolicy(client, bucketName,
				statementOf("Allow", "s3:Get*", List.of(arnOf(bucketName), arnOf(bucketName + "/*"))));

		var altClient = getAltClient();
		assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		assertEquals(1, altClient.getObjectAcl(g -> g.bucket(bucketName).key(key)).grants().size());
		assertTrue(altClient.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet().isEmpty());
		assertNull(altClient.getBucketVersioning(g -> g.bucket(bucketName)).status());
		altClient.getBucketLocation(g -> g.bucket(bucketName));
		altClient.getBucketAcl(g -> g.bucket(bucketName));
		// Get 으로 시작하지 않는 액션
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x")));
	}

	@Test
	@Tag("Wildcard")
	public void testBucketPolicyActionCaseInsensitive() {
		var key = "testBucketPolicyActionCaseInsensitive";
		var client = getClient();
		var bucketName = createBucket(client, 41);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		// 액션 이름은 대소문자를 구분하지 않는다
		putPolicy(client, bucketName, statementOf("Allow", "s3:getobject", arnOf(bucketName + "/*")));
		assertEquals("content", getBody(getAltClient().getObject(g -> g.bucket(bucketName).key(key))));
	}

	@Test
	@Tag("Resource")
	public void testBucketPolicyListBucketResource() {
		var key = "testBucketPolicyListBucketResource";
		var client = getClient();
		var bucketName = createBucket(client, 42);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		putPolicy(client, bucketName, statementOf("Allow", "s3:ListBucket", arnOf(bucketName)));

		var altClient = getAltClient();
		assertEquals(1, altClient.listObjectsV2(l -> l.bucket(bucketName)).keyCount().intValue());
		assertEquals(1, altClient.listObjects(l -> l.bucket(bucketName)).contents().size());
		altClient.headBucket(h -> h.bucket(bucketName));
		// ListBucket 만으로는 읽을 수 없고, 버전·멀티파트 목록은 다른 액션이다
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key(key)));
		assertDenied(() -> altClient.listObjectVersions(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.listMultipartUploads(l -> l.bucket(bucketName)));
		// 목록 권한이 있으면 없는 키는 404 다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> altClient.getObject(g -> g.bucket(bucketName).key("not-exist")));
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
		assertEquals(1,
				altClient.listObjectsV2(l -> l.bucket(bucketName).prefix("public/")).keyCount().intValue());
		assertEquals(1, altClient.listObjects(l -> l.bucket(bucketName).prefix("public/")).contents().size());
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName).prefix("public/a")));
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName).prefix("private/")));
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
		var response = altClient
				.listObjectsV2(l -> l.bucket(bucketName).prefix("public/").delimiter("/").maxKeys(10));
		assertEquals(1, response.keyCount().intValue());
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName).prefix("public/").maxKeys(10)));
		assertDenied(() -> altClient
				.listObjectsV2(l -> l.bucket(bucketName).prefix("public/").delimiter("/").maxKeys(11)));
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName).prefix("public/").delimiter("/")));
		assertDenied(() -> altClient
				.listObjectsV2(l -> l.bucket(bucketName).prefix("private/").delimiter("/").maxKeys(10)));
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
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName).prefix("private/")));
		// 조건은 요청의 prefix 값만 본다. 응답에 private/ 아래 키가 들어 있어도 막지 않는다
		assertEquals(2, altClient.listObjectsV2(l -> l.bucket(bucketName)).keyCount().intValue());
		assertEquals(1, altClient.listObjectsV2(l -> l.bucket(bucketName).prefix("priv")).keyCount().intValue());
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
		assertEquals(1, altClient.listObjectsV2(l -> l.bucket(bucketName).maxKeys(10)).keyCount().intValue());
		assertEquals(1, altClient.listObjects(l -> l.bucket(bucketName).maxKeys(5)).contents().size());
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName).maxKeys(11)));
		// max-keys 를 보내지 않으면 조건 키가 없어 맞지 않는다
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicySecureTransport() {
		var key = "testBucketPolicySecureTransport";
		var client = getClient();
		var bucketName = createBucket(client, 47);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		var deny = with(statementOf("Deny", "s3:GetObject", arnOf(bucketName + "/*")), MainData.POLICY_PRINCIPAL, "*");
		with(deny, MainData.POLICY_CONDITION, conditionOf("Bool", "aws:SecureTransport", "false"));
		putPolicy(client, bucketName, deny);

		var httpClient = createClient(false, config.mainUser, false, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
		var httpsClient = getClientHttps(false);
		assertEquals("content", getBody(httpsClient.getObject(g -> g.bucket(bucketName).key(key))));
		assertDenied(() -> httpClient.getObject(g -> g.bucket(bucketName).key(key)));
		// 거부한 액션이 아니면 http 로도 된다
		httpClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x"));
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
			client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
			putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")),
					MainData.POLICY_CONDITION, conditions.get(i)));
			var altClient = getAltClient();
			if (allowed.get(i))
				assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
			else
				assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key(key)));
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
		assertDenied(() -> owner.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content")));
		owner.putObject(p -> p.bucket(bucketName).key(key).serverSideEncryption(ServerSideEncryption.AES256),
				RequestBody.fromString("content"));
		// 멀티파트 시작과 복사도 PutObject 액션이다
		assertDenied(() -> owner.createMultipartUpload(c -> c.bucket(bucketName).key("multi")));
		assertDenied(() -> owner.copyObject(c -> c.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey("copy")));
		owner.copyObject(c -> c.sourceBucket(bucketName).sourceKey(key).destinationBucket(bucketName)
				.destinationKey("copy").serverSideEncryption(ServerSideEncryption.AES256));
		assertEquals("content", getBody(owner.getObject(g -> g.bucket(bucketName).key("copy"))));
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
		assertDenied(() -> owner.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content")));
		owner.putObject(p -> p.bucket(bucketName).key(key).serverSideEncryption(ServerSideEncryption.AES256),
				RequestBody.fromString("content"));
		assertEquals(ServerSideEncryption.AES256,
				owner.headObject(h -> h.bucket(bucketName).key(key)).serverSideEncryption());
	}

	@Test
	@Tag("Version")
	public void testBucketPolicyGetObjectVersionAction() {
		var key = "testBucketPolicyGetObjectVersionAction";
		var client = getClient();
		var bucketName = createBucket(client, 51);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var version1 = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("1")).versionId();
		var version2 = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("2")).versionId();
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		assertEquals("2", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		// 버전을 지정한 조회는 s3:GetObjectVersion 이 필요하다(최신 버전이어도)
		for (var versionId : List.of(version1, version2))
			assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key(key).versionId(versionId)));
		assertS3Error(HttpStatus.SC_FORBIDDEN, null,
				() -> altClient.headObject(h -> h.bucket(bucketName).key(key).versionId(version1)));
	}

	@Test
	@Tag("Version")
	public void testBucketPolicyVersionIdCondition() {
		var key = "testBucketPolicyVersionIdCondition";
		var client = getClient();
		var bucketName = createBucket(client, 52);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var version1 = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("1")).versionId();
		var version2 = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("2")).versionId();
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObjectVersion", arnOf(bucketName + "/*")),
				MainData.POLICY_CONDITION, conditionOf("StringEquals", "s3:VersionId", version1)));

		var altClient = getAltClient();
		assertEquals("1", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key).versionId(version1))));
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key(key).versionId(version2)));
		// 버전을 지정하지 않은 조회는 s3:GetObject 가 필요하다
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("Version")
	public void testBucketPolicyDeleteObjectVersionAction() {
		var key = "testBucketPolicyDeleteObjectVersionAction";
		var client = getClient();
		var bucketName = createBucket(client, 53);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("1")).versionId();
		putPolicy(client, bucketName, statementOf("Allow", "s3:DeleteObject", arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		// 버전을 지정한 삭제는 s3:DeleteObjectVersion 이 필요하다
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId)));
		var response = altClient.deleteObjects(d -> d.bucket(bucketName)
				.delete(o -> o.objects(ObjectIdentifier.builder().key(key).versionId(versionId).build())));
		assertTrue(response.deleted().isEmpty());
		assertEquals(MainData.ACCESS_DENIED, response.errors().get(0).code());
		// 버전을 지정하지 않은 삭제(삭제 마커)는 된다
		assertTrue(altClient.deleteObject(d -> d.bucket(bucketName).key(key)).deleteMarker());
		assertEquals("1", getBody(client.getObject(g -> g.bucket(bucketName).key(key).versionId(versionId))));
	}

	@Test
	@Tag("Condition")
	public void testBucketPolicyRequestObjectTagKeys() {
		var client = getClient();
		var bucketName = createBucket(client, 54, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		// 요청의 태그 키가 모두 project, team 안에 있어야 한다
		putPolicy(client, bucketName,
				with(statementOf("Allow", List.of("s3:PutObject", "s3:PutObjectTagging"), arnOf(bucketName + "/*")),
						MainData.POLICY_CONDITION, jsonObjectOf("ForAllValues:StringEquals",
								jsonObjectOf("s3:RequestObjectTagKeys", List.of("project", "team")))));

		var altClient = getAltClient();
		altClient.putObject(p -> p.bucket(bucketName).key("allowed").tagging("project=a"),
				RequestBody.fromString("x"));
		// 태그가 없으면 ForAllValues 는 참이다
		altClient.putObject(p -> p.bucket(bucketName).key("no-tag"), RequestBody.fromString("x"));
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key("denied").tagging("project=a&owner=b"),
				RequestBody.fromString("x")));
		var tagSet = client.getObjectTagging(g -> g.bucket(bucketName).key("allowed")).tagSet();
		assertEquals(1, tagSet.size());
		assertEquals("project", tagSet.get(0).key());
		assertEquals("a", tagSet.get(0).value());
	}

	@Test
	@Tag("Action")
	public void testBucketPolicyPutObjectAction() {
		var key = "testBucketPolicyPutObjectAction";
		var client = getClient();
		var bucketName = createBucket(client, 55, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		putPolicy(client, bucketName, statementOf("Allow", "s3:PutObject", arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		altClient.putObject(p -> p.bucket(bucketName).key("new"), RequestBody.fromString("x"));
		// 멀티파트 시작·파트 업로드는 PutObject 로, 시작한 사용자는 파트 목록·중단도 할 수 있다
		var uploadId = altClient.createMultipartUpload(c -> c.bucket(bucketName).key("multi")).uploadId();
		altClient.uploadPart(u -> u.bucket(bucketName).key("multi").uploadId(uploadId).partNumber(1),
				RequestBody.fromString("x"));
		assertEquals(1,
				altClient.listParts(l -> l.bucket(bucketName).key("multi").uploadId(uploadId)).parts().size());
		altClient.abortMultipartUpload(a -> a.bucket(bucketName).key("multi").uploadId(uploadId));

		// 태그를 붙이려면 PutObjectTagging, 복사하려면 원본의 GetObject 가 필요하다
		assertDenied(() -> altClient.putObject(p -> p.bucket(bucketName).key("tagged").tagging("a=b"),
				RequestBody.fromString("x")));
		assertDenied(() -> altClient.copyObject(c -> c.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey("copy")));
		assertDenied(() -> altClient.listMultipartUploads(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.getObject(g -> g.bucket(bucketName).key("new")));
		assertDenied(() -> altClient.deleteObject(d -> d.bucket(bucketName).key("new")));
	}

	@Test
	@Tag("Action")
	public void testBucketPolicyMultipartListActions() {
		var key = "testBucketPolicyMultipartListActions";
		var client = getClient();
		var bucketName = createBucket(client, 56);
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(key)).uploadId();
		client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1),
				RequestBody.fromString("x"));
		putPolicy(client, bucketName,
				statementOf("Allow", List.of("s3:ListMultipartUploadParts", "s3:ListBucketMultipartUploads"),
						List.of(arnOf(bucketName), arnOf(bucketName + "/*"))));

		var altClient = getAltClient();
		assertEquals(1, altClient.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadId)).parts().size());
		assertEquals(1, altClient.listMultipartUploads(l -> l.bucket(bucketName)).uploads().size());
		assertDenied(() -> altClient.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadId)));
		assertDenied(() -> altClient.uploadPart(
				u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(2), RequestBody.fromString("x")));
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
		assertNull(altClient.getBucketVersioning(g -> g.bucket(bucketName)).status());
		altClient.getBucketAcl(g -> g.bucket(bucketName));
		altClient.getBucketLocation(g -> g.bucket(bucketName));
		assertTrue(altClient.listObjectVersions(l -> l.bucket(bucketName)).versions().isEmpty());
		// 권한은 있고 설정이 없으면 403 이 아니라 404 다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_TAG_SET,
				() -> altClient.getBucketTagging(g -> g.bucket(bucketName)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_CORS_CONFIGURATION,
				() -> altClient.getBucketCors(g -> g.bucket(bucketName)));
		// 설정 삭제는 Put 액션으로 판정한다
		altClient.putBucketTagging(p -> p.bucket(bucketName).tagging(t -> t.tagSet(s -> s.key("a").value("b"))));
		var tagSet = client.getBucketTagging(g -> g.bucket(bucketName)).tagSet();
		assertEquals(1, tagSet.size());
		assertEquals("a", tagSet.get(0).key());
		assertEquals("b", tagSet.get(0).value());
		altClient.deleteBucketTagging(d -> d.bucket(bucketName));
		altClient.putBucketCors(p -> p.bucket(bucketName)
				.corsConfiguration(c -> c.corsRules(r -> r.allowedMethods("GET").allowedOrigins("*"))));
		altClient.deleteBucketCors(d -> d.bucket(bucketName));

		// 허용하지 않은 액션
		assertDenied(() -> altClient.listObjectsV2(l -> l.bucket(bucketName)));
		assertDenied(() -> altClient.putBucketVersioning(
				p -> p.bucket(bucketName).versioningConfiguration(c -> c.status(BucketVersioningStatus.ENABLED))));
		assertDenied(() -> altClient.getBucketLifecycleConfiguration(g -> g.bucket(bucketName)));
		assertDenied(() -> altClient.deleteBucket(d -> d.bucket(bucketName)));
	}

	@Test
	@Tag("Form")
	public void testBucketPolicyStatementForms() {
		var key = "testBucketPolicyStatementForms";
		var client = getClient();
		var bucketName = createBucket(client, 58);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		var resource = arnOf(bucketName + "/*");
		var statement = statementOf("Allow", "s3:GetObject", resource);

		// Statement 는 배열이 아니라 객체 하나여도 된다
		var single = with(jsonObjectOf(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE),
				MainData.POLICY_STATEMENT, statement);
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(single.toString()));
		assertEquals("content", getBody(getAltClient().getObject(g -> g.bucket(bucketName).key(key))));

		// Version 을 빼면 2008-10-17 로 저장된다
		var noVersion = jsonObjectOf(MainData.POLICY_STATEMENT, List.of(statement));
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(noVersion.toString()));
		assertEquals("2008-10-17",
				JsonParser.parseString(client.getBucketPolicy(g -> g.bucket(bucketName)).policy()).getAsJsonObject()
						.get(MainData.POLICY_VERSION).getAsString());
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policyDocumentOf("2008-10-17", statement)));

		// Id 와 Sid 는 그대로 돌려준다. Sid 가 겹쳐도 받는다
		var policy = jsonObjectOf(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);
		with(policy, "Id", "policy-id");
		with(policy, MainData.POLICY_STATEMENT, List.of(
				with(statementOf("Allow", "s3:GetObject", resource), "Sid", "same"),
				with(statementOf("Allow", "s3:GetObjectTagging", resource), "Sid", "same")));
		client.putBucketPolicy(p -> p.bucket(bucketName).policy(policy.toString()));
		var actual = JsonParser.parseString(client.getBucketPolicy(g -> g.bucket(bucketName)).policy())
				.getAsJsonObject();
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
		assertEquals("content", getBody(getAltClient().getObject(g -> g.bucket(bucketName).key(key))));
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
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET_POLICY,
				() -> client.getBucketPolicy(g -> g.bucket(bucketName)));
	}

	@Test
	@Tag("Delete")
	public void testBucketPolicyDeleteRevokes() {
		var key = "testBucketPolicyDeleteRevokes";
		var client = getClient();
		var bucketName = createBucket(client, 60);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		putPolicy(client, bucketName, statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/*")));
		assertEquals("content", getBody(getAltClient().getObject(g -> g.bucket(bucketName).key(key))));

		client.deleteBucketPolicy(d -> d.bucket(bucketName));
		assertDenied(() -> getAltClient().getObject(g -> g.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyOtherUserWrite() {
		var key = "testBucketPolicyOtherUserWrite";
		var client = getClient();
		var bucketName = createBucket(client, 61, ObjectOwnership.BUCKET_OWNER_ENFORCED);
		putPolicy(client, bucketName, statementOf("Allow",
				List.of("s3:PutObject", "s3:GetObject", "s3:DeleteObject"), arnOf(bucketName + "/*")));

		var altClient = getAltClient();
		altClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		// 다른 사용자가 올린 오브젝트도 버킷 소유자의 것이다
		assertEquals("content", getBody(client.getObject(g -> g.bucket(bucketName).key(key))));
		assertEquals("content", getBody(altClient.getObject(g -> g.bucket(bucketName).key(key))));
		var response = altClient.deleteObjects(d -> d.bucket(bucketName)
				.delete(o -> o.objects(ObjectIdentifier.builder().key(key).build(),
						ObjectIdentifier.builder().key("not-exist").build())));
		assertEquals(List.of("not-exist", key), response.deleted().stream().map(d -> d.key()).sorted().toList());
		assertTrue(response.errors().isEmpty());
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.getObject(g -> g.bucket(bucketName).key(key)));
	}

	@Test
	@Tag("Effect")
	public void testBucketPolicyAnonymousPrefix() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 62);
		createObjects(client, bucketName, List.of("public/a", "private/a"));
		putPolicy(client, bucketName, with(statementOf("Allow", "s3:GetObject", arnOf(bucketName + "/public/*")),
				MainData.POLICY_PRINCIPAL, "*"));
		assertTrue(client.getBucketPolicyStatus(g -> g.bucket(bucketName)).policyStatus().isPublic());

		var publicClient = getPublicClient();
		assertEquals("public/a", getBody(publicClient.getObject(g -> g.bucket(bucketName).key("public/a"))));
		assertDenied(() -> publicClient.getObject(g -> g.bucket(bucketName).key("private/a")));
		assertDenied(
				() -> publicClient.putObject(p -> p.bucket(bucketName).key("public/b"), RequestBody.fromString("x")));
		assertDenied(() -> publicClient.listObjectsV2(l -> l.bucket(bucketName)));
	}
	// endregion
}
