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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.example.Data.AES256;
import org.example.Data.MainData;
import org.example.Data.MultipartUploadData;
import org.example.Data.ObjectData;
import org.example.Data.RangeSet;
import org.example.Data.UserData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.s3tests.S3Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.platform.commons.util.StringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.IllegalBucketNameException;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MetadataDirective;
import com.amazonaws.services.s3.model.ObjectLockConfiguration;
import com.amazonaws.services.s3.model.ObjectLockRetention;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.TagSet;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class TestBase {

	/************************************************************************************************************/

	public enum EncryptionType {
		NORMAL, SSE_S3, SSE_C
	}

	/************************************************************************************************************/
	private static final int RANDOM_PREFIX_TEXT_LENGTH = 15;
	private static final int RANDOM_SUFFIX_TEXT_LENGTH = 5;
	private static final int BUCKET_MAX_LENGTH = 63;
	private static final String STR_RANDOM = "{random}";
	/************************************************************************************************************/

	private final ArrayList<String> buckets = new ArrayList<>();
	protected final S3Config config;

	protected TestBase() {
		String fileName = System.getenv(MainData.S3TESTS_INI);
		config = new S3Config(fileName);
		config.getConfig();
	}

	// region Create Client
	public AmazonS3 createClient(boolean isSecure, UserData user, Boolean useChunkEncoding, Boolean payloadSigning,
			String signatureVersion) {
		String address = "";
		ClientConfiguration s3Config;

		if (isSecure) {
			address = NetUtils.createURLToHTTPS(config.url, config.sslPort);
			System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
			s3Config = new ClientConfiguration().withProtocol(Protocol.HTTPS).withSignerOverride(signatureVersion);
		} else {
			address = NetUtils.createURLToHTTP(config.url, config.port);
			s3Config = new ClientConfiguration().withProtocol(Protocol.HTTP).withSignerOverride(signatureVersion);
		}
		s3Config.setSignerOverride(signatureVersion);
		s3Config.setMaxErrorRetry(1);
		s3Config.setConnectionTimeout(10000);
		s3Config.setSocketTimeout(10000);
		var clientBuilder = AmazonS3ClientBuilder.standard();

		if (user == null)
			clientBuilder.setCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
		else
			clientBuilder.setCredentials(
					new AWSStaticCredentialsProvider(new BasicAWSCredentials(user.accessKey, user.secretKey)));

		if (StringUtils.isBlank(config.url))
			clientBuilder.setRegion(config.regionName);
		else
			clientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(address, ""));

		clientBuilder.setClientConfiguration(s3Config);
		clientBuilder.setChunkedEncodingDisabled(useChunkEncoding);
		clientBuilder.setPayloadSigningEnabled(payloadSigning);
		clientBuilder.setPathStyleAccessEnabled(true);
		return clientBuilder.build();
	}

	public AmazonS3 getClient() {
		return createClient(config.isSecure, config.mainUser, true, true, config.getSignatureVersion());
	}

	public AmazonS3 getClientV2() {
		return createClient(config.isSecure, config.mainUser, true, true, S3Config.STR_SIGNATURE_VERSION_V2);
	}

	public AmazonS3 getClientV4(Boolean useChunkEncoding) {
		return createClient(config.isSecure, config.mainUser, useChunkEncoding, true,
				S3Config.STR_SIGNATURE_VERSION_V4);
	}

	public AmazonS3 getClientHttps() {
		return createClient(true, config.mainUser, true, true, config.getSignatureVersion());
	}

	public AmazonS3 getClientHttpsV4(Boolean useChunkEncoding, Boolean payloadSigning) {
		return createClient(true, config.mainUser, useChunkEncoding, payloadSigning, S3Config.STR_SIGNATURE_VERSION_V4);
	}

	public AmazonS3 getAltClient() {
		return createClient(config.isSecure, config.altUser, true, true, config.getSignatureVersion());
	}

	public AmazonS3 getPublicClient() {
		return createClient(config.isSecure, null, true, true, config.getSignatureVersion());
	}

	public AmazonS3 getBadAuthClient(String accessKey, String secretKey) {
		if (StringUtils.isBlank(accessKey))
			accessKey = "aaaaaaaaaaaaaaa";
		if (StringUtils.isBlank(secretKey))
			secretKey = "bbbbbbbbbbbbbbb";

		var dummyUser = new UserData();
		dummyUser.accessKey = accessKey;
		dummyUser.secretKey = secretKey;

		return createClient(config.isSecure, dummyUser, true, true, config.getSignatureVersion());
	}
	// endregion

	// region Create Data
	public AccessControlList getGrantList(String userId, Permission[] perms) {
		Permission[] allHeaders = new Permission[] { Permission.Read, Permission.Write, Permission.ReadAcp,
				Permission.WriteAcp, Permission.FullControl };

		var accessControlList = new AccessControlList();
		accessControlList.setOwner(new Owner(config.mainUser.userId, config.mainUser.displayName));

		if (StringUtils.isBlank(userId))
			userId = config.altUser.userId;
		var user = new CanonicalGrantee(userId);
		if (perms == null) {
			for (var perm : allHeaders)
				accessControlList.grantAllPermissions(new Grant(user, perm));
		} else {
			for (var Perm : perms)
				accessControlList.grantAllPermissions(new Grant(user, Perm));
		}
		return accessControlList;
	}

	public ObjectMetadata getACLHeader(String userId, String[] perms) {
		String[] allHeaders = { "read", "write", "read-acp", "write-acp", "full-control" };

		var headers = new ObjectMetadata();

		if (StringUtils.isBlank(userId))
			userId = config.altUser.userId;
		if (perms == null) {
			for (var perm : allHeaders)
				headers.setHeader(String.format("x-amz-grant-%s", perm), String.format("id=%s", userId));
		} else {
			for (var perm : perms)
				headers.setHeader(String.format("x-amz-grant-%s", perm), String.format("id=%s", userId));
		}
		return headers;
	}

	public String getPrefix() {
		return config.bucketPrefix.replace(STR_RANDOM, Utils.randomText(RANDOM_PREFIX_TEXT_LENGTH));
	}

	public String getNewBucketNameOnly() {
		String bucketName = getPrefix() + Utils.randomText(RANDOM_SUFFIX_TEXT_LENGTH);
		if (bucketName.length() > BUCKET_MAX_LENGTH)
			bucketName = bucketName.substring(0, BUCKET_MAX_LENGTH - 1);
		return bucketName;
	}

	public String getNewBucketName() {
		String bucketName = getPrefix() + Utils.randomText(RANDOM_SUFFIX_TEXT_LENGTH);
		if (bucketName.length() > BUCKET_MAX_LENGTH)
			bucketName = bucketName.substring(0, BUCKET_MAX_LENGTH - 1);
		buckets.add(bucketName);
		return bucketName;
	}

	public String getNewBucketName(String prefix) {
		String bucketName = prefix + Utils.randomText(RANDOM_PREFIX_TEXT_LENGTH);
		if (bucketName.length() > BUCKET_MAX_LENGTH)
			bucketName = bucketName.substring(0, BUCKET_MAX_LENGTH - 1);
		buckets.add(bucketName);
		return bucketName;
	}

	public String getNewBucketName(int length) {
		String bucketName = getPrefix() + Utils.randomText(63);
		bucketName = bucketName.substring(0, length);
		buckets.add(bucketName);
		return bucketName;
	}

	public String getNewBucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		var request = new CreateBucketRequest(bucketName).withObjectOwnership("BucketOwnerPreferred");
		client.createBucket(request);
		return bucketName;
	}

	public String getNewBucket(String prefix) {
		var bucketName = getNewBucketName(prefix);
		var client = getClient();
		client.createBucket(bucketName);
		return bucketName;
	}

	public String createObjects(List<String> keys) {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null) {
			for (var key : keys) {
				var body = key;
				if (key.endsWith("/"))
					body = "";
				client.putObject(bucketName, key, body);
			}
		}

		return bucketName;
	}

	public String createObjectsToBody(List<String> keys, String body) {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjectsV2(List<String> keys) {
		var client = getClientV2();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null) {
			for (var key : keys) {
				var body = key;
				if (key.endsWith("/"))
					body = "";
				client.putObject(bucketName, key, body);
			}
		}

		return bucketName;
	}

	public String createObjectsToBodyV2(List<String> keys, String body) {
		var client = getClientV2();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjectsV4(List<String> keys, Boolean useChunkEncoding) {
		var client = getClientV4(useChunkEncoding);
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null) {
			for (var key : keys) {
				var body = key;
				if (key.endsWith("/"))
					body = "";
				client.putObject(bucketName, key, body);
			}
		}

		return bucketName;
	}

	public String createObjectsHttps(List<String> keys, Boolean useChunkEncoding, Boolean payloadSigning) {
		var client = getClientHttpsV4(useChunkEncoding, payloadSigning);
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null) {
			for (var key : keys) {
				var body = key;
				if (key.endsWith("/"))
					body = "";
				client.putObject(bucketName, key, body);
			}
		}

		return bucketName;
	}

	public String createObjectsToBodyV4(List<String> keys, String body, Boolean useChunkEncoding) {
		var client = getClientV4(useChunkEncoding);
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjectsToBody(String bucketName, List<String> keys, String body) {
		var client = getClient();

		if (keys != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjects(List<String> keys, String bucketName) {
		var client = getClient();
		if (StringUtils.isBlank(bucketName)) {
			bucketName = getNewBucketName();
			client.createBucket(bucketName);
		}

		if (keys != null) {
			for (var key : keys) {
				var body = key;
				if (key.endsWith("/"))
					body = "";
				client.putObject(bucketName, key, body);
			}
		}

		return bucketName;
	}

	public URL getURL(String bucketName) throws MalformedURLException {
		var protocol = config.isSecure ? MainData.HTTPS : MainData.HTTP;
		var port = config.isSecure ? config.sslPort : config.port;

		return config.isAWS() ? NetUtils.getEndPoint(protocol, config.regionName, bucketName)
				: NetUtils.getEndPoint(protocol, config.url, port, bucketName);
	}

	public URL getURL(String bucketName, String key) throws MalformedURLException {
		var protocol = config.isSecure ? MainData.HTTPS : MainData.HTTP;
		var port = config.isSecure ? config.sslPort : config.port;

		return config.isAWS() ? NetUtils.getEndPoint(protocol, config.regionName, bucketName, key)
				: NetUtils.getEndPoint(protocol, config.url, port, bucketName, key);
	}

	public String makeArnResource(String path) {
		return String.format("arn:aws:s3:::%s", path);
	}

	public JsonObject makeJsonStatement(String action, String resource, String effect, JsonObject principal,
			JsonObject conditions) {
		if (principal == null) {
			principal = new JsonObject();
			principal.addProperty("AWS", "*");
		}

		if (StringUtils.isBlank(effect))
			effect = MainData.PolicyEffectAllow;

		var statement = new JsonObject();
		statement.addProperty(MainData.PolicyEffect, effect);
		statement.add(MainData.PolicyPrincipal, principal);
		statement.addProperty(MainData.PolicyAction, action);
		statement.addProperty(MainData.PolicyResource, resource);

		if (conditions != null)
			statement.add(MainData.PolicyCondition, conditions);

		return statement;
	}

	public JsonObject makeJsonPolicy(String action, String resource, JsonObject principal, JsonObject conditions) {
		var policy = new JsonObject();

		policy.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var statement = new JsonArray();
		statement.add(makeJsonStatement(action, resource, null, principal, conditions));
		policy.add(MainData.PolicyStatement, statement);

		return policy;
	}

	public JsonObject makeJsonPolicy(JsonObject... statementList) {
		var policy = new JsonObject();

		policy.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);
		var statements = new JsonArray();
		for (var statement : statementList)
			statements.add(statement);
		policy.add(MainData.PolicyStatement, statements);
		return policy;
	}

	public void checkConfigureVersioningRetry(String bucketName, String status) {
		var client = getClient();

		client.setBucketVersioningConfiguration(
				new SetBucketVersioningConfigurationRequest(bucketName, new BucketVersioningConfiguration(status)));

		String readStatus = null;

		for (int i = 0; i < 5; i++) {
			try {
				var response = client.getBucketVersioningConfiguration(bucketName);
				readStatus = response.getStatus();

				if (readStatus.equals(status))
					break;
				delay(1000);
			} catch (Exception e) {
				readStatus = null;
			}
		}

		assertEquals(status, readStatus);
	}

	public String setupBucketObjectACL(CannedAccessControlList bucketACL, CannedAccessControlList objectACL,
			String key) {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(bucketACL));
		client.putObject(bucketName, key, "");
		client.setObjectAcl(bucketName, key, objectACL);

		return bucketName;
	}

	public Date getTimeToAddSeconds(int seconds) {
		Calendar today = Calendar.getInstance();
		today.add(Calendar.SECOND, seconds);
		return new Date(today.getTimeInMillis());
	}

	public String getTimeToAddMinutes(int minutes) {
		var myFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		myFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		var time = Calendar.getInstance();
		time.add(Calendar.MINUTE, minutes);
		return myFormat.format(time.getTime());
	}

	public InputStream createBody(String body) {
		return new ByteArrayInputStream(body.getBytes());
	}

	public AccessControlList addObjectUserGrant(String bucketName, String key, Grant myGrant) {
		var client = getClient();

		var response = client.getObjectAcl(bucketName, key);
		var grants = response.getGrantsAsList();
		grants.add(myGrant);

		var myGrants = new AccessControlList();
		for (var grant : grants)
			myGrants.grantAllPermissions(grant);

		myGrants.setOwner(response.getOwner());

		return myGrants;
	}

	public AccessControlList addBucketUserGrant(String bucketName, Grant grant) {
		var client = getClient();

		var response = client.getBucketAcl(bucketName);
		var grants = response.getGrantsAsList();
		grants.add(grant);

		var myGrants = new AccessControlList();
		for (var Item : grants)
			myGrants.grantAllPermissions(Item);

		myGrants.setOwner(response.getOwner());

		return myGrants;
	}

	public String createKeyWithRandomContent(String key, int size, String bucketName, AmazonS3 client) {
		if (StringUtils.isBlank(bucketName))
			bucketName = getNewBucket();
		if (client == null)
			client = getClient();
		if (size <= 0)
			size = 7 * MainData.MB;

		var data = Utils.randomTextToLong(size);
		client.putObject(bucketName, key, data);

		return bucketName;
	}

	public String setGetMetadata(String meta, String bucketName) {
		if (StringUtils.isBlank(bucketName))
			bucketName = getNewBucket();

		var client = getClient();
		var key = "foo";
		var metadataKey = "x-amz-meta-meta1";

		var metadata = new ObjectMetadata();
		metadata.addUserMetadata(metadataKey, meta);
		metadata.setContentType("text/plain");
		metadata.setContentLength(3);
		client.putObject(bucketName, key, createBody("bar"), metadata);

		var response = client.getObject(bucketName, key);
		return response.getObjectMetadata().getUserMetaDataOf(metadataKey);
	}

	public String bucketACLGrantUserId(Permission permission) {
		var bucketName = getNewBucket();
		var client = getClient();

		var mainUser = new CanonicalGrantee(config.mainUser.userId);
		mainUser.setDisplayName(config.mainUser.displayName);
		var altUser = new CanonicalGrantee(config.altUser.userId);
		altUser.setDisplayName(config.altUser.displayName);

		var accessControlList = addBucketUserGrant(bucketName, new Grant(altUser, permission));
		client.setBucketAcl(bucketName, accessControlList);

		var response = client.getBucketAcl(bucketName);

		var getGrants = response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(mainUser, Permission.FullControl));
		myGrants.add(new Grant(altUser, permission));
		checkGrants(myGrants, new ArrayList<>(getGrants));

		return bucketName;
	}

	public String setupAccessTest(String key1, String key2, CannedAccessControlList bucketACL,
			CannedAccessControlList objectACL) {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setBucketAcl(bucketName, bucketACL);
		client.putObject(bucketName, key1, key1);
		client.setObjectAcl(bucketName, key1, objectACL);
		client.putObject(bucketName, key2, key2);

		return bucketName;
	}

	public void checkObjContent(AmazonS3 client, String bucketName, String key, String versionId, String content) {
		var response = client.getObject(new GetObjectRequest(bucketName, key).withVersionId(versionId));
		if (content != null) {
			var body = getBody(response.getObjectContent());
			assertTrue(content.equals(body), MainData.NOT_MATCHED);
		} else
			assertNull(response);
	}

	public void checkObjVersions(AmazonS3 client, String bucketName, String key, List<String> versionIds,
			List<String> contents) {
		var response = client.listVersions(bucketName, "");
		var versions = GetVersions(response.getVersionSummaries());

		Collections.reverse(versions);

		var index = 0;
		for (var version : versions) {
			assertEquals(version.getVersionId(), versionIds.get(index));
			if (StringUtils.isNotBlank(key))
				assertEquals(key, version.getKey());
			checkObjContent(client, bucketName, key, version.getVersionId(), contents.get(index++));
		}
	}

	public void createMultipleVersions(AmazonS3 client, String bucketName, String key, int numVersions,
			boolean checkVersion) {
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(bucketName, key, body);
			var versionId = response.getVersionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		if (checkVersion)
			checkObjVersions(client, bucketName, key, versionIds, contents);
	}

	public List<Tag> createSimpleTagSet(int count) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(new Tag(Integer.toString(i), Integer.toString(i)));
		return tagSets;
	}

	public List<Tag> createDetailTagSet(int count, int keySize, int valueSize) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(new Tag(Utils.randomTextToLong(keySize), Utils.randomTextToLong(valueSize)));
		return tagSets;
	}
	// endregion

	// region Get Data

	public List<String> getObjectList(String bucketName, String prefix) {
		var client = getClient();
		var response = client.listObjects(bucketName, prefix);
		return getKeys(response.getObjectSummaries());
	}

	public static List<String> getKeys(List<S3ObjectSummary> objectList) {
		if (objectList != null) {
			var temp = new ArrayList<String>();

			for (var S3Object : objectList)
				temp.add(S3Object.getKey());

			return temp;
		}
		return new ArrayList<>();
	}

	public static List<String> getKeys2(List<S3VersionSummary> objectList) {
		if (objectList != null) {
			var temp = new ArrayList<String>();

			for (var S3Object : objectList)
				temp.add(S3Object.getKey());

			return temp;
		}
		return null;
	}

	public static String getBody(S3ObjectInputStream data) {
		String Body = "";
		if (data != null) {
			try {
				ByteArrayOutputStream result = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				for (int length; (length = data.read(buffer)) != -1;) {
					result.write(buffer, 0, length);
				}
				return result.toString("UTF-8");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return Body;
	}

	public static ObjectData getObjectToKey(String key, List<ObjectData> keyList) {
		for (var Object : keyList) {
			if (Object.key.equals(key))
				return Object;
		}
		return null;
	}

	public List<String> getBucketList(List<Bucket> response) {
		if (response == null)
			return new ArrayList<>();
		var bucketList = new ArrayList<String>();

		for (var Item : response)
			bucketList.add(Item.getName());

		return bucketList;
	}

	public void checkVersioning(String bucketName, String StatusCode) {
		var client = getClient();

		var response = client.getBucketVersioningConfiguration(bucketName);
		assertEquals(StatusCode, response.getStatus());
	}

	public ArrayList<String> CutStringData(String Data, int PartSize) {
		ArrayList<String> StringList = new ArrayList<String>();

		int StartPoint = 0;
		while (StartPoint < Data.length()) {

			var EndPoint = Math.min(StartPoint + PartSize, Data.length());

			StringList.add(Data.substring(StartPoint, EndPoint));
			StartPoint += PartSize;
		}

		return StringList;
	}

	public static ArrayList<KeyVersion> GetKeyVersions(ArrayList<String> KeyList) {
		ArrayList<KeyVersion> KeyVersions = new ArrayList<KeyVersion>();
		for (var key : KeyList)
			KeyVersions.add(new KeyVersion(key, null));

		return KeyVersions;
	}

	public List<S3VersionSummary> GetVersions(List<S3VersionSummary> versions) {
		if (versions == null)
			return null;

		var Lists = new ArrayList<S3VersionSummary>();
		for (var item : versions)
			if (!item.isDeleteMarker())
				Lists.add(item);
		return Lists;
	}

	public List<String> GetVersionIDs(List<S3VersionSummary> versions) {
		if (versions == null)
			return null;

		var Lists = new ArrayList<String>();
		for (var item : versions)
			if (!item.isDeleteMarker())
				Lists.add(item.getVersionId());
		return Lists;
	}

	public List<S3VersionSummary> GetDeleteMarkers(List<S3VersionSummary> versions) {
		if (versions == null)
			return null;

		var DeleteMarkers = new ArrayList<S3VersionSummary>();

		for (var item : versions)
			if (item.isDeleteMarker())
				DeleteMarkers.add(item);
		return DeleteMarkers;
	}
	// endregion

	// region Check Data
	public void ACLTest(String bucketName, String key, AmazonS3 client, Boolean Pass) {
		if (Pass) {
			var response = client.getObject(bucketName, key);
			assertEquals(key, response.getKey());
		} else {
			var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
			var StatusCode = e.getStatusCode();
			var ErrorCode = e.getErrorCode();

			assertEquals(403, StatusCode);
			assertEquals(MainData.AccessDenied, ErrorCode);
		}
	}

	public String ValidateListObject(String bucketName, String Prefix, String Delimiter, String Marker, int MaxKeys,
			boolean IsTruncated, ArrayList<String> CheckKeys, ArrayList<String> checkPrefixes, String NextMarker) {
		var client = getClient();
		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(Delimiter)
				.withMarker(Marker).withMaxKeys(MaxKeys).withPrefix(Prefix));

		assertEquals(IsTruncated, response.isTruncated());
		assertEquals(NextMarker, response.getNextMarker());

		var Keys = getKeys(response.getObjectSummaries());
		var Prefixes = response.getCommonPrefixes();

		assertEquals(CheckKeys.size(), Keys.size());
		assertEquals(checkPrefixes.size(), Prefixes.size());
		assertEquals(CheckKeys, Keys);
		assertEquals(checkPrefixes, Prefixes);

		return response.getNextMarker();
	}

	public String validateListObjectV2(String bucketName, String prefix, String delimiter, String continuationToken,
			int maxKeys, boolean isTruncated, List<String> checkKeys, List<String> checkPrefixes, boolean last) {
		var client = getClient();
		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter)
						.withContinuationToken(continuationToken).withMaxKeys(maxKeys).withPrefix(prefix));

		assertEquals(isTruncated, response.isTruncated());
		if (last)
			assertNull(response.getNextContinuationToken());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(checkKeys, keys);
		assertEquals(checkPrefixes, prefixes);
		assertEquals(checkKeys.size(), keys.size());
		assertEquals(checkPrefixes.size(), prefixes.size());

		return response.getNextContinuationToken();
	}

	public void checkBadBucketName(String bucketName) {
		var client = getClient();

		assertThrows(IllegalBucketNameException.class, () -> client.createBucket(bucketName));
	}

	public void checkGoodBucketName(String Name, String Prefix) {
		if (StringUtils.isBlank(Prefix))
			Prefix = getPrefix();

		var bucketName = String.format("%s%s", Prefix, Name);
		buckets.add(bucketName);

		var client = getClient();
		var response = client.createBucket(bucketName);
		assertTrue(StringUtils.isNotBlank(response.getName()));
	}

	public void testBucketCreateNamingGoodLong(int length) {
		var bucketName = getPrefix();
		if (bucketName.length() < length)
			bucketName += Utils.randomText(length - bucketName.length());
		else
			bucketName = bucketName.substring(0, length - 1);
		buckets.add(bucketName);

		var client = getClient();
		var response = client.createBucket(bucketName);
		assertTrue(StringUtils.isNotBlank(response.getName()));
	}

	public void testBucketCreateNamingBadLong(int length) {
		var bucketName = getPrefix();
		if (bucketName.length() < length)
			bucketName += Utils.randomText(length - bucketName.length());
		else
			bucketName = bucketName.substring(0, length - 1);
		checkBadBucketName(bucketName);
	}

	public HttpResponse getObject(URL address) {
		var client = HttpClientBuilder.create().build();
		var getRequest = new HttpGet(address.toString());
		try {
			return client.execute(getRequest);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public HttpResponse putObject(URL address, String body) {
		var client = HttpClientBuilder.create().build();
		var putRequest = new HttpPut(address.toString());
		if (body != null) {
			var requestEntity = new StringEntity(body, "utf-8");
			requestEntity.setContentType(new BasicHeader("Content-Type", "application/txt"));
			putRequest.setEntity(requestEntity);
		}
		try {
			return client.execute(putRequest);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public MultipartUploadData multipartUpload(AmazonS3 client, String bucketName, String key, int size,
			MultipartUploadData uploadData) {
		var partSize = 5 * MainData.MB;
		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			var partresPonse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(uploadData.uploadId)
							.withPartNumber(uploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			uploadData.addPart(partresPonse.getPartETag());
		}

		return uploadData;
	}

	public MultipartUploadData setupMultipartUpload(AmazonS3 client, String bucketName, String key, int size,
			ObjectMetadata metadataList) {
		var uploadData = new MultipartUploadData();
		if (metadataList == null) {
			metadataList = new ObjectMetadata();
			metadataList.setContentType("text/plain");
		}
		metadataList.setContentLength(size);

		var InitMultiPartresponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, metadataList));
		uploadData.uploadId = InitMultiPartresponse.getUploadId();

		var Parts = Utils.generateRandomString(size, uploadData.partSize);

		for (var Part : Parts) {
			uploadData.appendBody(Part);

			var PartresPonse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(uploadData.uploadId)
							.withPartNumber(uploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			uploadData.parts.add(PartresPonse.getPartETag());
		}

		return uploadData;
	}

	public MultipartUploadData MultipartUploadResend(AmazonS3 client, String bucketName, String key, int Size,
			ObjectMetadata MetadataList, ArrayList<Integer> ResendParts) {
		var UploadData = new MultipartUploadData();
		if (MetadataList == null) {
			MetadataList = new ObjectMetadata();
			MetadataList.setContentType("text/plain");
		}
		MetadataList.setContentLength(Size);

		var InitMultiPartresponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, MetadataList));
		UploadData.uploadId = InitMultiPartresponse.getUploadId();

		var Parts = Utils.generateRandomString(Size, UploadData.partSize);

		for (var Part : Parts) {
			UploadData.appendBody(Part);
			int PartNumber = UploadData.nextPartNumber();

			var PartresPonse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(UploadData.uploadId)
							.withPartNumber(PartNumber)
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			UploadData.parts.add(PartresPonse.getPartETag());

			if (ResendParts != null && ResendParts.contains(PartNumber))
				client.uploadPart(new UploadPartRequest()
						.withBucketName(bucketName)
						.withKey(key)
						.withUploadId(UploadData.uploadId)
						.withPartNumber(PartNumber)
						.withInputStream(createBody(Part))
						.withPartSize(Part.length()));
		}

		return UploadData;
	}

	public MultipartUploadData setupMultipartUpload(AmazonS3 client, String bucketName, String key, int Size,
			int PartSize, ObjectMetadata MetadataList, SSECustomerKey SSE_C) {
		var UploadData = new MultipartUploadData();
		if (PartSize <= 0)
			PartSize = 5 * MainData.MB;
		if (MetadataList == null) {
			MetadataList = new ObjectMetadata();
			MetadataList.setContentType("text/plain");
		}
		MetadataList.setContentLength(Size);

		var InitRequest = new InitiateMultipartUploadRequest(bucketName, key, MetadataList);
		if (SSE_C != null)
			InitRequest.setSSECustomerKey(SSE_C);

		var InitMultiPartresponse = client.initiateMultipartUpload(InitRequest);
		UploadData.uploadId = InitMultiPartresponse.getUploadId();

		var Parts = Utils.generateRandomString(Size, PartSize);

		for (var Part : Parts) {
			UploadData.appendBody(Part);
			var Request = new UploadPartRequest()
					.withBucketName(bucketName)
					.withKey(key)
					.withUploadId(UploadData.uploadId)
					.withPartNumber(UploadData.nextPartNumber())
					.withInputStream(createBody(Part))
					.withPartSize(Part.length());
			if (SSE_C != null)
				Request.setSSECustomerKey(SSE_C);

			var response = client.uploadPart(Request);
			UploadData.parts.add(response.getPartETag());
		}
		return UploadData;
	}

	public void CheckCopyContent(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey) {
		var client = getClient();

		var response = client.getObject(new GetObjectRequest(SourceBucketName, SourceKey));
		var SourceSize = response.getObjectMetadata().getContentLength();
		var SourceData = getBody(response.getObjectContent());

		response = client.getObject(TargetBucketName, TargetKey);
		var TargetSize = response.getObjectMetadata().getContentLength();
		var TargetData = getBody(response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void CheckCopyContent(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey,
			String versionId) {
		var client = getClient();

		var response = client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withVersionId(versionId));
		var SourceSize = response.getObjectMetadata().getContentLength();
		var SourceData = getBody(response.getObjectContent());

		response = client.getObject(TargetBucketName, TargetKey);
		var TargetSize = response.getObjectMetadata().getContentLength();
		var TargetData = getBody(response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void CheckCopyContentSSE_C(AmazonS3 client, String SourceBucketName, String SourceKey,
			String TargetBucketName, String TargetKey, SSECustomerKey SSE_C) {

		var Sourceresponse = client
				.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withSSECustomerKey(SSE_C));
		var SourceSize = Sourceresponse.getObjectMetadata().getContentLength();
		var SourceData = getBody(Sourceresponse.getObjectContent());

		var Targetresponse = client
				.getObject(new GetObjectRequest(TargetBucketName, TargetKey).withSSECustomerKey(SSE_C));
		var TargetSize = Targetresponse.getObjectMetadata().getContentLength();
		var TargetData = getBody(Targetresponse.getObjectContent());

		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void CheckCopyContentUsingRange(String SourceBucketName, String SourceKey, String TargetBucketName,
			String TargetKey) {
		var client = getClient();

		var response = client.getObject(new GetObjectRequest(TargetBucketName, TargetKey));
		var TargetSize = response.getObjectMetadata().getContentLength();
		var TargetData = getBody(response.getObjectContent());

		response = client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withRange(0, TargetSize - 1));
		var SourceSize = response.getObjectMetadata().getContentLength();
		var SourceData = getBody(response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentUsingRange(String SourceBucketName, String SourceKey, String TargetBucketName,
			String TargetKey, int Step) {
		var client = getClient();

		var response = client.getObjectMetadata(SourceBucketName, SourceKey);
		var Size = response.getContentLength();

		long StartPosition = 0;
		while (StartPosition < Size) {
			var EndPosition = StartPosition + Step;
			if (EndPosition > Size)
				EndPosition = Size - 1;

			var Sourceresponse = client.getObject(
					new GetObjectRequest(SourceBucketName, SourceKey).withRange(StartPosition, EndPosition - 1));
			var SourceBody = getBody(Sourceresponse.getObjectContent());
			var Targetresponse = client.getObject(
					new GetObjectRequest(SourceBucketName, SourceKey).withRange(StartPosition, EndPosition - 1));
			var TargetBody = getBody(Targetresponse.getObjectContent());

			assertEquals(Sourceresponse.getObjectMetadata().getContentLength(),
					Targetresponse.getObjectMetadata().getContentLength());
			assertTrue(SourceBody.equals(TargetBody), MainData.NOT_MATCHED);
			StartPosition += Step;
		}
	}

	public void checkUploadMultipartResend(String bucketName, String key, int Size, ArrayList<Integer> ResendParts) {
		var ContentType = "text/bla";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(ContentType);
		var client = getClient();
		var UploadData = MultipartUploadResend(client, bucketName, key, Size, metadata, ResendParts);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, UploadData.uploadId, UploadData.parts));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(ContentType, response.getContentType());
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());

		var Body = UploadData.getBody();
		checkContentUsingRange(bucketName, key, Body, MainData.MB);
		checkContentUsingRange(bucketName, key, Body, 10 * MainData.MB);
	}

	public String doTestMultipartUploadContents(String bucketName, String key, int NumParts) {
		var Payload = Utils.randomTextToLong(5 * MainData.MB);
		var client = getClient();

		var Initresponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var UploadID = Initresponse.getUploadId();
		var Parts = new ArrayList<PartETag>();
		var AllPayload = "";

		for (int i = 0; i < NumParts; i++) {
			var PartNumber = i + 1;
			var Partresponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
					.withUploadId(UploadID).withPartNumber(PartNumber)
					.withInputStream(new ByteArrayInputStream(Payload.getBytes())).withPartSize(Payload.length()));
			Parts.add(new PartETag(PartNumber, Partresponse.getETag()));
			AllPayload += Payload;
		}
		var LestPayload = Utils.randomTextToLong(MainData.MB);
		var LestPartresponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
				.withUploadId(UploadID).withPartNumber(NumParts + 1)
				.withInputStream(new ByteArrayInputStream(LestPayload.getBytes())).withPartSize(LestPayload.length()));
		Parts.add(new PartETag(NumParts + 1, LestPartresponse.getETag()));
		AllPayload += LestPayload;

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, UploadID, Parts));

		var response = client.getObject(bucketName, key);
		var Text = getBody(response.getObjectContent());

		assertTrue(AllPayload.equals(Text), MainData.NOT_MATCHED);

		return AllPayload;
	}

	public MultipartUploadData multipartCopy(String SourceBucketName, String SourceKey, String TargetBucketName,
			String TargetKey, int Size, AmazonS3 client, int PartSize, String versionId) {
		var Data = new MultipartUploadData();
		if (client == null)
			client = getClient();
		if (PartSize <= 0)
			PartSize = 5 * MainData.MB;

		var response = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey));
		Data.uploadId = response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartresPonse = client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
					.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
					.withUploadId(Data.uploadId).withPartNumber(UploadCount).withFirstByte(Start).withLastByte(End)
					.withSourceVersionId(versionId));
			Data.parts.add(new PartETag(UploadCount++, PartresPonse.getETag()));

			Start = End + 1;
		}

		return Data;
	}

	public MultipartUploadData multipartCopy(AmazonS3 client, String SourceBucketName, String SourceKey,
			String TargetBucketName, String TargetKey,
			int Size, ObjectMetadata metadata) {
		var Data = new MultipartUploadData();
		var PartSize = 5 * MainData.MB;
		if (metadata == null)
			metadata = new ObjectMetadata();

		var response = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey, metadata));
		Data.uploadId = response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartresPonse = client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
					.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
					.withUploadId(Data.uploadId).withPartNumber(UploadCount).withFirstByte(Start).withLastByte(End));
			Data.parts.add(new PartETag(UploadCount++, PartresPonse.getETag()));

			Start = End + 1;
		}

		return Data;
	}

	public MultipartUploadData MultipartCopySSE_C(AmazonS3 client, String SourceBucketName, String SourceKey,
			String TargetBucketName, String TargetKey, int Size, ObjectMetadata metadata, SSECustomerKey SSE_C) {
		var Data = new MultipartUploadData();
		var PartSize = 5 * MainData.MB;
		if (metadata == null)
			metadata = new ObjectMetadata();

		var response = client.initiateMultipartUpload(
				new InitiateMultipartUploadRequest(TargetBucketName, TargetKey, metadata).withSSECustomerKey(SSE_C));
		Data.uploadId = response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartresPonse = client.copyPart(new CopyPartRequest()
					.withSourceBucketName(SourceBucketName)
					.withSourceKey(SourceKey)
					.withDestinationBucketName(TargetBucketName)
					.withDestinationKey(TargetKey)
					.withSourceSSECustomerKey(SSE_C)
					.withDestinationSSECustomerKey(SSE_C)
					.withUploadId(Data.uploadId)
					.withPartNumber(UploadCount)
					.withFirstByte(Start)
					.withLastByte(End));
			Data.parts.add(new PartETag(UploadCount++, PartresPonse.getETag()));

			Start = End + 1;
		}

		return Data;
	}

	public static long GetBytesUsed(ListObjectsV2Result response) {
		if (response == null)
			return 0;
		if (response.getObjectSummaries() == null)
			return 0;
		if (response.getObjectSummaries().size() > 1)
			return 0;

		long Size = 0;

		for (var Obj : response.getObjectSummaries())
			Size += Obj.getSize();

		return Size;
	}

	public void checkContentUsingRange(String bucketName, String key, String data, long step) {
		var client = getClient();
		var getResponse = client.getObjectMetadata(bucketName, key);
		var size = getResponse.getContentLength();
		assertEquals(data.length(), size);

		long startPosition = 0;
		while (startPosition < size) {
			var endPosition = startPosition + step;
			if (endPosition > size)
				endPosition = size - 1;

			var response = client
					.getObject(new GetObjectRequest(bucketName, key).withRange(startPosition, endPosition - 1));
			var body = getBody(response.getObjectContent());
			var length = endPosition - startPosition;
			var partBody = data.substring((int) startPosition, (int) endPosition);

			assertEquals(length, response.getObjectMetadata().getContentLength(), bucketName + "/" + key + " : " + length + " != " + response.getObjectMetadata().getContentLength());
			assertTrue(partBody.equals(body), MainData.NOT_MATCHED);
			startPosition += step;
		}
	}

	public void CheckContentUsingRangeEnc(AmazonS3 client, String bucketName, String key, String Data, long Step,
			SSECustomerKey SSE_C) {
		if (client == null)
			client = getClient();
		var Getresponse = client
				.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key).withSSECustomerKey(SSE_C));
		var Size = Getresponse.getContentLength();

		long StartPosition = 0;
		while (StartPosition < Size) {
			var EndPosition = StartPosition + Step;
			if (EndPosition > Size)
				EndPosition = Size - 1;

			var response = client.getObject(new GetObjectRequest(bucketName, key)
					.withRange(StartPosition, EndPosition - 1).withSSECustomerKey(SSE_C));
			var Body = getBody(response.getObjectContent());
			var Length = EndPosition - StartPosition;
			var PartBody = Data.substring((int) StartPosition, (int) EndPosition);

			assertEquals(Length, response.getObjectMetadata().getContentLength());
			assertTrue(PartBody.equals(Body), MainData.NOT_MATCHED);
			StartPosition += Step;
		}
	}

	public RangeSet GetRandomRange(int FileSize) {
		int MAX_LENGTH = 500;
		Random rand = new Random();

		var Start = rand.nextInt(FileSize - MAX_LENGTH * 2);
		var MaxLength = FileSize - Start;

		if (MaxLength > MAX_LENGTH)
			MaxLength = MAX_LENGTH;
		var Length = rand.nextInt(MaxLength) + MAX_LENGTH - 1;
		if (Length <= 0)
			Length = 1;

		return new RangeSet(Start, Length);

	}

	public void checkContent(String bucketName, String key, String Data, int LoopCount) {
		var client = getClient();

		for (int i = 0; i < LoopCount; i++) {
			var response = client.getObject(bucketName, key);
			var Body = getBody(response.getObjectContent());
			assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
		}
	}

	public void CheckContentEnc(String bucketName, String key, String Data, int LoopCount, SSECustomerKey SSE_C) {
		var client = getClientHttps();

		for (int i = 0; i < LoopCount; i++) {
			var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
			var Body = getBody(response.getObjectContent());
			assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRange(String bucketName, String key, String Data, int LoopCount) {
		var client = getClient();
		int FileSize = Data.length();

		for (int i = 0; i < LoopCount; i++) {
			var Range = GetRandomRange(FileSize);
			var RangeBody = Data.substring(Range.start, Range.end + 1);

			var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(Range.start, Range.end));
			var Body = getBody(response.getObjectContent());

			assertEquals(Range.length, response.getObjectMetadata().getContentLength() - 1);
			assertTrue(RangeBody.equals(Body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRangeEnc(AmazonS3 client, String bucketName, String key, String Data,
			int FileSize, int LoopCount, SSECustomerKey SSE_C) {
		for (int i = 0; i < LoopCount; i++) {
			var Range = GetRandomRange(FileSize);

			var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(Range.start, Range.end - 1)
					.withSSECustomerKey(SSE_C));
			var Body = getBody(response.getObjectContent());
			var RangeBody = Data.substring(Range.start, Range.end);

			assertEquals(Range.length, response.getObjectMetadata().getContentLength());
			assertTrue(RangeBody.equals(Body), MainData.NOT_MATCHED);
		}
	}

	public AmazonServiceException SetGetMetadataUnreadable(String metadata, String Bucket) {
		if (StringUtils.isBlank(Bucket))
			Bucket = getNewBucket();
		var bucketName = Bucket;
		var client = getClient();
		var key = "foo";
		var MetadataKey = "x-amz-meta-meta1";
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata(MetadataKey, metadata);

		return assertThrows(AmazonServiceException.class,
				() -> client.putObject(bucketName, key, createBody("bar"), MetadataList));
	}

	public boolean ErrorCheck(Integer StatusCode) {
		if (StatusCode.equals(400))
			return true;
		if (StatusCode.equals(403))
			return true;
		return false;
	}

	public void testEncryptionCSEWrite(int FileSize) {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "testobj";
		var AESKey = Utils.randomTextToLong(32);
		var Data = Utils.randomTextToLong(FileSize);

		try {
			var EncodingData = AES256.encrypt(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("x-amz-meta-key", AESKey);
			MetadataList.setContentType("text/plain");
			MetadataList.setContentLength(EncodingData.length());

			client.putObject(bucketName, key, createBody(EncodingData), MetadataList);

			var response = client.getObject(bucketName, key);
			var EncodingBody = getBody(response.getObjectContent());
			var Body = AES256.decrypt(EncodingBody, AESKey);
			assertTrue(Data.equals(Body), MainData.NOT_MATCHED);

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testEncryptionSSECustomerWrite(int FileSize) {
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "testobj";
		var data = Utils.randomTextToLong(FileSize);
		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(FileSize);
		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C));

		var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
		var body = getBody(response.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());
	}

	public void testEncryptionSSE_S3CustomerWrite(int FileSize) {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "testobj";
		var Data = Utils.randomTextToLong(FileSize);

		var metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType("text/plain");
		metadata.setContentLength(FileSize);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(Data), metadata));

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertTrue(Data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSEAlgorithm());
	}

	public void testEncryptionSSE_S3Copy(int FileSize) {
		var bucketName = getNewBucket();
		var client = getClient();
		var Data = Utils.randomTextToLong(FileSize);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(SSE_S3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var SourceKey = "bar";
		client.putObject(bucketName, SourceKey, Data);

		var Sourceresponse = client.getObject(bucketName, SourceKey);
		var SourceBody = getBody(Sourceresponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), Sourceresponse.getObjectMetadata().getSSEAlgorithm());

		var TargetKey = "foo";
		client.copyObject(bucketName, SourceKey, bucketName, TargetKey);
		var Targetresponse = client.getObject(bucketName, TargetKey);
		assertEquals(SSEAlgorithm.AES256.toString(), Targetresponse.getObjectMetadata().getSSEAlgorithm());

		var TargetBody = getBody(Targetresponse.getObjectContent());
		assertTrue(SourceBody.equals(TargetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(Boolean SourceObjectEncryption, Boolean SourceBucketEncryption,
			Boolean TargetBucketEncryption, Boolean TargetObjectEncryption, int FileSize) {
		var SourceKey = "SourceKey";
		var TargetKey = "TargetKey";
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();
		var client = getClient();
		var Data = Utils.randomTextToLong(FileSize);

		// SSE-S3 Config
		var metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType("text/plain");
		metadata.setContentLength(FileSize);

		// Source Put Object
		if (SourceObjectEncryption)
			client.putObject(new PutObjectRequest(SourceBucketName, SourceKey, createBody(Data), metadata));
		else
			client.putObject(SourceBucketName, SourceKey, Data);

		//// Source Object Check
		var Sourceresponse = client.getObject(SourceBucketName, SourceKey);
		var SourceBody = getBody(Sourceresponse.getObjectContent());
		if (SourceObjectEncryption)
			assertEquals(SSEAlgorithm.AES256.toString(), Sourceresponse.getObjectMetadata().getSSEAlgorithm());
		else
			assertNull(Sourceresponse.getObjectMetadata().getSSEAlgorithm());
		assertEquals(Data, SourceBody);

		var SSE_S3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		// Source Bucket Encryption
		if (SourceBucketEncryption) {
			client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(SourceBucketName)
					.withServerSideEncryptionConfiguration(SSE_S3Config));

			var Encryptionresponse = client.getBucketEncryption(SourceBucketName);
			assertEquals(SSE_S3Config.getRules(), Encryptionresponse.getServerSideEncryptionConfiguration().getRules());
		}

		// Target Bucket Encryption
		if (TargetBucketEncryption) {
			client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(TargetBucketName)
					.withServerSideEncryptionConfiguration(SSE_S3Config));

			var Encryptionresponse = client.getBucketEncryption(TargetBucketName);
			assertEquals(SSE_S3Config.getRules(), Encryptionresponse.getServerSideEncryptionConfiguration().getRules());
		}

		// Source Copy Object
		if (TargetObjectEncryption)
			client.copyObject(new CopyObjectRequest(SourceBucketName, SourceKey, TargetBucketName, TargetKey)
					.withNewObjectMetadata(metadata));
		else
			client.copyObject(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		// Target Object Check
		var Targetresponse = client.getObject(TargetBucketName, TargetKey);
		var TargetBody = getBody(Targetresponse.getObjectContent());
		if (TargetBucketEncryption || TargetObjectEncryption)
			assertEquals(SSEAlgorithm.AES256.toString(), Targetresponse.getObjectMetadata().getSSEAlgorithm());
		else
			assertNull(Targetresponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(SourceBody.equals(TargetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(EncryptionType Source, EncryptionType Target, int FileSize) {
		var sourceKey = "SourceKey";
		var targetKey = "TargetKey";
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var data = Utils.randomTextToLong(FileSize);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(FileSize);

		// SSE-S3 Config
		var SSEMetadata = new ObjectMetadata();
		SSEMetadata.setContentType("text/plain");
		SSEMetadata.setContentLength(FileSize);
		SSEMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		// SSE-C Config
		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		// Source Put Object
		var SourcePutRequest = new PutObjectRequest(bucketName, sourceKey, createBody(data), metadata);
		var SourceGetRequest = new GetObjectRequest(bucketName, sourceKey);
		var TargetGetRequest = new GetObjectRequest(bucketName, targetKey);
		var CopyRequest = new CopyObjectRequest(bucketName, sourceKey, bucketName, targetKey)
				.withMetadataDirective(MetadataDirective.REPLACE);

		// Source Options
		switch (Source) {
			case SSE_S3:
				SourcePutRequest.setMetadata(SSEMetadata);
				break;
			case SSE_C:
				SourcePutRequest.setSSECustomerKey(SSE_C);
				SourceGetRequest.setSSECustomerKey(SSE_C);
				CopyRequest.setSourceSSECustomerKey(SSE_C);
				break;
			case NORMAL:
				break;
		}

		// Target Options
		switch (Target) {
			case SSE_S3:
				CopyRequest.setNewObjectMetadata(SSEMetadata);
				break;
			case SSE_C:
				CopyRequest.setDestinationSSECustomerKey(SSE_C);
				TargetGetRequest.setSSECustomerKey(SSE_C);
				break;
			case NORMAL:
				CopyRequest.setNewObjectMetadata(metadata);
				break;
		}

		// Source Put Object
		client.putObject(SourcePutRequest);

		// Source Get Object
		var Sourceresponse = client.getObject(SourceGetRequest);
		var SourceBody = getBody(Sourceresponse.getObjectContent());
		assertTrue(data.equals(SourceBody), MainData.NOT_MATCHED);

		// Copy Object
		client.copyObject(CopyRequest);

		// Target Object Check
		var Targetresponse = client.getObject(TargetGetRequest);
		var TargetBody = getBody(Targetresponse.getObjectContent());
		assertTrue(SourceBody.equals(TargetBody), MainData.NOT_MATCHED);
	}

	public int GetPermissionPriority(Permission permission) {
		if (permission == Permission.FullControl)
			return 0;
		if (permission == Permission.Read)
			return 1;
		if (permission == Permission.ReadAcp)
			return 2;
		if (permission == Permission.Write)
			return 3;
		if (permission == Permission.WriteAcp)
			return 4;
		return 0;
	}

	public int GetPermissionPriority(Grant A, Grant B) {
		int PriorityA = GetPermissionPriority(A.getPermission());
		int PriorityB = GetPermissionPriority(B.getPermission());

		if (PriorityA < PriorityB)
			return 1;
		if (PriorityA == PriorityB)
			return 0;
		return -1;

	}

	public int GetGranteeIdentifierPriority(Grant A, Grant B) {
		return A.getGrantee().getIdentifier().compareTo(B.getGrantee().getIdentifier());
	}

	public ArrayList<Grant> GrantsSort(ArrayList<Grant> Data) {
		var newList = new ArrayList<Grant>();

		Comparator<String> comparator = (s1, s2) -> s2.compareTo(s1);
		TreeMap<String, Grant> kk = new TreeMap<String, Grant>(comparator);

		for (var Temp : Data) {
			kk.put(Temp.getGrantee().getIdentifier() + Temp.getPermission(), Temp);
		}

		for (Map.Entry<String, Grant> entry : kk.entrySet()) {
			newList.add(entry.getValue());
		}
		return newList;
	}

	public void checkGrants(ArrayList<Grant> Expected, ArrayList<Grant> Actual) {
		assertEquals(Expected.size(), Actual.size());

		Expected = GrantsSort(Expected);
		Actual = GrantsSort(Actual);

		for (int i = 0; i < Expected.size(); i++) {
			assertEquals(Expected.get(i).getPermission(), Actual.get(i).getPermission());
			assertEquals(Expected.get(i).getGrantee().getIdentifier(), Actual.get(i).getGrantee().getIdentifier());
			assertEquals(Expected.get(i).getGrantee().getTypeIdentifier(),
					Actual.get(i).getGrantee().getTypeIdentifier());
		}
	}

	public void checkObjectACL(Permission permission) {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "");
		var response = client.getObjectAcl(bucketName, key);
		var Policy = new AccessControlList();
		Policy.setOwner(response.getOwner());
		Policy.grantPermission(response.getGrantsAsList().get(0).getGrantee(), permission);

		client.setObjectAcl(bucketName, key, Policy);

		response = client.getObjectAcl(bucketName, key);
		var GetGrants = response.getGrantsAsList();

		var User = new CanonicalGrantee(config.mainUser.userId);
		User.setDisplayName(config.mainUser.displayName);

		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(User, permission));
		checkGrants(myGrants, new ArrayList<Grant>(GetGrants));
	}

	public void checkBucketACLGrantCanRead(String bucketName) {
		var AltClient = getAltClient();
		AltClient.headBucket(new HeadBucketRequest(bucketName));
	}

	public void checkBucketACLGrantCantRead(String bucketName) {
		var AltClient = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient.headBucket(new HeadBucketRequest(bucketName)));
	}

	public void checkBucketACLGrantCanReadACP(String bucketName) {
		var AltClient = getAltClient();
		AltClient.getBucketAcl(bucketName);
	}

	public void checkBucketACLGrantCantReadACP(String bucketName) {
		var AltClient = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient.getBucketAcl(bucketName));
	}

	public void checkBucketACLGrantCanWrite(String bucketName) {
		var AltClient = getAltClient();
		AltClient.putObject(bucketName, "foo-write", "bar");
	}

	public void checkBucketACLGrantCantWrite(String bucketName) {
		var AltClient = getAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(bucketName, "foo-write", "bar"));
	}

	public void checkBucketACLGrantCanWriteACP(String bucketName) {
		var AltClient = getAltClient();
		AltClient.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	public void checkBucketACLGrantCantWriteACP(String bucketName) {
		var AltClient = getAltClient();
		assertThrows(AmazonServiceException.class,
				() -> AltClient.setBucketAcl(bucketName, CannedAccessControlList.PublicRead));
	}

	public void PrefixLifecycleConfigurationCheck(List<Rule> Expected, List<Rule> Actual) {
		assertEquals(Expected.size(), Actual.size());

		for (int i = 0; i < Expected.size(); i++) {
			assertEquals(Expected.get(i).getId(), Actual.get(i).getId());
			assertEquals(Expected.get(i).getExpirationDate(), Actual.get(i).getExpirationDate());
			assertEquals(Expected.get(i).getExpirationInDays(), Actual.get(i).getExpirationInDays());
			assertEquals(Expected.get(i).isExpiredObjectDeleteMarker(), Actual.get(i).isExpiredObjectDeleteMarker());
			assertEquals(((LifecyclePrefixPredicate) Expected.get(i).getFilter().getPredicate()).getPrefix(),
					((LifecyclePrefixPredicate) Actual.get(i).getFilter().getPredicate()).getPrefix());

			assertEquals(Expected.get(i).getStatus(), Actual.get(i).getStatus());
			assertEquals(Expected.get(i).getNoncurrentVersionTransitions(),
					Actual.get(i).getNoncurrentVersionTransitions());
			assertEquals(Expected.get(i).getTransitions(), Actual.get(i).getTransitions());
			if (Expected.get(i).getNoncurrentVersionExpiration() != null)
				assertEquals(Expected.get(i).getNoncurrentVersionExpiration().getDays(),
						Actual.get(i).getNoncurrentVersionExpiration().getDays());
			assertEquals(Expected.get(i).getAbortIncompleteMultipartUpload(),
					Actual.get(i).getAbortIncompleteMultipartUpload());
		}
	}

	public BucketLifecycleConfiguration SetupLifecycleExpiration(AmazonS3 client, String bucketName, String RuleID,
			int DeltaDays, String RulePrefix) {
		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId(RuleID).withExpirationInDays(DeltaDays)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(RulePrefix)))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
		client.setBucketLifecycleConfiguration(bucketName, MyLifeCycle);

		var key = RulePrefix + "/foo";
		var Body = "bar";
		client.putObject(bucketName, key, Body);

		return client.getBucketLifecycleConfiguration(bucketName);
	}

	public void LockCompare(ObjectLockConfiguration Expected, ObjectLockConfiguration Actual) {
		assertEquals(Expected.getObjectLockEnabled(), Actual.getObjectLockEnabled());
		assertEquals(Expected.getRule().getDefaultRetention().getMode(),
				Actual.getRule().getDefaultRetention().getMode());
		assertEquals(Expected.getRule().getDefaultRetention().getYears(),
				Actual.getRule().getDefaultRetention().getYears());
		assertEquals(Expected.getRule().getDefaultRetention().getDays(),
				Actual.getRule().getDefaultRetention().getDays());
	}

	public void RetentionCompare(ObjectLockRetention Expected, ObjectLockRetention Actual) {
		assertEquals(Expected.getMode(), Actual.getMode());
		assertEquals(Expected.getRetainUntilDate(), Actual.getRetainUntilDate());

	}

	public void ReplicationConfigCompare(BucketReplicationConfiguration Expected,
			BucketReplicationConfiguration Actual) {
		assertEquals(Expected.getRoleARN(), Actual.getRoleARN());
		var ExpectedRules = Expected.getRules();
		var ActualRules = Actual.getRules();
		var GetKeys = ExpectedRules.keySet();

		for (var key : GetKeys) {
			var ExpectedRule = ExpectedRules.get(key);
			var ActualRule = ActualRules.get(key);
			assertEquals(ExpectedRule.getDeleteMarkerReplication(), ActualRule.getDeleteMarkerReplication());
			assertEquals(ExpectedRule.getDestinationConfig().toString(), ActualRule.getDestinationConfig().toString());
			assertEquals(ExpectedRule.getExistingObjectReplication(), ActualRule.getExistingObjectReplication());
			assertEquals(ExpectedRule.getFilter(), ActualRule.getFilter());
			assertEquals(ExpectedRule.getSourceSelectionCriteria(), ActualRule.getSourceSelectionCriteria());
			assertEquals(ExpectedRule.getStatus(), ActualRule.getStatus());
		}
	}

	public List<Tag> TaggingSort(List<Tag> TagList) {
		Collections.sort(TagList, new Comparator<Tag>() {
			@Override
			public int compare(Tag t1, Tag t2) {
				return t1.getKey().compareTo(t2.getKey());
			}
		});
		return TagList;
	}

	public void BucketTaggingCompare(List<TagSet> Expected, List<TagSet> Actual) {
		assertEquals(Expected.size(), Actual.size());

		for (int i = 0; i < Expected.size(); i++) {
			var subExpected = Expected.get(i).getAllTags();
			var subActual = Actual.get(i).getAllTags();

			for (var key : subExpected.keySet()) {
				assertEquals(subExpected.get(key), subActual.get(key));
			}
		}
	}

	public void tagCompare(List<Tag> Expected, List<Tag> Actual) {
		assertEquals(Expected.size(), Actual.size());

		var OrderExpected = TaggingSort(Expected);
		var OrderActual = TaggingSort(Actual);

		for (int i = 0; i < Expected.size(); i++) {
			assertEquals(OrderExpected.get(i).getKey(), OrderActual.get(i).getKey());
			assertEquals(OrderExpected.get(i).getValue(), OrderActual.get(i).getValue());
		}
	}

	public void deleteSuspendedVersioningObj(AmazonS3 client, String bucketName, String key, List<String> versionIds,
			List<String> Contents) {
		client.deleteObject(bucketName, key);

		assertEquals(versionIds.size(), Contents.size());

		for (int i = versionIds.size() - 1; i >= 0; i--) {
			if (versionIds.get(i).equals("null")) {
				versionIds.remove(i);
				Contents.remove(i);
			}
		}
	}

	public void overwriteSuspendedVersioningObj(AmazonS3 client, String bucketName, String key, List<String> versionIds,
			List<String> Contents, String Content) {
		client.putObject(bucketName, key, Content);

		assertEquals(versionIds.size(), Contents.size());

		for (int i = versionIds.size() - 1; i >= 0; i--) {
			if (versionIds.get(i).equals("null")) {
				versionIds.remove(i);
				Contents.remove(i);
			}
		}
		Contents.add(Content);
		versionIds.add("null");
	}

	public void removeObjVersion(AmazonS3 client, String bucketName, String key, List<String> versionIds,
			List<String> Contents, int index) {
		assertEquals(versionIds.size(), Contents.size());
		var rmVersionID = versionIds.get(index);
		versionIds.remove(index);
		var rmContent = Contents.get(index);
		Contents.remove(index);

		checkObjContent(client, bucketName, key, rmVersionID, rmContent);

		client.deleteVersion(bucketName, key, rmVersionID);

		if (versionIds.size() != 0)
			checkObjVersions(client, bucketName, key, versionIds, Contents);
	}

	public void createMultipleVersions(AmazonS3 client, String bucketName, String key, int NumVersions,
			List<String> versionIds, List<String> Contents, boolean CheckVersion) {

		for (int i = 0; i < NumVersions; i++) {
			var Body = String.format("content-%s", i);
			var response = client.putObject(bucketName, key, Body);
			var versionId = response.getVersionId();

			Contents.add(Body);
			versionIds.add(versionId);
		}

		if (CheckVersion)
			checkObjVersions(client, bucketName, key, versionIds, Contents);

	}

	public void CreateMultipleVersion(AmazonS3 client, String bucketName, String key, int NumVersions,
			boolean CheckVersion) {
		var versionIds = new ArrayList<String>();
		var Contents = new ArrayList<String>();

		for (int i = 0; i < NumVersions; i++) {
			var Body = String.format("content-%s", i);
			var response = client.putObject(bucketName, key, Body);
			var versionId = response.getVersionId();

			Contents.add(Body);
			versionIds.add(versionId);
		}

		if (CheckVersion)
			checkObjVersions(client, bucketName, key, versionIds, Contents);
	}

	public void CreateMultipleVersion(AmazonS3 client, String bucketName, String key, int NumVersions,
			boolean CheckVersion, String Body) {
		var versionIds = new ArrayList<String>();
		var Contents = new ArrayList<String>();

		for (int i = 0; i < NumVersions; i++) {
			var response = client.putObject(bucketName, key, Body);
			var versionId = response.getVersionId();

			Contents.add(Body);
			versionIds.add(versionId);
		}

		if (CheckVersion)
			checkObjVersions(client, bucketName, key, versionIds, Contents);
	}

	public void doTestCreateRemoveVersions(AmazonS3 client, String bucketName, String key, int numVersions,
			int RemoveStartIdx, int IdxInc) {
		var versionIds = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, Contents, true);
		var Idx = RemoveStartIdx;

		for (int i = 0; i < numVersions; i++) {
			removeObjVersion(client, bucketName, key, versionIds, Contents, Idx);
			Idx += IdxInc;
		}
	}

	public List<Thread> doCreateVersionedObjConcurrent(AmazonS3 client, String bucketName, String key, int Num) {
		var TList = new ArrayList<Thread>();
		for (int i = 0; i < Num; i++) {
			var Item = Integer.toString(i);
			var mThread = new Thread(() -> client.putObject(bucketName, key, String.format("Data %s", Item)));
			mThread.start();
			TList.add(mThread);
		}
		return TList;
	}

	public List<Thread> doClearVersionedBucketConcurrent(AmazonS3 client, String bucketName) {
		var TList = new ArrayList<Thread>();
		var response = client.listVersions(bucketName, "");

		for (var Version : response.getVersionSummaries()) {
			var mThread = new Thread(() -> client.deleteVersion(bucketName, Version.getKey(), Version.getVersionId()));
			mThread.start();
			TList.add(mThread);
		}
		return TList;
	}

	public void CorsRequestAndCheck(String Method, String bucketName, Map<String, String> Headers, int StatusCode,
			String ExpectAllowOrigin, String ExpectAllowMethods, String key) {

		try {
			var url = getURL(bucketName);
			if (key != null)
				url = getURL(bucketName, key);

			System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			for (String Item : Headers.keySet()) {
				connection.setRequestProperty(Item, Headers.get(Item));
			}
			connection.setRequestMethod(Method);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setConnectTimeout(15000);

			assertEquals(StatusCode, connection.getResponseCode());
			assertEquals(ExpectAllowOrigin, connection.getHeaderField("Access-Control-Allow-Origin"));
			assertEquals(ExpectAllowMethods, connection.getHeaderField("Access-Control-Allow-Methods"));

			connection.disconnect();
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	public void partsETagCompare(List<PartETag> expected, List<PartSummary> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).getPartNumber(), actual.get(i).getPartNumber());
			assertEquals(expected.get(i).getETag(), actual.get(i).getETag());
		}
	}

	public void VersionIDsCompare(List<S3VersionSummary> expected, List<S3VersionSummary> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).getVersionId(), actual.get(i).getVersionId());
			assertEquals(expected.get(i).getETag(), actual.get(i).getETag());
			assertEquals(expected.get(i).getKey(), actual.get(i).getKey());
			assertEquals(expected.get(i).getSize(), actual.get(i).getSize());
		}
	}

	public void s3eventCompare(Object[] expected, Object[] actual) {
		assertEquals(expected.length, actual.length);
		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i].toString(), actual[i].toString());
		}
	}

	// endregion

	// region Bucket Clear
	public void DeleteBucketList(String bucketName) {
		buckets.remove(bucketName);
	}

	@AfterEach
	public void Clear() {
		if (!config.notDelete)
			BucketClear();
	}

	public void BucketClear(AmazonS3 client, String bucketName) {
		if (client == null)
			return;
		if (StringUtils.isBlank(bucketName))
			return;

		try {
			var isTruncated = true;
			while (isTruncated) {
				var response = client
						.listVersions(new ListVersionsRequest().withBucketName(bucketName).withMaxResults(1000));
				var objects = response.getVersionSummaries();

				for (var version : objects)
					client.deleteVersion(
							new DeleteVersionRequest(bucketName, version.getKey(), version.getVersionId()));
				isTruncated = response.isTruncated();
			}

		} catch (AmazonServiceException e) {
			System.out.format("Error : Bucket(%s) Clear Failed(%s, %d)\n", bucketName, e.getErrorCode(),
					e.getStatusCode());
		}

		try {
			client.deleteBucket(bucketName);
		} catch (AmazonServiceException e) {
			System.out.format("Error : Bucket(%s) Delete Failed(%s, %d)\n", bucketName, e.getErrorCode(),
					e.getStatusCode());
		}
	}

	public void BucketClear() {
		var client = getClient();
		if (client == null)
			return;
		if (buckets == null)
			return;
		if (buckets.size() < 1)
			return;

		var iter = buckets.iterator();
		while (iter.hasNext()) {
			String bucketName = (String) iter.next();
			if (StringUtils.isNotBlank(bucketName))
				BucketClear(client, bucketName);
			iter.remove();
		}

	}
	// endregion

	// region Utils
	public void delay(int milliseconds) {
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
		}
	}

	public Date getExpiredDate(int days) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, days);
		return cal.getTime();
	}

	public Date getExpiredDate(Date day, int days) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(day);
		cal.add(Calendar.DATE, days);
		return cal.getTime();
	}

	// endregion
}
