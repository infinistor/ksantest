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

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
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
import org.example.Data.MultipartUploadV2Data;
import org.example.Data.ObjectDataV2;
import org.example.Data.RangeSet;
import org.example.Data.UserData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.s3tests.S3Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.platform.commons.util.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectLockConfiguration;
import software.amazon.awssdk.services.s3.model.ObjectLockRetention;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ReplicationConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionConfiguration;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Type;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@SuppressWarnings("unchecked")
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

	public S3Client createClient(boolean isSecure, UserData user, boolean useChunkEncoding) {
		String address = "";

		if (isSecure) {
			address = NetUtils.createURLToHTTPS(config.url, config.sslPort);
			System.setProperty(
					"software.amazon.awssdk.http.SdkHttpConfigurationOption#DISABLE_CERT_CHECKING_SYSTEM_PROPERTY",
					"true");
		} else {
			address = NetUtils.createURLToHTTP(config.url, config.port);
		}
		AwsCredentialsProvider awsCred = null;
		if (user == null)
			awsCred = AnonymousCredentialsProvider.create();
		else
			awsCred = StaticCredentialsProvider.create(AwsBasicCredentials.create(user.accessKey, user.secretKey));

		var s3Config = S3Configuration.builder()
				.checksumValidationEnabled(false)
				.chunkedEncodingEnabled(!useChunkEncoding)
				.pathStyleAccessEnabled(true)
				.build();

		return S3Client.builder()
				.region(config.regionName != null ? Region.of(config.regionName) : Region.AP_NORTHEAST_2)
				.endpointOverride(URI.create(address))
				.credentialsProvider(awsCred)
				.httpClientBuilder(ApacheHttpClient.builder()
						.connectionTimeout(Duration.ofSeconds(10))
						.socketTimeout(Duration.ofSeconds(10)))
				.overrideConfiguration(ClientOverrideConfiguration.builder()
						.retryPolicy(RetryPolicy.builder().numRetries(1).build()).build())
				.serviceConfiguration(s3Config).build();
	}

	public S3Client getClient() {
		return createClient(config.isSecure, config.mainUser, true);
	}

	public S3Client getClient(boolean useChunkEncoding) {
		return createClient(config.isSecure, config.mainUser, useChunkEncoding);
	}

	public S3Client getClientHttps(boolean useChunkEncoding) {
		return createClient(true, config.mainUser, useChunkEncoding);
	}

	public S3Client getAltClient() {
		return createClient(config.isSecure, config.altUser, true);
	}

	public S3Client getPublicClient() {
		return createClient(config.isSecure, null, true);
	}

	public S3Presigner getS3Presigner() {
		return S3Presigner.builder().s3Client(getClient()).build();
	}

	public S3Client getBadAuthClient(String accessKey, String secretKey) {
		if (StringUtils.isBlank(accessKey))
			accessKey = "aaaaaaaaaaaaaaa";
		if (StringUtils.isBlank(secretKey))
			secretKey = "bbbbbbbbbbbbbbb";

		var dummyUser = new UserData();
		dummyUser.accessKey = accessKey;
		dummyUser.secretKey = secretKey;

		return createClient(config.isSecure, dummyUser, true);
	}
	// endregion

	public Map<String, String> getACLHeader(String userId, String[] perms) {
		String[] allHeaders = { "read", "write", "read-acp", "write-acp",
				"full-control" };

		var headers = new HashMap<String, String>();

		if (StringUtils.isBlank(userId))
			userId = config.altUser.userId;
		if (perms == null) {
			for (var perm : allHeaders)
				headers.put(String.format("x-amz-grant-%s", perm),
						String.format("id=%s", userId));
		} else {
			for (var perm : perms)
				headers.put(String.format("x-amz-grant-%s", perm),
						String.format("id=%s", userId));
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
		client.createBucket(c -> c.bucket(bucketName).objectOwnership("BucketOwnerPreferred"));
		return bucketName;
	}

	public String getNewBucket(String prefix) {
		var bucketName = getNewBucketName(prefix);
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName));
		return bucketName;
	}

	public String createObjects(List<String> keys) {
		var client = getClient();
		var bucketName = getNewBucket();

		if (keys != null) {
			for (var key : keys) {
				var body = RequestBody.fromString(key);
				if (key.endsWith("/"))
					body = RequestBody.empty();
				client.putObject(p -> p.bucket(bucketName).key(key), body);
			}
		}

		return bucketName;
	}

	public String createObjects(List<String> keys, boolean useChunkEncoding) {
		var client = getClient(useChunkEncoding);
		var bucketName = getNewBucket();

		if (keys != null) {
			for (var key : keys) {
				var body = RequestBody.fromString(key);
				if (key.endsWith("/"))
					body = RequestBody.empty();
				client.putObject(p -> p.bucket(bucketName).key(key), body);
			}
		}

		return bucketName;
	}

	public String createObjects(List<String> keys, String bucketName) {
		var client = getClient();

		if (keys != null) {
			for (var key : keys) {
				var body = RequestBody.fromString(key);
				if (key.endsWith("/"))
					body = RequestBody.empty();
				client.putObject(p -> p.bucket(bucketName).key(key), body);
			}
		}
		return bucketName;
	}

	public String createObjectsHttps(List<String> keys, boolean useChunkEncoding) {
		var client = getClientHttps(useChunkEncoding);
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName));

		if (keys != null) {
			for (var key : keys) {
				var body = RequestBody.fromString(key);
				if (key.endsWith("/"))
					body = RequestBody.empty();
				client.putObject(p -> p.bucket(bucketName).key(key), body);
			}
		}

		return bucketName;
	}

	public String createObjectsToBody(List<String> keys, String body) {
		var client = getClient();
		var bucketName = getNewBucket();

		if (keys != null) {
			for (var key : keys)
				client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));
		}

		return bucketName;
	}

	public String createObjectsToBody(String bucketName, List<String> keys, String body) {
		var client = getClient();

		if (keys != null) {
			for (var key : keys)
				client.putObject(p -> p.bucket(bucketName).key(key),
						RequestBody.fromString(body));
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

	public void checkConfigureVersioningRetry(String bucketName, BucketVersioningStatus status) {
		var client = getClient();

		client.putBucketVersioning(p -> p.bucket(bucketName).versioningConfiguration(c -> c.status(status)));

		BucketVersioningStatus readStatus = null;

		for (int i = 0; i < 5; i++) {
			try {
				var response = client.getBucketVersioning(g -> g.bucket(bucketName));
				readStatus = response.status();

				if (readStatus.equals(status))
					break;
				delay(1000);
			} catch (Exception e) {
				readStatus = null;
			}
		}

		assertEquals(status, readStatus);
	}

	public String setupBucketObjectACL(BucketCannedACL bucketACL, ObjectCannedACL objectACL, String key) {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).acl(bucketACL));
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(objectACL));

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

	public AccessControlPolicy addObjectUserGrant(String bucketName, String key, Grant myGrant) {
		var client = getClient();

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		var grants = response.grants();
		grants.add(myGrant);

		var myGrants = AccessControlPolicy.builder();
		myGrants.grants(grants);
		myGrants.owner(response.owner());

		return myGrants.build();
	}

	public AccessControlPolicy addBucketUserGrant(String bucketName, Grant grant) {
		var client = getClient();

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		var grants = response.grants();
		grants.add(grant);

		var myGrants = AccessControlPolicy.builder();
		myGrants.grants(grants);
		myGrants.owner(response.owner());

		return myGrants.build();
	}

	public void createKeyWithRandomContent(String key, int size, String bucketName, S3Client client) {
		if (client == null)
			client = getClient();
		if (size <= 0)
			size = 7 * MainData.MB;

		var data = Utils.randomTextToLong(size);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));
	}

	public String createKeyWithRandomContent(String key, int size, S3Client client) {
		var bucketName = getNewBucket();
		if (client == null)
			client = getClient();
		if (size <= 0)
			size = 7 * MainData.MB;

		var data = Utils.randomTextToLong(size);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));

		return bucketName;
	}

	public String bucketACLGrantUserId(Permission permission) {
		var bucketName = getNewBucket();
		var client = getClient();

		var mainUser = config.mainUser.toGrantee();
		var altUser = config.altUser.toGrantee();

		var accessControls = addBucketUserGrant(bucketName,
				Grant.builder().grantee(altUser).permission(permission).build());
		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(accessControls));

		var response = client.getBucketAcl(g -> g.bucket(bucketName));

		var getGrants = response.grants();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(Grant.builder().grantee(mainUser).permission(Permission.FULL_CONTROL).build());
		myGrants.add(Grant.builder().grantee(altUser).permission(permission).build());
		checkGrants(myGrants, new ArrayList<>(getGrants));

		return bucketName;
	}

	public String setupAccessTest(String key1, String key2, BucketCannedACL bucketACL, ObjectCannedACL objectACL) {
		var bucketName = getNewBucket();
		var client = getClient();

		client.putBucketAcl(p -> p.bucket(bucketName).acl(bucketACL));
		client.putObject(p -> p.bucket(bucketName).key(key1).acl(objectACL), RequestBody.fromString(key1));
		client.putObject(p -> p.bucket(bucketName).key(key2), RequestBody.fromString(key2));

		return bucketName;
	}

	public void checkObjContent(S3Client client, String bucketName, String key, String versionId, String content) {
		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).versionId(versionId));
		if (content != null) {
			var body = getBody(response);
			assertTrue(content.equals(body), MainData.NOT_MATCHED);
		} else
			assertNull(response);
	}

	public void checkObjVersions(S3Client client, String bucketName, String key, List<String> versionIds,
			List<String> contents) {
		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = response.versions();

		Collections.reverse(versions);

		var index = 0;
		for (var version : versions) {
			assertEquals(version.versionId(), versionIds.get(index));
			if (StringUtils.isNotBlank(key))
				assertEquals(key, version.key());
			checkObjContent(client, bucketName, key, version.versionId(), contents.get(index++));
		}
	}

	public void createMultipleVersions(S3Client client, String bucketName, String key, int numVersions,
			boolean checkVersion) {
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(p -> p.bucket(bucketName).key(key),
					RequestBody.fromString(body));
			var versionId = response.versionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		if (checkVersion)
			checkObjVersions(client, bucketName, key, versionIds, contents);
	}

	public List<Tag> createSimpleTagSet(int count) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(Tag.builder().key(Integer.toString(i)).value(Integer.toString(i)).build());
		return tagSets;
	}

	public List<Tag> createDetailTagSet(int count, int keySize, int valueSize) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(Tag.builder().key(Utils.randomTextToLong(keySize)).value(Utils.randomTextToLong(valueSize))
					.build());
		return tagSets;
	}

	public Grantee createPublicGrantee() {
		return Grantee.builder().type(Type.GROUP).uri("http://acs.amazonaws.com/groups/global/AllUsers").build();
	}

	public Grantee createAuthenticatedGrantee() {
		return Grantee.builder().type(Type.GROUP).uri("http://acs.amazonaws.com/groups/global/AuthenticatedUsers")
				.build();
	}

	// endregion

	// region Get data

	public List<String> getObjectList(String bucketName, String prefix) {
		var client = getClient();
		var response = client.listObjects(l -> l.bucket(bucketName).prefix(prefix));
		return getKeys(response.contents());
	}

	public static List<String> getKeys(List<S3Object> objectList) {
		if (objectList != null) {
			var temp = new ArrayList<String>();

			for (var S3Object : objectList)
				temp.add(S3Object.key());

			return temp;
		}
		return new ArrayList<>();
	}

	public static List<String> getKeys2(List<ObjectVersion> objectList) {
		if (objectList != null) {
			var temp = new ArrayList<String>();

			for (var S3Object : objectList)
				temp.add(S3Object.key());

			return temp;
		}
		return Collections.emptyList();
	}

	public static String getBody(ResponseInputStream<GetObjectResponse> data) {
		var body = "";
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
		return body;
	}

	public static ObjectDataV2 getObjectToKey(String key, List<ObjectDataV2> keyList) {
		for (var Object : keyList) {
			if (Object.key.equals(key))
				return Object;
		}
		return null;
	}

	public List<String> getBucketList(ListBucketsResponse response) {
		if (response == null)
			return new ArrayList<>();
		var bucketList = new ArrayList<String>();

		for (var Item : response.buckets())
			bucketList.add(Item.name());

		return bucketList;
	}

	public List<String> getPrefixList(List<CommonPrefix> prefixList) {
		if (prefixList == null)
			return new ArrayList<>();
		var prefix = new ArrayList<String>();

		for (var Item : prefixList)
			prefix.add(Item.prefix());

		return prefix;
	}

	public List<String> cutStringData(String data, int partSize) {
		var stringList = new ArrayList<String>();

		int startPoint = 0;
		while (startPoint < data.length()) {

			var endpoint = Math.min(startPoint + partSize, data.length());

			stringList.add(data.substring(startPoint, endpoint));
			startPoint += partSize;
		}

		return stringList;
	}

	public static List<ObjectIdentifier> getKeyVersions(List<String> keyList) {
		var keyVersions = new ArrayList<ObjectIdentifier>();
		for (var key : keyList)
			keyVersions.add(ObjectIdentifier.builder().key(key).build());

		return keyVersions;
	}

	public List<String> versionIDs(List<ObjectVersion> versions) {
		if (versions == null)
			return Collections.emptyList();

		var items = new ArrayList<String>();
		for (var item : versions)
			items.add(item.versionId());
		return items;
	}

	// endregion

	// region Check data
	public void aclTest(String bucketName, String key, S3Client client, boolean pass) {
		if (pass) {
			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			assertTrue(response != null);
		} else {
			var e = assertThrows(S3Exception.class,
					() -> client.getObject(g -> g.bucket(bucketName).key(key)));
			var statusCode = e.statusCode();
			var errorCode = e.awsErrorDetails().errorCode();

			assertEquals(403, statusCode);
			assertEquals(MainData.AccessDenied, errorCode);
		}
	}

	public String validateListObject(String bucketName, String prefix, String delimiter, String marker, int maxKeys,
			boolean isTruncated, List<String> checkKeys, List<String> checkPrefixes, String nextMarker) {
		var client = getClient();
		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter)
				.marker(marker).maxKeys(maxKeys).prefix(prefix));

		assertEquals(isTruncated, response.isTruncated());
		assertEquals(nextMarker, response.nextMarker());

		var keys = response.contents();
		var prefixes = response.commonPrefixes();

		assertEquals(checkKeys.size(), keys.size());
		assertEquals(checkPrefixes.size(), prefixes.size());
		assertEquals(checkKeys, keys);
		assertEquals(checkPrefixes, prefixes);

		return response.nextMarker();
	}

	public String validateListObjectV2(String bucketName, String prefix, String delimiter, String continuationToken,
			int maxKeys, boolean isTruncated, List<String> checkKeys, List<String> checkPrefixes, boolean last) {
		var client = getClient();
		var response = client
				.listObjectsV2(l -> l.bucket(bucketName).delimiter(delimiter)
						.continuationToken(continuationToken).maxKeys(maxKeys).prefix(prefix));

		assertEquals(isTruncated, response.isTruncated());
		if (last)
			assertNull(response.nextContinuationToken());

		var keys = getKeys(response.contents());
		var prefixes = response.commonPrefixes();

		assertEquals(checkKeys, keys);
		assertEquals(checkPrefixes, prefixes);
		assertEquals(checkKeys.size(), keys.size());
		assertEquals(checkPrefixes.size(), prefixes.size());

		return response.nextContinuationToken();
	}

	public void checkBadBucketName(String bucketName) {
		var client = getClient();

		assertThrows(S3Exception.class,
				() -> client.createBucket(c -> c.bucket(bucketName)));
	}

	public void checkGoodBucketName(String name, String prefix) {
		if (StringUtils.isBlank(prefix))
			prefix = getPrefix();

		var bucketName = String.format("%s%s", prefix, name);
		buckets.add(bucketName);

		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName));
	}

	public void testBucketCreateNamingGoodLong(int length) {
		var bucketName = getNewBucketName(length);

		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName));
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

	public MultipartUploadV2Data multipartUpload(S3Client client, String bucketName, String key, int size,
			MultipartUploadV2Data uploadData) {
		var partSize = 5 * MainData.MB;
		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.partNumber(uploadData.nextPartNumber()),
					RequestBody.fromString(Part));
			uploadData.addPart(partResponse.eTag());
		}

		return uploadData;
	}

	public MultipartUploadV2Data setupMultipartUpload(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList) {
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key).metadata(metadataList));
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, uploadData.partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);

			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.partNumber(uploadData.nextPartNumber()),
					RequestBody.fromString(Part));
			uploadData.addPart(partResponse.eTag());
		}

		return uploadData;
	}

	public MultipartUploadV2Data setupSSEMultipartUpload(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList) {
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key).metadata(metadataList)
				.serverSideEncryption(ServerSideEncryption.AES256));
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, uploadData.partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);

			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.partNumber(uploadData.nextPartNumber()),
					RequestBody.fromString(Part));
			uploadData.addPart(partResponse.eTag());
		}

		return uploadData;
	}

	public MultipartUploadV2Data multipartUploadResend(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList, List<Integer> resendParts) {
		var uploadData = new MultipartUploadV2Data();
		var contentType = "text/plain";

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.metadata(metadataList)
				.contentType(contentType));
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, uploadData.partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			int partNumber = uploadData.nextPartNumber();

			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.partNumber(partNumber),
					RequestBody.fromString(Part));
			uploadData.addPart(partNumber, partResponse.eTag());

			if (resendParts != null && resendParts.contains(partNumber))
				client.uploadPart(u -> u
						.bucket(bucketName)
						.key(key)
						.uploadId(uploadData.uploadId)
						.partNumber(partNumber),
						RequestBody.fromString(Part));
		}

		return uploadData;
	}

	public MultipartUploadV2Data setupMultipartUpload(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList, String sseC) {
		var uploadData = new MultipartUploadV2Data();
		var partSize = 5 * MainData.MB;

		var contentType = "text/plain";

		var initRequest = CreateMultipartUploadRequest.builder()
				.bucket(bucketName)
				.key(key)
				.metadata(metadataList)
				.contentType(contentType);
		if (sseC != null)
			initRequest.sseCustomerKey(sseC);

		var createResponse = client.createMultipartUpload(initRequest.build());
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			var request = UploadPartRequest.builder()
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.partNumber(uploadData.nextPartNumber());
			if (sseC != null)
				request.sseCustomerKey(sseC);

			var response = client.uploadPart(request.build(), RequestBody.fromString(Part));
			uploadData.addPart(response.eTag());
		}
		return uploadData;
	}

	public void checkCopyContent(String sourceBucketName, String sourceKey, String targetBucketName, String targetKey) {
		var client = getClient();

		var sourceResponse = client
				.getObject(g -> g.bucket(sourceBucketName).key(sourceKey));
		var sourceSize = sourceResponse.response().contentLength();
		var sourceData = getBody(sourceResponse);

		var targetResponse = client
				.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		var targetSize = targetResponse.response().contentLength();
		var targetData = getBody(targetResponse);

		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContent(String sourceBucketName, String sourceKey, String targetBucketName, String targetKey,
			String versionId) {
		var client = getClient();

		var sourceResponse = client.getObject(
				g -> g.bucket(sourceBucketName).key(sourceKey).versionId(versionId));
		var sourceSize = sourceResponse.response().contentLength();
		var sourceData = getBody(sourceResponse);

		var targetResponse = client
				.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		var targetSize = targetResponse.response().contentLength();
		var targetData = getBody(targetResponse);

		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentSseC(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, String sseC) {

		var sourceResponse = client.getObject(
				g -> g.bucket(sourceBucketName).key(sourceKey).sseCustomerKey(sseC));
		var sourceSize = sourceResponse.response().contentLength();
		var sourceData = getBody(sourceResponse);

		var targetResponse = client
				.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		var targetSize = targetResponse.response().contentLength();
		var targetData = getBody(targetResponse);

		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentUsingRange(String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey) {
		var client = getClient();

		var targetResponse = client
				.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		var targetSize = targetResponse.response().contentLength();
		var targetData = getBody(targetResponse);

		var sourceResponse = client.getObject(g -> g.bucket(sourceBucketName).key(sourceKey)
				.range("bytes=0-" + (targetSize - 1)));
		var sourceSize = sourceResponse.response().contentLength();
		var sourceData = getBody(sourceResponse);

		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentUsingRange(String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey, int step) {
		var client = getClient();

		var response = client.headObject(h -> h.bucket(sourceBucketName).key(sourceKey));
		var size = response.contentLength();

		long index = 0;
		while (index < size) {
			var start = index;
			var end = Math.min(index + step, size) - 1;

			var sourceResponse = client.getObject(g -> g.bucket(sourceBucketName).key(sourceKey)
					.range("bytes=" + start + "-" + end));
			var sourceSize = sourceResponse.response().contentLength();
			var sourceData = getBody(sourceResponse);

			var targetResponse = client.getObject(g -> g.bucket(targetBucketName).key(targetKey)
					.range("bytes=" + start + "-" + end));
			var targetSize = targetResponse.response().contentLength();
			var targetData = getBody(targetResponse);

			assertEquals(sourceSize, targetSize);
			assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
			index += step;
		}
	}

	public void checkUploadMultipartResend(String bucketName, String key, int size, List<Integer> resendParts) {
		var contentType = "text/bla";
		var metadata = new HashMap<String, String>();
		metadata.put("x-amz-meta-foo", "bar");

		var client = getClient();
		var uploadData = multipartUploadResend(client, bucketName, key, size, metadata, resendParts);
		client.completeMultipartUpload(c -> c
				.bucket(bucketName).key(key)
				.uploadId(uploadData.uploadId)
				.multipartUpload(uploadData.completedMultipartUpload()));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(contentType, response.contentType());
		assertEquals(metadata, response.metadata());

		var body = uploadData.getBody();
		checkContentUsingRange(bucketName, key, body, MainData.MB);
		checkContentUsingRange(bucketName, key, body, 10L * MainData.MB);
	}

	public String doTestMultipartUploadContents(String bucketName, String key, int numParts) {
		var payload = Utils.randomTextToLong(5 * MainData.MB);
		var client = getClient();

		var createResponse = client
				.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		var uploadID = createResponse.uploadId();
		var parts = new ArrayList<CompletedPart>();
		StringBuilder allPayload = new StringBuilder();

		for (int i = 0; i < numParts; i++) {
			var partNumber = i + 1;
			var partResponse = client.uploadPart(u -> u.bucket(bucketName).key(key)
					.uploadId(uploadID).partNumber(partNumber), RequestBody.fromString(payload));
			parts.add(CompletedPart.builder().partNumber(partNumber).eTag(partResponse.eTag()).build());
			allPayload.append(payload);
		}
		var lestPayload = Utils.randomTextToLong(MainData.MB);
		var lestPartResponse = client.uploadPart(u -> u.bucket(bucketName).key(key)
				.uploadId(uploadID).partNumber(numParts + 1),
				RequestBody.fromString(lestPayload));
		parts.add(CompletedPart.builder().partNumber(numParts + 1).eTag(lestPartResponse.eTag()).build());
		allPayload.append(lestPayload);

		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key)
				.uploadId(uploadID).multipartUpload(p -> p.parts(parts)));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var data = getBody(response);

		assertTrue(allPayload.toString().equals(data), MainData.NOT_MATCHED);

		return allPayload.toString();
	}

	public MultipartUploadV2Data multipartCopy(String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey, int size, S3Client client, int partSize, String versionId) {
		var data = new MultipartUploadV2Data();
		if (client == null)
			client = getClient();
		if (partSize <= 0)
			partSize = 5 * MainData.MB;

		var response = client.createMultipartUpload(c -> c.bucket(targetBucketName).key(targetKey));
		data.uploadId = response.uploadId();

		int count = 1;
		long index = 0;
		while (index < size) {
			var partNumber = count;
			var start = index;
			var end = Math.min(index + partSize, size) - 1;

			var partResponse = client.uploadPartCopy(c -> c.sourceBucket(sourceBucketName)
					.sourceKey(sourceKey).destinationBucket(targetBucketName).destinationKey(targetKey)
					.uploadId(data.uploadId).partNumber(partNumber).copySourceRange("bytes=" + start + "-" + end)
					.sourceVersionId(versionId));
			data.addPart(count++, partResponse.copyPartResult().eTag());

			index = end + 1;
		}

		return data;
	}

	public MultipartUploadV2Data multipartCopy(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, int size, Map<String, String> metadata) {
		var data = new MultipartUploadV2Data();
		var partSize = 5 * MainData.MB;
		var request = CreateMultipartUploadRequest.builder().bucket(targetBucketName).key(targetKey);
		if (metadata != null)
			request.metadata(metadata);

		var response = client.createMultipartUpload(request.build());

		data.uploadId = response.uploadId();

		var count = 1;
		var index = 0L;
		while (index < size) {
			var partNumber = count;
			var start = index;
			long end = Math.min(start + partSize, size) - 1;

			var partResponse = client.uploadPartCopy(c -> c.sourceBucket(sourceBucketName)
					.sourceKey(sourceKey).destinationBucket(targetBucketName).destinationKey(targetKey)
					.uploadId(data.uploadId).partNumber(partNumber).copySourceRange("bytes=" + start + "-" + end));
			data.addPart(count++, partResponse.copyPartResult().eTag());
			index = end + 1;
		}

		return data;
	}

	public MultipartUploadV2Data multipartCopySseC(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, int size, Map<String, String> metadata, String sseC) {
		var data = new MultipartUploadV2Data();
		var partSize = 5 * MainData.MB;

		var request = CreateMultipartUploadRequest.builder().bucket(targetBucketName).key(targetKey)
				.sseCustomerKey(sseC);
		if (metadata != null)
			request.metadata(metadata);

		var response = client.createMultipartUpload(request.build());
		data.uploadId = response.uploadId();

		int count = 1;
		long index = 0;
		while (index < size) {
			var partNumber = count;
			var start = index;
			long end = Math.min(start + partSize, size) - 1;

			var partResponse = client.uploadPartCopy(c -> c
					.sourceBucket(sourceBucketName)
					.sourceKey(sourceKey)
					.destinationBucket(targetBucketName)
					.destinationKey(targetKey)
					.copySourceSSECustomerKey(sseC)
					.sseCustomerKey(sseC)
					.uploadId(data.uploadId)
					.partNumber(partNumber)
					.copySourceRange("bytes=" + start + "-" + end));
			data.addPart(count++, partResponse.copyPartResult().eTag());

			index = end + 1;
		}

		return data;
	}

	public static long getBytesUsed(ListObjectsV2Response response) {
		long size = 0;
		if (response == null || response.contents() == null || response.contents().isEmpty())
			return size;

		for (var Obj : response.contents())
			size += Obj.size();

		return size;
	}

	public void checkContentUsingRange(String bucketName, String key, String data, long step) {
		var client = getClient();
		var headResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		var size = headResponse.contentLength();
		assertEquals(data.length(), size);

		var index = 0L;
		while (index < size) {
			var start = index;
			var end = Math.min(index + step, size - 1L) - 1;

			var response = client.getObject(
					g -> g.bucket(bucketName).key(key).range("bytes=" + start + "-" + end));
			var body = getBody(response);
			var length = end - start;
			var partBody = data.substring((int) start, (int) end);

			assertEquals(length, response.response().contentLength());
			assertTrue(partBody.equals(body), MainData.NOT_MATCHED);
			index += step;
		}
	}

	public void checkContentUsingRangeEnc(S3Client client, String bucketName, String key, String data, long step,
			String sseC) {
		if (client == null)
			client = getClient();
		var headResponse = client
				.headObject(h -> h.bucket(bucketName).key(key).sseCustomerKey(sseC));
		var size = headResponse.contentLength();

		var index = 0L;
		while (index < size) {
			long start = index;
			var end = Math.min(index + step, size - 1L) - 1;

			var response = client.getObject(g -> g.bucket(bucketName).key(key)
					.range("bytes=" + start + "-" + end).sseCustomerKey(sseC));
			assertNotNull(response);
			var body = getBody(response);
			var length = end - start;
			var partBody = data.substring((int) start, (int) end);

			assertEquals(length, response.response().contentLength());
			assertTrue(partBody.equals(body), MainData.NOT_MATCHED);
			index += step;
		}
	}

	public RangeSet getRandomRange(int fileSize) {
		int maxSize = 500;
		Random rand = new Random(System.currentTimeMillis());

		var start = rand.nextInt(fileSize - maxSize * 2);
		var maxLength = fileSize - start;

		if (maxLength > maxSize)
			maxLength = maxSize;
		var size = rand.nextInt(maxLength) + maxSize - 1;
		if (size <= 0)
			size = 1;

		return new RangeSet(start, size);

	}

	public void checkContent(String bucketName, String key, String data, int loopCount) {
		var client = getClient();

		for (int i = 0; i < loopCount; i++) {
			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			var body = getBody(response);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentEnc(String bucketName, String key, String data, int loopCount, String sseC) {
		var client = getClientHttps(false);

		for (int i = 0; i < loopCount; i++) {
			var response = client
					.getObject(g -> g.bucket(bucketName).key(key).sseCustomerKey(sseC));
			var body = getBody(response);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRange(String bucketName, String key, String data, int loopCount) {
		var client = getClient();
		int fileSize = data.length();

		for (int i = 0; i < loopCount; i++) {
			var range = getRandomRange(fileSize);
			var rangeBody = data.substring(range.start, range.end + 1);

			var response = client.getObject(
					g -> g.bucket(bucketName).key(key).range("bytes=" + range.start + "-" + (range.end - 1)));
			var body = getBody(response);

			assertEquals(range.length, response.response().contentLength() - 1);
			assertTrue(rangeBody.equals(body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRangeEnc(S3Client client, String bucketName, String key, String data,
			int fileSize, int loopCount, String sseC) {
		for (int i = 0; i < loopCount; i++) {
			var range = getRandomRange(fileSize);

			var response = client.getObject(g -> g.bucket(bucketName).key(key)
					.range("bytes=" + range.start + "-" + (range.end - 1)).sseCustomerKey(sseC));
			var body = getBody(response);
			var rangeBody = data.substring(range.start, range.end);

			assertEquals(range.length, response.response().contentLength());
			assertTrue(rangeBody.equals(body), MainData.NOT_MATCHED);
		}
	}

	public boolean errorCheck(Integer statusCode) {
		return statusCode.equals(400) || statusCode.equals(403);
	}

	public void testEncryptionCSEWrite(int fileSize) {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "test";
		var aesKey = Utils.randomTextToLong(32);
		var data = Utils.randomTextToLong(fileSize);

		try {
			var encodingData = AES256.encrypt(data, aesKey);
			var metadata = new HashMap<String, String>();
			metadata.put("x-amz-meta-key", aesKey);

			client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata)
					.contentType("text/plain").contentLength((long) encodingData.length()),
					RequestBody.fromString(encodingData));

			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			var encodingBody = getBody(response);
			var body = AES256.decrypt(encodingBody, aesKey);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);

		} catch (Exception e) {
			fail(e);
		}
	}

	public void testEncryptionSSECustomerWrite(int fileSize) {
		var bucketName = getNewBucket();
		var client = getClientHttps(false);
		var key = "test";
		var data = Utils.randomTextToLong(fileSize);
		var sseC = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
		var sseCMd5 = "DWygnHRtgiJ77HCm+1rvHw==";

		client.putObject(
				p -> p.bucket(bucketName).key(key).sseCustomerKey(sseC).sseCustomerKeyMD5(sseCMd5),
				RequestBody.fromString(data));

		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).sseCustomerKey(sseC));
		var body = getBody(response);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(sseCMd5, response.response().sseCustomerKeyMD5());
	}

	public void testEncryptionSseS3CustomerWrite(int fileSize) {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "test";
		var data = Utils.randomTextToLong(fileSize);

		client.putObject(p -> p.bucket(bucketName).key(key).serverSideEncryption(ServerSideEncryption.AES256),
				RequestBody.fromString(data));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(ServerSideEncryption.AES256, response.response().serverSideEncryption());
	}

	public void testEncryptionSseS3Copy(int fileSize) {
		var bucketName = getNewBucket();
		var client = getClient();
		var data = Utils.randomTextToLong(fileSize);

		var sseS3Config = ServerSideEncryptionConfiguration.builder()
				.rules(s -> s.applyServerSideEncryptionByDefault(d -> d
						.sseAlgorithm(ServerSideEncryption.AES256))
						.bucketKeyEnabled(false))
				.build();

		client.putBucketEncryption(p -> p.bucket(bucketName).serverSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(g -> g.bucket(bucketName));
		assertEquals(sseS3Config.rules(), response.serverSideEncryptionConfiguration().rules());

		var sourceKey = "bar";
		client.putObject(p -> p.bucket(bucketName).key(sourceKey),
				RequestBody.fromString(data));

		var sourceResponse = client.getObject(g -> g.bucket(bucketName).key(sourceKey));
		var sourceBody = getBody(sourceResponse);
		assertEquals(ServerSideEncryption.AES256, sourceResponse.response().serverSideEncryptionAsString());

		var targetKey = "foo";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(sourceKey)
				.destinationBucket(bucketName).destinationKey(targetKey));
		var targetResponse = client.getObject(g -> g.bucket(bucketName).key(targetKey));
		assertEquals(ServerSideEncryption.AES256, targetResponse.response().serverSideEncryptionAsString());

		var targetBody = getBody(targetResponse);
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(boolean sourceObjectEncryption, boolean sourceBucketEncryption,
			boolean targetBucketEncryption, boolean targetObjectEncryption, int fileSize) {
		var sourceKey = "sourceKey";
		var targetKey = "targetKey";
		var sourceBucketName = getNewBucket();
		var targetBucketName = getNewBucket();
		var client = getClient();
		var data = Utils.randomTextToLong(fileSize);

		// SSE-S3 Config
		var sseS3Config = ServerSideEncryptionConfiguration.builder()
				.rules(s -> s.applyServerSideEncryptionByDefault(d -> d
						.sseAlgorithm(ServerSideEncryption.AES256))
						.bucketKeyEnabled(false))
				.build();

		// Source Bucket Encryption
		if (sourceBucketEncryption) {
			client.putBucketEncryption(p -> p.bucket(sourceBucketName)
					.serverSideEncryptionConfiguration(sseS3Config));

			var response = client
					.getBucketEncryption(g -> g.bucket(sourceBucketName));
			assertEquals(sseS3Config.rules(), response.serverSideEncryptionConfiguration().rules());
		}

		// Target Bucket Encryption
		if (targetBucketEncryption) {
			client.putBucketEncryption(p -> p.bucket(targetBucketName)
					.serverSideEncryptionConfiguration(sseS3Config));

			var response = client
					.getBucketEncryption(g -> g.bucket(targetBucketName));
			assertEquals(sseS3Config.rules(), response.serverSideEncryptionConfiguration().rules());
		}

		// Source Put Object
		if (sourceObjectEncryption)
			client.putObject(p -> p.bucket(sourceBucketName).key(sourceKey)
					.serverSideEncryption(ServerSideEncryption.AES256), RequestBody.fromString(data));
		else
			client.putObject(p -> p.bucket(sourceBucketName).key(sourceKey),
					RequestBody.fromString(data));
		//// Source Object Check
		var sourceResponse = client
				.getObject(g -> g.bucket(sourceBucketName).key(sourceKey));
		var sourceBody = getBody(sourceResponse);
		if (sourceObjectEncryption)
			assertEquals(ServerSideEncryption.AES256, sourceResponse.response().serverSideEncryptionAsString());
		else
			assertNull(sourceResponse.response().serverSideEncryptionAsString());
		assertEquals(data, sourceBody);

		// Source Copy Object
		if (targetObjectEncryption)
			client.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(sourceKey)
					.destinationBucket(targetBucketName).destinationKey(targetKey)
					.serverSideEncryption(ServerSideEncryption.AES256));
		else
			client.copyObject(c -> c.sourceBucket(sourceBucketName).sourceKey(sourceKey)
					.destinationBucket(targetBucketName).destinationKey(targetKey));
		// Target Object Check
		var targetResponse = client
				.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		var targetBody = getBody(targetResponse);
		if (targetBucketEncryption || targetObjectEncryption)
			assertEquals(ServerSideEncryption.AES256, targetResponse.response().serverSideEncryptionAsString());
		else
			assertNull(targetResponse.response().serverSideEncryptionAsString());
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(EncryptionType source, EncryptionType target, int size) {
		var sourceKey = "sourceKey";
		var targetKey = "targetKey";
		var bucketName = getNewBucket();
		var client = getClientHttps(false);
		var data = Utils.randomTextToLong(size);

		var sseC = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
		var sseCMd5 = "DWygnHRtgiJ77HCm+1rvHw==";

		// Source Put Object
		var sourcePutRequest = PutObjectRequest.builder().bucket(bucketName).key(sourceKey).sseCustomerKey(sseC)
				.sseCustomerKeyMD5(sseCMd5);
		var sourceGetRequest = GetObjectRequest.builder().bucket(bucketName).key(sourceKey).sseCustomerKey(sseC);
		var targetGetRequest = GetObjectRequest.builder().bucket(bucketName).key(targetKey);
		var copyRequest = CopyObjectRequest.builder().sourceBucket(bucketName).sourceKey(sourceKey)
				.destinationBucket(bucketName).destinationKey(targetKey).metadataDirective(MetadataDirective.REPLACE);

		// Source Options
		switch (source) {
			case SSE_S3:
				sourcePutRequest.serverSideEncryption(ServerSideEncryption.AES256);
				break;
			case SSE_C:
				sourcePutRequest.sseCustomerKey(sseC);
				sourceGetRequest.sseCustomerKey(sseC);
				copyRequest.copySourceSSECustomerKey(sseC);
				break;
			case NORMAL:
				break;
		}

		// Target Options
		switch (target) {
			case SSE_S3:
				copyRequest.serverSideEncryption(ServerSideEncryption.AES256);
				break;
			case SSE_C:
				copyRequest.sseCustomerKey(sseC);
				targetGetRequest.sseCustomerKey(sseC);
				break;
			case NORMAL:
				copyRequest.metadata(null);
				break;
		}

		// Source Put Object
		client.putObject(sourcePutRequest.build(), RequestBody.fromString(data));

		// Source Get Object
		var sourceResponse = client.getObject(sourceGetRequest.build());
		var sourceBody = getBody(sourceResponse);
		assertTrue(data.equals(sourceBody), MainData.NOT_MATCHED);

		// Copy Object
		client.copyObject(copyRequest.build());

		// Target Object Check
		var targetResponse = client.getObject(targetGetRequest.build());
		var targetBody = getBody(targetResponse);
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public int getPermissionPriority(Permission permission) {
		if (permission == Permission.FULL_CONTROL)
			return 0;
		if (permission == Permission.READ)
			return 1;
		if (permission == Permission.READ_ACP)
			return 2;
		if (permission == Permission.WRITE)
			return 3;
		if (permission == Permission.WRITE_ACP)
			return 4;
		return 0;
	}

	public int getPermissionPriority(Grant a, Grant b) {
		int priorityA = getPermissionPriority(a.permission());
		int priorityB = getPermissionPriority(b.permission());

		return Integer.compare(priorityA, priorityB);
	}

	public int getGranteeIdentifierPriority(Grant a, Grant b) {
		return a.grantee().type().compareTo(b.grantee().type());
	}

	public List<Grant> grantsSort(List<Grant> data) {
		var newList = new ArrayList<Grant>();

		Comparator<String> comparator = (s1, s2) -> s2.compareTo(s1);
		var kk = new TreeMap<String, Grant>(comparator);

		for (var Temp : data) {
			kk.put(Temp.grantee().type().toString() + Temp.permission().toString(), Temp);
		}

		for (Map.Entry<String, Grant> entry : kk.entrySet()) {
			newList.add(entry.getValue());
		}
		return newList;
	}

	public void checkGrants(List<Grant> expected, List<Grant> actual) {
		assertEquals(expected.size(), actual.size());

		expected = grantsSort(expected);
		actual = grantsSort(actual);

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).permission(), actual.get(i).permission());
			assertEquals(expected.get(i).grantee().id(), actual.get(i).grantee().id());
			assertEquals(expected.get(i).grantee().type(), actual.get(i).grantee().type());
		}
	}

	public void checkObjectACL(Permission permission) {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());
		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		var policy = AccessControlPolicy.builder().owner(response.owner()).grants(response.grants().get(0)).build();

		client.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(policy));

		response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		var getGrants = response.grants();

		var user = Grant.builder().grantee(config.mainUser.toGrantee()).permission(permission).build();

		var myGrants = new ArrayList<Grant>();
		myGrants.add(user);
		checkGrants(myGrants, getGrants);
	}

	public void checkBucketACLGrantCanRead(String bucketName) {
		var altClient = getAltClient();
		altClient.headBucket(h -> h.bucket(bucketName));
	}

	public void checkBucketACLGrantCantRead(String bucketName) {
		var altClient = getAltClient();
		assertThrows(S3Exception.class,
				() -> altClient.headBucket(h -> h.bucket(bucketName)));
	}

	public void checkBucketACLGrantCanReadACP(String bucketName) {
		var altClient = getAltClient();
		altClient.getBucketAcl(g -> g.bucket(bucketName));
	}

	public void checkBucketACLGrantCantReadACP(String bucketName) {
		var altClient = getAltClient();
		assertThrows(S3Exception.class,
				() -> altClient.getBucketAcl(g -> g.bucket(bucketName)));
	}

	public void checkBucketACLGrantCanWrite(String bucketName) {
		var altClient = getAltClient();
		altClient.putObject(
				p -> p
						.bucket(bucketName)
						.key("foo-write"),
				RequestBody.fromString("bar"));
	}

	public void checkBucketACLGrantCantWrite(String bucketName) {
		var altClient = getAltClient();
		assertThrows(S3Exception.class,
				() -> altClient.putObject(
						p -> p
								.bucket(bucketName)
								.key("foo-write"),
						RequestBody.fromString("bar")));
	}

	public void checkBucketACLGrantCanWriteACP(String bucketName) {
		var altClient = getAltClient();
		altClient.putBucketAcl(
				p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE));
	}

	public void checkBucketACLGrantCantWriteACP(String bucketName) {
		var altClient = getAltClient();
		assertThrows(S3Exception.class,
				() -> altClient.putBucketAcl(p -> p
						.bucket(bucketName)
						.acl(BucketCannedACL.PUBLIC_READ)));
	}

	public void prefixLifecycleConfigurationCheck(List<LifecycleRule> expected, List<LifecycleRule> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			var e = expected.get(i);
			var a = actual.get(i);

			assertEquals(e.id(), a.id());
			assertEquals(e.expiration().date(), a.expiration().date());
			assertEquals(e.expiration().days(), a.expiration().days());
			assertEquals(e.expiration().expiredObjectDeleteMarker(), a.expiration().expiredObjectDeleteMarker());
			assertEquals(e.filter().prefix(), a.filter().prefix());

			assertEquals(e.status(), a.status());
			if (e.noncurrentVersionExpiration() != null)
				assertEquals(e.noncurrentVersionExpiration().noncurrentDays(),
						a.noncurrentVersionExpiration().noncurrentDays());
			assertEquals(e.abortIncompleteMultipartUpload().daysAfterInitiation(),
					a.abortIncompleteMultipartUpload().daysAfterInitiation());
		}
	}

	public GetBucketLifecycleConfigurationResponse setupLifecycleExpiration(S3Client client, String bucketName,
			String ruleId, int deltaDays, String rulePrefix) {
		client.putBucketLifecycleConfiguration(p -> p
				.bucket(bucketName)
				.lifecycleConfiguration(l -> l.rules(r -> r
						.id(ruleId)
						.filter(f -> f.prefix(rulePrefix))
						.status(ExpirationStatus.ENABLED)
						.expiration(e -> e.days(deltaDays)))));

		var key = rulePrefix + "/foo";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		return client.getBucketLifecycleConfiguration(g -> g.bucket(bucketName));
	}

	public void lockCompare(ObjectLockConfiguration expected, ObjectLockConfiguration actual) {
		assertEquals(expected.objectLockEnabled(), actual.objectLockEnabled());
		assertEquals(expected.rule().defaultRetention().mode(), actual.rule().defaultRetention().mode());
		assertEquals(expected.rule().defaultRetention().years(), actual.rule().defaultRetention().years());
		assertEquals(expected.rule().defaultRetention().days(), actual.rule().defaultRetention().days());
	}

	public void retentionCompare(ObjectLockRetention expected, ObjectLockRetention actual) {
		assertEquals(expected.mode(), actual.mode());
		assertEquals(expected.retainUntilDate(), actual.retainUntilDate());

	}

	public void replicationConfigCompare(ReplicationConfiguration expected, ReplicationConfiguration actual) {
		assertEquals(expected.role(), actual.role());
		var expectedRules = expected.rules();
		var actualRules = actual.rules();

		for (int i = 0; i < expectedRules.size(); i++) {
			var expectedRule = expectedRules.get(i);
			var actualRule = actualRules.get(i);
			assertEquals(expectedRule.deleteMarkerReplication(), actualRule.deleteMarkerReplication());
			assertEquals(expectedRule.destination().toString(), actualRule.destination().toString());
			assertEquals(expectedRule.existingObjectReplication(), actualRule.existingObjectReplication());
			assertEquals(expectedRule.filter(), actualRule.filter());
			assertEquals(expectedRule.sourceSelectionCriteria(), actualRule.sourceSelectionCriteria());
			assertEquals(expectedRule.status(), actualRule.status());
		}
	}

	public List<Tag> taggingSort(List<Tag> tags) {
		Collections.sort(tags, (t1, t2) -> t1.key().compareTo(t2.key()));
		return tags;
	}

	public void tagCompare(List<Tag> expected, List<Tag> actual) {
		assertEquals(expected.size(), actual.size());

		var orderExpected = taggingSort(expected);
		var orderActual = taggingSort(actual);

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(orderExpected.get(i).key(), orderActual.get(i).key());
			assertEquals(orderExpected.get(i).value(), orderActual.get(i).value());
		}
	}

	public void deleteSuspendedVersioningObj(S3Client client, String bucketName, String key, List<String> versionIds,
			List<String> contents) {
		client.deleteObject(b -> b.bucket(bucketName).key(key));

		assertEquals(versionIds.size(), contents.size());

		for (int i = versionIds.size() - 1; i >= 0; i--) {
			if (versionIds.get(i).equals("null")) {
				versionIds.remove(i);
				contents.remove(i);
			}
		}
	}

	public void overwriteSuspendedVersioningObj(S3Client client, String bucketName, String key, List<String> versionIds,
			List<String> contents, String content) {
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		assertEquals(versionIds.size(), contents.size());

		for (int i = versionIds.size() - 1; i >= 0; i--) {
			if (versionIds.get(i).equals("null")) {
				versionIds.remove(i);
				contents.remove(i);
			}
		}
		contents.add(content);
		versionIds.add("null");
	}

	public void removeObjVersion(S3Client client, String bucketName, String key, List<String> versionIds,
			List<String> contents, int index) {
		assertEquals(versionIds.size(), contents.size());
		var rmVersionID = versionIds.get(index);
		versionIds.remove(index);
		var rmContent = contents.get(index);
		contents.remove(index);

		checkObjContent(client, bucketName, key, rmVersionID, rmContent);

		client.deleteObject(b -> b.bucket(bucketName).key(key).versionId(rmVersionID));

		if (!versionIds.isEmpty())
			checkObjVersions(client, bucketName, key, versionIds, contents);
	}

	public void createMultipleVersions(S3Client client, String bucketName, String key, int numVersions,
			List<String> versionIds, List<String> contents, boolean checkVersion) {

		for (int i = 0; i < numVersions; i++) {
			var body = String.format("content-%s", i);
			var response = client.putObject(b -> b.bucket(bucketName).key(key), RequestBody.fromString(body));
			var versionId = response.versionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		if (checkVersion)
			checkObjVersions(client, bucketName, key, versionIds, contents);

	}

	public void doTestCreateRemoveVersions(S3Client client, String bucketName, String key, int numVersions,
			int removeStartIdx, int idxInc) {
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();
		createMultipleVersions(client, bucketName, key, numVersions, versionIds, contents, true);
		var idx = removeStartIdx;

		for (int i = 0; i < numVersions; i++) {
			removeObjVersion(client, bucketName, key, versionIds, contents, idx);
			idx += idxInc;
		}
	}

	public List<Thread> doCreateVersionedObjConcurrent(S3Client client, String bucketName, String key, int count) {
		var threads = new ArrayList<Thread>();
		for (int i = 0; i < count; i++) {
			var item = Integer.toString(i);
			var mThread = new Thread(() -> client.putObject(p -> p.bucket(bucketName).key(key + item),
					RequestBody.fromString(String.format("data %s", item))));
			mThread.start();
			threads.add(mThread);
		}
		return threads;
	}

	public List<Thread> doClearVersionedBucketConcurrent(S3Client client, String bucketName) {
		var threads = new ArrayList<Thread>();
		var response = client.listObjectVersions(l -> l.bucket(bucketName));

		for (var Version : response.versions()) {
			var mThread = new Thread(() -> client.deleteObject(
					d -> d.bucket(bucketName).key(Version.key()).versionId(Version.versionId())));
			mThread.start();
			threads.add(mThread);
		}
		return threads;
	}

	public void corsRequestAndCheck(String method, String bucketName, Map<String, String> headers, int statusCode,
			String expectAllowOrigin, String expectAllowMethods, String key) {

		try {
			var url = getURL(bucketName);
			if (key != null)
				url = getURL(bucketName, key);

			System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			for (Map.Entry<String, String> entry : headers.entrySet()) {
				connection.setRequestProperty(entry.getKey(), entry.getValue());
			}
			connection.setRequestMethod(method);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setConnectTimeout(15000);

			assertEquals(statusCode, connection.getResponseCode());
			assertEquals(expectAllowOrigin, connection.getHeaderField("Access-Control-Allow-Origin"));
			assertEquals(expectAllowMethods, connection.getHeaderField("Access-Control-Allow-Methods"));

			connection.disconnect();
		} catch (IOException e) {
			fail(e);
		}
	}

	public void partsETagCompare(List<CompletedPart> expected, List<Part> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).partNumber(), actual.get(i).partNumber());
			assertEquals(expected.get(i).eTag(), actual.get(i).eTag());
		}
	}

	public void versionIDsCompare(List<ObjectVersion> expected, List<ObjectVersion> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).versionId(), actual.get(i).versionId());
			assertEquals(expected.get(i).eTag(), actual.get(i).eTag());
			assertEquals(expected.get(i).key(), actual.get(i).key());
			assertEquals(expected.get(i).size(), actual.get(i).size());
		}
	}

	public void s3eventCompare(List<Event> expected, List<Event> actual) {
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i), actual.get(i));
		}
	}

	// endregion

	// region Bucket Clear
	public void deleteBucketList(String bucketName) {
		buckets.remove(bucketName);
	}

	@AfterEach
	public void clear() {
		if (!config.notDelete)
			bucketClear();
	}

	public void bucketClear(S3Client client, String bucketName) {
		if (client == null)
			return;
		if (StringUtils.isBlank(bucketName))
			return;

		try {
			var isTruncated = true;
			while (isTruncated) {
				var response = client.listObjectVersions(l -> l.bucket(bucketName));
				var objects = response.versions();

				for (var version : objects)
					client.deleteObject(d -> d.bucket(bucketName).key(version.key()).versionId(version.versionId()));
				isTruncated = response.isTruncated();
			}

		} catch (AwsServiceException e) {
			System.out.printf("Error : Bucket(%s) Clear Failed(%s, %d)%n", bucketName, e.awsErrorDetails().errorCode(),
					e.statusCode());
		}

		try {
			client.deleteBucket(d -> d.bucket(bucketName));
		} catch (AwsServiceException e) {
			System.out.printf("Error : Bucket(%s) Delete Failed(%s, %d)%n", bucketName, e.awsErrorDetails().errorCode(),
					e.statusCode());
		}
	}

	public void bucketClear() {
		var client = getClient();
		if (client == null)
			return;

		var iter = buckets.iterator();
		while (iter.hasNext()) {
			String bucketName = iter.next();
			if (StringUtils.isNotBlank(bucketName))
				bucketClear(client, bucketName);
			iter.remove();
		}

	}
	// endregion

	// region Utils
	public void delay(int milliseconds) {
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
			fail(e);
			Thread.currentThread().interrupt();
		}
	}

	public Date getExpiredDate(int days) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, days);
		return cal.getTime();
	}

	public Date getExpiredDate(Instant day, int days) {
		Calendar cal = Calendar.getInstance();

		cal.setTime(Date.from(day));
		cal.add(Calendar.DATE, days);
		return cal.getTime();
	}

	// endregion
}
