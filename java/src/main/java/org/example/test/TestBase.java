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
import java.nio.charset.StandardCharsets;
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

import org.apache.hc.core5.http.HttpStatus;
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
import org.junit.jupiter.api.TestInfo;
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
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.GroupGrantee;
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
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
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
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.TagSet;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class TestBase {

	/************************************************************************************************************/

	public enum EncryptionType {
		NORMAL, SSE_S3, SSE_C
	}

	/************************************************************************************************************/
	static final int DEFAULT_PART_SIZE = 5 * MainData.MB;
	static final int RANDOM_PREFIX_TEXT_LENGTH = 15;
	static final int RANDOM_SUFFIX_TEXT_LENGTH = 5;
	static final int BUCKET_MAX_LENGTH = 63;
	static final int MAX_LENGTH = 500;
	static final Random rand = new Random();
	// cSpell:disable
	static final String SSE_KEY = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
	static final String SSE_KEY_MD5 = "DWygnHRtgiJ77HCm+1rvHw==";
	// cSpell:enable
	/************************************************************************************************************/

	final ArrayList<String> buckets = new ArrayList<>();
	protected final S3Config config;

	protected TestBase() {
		String fileName = System.getProperty("s3tests.ini", "default.ini");
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
		s3Config.setConnectionTimeout(60000);
		s3Config.setSocketTimeout(60000);
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

	public AmazonS3 getOldClient() {
		var address = NetUtils.createURLToHTTP(config.url, config.oldPort);
		var credential = new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.mainUser.accessKey,
				config.mainUser.secretKey));
		var s3Config = new ClientConfiguration()
				.withProtocol(Protocol.HTTP)
				.withMaxErrorRetry(1)
				.withConnectionTimeout(60000)
				.withSocketTimeout(60000);

		return AmazonS3ClientBuilder.standard()
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(address, ""))
				.withCredentials(credential)
				.withClientConfiguration(s3Config)
				.withChunkedEncodingDisabled(true)
				.withPayloadSigningEnabled(false)
				.withPathStyleAccessEnabled(true)
				.build();
	}

	public AmazonS3 getClient() {
		return createClient(config.isSecure, config.mainUser, true, true, config.getSignatureVersion());
	}

	public AmazonS3 getClientV2() {
		return createClient(config.isSecure, config.mainUser, true, true, S3Config.STR_SIGNATURE_VERSION_V2);
	}

	public AmazonS3 getClientV4(Boolean useChunkEncoding, Boolean payloadSigning) {
		return createClient(config.isSecure, config.mainUser, useChunkEncoding, payloadSigning,
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

	public static InputStream createBody(String body) {
		return new ByteArrayInputStream(body.getBytes());
	}

	public static String getNewBucketName(String prefix) {
		String bucketName = prefix + Utils.randomText(BUCKET_MAX_LENGTH);
		return bucketName.substring(0, BUCKET_MAX_LENGTH - 1);
	}

	public String getNewBucketName() {
		var bucketName = getNewBucketName(getPrefix());
		buckets.add(bucketName);
		return bucketName;
	}

	public String getPrefix() {
		return "v1-" + config.bucketPrefix;
	}

	public String getNewBucketNameOnly() {
		return getNewBucketName(getPrefix());
	}

	public String getNewBucketNameOnly(int length) {
		var bucketName = getNewBucketName(getPrefix());
		if (bucketName.length() > length)
			bucketName = bucketName.substring(0, length);
		else if (bucketName.length() < length)
			bucketName = bucketName + Utils.randomText(length - bucketName.length());
		return bucketName;
	}

	public String createBucket() {
		var client = getClient();
		return createBucket(client);
	}

	public String createBucket(AmazonS3 client) {
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);
		if (config.isOldSystem()) {
			var oldClient = getOldClient();
			oldClient.createBucket(bucketName);
		}
		return bucketName;
	}

	public String createBucket(AmazonS3 client, ObjectOwnership ownership) {
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectOwnership(ownership));
		return bucketName;
	}

	public String createBucket(AmazonS3 client, ObjectOwnership ownership, CannedAccessControlList acl) {
		var bucketName = createBucket(client, ownership);
		client.setPublicAccessBlock(new SetPublicAccessBlockRequest().withBucketName(bucketName)
				.withPublicAccessBlockConfiguration(new PublicAccessBlockConfiguration().withBlockPublicAcls(false)
						.withIgnorePublicAcls(false).withBlockPublicPolicy(false).withRestrictPublicBuckets(false)));
		if (acl != null)
			client.setBucketAcl(bucketName, acl);
		return bucketName;
	}

	public String createBucketCannedAcl(AmazonS3 client) {
		return createBucket(client, ObjectOwnership.ObjectWriter, null);
	}

	public static void createObjects(AmazonS3 client, String bucketName, List<String> keys) {
		if (keys != null) {
			for (var key : keys) {
				var body = key.endsWith("/") ? "" : key;
				client.putObject(bucketName, key, body);
			}
		}
	}

	public String createObjects(AmazonS3 client, String... keys) {
		var bucketName = createBucket(client);
		createObjects(client, bucketName, List.of(keys));
		return bucketName;
	}

	public String createObjects(AmazonS3 client, List<String> keys) {
		var bucketName = createBucket(client);
		createObjects(client, bucketName, keys);
		return bucketName;
	}

	public String createObjects(List<String> keys) {
		return createObjects(getClient(), keys);
	}

	public static AccessControlList createAcl(Owner owner, Grantee grantee, Permission... permissions) {
		var acl = new AccessControlList().withOwner(owner);
		acl.grantPermission(new CanonicalGrantee(owner.getId()), Permission.FullControl);
		if (permissions != null)
			for (var permission : permissions)
				acl.grantPermission(grantee, permission);
		return acl;
	}

	public AccessControlList createPublicAcl(Permission... permissions) {
		return createAcl(config.mainUser.toOwner(), GroupGrantee.AllUsers, permissions);
	}

	public AccessControlList createAuthenticatedAcl(Permission... permissions) {
		return createAcl(config.mainUser.toOwner(), GroupGrantee.AuthenticatedUsers, permissions);
	}

	public AccessControlList createAltAcl(Permission... permissions) {
		return createAcl(config.mainUser.toOwner(), config.altUser.toGrantee(), permissions);
	}

	public String createKeyWithRandomContent(AmazonS3 client, String key, int size) {
		var bucketName = createBucket(client);
		if (client == null)
			client = getClient();
		if (size < 1)
			size = 7 * MainData.MB;

		var data = Utils.randomTextToLong(size);
		client.putObject(bucketName, key, data);

		return bucketName;
	}

	public void createKeyWithRandomContent(AmazonS3 client, String key, int size, String bucketName) {
		if (client == null)
			client = getClient();
		if (size < 1)
			size = 7 * MainData.MB;

		var data = Utils.randomTextToLong(size);
		client.putObject(bucketName, key, data);
	}

	public URL createURL(String bucketName) throws MalformedURLException {
		var protocol = config.isSecure ? MainData.HTTPS : MainData.HTTP;
		var port = config.isSecure ? config.sslPort : config.port;

		return config.isAWS() ? NetUtils.getEndpoint(protocol, config.regionName, bucketName)
				: NetUtils.getEndpoint(protocol, config.url, port, bucketName);
	}

	public URL createURL(String bucketName, String key) throws MalformedURLException {
		var protocol = config.isSecure ? MainData.HTTPS : MainData.HTTP;
		var port = config.isSecure ? config.sslPort : config.port;

		return config.isAWS() ? NetUtils.getEndpoint(protocol, config.regionName, bucketName, key)
				: NetUtils.getEndpoint(protocol, config.url, port, bucketName, key);
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
			effect = MainData.POLICY_EFFECT_ALLOW;

		var statement = new JsonObject();
		statement.addProperty(MainData.POLICY_EFFECT, effect);
		statement.add(MainData.POLICY_PRINCIPAL, principal);
		statement.addProperty(MainData.POLICY_ACTION, action);
		statement.addProperty(MainData.POLICY_RESOURCE, resource);

		if (conditions != null)
			statement.add(MainData.POLICY_CONDITION, conditions);

		return statement;
	}

	public JsonObject makeJsonPolicy(String action, String resource, JsonObject principal, JsonObject conditions) {
		var policy = new JsonObject();

		policy.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statement = new JsonArray();
		statement.add(makeJsonStatement(action, resource, null, principal, conditions));
		policy.add(MainData.POLICY_STATEMENT, statement);

		return policy;
	}

	public JsonObject makeJsonPolicy(JsonObject... statementList) {
		var policy = new JsonObject();

		policy.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);
		var statements = new JsonArray();
		for (var statement : statementList)
			statements.add(statement);
		policy.add(MainData.POLICY_STATEMENT, statements);
		return policy;
	}

	public List<Tag> makeSimpleTagSet(int count) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(new Tag(Integer.toString(i), Integer.toString(i)));
		return tagSets;
	}

	public List<Tag> makeDetailTagSet(int count, int keySize, int valueSize) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(new Tag(Utils.randomTextToLong(keySize), Utils.randomTextToLong(valueSize)));
		return tagSets;
	}

	public String setupAclBucket(ObjectOwnership ownership, CannedAccessControlList acl, List<String> keys) {
		var client = getClient();
		var bucketName = createBucket(client, ownership, acl);
		createObjects(client, bucketName, keys);
		return bucketName;
	}

	public String setupAclBucket(CannedAccessControlList acl, List<String> keys) {
		return setupAclBucket(ObjectOwnership.ObjectWriter, acl, keys);
	}

	public String setupAclObjects(ObjectOwnership ownership, CannedAccessControlList bucketAcl,
			CannedAccessControlList objectAcl, String... keys) {
		var client = getClient();
		var bucketName = createBucket(client, ownership, bucketAcl);
		for (var key : keys)
			client.putObject(
					new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
							.withCannedAcl(objectAcl));

		return bucketName;
	}

	public String setupAclObjects(CannedAccessControlList bucketAcl, CannedAccessControlList objectAcl,
			String... keys) {
		return setupAclObjects(ObjectOwnership.ObjectWriter, bucketAcl, objectAcl, keys);
	}

	public String setupAclObjectsByAlt(ObjectOwnership ownership, CannedAccessControlList bucketAcl,
			CannedAccessControlList objectAcl, String... keys) {
		var altClient = getAltClient();
		var bucketName = createBucket(getClient(), ownership, bucketAcl);
		for (var key : keys)
			altClient.putObject(
					new PutObjectRequest(bucketName, key, createBody(key), new ObjectMetadata())
							.withCannedAcl(objectAcl));

		return bucketName;
	}

	public String setupAclObjectsByAlt(CannedAccessControlList bucketAcl, CannedAccessControlList objectAcl,
			String... keys) {
		return setupAclObjectsByAlt(ObjectOwnership.ObjectWriter, bucketAcl, objectAcl, keys);
	}

	public BucketLifecycleConfiguration setupLifecycleExpiration(AmazonS3 client, String bucketName, String ruleId,
			int deltaDays, String rulePrefix) {
		var rules = new ArrayList<Rule>();
		rules.add(new Rule().withId(ruleId).withExpirationInDays(deltaDays)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(rulePrefix)))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var myLifeCycle = new BucketLifecycleConfiguration(rules);
		client.setBucketLifecycleConfiguration(bucketName, myLifeCycle);

		var key = rulePrefix + "/foo";
		var body = "bar";
		client.putObject(bucketName, key, body);

		return client.getBucketLifecycleConfiguration(bucketName);
	}

	public String setupMetadata(String meta, String bucketName) {
		if (StringUtils.isBlank(bucketName))
			bucketName = createBucket();

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

	public String setupBucketPermission(Permission permission) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		var acl = createAltAcl(permission);
		client.setBucketAcl(bucketName, acl);

		return bucketName;
	}

	public String setupObjectPermission(String key, Permission permission) {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.ObjectWriter, CannedAccessControlList.PublicReadWrite);

		client.putObject(bucketName, key, key);

		var acl = createAltAcl(permission);
		client.setObjectAcl(bucketName, key, acl);

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

	public AccessControlList addBucketUserGrant(String bucketName, Grant grant) {
		var client = getClient();

		var response = client.getBucketAcl(bucketName);
		var grants = response.getGrantsAsList();
		var acl = new AccessControlList();

		acl.setOwner(response.getOwner());
		grants.add(grant);

		for (var Item : grants)
			acl.grantAllPermissions(Item);

		return acl;
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
		return Collections.emptyList();
	}

	public static String getBody(S3ObjectInputStream data) {
		String body = "";
		if (data != null) {
			try {
				ByteArrayOutputStream result = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				for (int length; (length = data.read(buffer)) != -1;) {
					result.write(buffer, 0, length);
				}
				return result.toString(StandardCharsets.UTF_8);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return body;
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

	public void checkVersioning(String bucketName, String statusCode) {
		var client = getClient();

		var response = client.getBucketVersioningConfiguration(bucketName);
		assertEquals(statusCode, response.getStatus());
	}

	public List<String> cutStringData(String data, int partSize) {
		var stringList = new ArrayList<String>();

		int startPoint = 0;
		while (startPoint < data.length()) {

			var endPoint = Math.min(startPoint + partSize, data.length());

			stringList.add(data.substring(startPoint, endPoint));
			startPoint += partSize;
		}

		return stringList;
	}

	public static List<KeyVersion> getKeyVersions(List<String> keyList) {
		var keyVersions = new ArrayList<KeyVersion>();
		for (var key : keyList)
			keyVersions.add(new KeyVersion(key, null));

		return keyVersions;
	}

	public List<S3VersionSummary> getVersions(List<S3VersionSummary> versions) {
		if (versions == null)
			return Collections.emptyList();

		var result = new ArrayList<S3VersionSummary>();
		for (var item : versions)
			if (!item.isDeleteMarker())
				result.add(item);
		return result;
	}

	public List<String> getVersionIDs(List<S3VersionSummary> versions) {
		if (versions == null)
			return Collections.emptyList();

		var result = new ArrayList<String>();
		for (var item : versions)
			if (!item.isDeleteMarker())
				result.add(item.getVersionId());
		return result;
	}

	public List<S3VersionSummary> getDeleteMarkers(List<S3VersionSummary> versions) {
		if (versions == null)
			return Collections.emptyList();

		var deleteMarkers = new ArrayList<S3VersionSummary>();

		for (var item : versions)
			if (item.isDeleteMarker())
				deleteMarkers.add(item);
		return deleteMarkers;
	}
	// endregion

	// region Check Data
	public static void checkGetObject(AmazonS3 client, String bucketName, String key, boolean pass) {
		if (pass) {
			var response = client.getObject(bucketName, key);
			assertEquals(key, response.getKey());
		} else {
			var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
			assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
			assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
		}
	}

	public static void succeedGetObject(AmazonS3 client, String bucketName, String key, String content) {
		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertEquals(content, body);
	}

	public static void failedGetObject(AmazonS3 client, String bucketName, String key, int statusCode,
			String errorCode) {
		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		assertEquals(statusCode, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());
	}

	public static void succeedPutObject(AmazonS3 client, String bucketName, String key, String content) {
		client.putObject(bucketName, key, content);
	}

	public static void failedPutObject(AmazonS3 client, String bucketName, String key, int statusCode,
			String errorCode) {
		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, key));

		assertEquals(statusCode, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());
	}

	public static void succeedListObjects(AmazonS3 client, String bucketName, List<String> keys) {
		var response = client.listObjects(bucketName);
		var keyList = getKeys(response.getObjectSummaries());

		assertEquals(keys, keyList);
	}

	public static void failedListObjects(AmazonS3 client, String bucketName, int statusCode, String errorCode) {
		var e = assertThrows(AmazonServiceException.class, () -> client.listObjects(bucketName));

		assertEquals(statusCode, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());
	}

	public String validateListObject(String bucketName, String prefix, String delimiter, String marker, int maxKeys,
			boolean isTruncated, List<String> checkKeys, List<String> checkPrefixes, String nextMarker) {
		var client = getClient();
		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter)
				.withMarker(marker).withMaxKeys(maxKeys).withPrefix(prefix));

		assertEquals(isTruncated, response.isTruncated());
		assertEquals(nextMarker, response.getNextMarker());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(checkKeys.size(), keys.size());
		assertEquals(checkPrefixes.size(), prefixes.size());
		assertEquals(checkKeys, keys);
		assertEquals(checkPrefixes, prefixes);

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
		assertThrows(IllegalBucketNameException.class, () -> getClient().createBucket(bucketName));
	}

	public void checkGoodBucketName(String bucketName) {
		var client = getClient();
		buckets.add(bucketName);
		client.createBucket(bucketName);
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
		var versions = getVersions(response.getVersionSummaries());

		Collections.reverse(versions);

		var index = 0;
		for (var version : versions) {
			assertEquals(version.getVersionId(), versionIds.get(index));
			if (StringUtils.isNotBlank(key))
				assertEquals(key, version.getKey());
			checkObjContent(client, bucketName, key, version.getVersionId(), contents.get(index++));
		}
	}

	public void testBucketCreateNamingGoodLong(int length) {
		var bucketName = getNewBucketNameOnly(length);
		buckets.add(bucketName);
		getClient().createBucket(bucketName);
	}

	public void testBucketCreateNamingBadLong(int length) {
		var bucketName = getNewBucketNameOnly(length);
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
			var partResponse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(uploadData.uploadId)
							.withPartNumber(uploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			uploadData.addPart(partResponse.getPartETag());
		}

		return uploadData;
	}

	public MultipartUploadData setupMultipartUpload(AmazonS3 client, String bucketName, String key, int size) {
		var uploadData = new MultipartUploadData();

		var initMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		uploadData.uploadId = initMultiPartResponse.getUploadId();

		var parts = Utils.generateRandomString(size, DEFAULT_PART_SIZE);

		for (var Part : parts) {
			uploadData.appendBody(Part);

			var partResponse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(uploadData.uploadId)
							.withPartNumber(uploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			uploadData.parts.add(partResponse.getPartETag());
		}

		return uploadData;
	}

	public MultipartUploadData setupMultipartUpload(AmazonS3 client, String bucketName, String key, int size,
			int partSize) {
		var uploadData = new MultipartUploadData();

		var initMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		uploadData.uploadId = initMultiPartResponse.getUploadId();

		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);

			var partResponse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(uploadData.uploadId)
							.withPartNumber(uploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			uploadData.parts.add(partResponse.getPartETag());
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

		var initMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, metadataList));
		uploadData.uploadId = initMultiPartResponse.getUploadId();

		var parts = Utils.generateRandomString(size, DEFAULT_PART_SIZE);

		for (var Part : parts) {
			uploadData.appendBody(Part);

			var partResponse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(uploadData.uploadId)
							.withPartNumber(uploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			uploadData.parts.add(partResponse.getPartETag());
		}

		return uploadData;
	}

	public MultipartUploadData multipartUploadResend(AmazonS3 client, String bucketName, String key, int size,
			ObjectMetadata metadataList, List<Integer> resendParts) {
		var uploadData = new MultipartUploadData();
		if (metadataList == null) {
			metadataList = new ObjectMetadata();
			metadataList.setContentType("text/plain");
		}
		metadataList.setContentLength(size);

		var initMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, metadataList));
		uploadData.uploadId = initMultiPartResponse.getUploadId();

		var parts = Utils.generateRandomString(size, DEFAULT_PART_SIZE);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			int partNumber = uploadData.nextPartNumber();

			var partResponse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(uploadData.uploadId)
							.withPartNumber(partNumber)
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			uploadData.parts.add(partResponse.getPartETag());

			if (resendParts != null && resendParts.contains(partNumber))
				client.uploadPart(new UploadPartRequest()
						.withBucketName(bucketName)
						.withKey(key)
						.withUploadId(uploadData.uploadId)
						.withPartNumber(partNumber)
						.withInputStream(createBody(Part))
						.withPartSize(Part.length()));
		}

		return uploadData;
	}

	public MultipartUploadData setupSseCMultipartUpload(AmazonS3 client, String bucketName, String key, int size,
			ObjectMetadata metadataList) {
		var uploadData = new MultipartUploadData();

		var sse = new SSECustomerKey(SSE_KEY)
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5(SSE_KEY_MD5);

		var initMultiPartResponse = client.initiateMultipartUpload(
				new InitiateMultipartUploadRequest(bucketName, key, metadataList).withSSECustomerKey(sse));
		uploadData.uploadId = initMultiPartResponse.getUploadId();

		var parts = Utils.generateRandomString(size, DEFAULT_PART_SIZE);

		for (var Part : parts) {
			uploadData.appendBody(Part);

			var response = client.uploadPart(new UploadPartRequest()
					.withBucketName(bucketName)
					.withKey(key)
					.withUploadId(uploadData.uploadId)
					.withPartNumber(uploadData.nextPartNumber())
					.withPartSize(Part.length())
					.withSSECustomerKey(sse)
					.withInputStream(createBody(Part)));
			uploadData.parts.add(response.getPartETag());
		}
		return uploadData;
	}

	public void checkCopyContent(String sourceBucketName, String sourceKey, String targetBucketName, String targetKey) {
		var client = getClient();

		var sourceResponse = client.getObject(sourceBucketName, sourceKey);
		var sourceSize = sourceResponse.getObjectMetadata().getContentLength();
		var sourceData = getBody(sourceResponse.getObjectContent());

		var targetResponse = client.getObject(targetBucketName, targetKey);
		var targetSize = targetResponse.getObjectMetadata().getContentLength();
		var targetData = getBody(targetResponse.getObjectContent());
		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContent(String sourceBucketName, String sourceKey, String targetBucketName, String targetKey,
			String versionId) {
		var client = getClient();

		var sourceResponse = client
				.getObject(new GetObjectRequest(sourceBucketName, sourceKey).withVersionId(versionId));
		var sourceSize = sourceResponse.getObjectMetadata().getContentLength();
		var sourceData = getBody(sourceResponse.getObjectContent());

		var targetResponse = client.getObject(targetBucketName, targetKey);
		var targetSize = targetResponse.getObjectMetadata().getContentLength();
		var targetData = getBody(targetResponse.getObjectContent());
		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentSseC(AmazonS3 client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, SSECustomerKey sseC) {

		var sourceResponse = client
				.getObject(new GetObjectRequest(sourceBucketName, sourceKey).withSSECustomerKey(sseC));
		var sourceSize = sourceResponse.getObjectMetadata().getContentLength();
		var sourceData = getBody(sourceResponse.getObjectContent());

		var targetResponse = client
				.getObject(new GetObjectRequest(targetBucketName, targetKey).withSSECustomerKey(sseC));
		var targetSize = targetResponse.getObjectMetadata().getContentLength();
		var targetData = getBody(targetResponse.getObjectContent());

		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentUsingRange(String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey) {
		var client = getClient();

		var targetResponse = client.getObject(new GetObjectRequest(targetBucketName, targetKey));
		var targetSize = targetResponse.getObjectMetadata().getContentLength();
		var targetData = getBody(targetResponse.getObjectContent());

		var sourceResponse = client
				.getObject(new GetObjectRequest(sourceBucketName, sourceKey).withRange(0, targetSize - 1));
		var sourceSize = sourceResponse.getObjectMetadata().getContentLength();
		var sourceData = getBody(sourceResponse.getObjectContent());
		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentUsingRange(String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey, int step) {
		var client = getClient();

		var response = client.getObjectMetadata(sourceBucketName, sourceKey);
		var size = response.getContentLength();

		long start = 0;
		while (start < size) {
			var end = Math.min(start + step, size) - 1;

			var sourceResponse = client.getObject(
					new GetObjectRequest(sourceBucketName, sourceKey).withRange(start, end));
			var sourceBody = getBody(sourceResponse.getObjectContent());
			var targetResponse = client.getObject(
					new GetObjectRequest(targetBucketName, targetKey).withRange(start, end));
			var targetBody = getBody(targetResponse.getObjectContent());

			assertEquals(sourceResponse.getObjectMetadata().getContentLength(),
					targetResponse.getObjectMetadata().getContentLength());
			assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
			start += step;
		}
	}

	public void checkUploadMultipartResend(String bucketName, String key, int size, List<Integer> resendParts) {
		var contentType = "text/bla";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		var client = getClient();
		var uploadData = multipartUploadResend(client, bucketName, key, size, metadata, resendParts);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(contentType, response.getContentType());
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());

		var body = uploadData.getBody();
		checkContentUsingRange(bucketName, key, body, MainData.MB);
		checkContentUsingRange(bucketName, key, body, 10L * MainData.MB);
	}

	public String doTestMultipartUploadContents(String bucketName, String key, int partCount) {
		var payload = Utils.randomTextToLong(5 * MainData.MB);
		var client = getClient();

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();
		var parts = new ArrayList<PartETag>();
		var allPayload = new StringBuilder();

		for (int i = 0; i < partCount; i++) {
			var partNumber = i + 1;
			var partResponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
					.withUploadId(uploadId).withPartNumber(partNumber)
					.withInputStream(new ByteArrayInputStream(payload.getBytes())).withPartSize(payload.length()));
			parts.add(new PartETag(partNumber, partResponse.getETag()));
			allPayload.append(payload);
		}
		var lestPayload = Utils.randomTextToLong(MainData.MB);
		var lestPartResponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
				.withUploadId(uploadId).withPartNumber(partCount + 1)
				.withInputStream(new ByteArrayInputStream(lestPayload.getBytes())).withPartSize(lestPayload.length()));
		parts.add(new PartETag(partCount + 1, lestPartResponse.getETag()));
		allPayload.append(lestPayload);

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, parts));

		var response = client.getObject(bucketName, key);
		var text = getBody(response.getObjectContent());
		var result = allPayload.toString();

		assertTrue(result.equals(text), MainData.NOT_MATCHED);

		return result;
	}

	public MultipartUploadData multipartCopy(String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey, int size, AmazonS3 client, int partSize, String versionId) {
		var data = new MultipartUploadData();
		if (client == null)
			client = getClient();
		if (partSize < 1)
			partSize = 5 * MainData.MB;

		var response = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetBucketName, targetKey));
		data.uploadId = response.getUploadId();

		int uploadCount = 1;
		long start = 0;
		while (start < size) {
			long end = Math.min(start + partSize - 1, size - 1L);

			var partResponse = client.copyPart(new CopyPartRequest().withSourceBucketName(sourceBucketName)
					.withSourceKey(sourceKey).withDestinationBucketName(targetBucketName).withDestinationKey(targetKey)
					.withUploadId(data.uploadId).withPartNumber(uploadCount).withFirstByte(start).withLastByte(end)
					.withSourceVersionId(versionId));
			data.parts.add(new PartETag(uploadCount++, partResponse.getETag()));

			start = end + 1;
		}

		return data;
	}

	public MultipartUploadData multipartCopy(AmazonS3 client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, int size, ObjectMetadata metadata) {
		var data = new MultipartUploadData();
		var partSize = 5 * MainData.MB;
		if (metadata == null)
			metadata = new ObjectMetadata();

		var response = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(targetBucketName, targetKey, metadata));
		data.uploadId = response.getUploadId();

		int uploadCount = 1;
		long start = 0;
		while (start < size) {
			long end = Math.min(start + partSize - 1, size - 1L);

			var partResponse = client.copyPart(new CopyPartRequest().withSourceBucketName(sourceBucketName)
					.withSourceKey(sourceKey).withDestinationBucketName(targetBucketName).withDestinationKey(targetKey)
					.withUploadId(data.uploadId).withPartNumber(uploadCount).withFirstByte(start).withLastByte(end));
			data.parts.add(new PartETag(uploadCount++, partResponse.getETag()));

			start = end + 1;
		}

		return data;
	}

	public MultipartUploadData multipartCopySseC(AmazonS3 client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, int size, ObjectMetadata metadata, SSECustomerKey sseC) {
		var data = new MultipartUploadData();
		var partSize = 5 * MainData.MB;
		if (metadata == null)
			metadata = new ObjectMetadata();

		var response = client.initiateMultipartUpload(
				new InitiateMultipartUploadRequest(targetBucketName, targetKey, metadata).withSSECustomerKey(sseC));
		data.uploadId = response.getUploadId();

		int uploadCount = 1;
		long start = 0;
		while (start < size) {
			long end = Math.min(start + partSize - 1, size - 1L);

			var partResponse = client.copyPart(new CopyPartRequest()
					.withSourceBucketName(sourceBucketName)
					.withSourceKey(sourceKey)
					.withDestinationBucketName(targetBucketName)
					.withDestinationKey(targetKey)
					.withSourceSSECustomerKey(sseC)
					.withDestinationSSECustomerKey(sseC)
					.withUploadId(data.uploadId)
					.withPartNumber(uploadCount)
					.withFirstByte(start)
					.withLastByte(end));
			data.parts.add(new PartETag(uploadCount++, partResponse.getETag()));

			start = end + 1;
		}

		return data;
	}

	public static long getBytesUsed(ListObjectsV2Result response) {
		if (response == null)
			return 0;
		if (response.getObjectSummaries() == null)
			return 0;
		if (response.getObjectSummaries().size() > 1)
			return 0;

		long size = 0;

		for (var Obj : response.getObjectSummaries())
			size += Obj.getSize();

		return size;
	}

	public void checkContentUsingRange(String bucketName, String key, String data, long step) {
		var client = getClient();
		var headResponse = client.getObjectMetadata(bucketName, key);
		var size = headResponse.getContentLength();
		assertEquals(data.length(), size, bucketName + "/" + key + " : " + data.length() + " != " + size);

		long start = 0;
		while (start < size) {
			var end = Math.min(start + step, size - 1);

			var response = client
					.getObject(new GetObjectRequest(bucketName, key).withRange(start, end - 1));
			var body = getBody(response.getObjectContent());
			var length = end - start;
			var rangeBody = data.substring((int) start, (int) end);

			assertEquals(length, response.getObjectMetadata().getContentLength(),
					bucketName + "/" + key + " : " + length + " != " + response.getObjectMetadata().getContentLength());
			assertTrue(rangeBody.equals(body), MainData.NOT_MATCHED);
			start += step;
		}
	}

	public void checkContentUsingRangeEnc(AmazonS3 client, String bucketName, String key, String data, long step,
			SSECustomerKey sse) {
		if (client == null)
			client = getClient();
		var getResponse = client
				.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key).withSSECustomerKey(sse));
		var size = getResponse.getContentLength();

		long start = 0;
		while (start < size) {
			var end = Math.min(start + step, size - 1);

			var response = client.getObject(new GetObjectRequest(bucketName, key)
					.withRange(start, end - 1).withSSECustomerKey(sse));
			var body = getBody(response.getObjectContent());
			var length = end - start;
			var partBody = data.substring((int) start, (int) end);

			assertEquals(length, response.getObjectMetadata().getContentLength());
			assertTrue(partBody.equals(body), MainData.NOT_MATCHED);
			start += step;
		}
	}

	public RangeSet getRandomRange(int fileSize) {
		var start = rand.nextInt(fileSize - MAX_LENGTH * 2);
		var maxLength = fileSize - start;

		if (maxLength > MAX_LENGTH)
			maxLength = MAX_LENGTH;
		var length = rand.nextInt(maxLength) + MAX_LENGTH - 1;
		if (length <= 0)
			length = 1;

		return new RangeSet(start, length);

	}

	public void checkContent(String bucketName, String key, String data, int loopCount) {
		var client = getClient();

		for (int i = 0; i < loopCount; i++) {
			var response = client.getObject(bucketName, key);
			var body = getBody(response.getObjectContent());
			assertTrue(data.equals(body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentEnc(String bucketName, String key, String data, int loopCount, SSECustomerKey sse) {
		var client = getClientHttps();

		for (int i = 0; i < loopCount; i++) {
			var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sse));
			var body = getBody(response.getObjectContent());
			assertTrue(data.equals(body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRange(String bucketName, String key, String data, int loopCount) {
		var client = getClient();
		int fileSize = data.length();

		for (int i = 0; i < loopCount; i++) {
			var range = getRandomRange(fileSize);
			var rangeBody = data.substring(range.start, range.end + 1);

			var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(range.start, range.end));
			var body = getBody(response.getObjectContent());

			assertEquals(range.length, response.getObjectMetadata().getContentLength() - 1);
			assertTrue(rangeBody.equals(body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRangeEnc(AmazonS3 client, String bucketName, String key, String data,
			int fileSize, int loopCount, SSECustomerKey sse) {
		for (int i = 0; i < loopCount; i++) {
			var range = getRandomRange(fileSize);

			var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(range.start, range.end - 1L)
					.withSSECustomerKey(sse));
			var body = getBody(response.getObjectContent());
			var rangeBody = data.substring(range.start, range.end);

			assertEquals(range.length, response.getObjectMetadata().getContentLength());
			assertTrue(rangeBody.equals(body), MainData.NOT_MATCHED);
		}
	}

	public AmazonServiceException setGetMetadataUnreadable(String metadata, String bucket) {
		if (StringUtils.isBlank(bucket))
			bucket = createBucket();
		var bucketName = bucket;
		var client = getClient();
		var key = "foo";
		var metadataKey = "x-amz-meta-meta1";
		var metadataList = new ObjectMetadata();
		metadataList.addUserMetadata(metadataKey, metadata);

		return assertThrows(AmazonServiceException.class,
				() -> client.putObject(bucketName, key, createBody("bar"), metadataList));
	}

	public boolean errorCheck(Integer statusCode) {
		return statusCode.equals(HttpStatus.SC_BAD_REQUEST) || statusCode.equals(HttpStatus.SC_FORBIDDEN);
	}

	public void testEncryptionCSEWrite(int fileSize) {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "test_obj";
		var aesKey = Utils.randomTextToLong(32);
		var data = Utils.randomTextToLong(fileSize);

		try {
			var encodingData = AES256.encrypt(data, aesKey);
			var metadataList = new ObjectMetadata();
			metadataList.addUserMetadata("x-amz-meta-key", aesKey);
			metadataList.setContentType("text/plain");
			metadataList.setContentLength(encodingData.length());

			client.putObject(bucketName, key, createBody(encodingData), metadataList);

			var response = client.getObject(bucketName, key);
			var encodingBody = getBody(response.getObjectContent());
			var body = AES256.decrypt(encodingBody, aesKey);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void testEncryptionSSECustomerWrite(int fileSize) {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var key = "test_obj";
		var data = Utils.randomTextToLong(fileSize);
		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(fileSize);
		var sse = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(sse));

		var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(sse));
		var body = getBody(response.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());
	}

	public void testEncryptionSseS3CustomerWrite(int fileSize) {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "test_obj";
		var data = Utils.randomTextToLong(fileSize);

		var metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType("text/plain");
		metadata.setContentLength(fileSize);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata));

		var response = client.getObject(bucketName, key);
		var body = getBody(response.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSEAlgorithm());
	}

	public void testEncryptionSseS3Copy(int fileSize) {
		var client = getClient();
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(fileSize);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName)
				.withServerSideEncryptionConfiguration(sseS3Config));

		var response = client.getBucketEncryption(bucketName);
		assertEquals(sseS3Config.getRules(), response.getServerSideEncryptionConfiguration().getRules());

		var sourceKey = "bar";
		client.putObject(bucketName, sourceKey, data);

		var sourceResponse = client.getObject(bucketName, sourceKey);
		var sourceBody = getBody(sourceResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), sourceResponse.getObjectMetadata().getSSEAlgorithm());

		var targetKey = "foo";
		client.copyObject(bucketName, sourceKey, bucketName, targetKey);
		var targetResponse = client.getObject(bucketName, targetKey);
		assertEquals(SSEAlgorithm.AES256.toString(), targetResponse.getObjectMetadata().getSSEAlgorithm());

		var targetBody = getBody(targetResponse.getObjectContent());
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(boolean sourceObjectEncryption, boolean sourceBucketEncryption,
			boolean targetBucketEncryption, boolean targetObjectEncryption, int fileSize) {
		var sourceKey = "SourceKey";
		var targetKey = "TargetKey";
		var data = Utils.randomTextToLong(fileSize);
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);

		// SSE-S3 Config
		var metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		metadata.setContentType("text/plain");
		metadata.setContentLength(fileSize);

		// Source Put Object
		if (sourceObjectEncryption || sourceBucketEncryption)
			client.putObject(new PutObjectRequest(sourceBucketName, sourceKey, createBody(data), metadata));
		else
			client.putObject(sourceBucketName, sourceKey, data);

		//// Source Object Check
		var sourceResponse = client.getObject(sourceBucketName, sourceKey);
		var sourceBody = getBody(sourceResponse.getObjectContent());
		if (sourceObjectEncryption || sourceBucketEncryption || config.isAWS())
			assertEquals(SSEAlgorithm.AES256.toString(), sourceResponse.getObjectMetadata().getSSEAlgorithm());
		else
			assertNull(sourceResponse.getObjectMetadata().getSSEAlgorithm());
		assertEquals(data, sourceBody);

		var sseS3Config = new ServerSideEncryptionConfiguration()
				.withRules(new ServerSideEncryptionRule()
						.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
								.withSSEAlgorithm(SSEAlgorithm.AES256))
						.withBucketKeyEnabled(false));

		// Source Bucket Encryption
		if (sourceBucketEncryption) {
			client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(sourceBucketName)
					.withServerSideEncryptionConfiguration(sseS3Config));

			var encryptionResponse = client.getBucketEncryption(sourceBucketName);
			assertEquals(sseS3Config.getRules(), encryptionResponse.getServerSideEncryptionConfiguration().getRules());
		}

		// Target Bucket Encryption
		if (targetBucketEncryption) {
			client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(targetBucketName)
					.withServerSideEncryptionConfiguration(sseS3Config));

			var encryptionResponse = client.getBucketEncryption(targetBucketName);
			assertEquals(sseS3Config.getRules(), encryptionResponse.getServerSideEncryptionConfiguration().getRules());
		}

		// Source Copy Object
		if (targetObjectEncryption)
			client.copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, targetBucketName, targetKey)
					.withNewObjectMetadata(metadata));
		else
			client.copyObject(sourceBucketName, sourceKey, targetBucketName, targetKey);

		// Target Object Check
		var targetResponse = client.getObject(targetBucketName, targetKey);
		var targetBody = getBody(targetResponse.getObjectContent());
		if (targetBucketEncryption || targetObjectEncryption || config.isAWS())
			assertEquals(SSEAlgorithm.AES256.toString(), targetResponse.getObjectMetadata().getSSEAlgorithm());
		else
			assertNull(targetResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(EncryptionType source, EncryptionType target, int fileSize) {
		var sourceKey = "SourceKey";
		var targetKey = "TargetKey";
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(fileSize);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(fileSize);

		// SSE-S3 Config
		var sseMetadata = new ObjectMetadata();
		sseMetadata.setContentType("text/plain");
		sseMetadata.setContentLength(fileSize);
		sseMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		// SSE-C Config
		var sseC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		// Source Put Object
		var sourcePutRequest = new PutObjectRequest(bucketName, sourceKey, createBody(data), metadata);
		var sourceGetRequest = new GetObjectRequest(bucketName, sourceKey);
		var targetGetRequest = new GetObjectRequest(bucketName, targetKey);
		var copyRequest = new CopyObjectRequest(bucketName, sourceKey, bucketName, targetKey)
				.withMetadataDirective(MetadataDirective.REPLACE);

		// Source Options
		switch (source) {
			case SSE_S3:
				sourcePutRequest.setMetadata(sseMetadata);
				break;
			case SSE_C:
				sourcePutRequest.setSSECustomerKey(sseC);
				sourceGetRequest.setSSECustomerKey(sseC);
				copyRequest.setSourceSSECustomerKey(sseC);
				break;
			case NORMAL:
				break;
		}

		// Target Options
		switch (target) {
			case SSE_S3:
				copyRequest.setNewObjectMetadata(sseMetadata);
				break;
			case SSE_C:
				copyRequest.setDestinationSSECustomerKey(sseC);
				targetGetRequest.setSSECustomerKey(sseC);
				break;
			case NORMAL:
				copyRequest.setNewObjectMetadata(metadata);
				break;
		}

		// Source Put Object
		client.putObject(sourcePutRequest);

		// Source Get Object
		var sourceResponse = client.getObject(sourceGetRequest);
		var sourceBody = getBody(sourceResponse.getObjectContent());
		assertTrue(data.equals(sourceBody), MainData.NOT_MATCHED);

		// Copy Object
		client.copyObject(copyRequest);

		// Target Object Check
		var targetResponse = client.getObject(targetGetRequest);
		var targetBody = getBody(targetResponse.getObjectContent());
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public int getPermissionPriority(Permission permission) {
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

	public int getPermissionPriority(Grant expected, Grant actual) {
		int priorityA = getPermissionPriority(expected.getPermission());
		int priorityB = getPermissionPriority(actual.getPermission());

		if (priorityA < priorityB)
			return 1;
		if (priorityA == priorityB)
			return 0;
		return -1;

	}

	public int getGranteeIdentifierPriority(Grant expected, Grant actual) {
		return expected.getGrantee().getIdentifier().compareTo(actual.getGrantee().getIdentifier());
	}

	public static List<Grant> grantsSort(List<Grant> data) {
		var newList = new ArrayList<Grant>();

		Comparator<String> comparator = (s1, s2) -> s2.compareTo(s1);
		var kk = new TreeMap<String, Grant>(comparator);

		for (var Temp : data) {
			kk.put(Temp.getGrantee().getIdentifier() + Temp.getPermission(), Temp);
		}

		for (Map.Entry<String, Grant> entry : kk.entrySet()) {
			newList.add(entry.getValue());
		}
		return newList;
	}

	public void checkAcl(AccessControlList expected, AccessControlList actual) {
		if (config.isAWS())
			assertEquals(expected.getOwner().getId(), actual.getOwner().getId(), "Owner Id is not equal");
		else
			assertEquals(expected.getOwner(), actual.getOwner(), "Owner is not equal");

		assertEquals(expected.getGrantsAsList().size(), actual.getGrantsAsList().size());
		checkGrants(expected.getGrantsAsList(), actual.getGrantsAsList());
	}

	public void checkGrants(List<Grant> expected, List<Grant> actual) {
		assertEquals(expected.size(), actual.size());

		expected = grantsSort(expected);
		actual = grantsSort(actual);

		for (int i = 0; i < expected.size(); i++) {
			var a = expected.get(i);
			var b = actual.get(i);

			assertEquals(a.getPermission(), b.getPermission());
			assertEquals(a.getGrantee().getIdentifier(), b.getGrantee().getIdentifier());
			assertEquals(a.getGrantee().getTypeIdentifier(), b.getGrantee().getTypeIdentifier());
		}
	}

	public void checkBucketAcl(Permission permission) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		var acl = createAltAcl(permission);
		client.setBucketAcl(bucketName, acl);

		var response = client.getBucketAcl(bucketName);
		checkAcl(acl, response);
	}

	public void checkObjectAcl(Permission permission) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		var key = "testObjectPermission" + permission;

		client.putObject(bucketName, key, key);

		var acl = createAltAcl(permission);
		client.setObjectAcl(bucketName, key, acl);

		var response = client.getObjectAcl(bucketName, key);
		checkAcl(acl, response);
	}

	public static void checkBucketAclAllowRead(AmazonS3 client, String bucketName) {
		client.headBucket(new HeadBucketRequest(bucketName));
	}

	public static void checkBucketAclDenyRead(AmazonS3 client, String bucketName) {
		var e = assertThrows(AmazonServiceException.class, () -> client.headBucket(new HeadBucketRequest(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	public static void checkBucketAclAllowReadACP(AmazonS3 client, String bucketName) {
		client.getBucketAcl(bucketName);
	}

	public static void checkBucketAclDenyReadACP(AmazonS3 client, String bucketName) {
		var e = assertThrows(AmazonServiceException.class, () -> client.getBucketAcl(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	public static void checkBucketAclAllowWrite(AmazonS3 client, String bucketName) {
		var key = "checkBucketAclAllowWrite";
		client.putObject(bucketName, key, key);
		client.deleteObject(bucketName, key);
	}

	public static void checkBucketAclDenyWrite(AmazonS3 client, String bucketName) {
		var key = "checkBucketAclDenyWrite";
		failedPutObject(client, bucketName, key, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	public static void checkBucketAclAllowWriteACP(AmazonS3 client, String bucketName) {
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
	}

	public static void checkBucketAclDenyWriteACP(AmazonS3 client, String bucketName) {
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
	}

	public static void checkObjectAclAllowRead(AmazonS3 client, String bucketName, String key) {
		succeedGetObject(client, bucketName, key, key);
	}

	public static void checkObjectAclDenyRead(AmazonS3 client, String bucketName, String key) {
		failedGetObject(client, bucketName, key, HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED);
	}

	public static void checkObjectAclAllowReadACP(AmazonS3 client, String bucketName, String key) {
		client.getObjectAcl(bucketName, key);
	}

	public static void checkObjectAclDenyReadACP(AmazonS3 client, String bucketName, String key) {
		var e = assertThrows(AmazonServiceException.class, () -> client.getObjectAcl(bucketName, key));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	public static void checkObjectAclAllowWrite(AmazonS3 client, String bucketName, String key) {
		var tagging = new ObjectTagging(List.of(new Tag("key", "value")));
		client.setObjectTagging(new SetObjectTaggingRequest(bucketName, key, tagging));
	}

	public static void checkObjectAclDenyWrite(AmazonS3 client, String bucketName, String key) {
		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, key));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	public static void checkObjectAclAllowWriteACP(AmazonS3 client, String bucketName, String key) {
		client.setObjectAcl(bucketName, key, CannedAccessControlList.PublicRead);
	}

	public static void checkObjectAclDenyWriteACP(AmazonS3 client, String bucketName, String key) {
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setObjectAcl(bucketName, key, CannedAccessControlList.PublicRead));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	public void checkPrefixLifecycleConfiguration(List<Rule> expected, List<Rule> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).getId(), actual.get(i).getId());
			assertEquals(expected.get(i).getExpirationDate(), actual.get(i).getExpirationDate());
			assertEquals(expected.get(i).getExpirationInDays(), actual.get(i).getExpirationInDays());
			assertEquals(expected.get(i).isExpiredObjectDeleteMarker(), actual.get(i).isExpiredObjectDeleteMarker());
			assertEquals(((LifecyclePrefixPredicate) expected.get(i).getFilter().getPredicate()).getPrefix(),
					((LifecyclePrefixPredicate) actual.get(i).getFilter().getPredicate()).getPrefix());

			assertEquals(expected.get(i).getStatus(), actual.get(i).getStatus());
			assertEquals(expected.get(i).getNoncurrentVersionTransitions(),
					actual.get(i).getNoncurrentVersionTransitions());
			assertEquals(expected.get(i).getTransitions(), actual.get(i).getTransitions());
			if (expected.get(i).getNoncurrentVersionExpiration() != null)
				assertEquals(expected.get(i).getNoncurrentVersionExpiration().getDays(),
						actual.get(i).getNoncurrentVersionExpiration().getDays());
			assertEquals(expected.get(i).getAbortIncompleteMultipartUpload(),
					actual.get(i).getAbortIncompleteMultipartUpload());
		}
	}

	public void lockCompare(ObjectLockConfiguration expected, ObjectLockConfiguration actual) {
		assertEquals(expected.getObjectLockEnabled(), actual.getObjectLockEnabled());
		assertEquals(expected.getRule().getDefaultRetention().getMode(),
				actual.getRule().getDefaultRetention().getMode());
		assertEquals(expected.getRule().getDefaultRetention().getYears(),
				actual.getRule().getDefaultRetention().getYears());
		assertEquals(expected.getRule().getDefaultRetention().getDays(),
				actual.getRule().getDefaultRetention().getDays());
	}

	public void retentionCompare(ObjectLockRetention expected, ObjectLockRetention actual) {
		assertEquals(expected.getMode(), actual.getMode());
		assertEquals(expected.getRetainUntilDate(), actual.getRetainUntilDate());

	}

	public void replicationConfigCompare(BucketReplicationConfiguration expected,
			BucketReplicationConfiguration actual) {
		assertEquals(expected.getRoleARN(), actual.getRoleARN());
		var expectedRules = expected.getRules();
		var actualRules = actual.getRules();
		var getKeys = expectedRules.keySet();

		for (var key : getKeys) {
			var expectedRule = expectedRules.get(key);
			var actualRule = actualRules.get(key);
			assertEquals(expectedRule.getDeleteMarkerReplication(), actualRule.getDeleteMarkerReplication());
			assertEquals(expectedRule.getDestinationConfig().toString(), actualRule.getDestinationConfig().toString());
			assertEquals(expectedRule.getExistingObjectReplication(), actualRule.getExistingObjectReplication());
			assertEquals(expectedRule.getFilter(), actualRule.getFilter());
			assertEquals(expectedRule.getSourceSelectionCriteria(), actualRule.getSourceSelectionCriteria());
			assertEquals(expectedRule.getStatus(), actualRule.getStatus());
		}
	}

	public List<Tag> taggingSort(List<Tag> tagList) {
		var newList = new ArrayList<>(tagList);
		newList.sort(Comparator.comparing(Tag::getKey));
		return newList;
	}

	public void bucketTaggingCompare(List<TagSet> expected, List<TagSet> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			var subExpected = expected.get(i).getAllTags();
			var subActual = actual.get(i).getAllTags();

			for (var item : subExpected.entrySet()) {
				assertEquals(item.getValue(), subActual.get(item.getKey()));
			}
		}
	}

	public void tagCompare(List<Tag> expected, List<Tag> actual) {
		assertEquals(expected.size(), actual.size());

		var orderExpected = taggingSort(expected);
		var orderActual = taggingSort(actual);

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(orderExpected.get(i).getKey(), orderActual.get(i).getKey());
			assertEquals(orderExpected.get(i).getValue(), orderActual.get(i).getValue());
		}
	}

	public void deleteSuspendedVersioningObj(AmazonS3 client, String bucketName, String key, List<String> versionIds,
			List<String> contents) {
		client.deleteObject(bucketName, key);

		assertEquals(versionIds.size(), contents.size());

		for (int i = versionIds.size() - 1; i >= 0; i--) {
			if (versionIds.get(i).equals("null")) {
				versionIds.remove(i);
				contents.remove(i);
			}
		}
	}

	public void overwriteSuspendedVersioningObj(AmazonS3 client, String bucketName, String key, List<String> versionIds,
			List<String> contents, String content) {
		client.putObject(bucketName, key, content);

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

	public void removeObjVersion(AmazonS3 client, String bucketName, String key, List<String> versionIds,
			List<String> contents, int index) {
		assertEquals(versionIds.size(), contents.size());
		var rmVersionID = versionIds.get(index);
		versionIds.remove(index);
		var rmContent = contents.get(index);
		contents.remove(index);

		checkObjContent(client, bucketName, key, rmVersionID, rmContent);

		client.deleteVersion(bucketName, key, rmVersionID);

		if (!versionIds.isEmpty())
			checkObjVersions(client, bucketName, key, versionIds, contents);
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

	public void createMultipleVersion(AmazonS3 client, String bucketName, String key, int numVersions,
			boolean checkVersion, String body) {
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++) {
			var response = client.putObject(bucketName, key, body);
			var versionId = response.getVersionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		if (checkVersion)
			checkObjVersions(client, bucketName, key, versionIds, contents);
	}

	public void createMultipleVersions(AmazonS3 client, String bucketName, String key, int numVersions,
			List<String> versionIds, List<String> contents, boolean checkVersion) {

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

	public void doTestCreateRemoveVersions(AmazonS3 client, String bucketName, String key, int numVersions,
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

	public List<Thread> doCreateVersionedObjConcurrent(AmazonS3 client, String bucketName, String key, int num) {
		var threadList = new ArrayList<Thread>();
		for (int i = 0; i < num; i++) {
			var item = Integer.toString(i);
			var mThread = new Thread(() -> client.putObject(bucketName, key, String.format("Data %s", item)));
			mThread.start();
			threadList.add(mThread);
		}
		return threadList;
	}

	public List<Thread> doClearVersionedBucketConcurrent(AmazonS3 client, String bucketName) {
		var threadList = new ArrayList<Thread>();
		var response = client.listVersions(bucketName, "");

		for (var Version : response.getVersionSummaries()) {
			var mThread = new Thread(() -> client.deleteVersion(bucketName, Version.getKey(), Version.getVersionId()));
			mThread.start();
			threadList.add(mThread);
		}
		return threadList;
	}

	public void corsRequestAndCheck(String method, String bucketName, Map<String, String> headers, int statusCode,
			String expectAllowOrigin, String expectAllowMethods, String key) {

		try {
			var url = createURL(bucketName);
			if (key != null)
				url = createURL(bucketName, key);

			System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
			var connection = (HttpURLConnection) url.openConnection();
			for (var Item : headers.entrySet()) {
				connection.setRequestProperty(Item.getKey(), Item.getValue());
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

	public void versionIdsCompare(List<S3VersionSummary> expected, List<S3VersionSummary> actual) {
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
	public void deleteBucketList(String bucketName) {
		buckets.remove(bucketName);
	}

	@AfterEach
	public void clear(TestInfo testInfo) {
		System.out.println("Test End : " + testInfo.getDisplayName());
		for (var bucketName : buckets)
			System.out.println("Bucket : " + bucketName);
		if (!config.notDelete)
			bucketClear();
	}

	public void bucketClear(AmazonS3 client, String bucketName) {
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
			System.out.format("Error : Bucket(%s) Clear Failed(%s, %d)%n", bucketName, e.getErrorCode(),
					e.getStatusCode());
		}

		try {
			client.deleteBucket(bucketName);
		} catch (AmazonServiceException e) {
			System.out.format("Error : Bucket(%s) Delete Failed(%s, %d)%n", bucketName, e.getErrorCode(),
					e.getStatusCode());
		}
	}

	public void bucketClear() {
		var client = getClient();
		if (client == null || buckets.isEmpty())
			return;

		var iter = buckets.iterator();
		while (iter.hasNext()) {
			var bucketName = iter.next();
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
			e.printStackTrace();
			Thread.currentThread().interrupt();
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
		if (config.isAWS()) {
			cal.setTimeZone(TimeZone.getTimeZone("UTC"));
			cal.add(Calendar.DATE, days);
			cal.set(Calendar.HOUR, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
		}
		cal.add(Calendar.DATE, days);

		return cal.getTime();
	}

	// endregion
}
