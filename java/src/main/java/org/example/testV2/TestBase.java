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
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

import org.apache.hc.core5.http.HttpStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.example.Data.AES256;
import org.example.Data.BackendHeaders;
import org.example.Data.MainData;
import org.example.Data.MultipartUploadV2Data;
import org.example.Data.ObjectDataV2;
import org.example.Data.RangeSet;
import org.example.Data.UserData;
import org.example.Utility.CheckSum;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.s3tests.S3Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.platform.commons.util.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
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
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ReplicationConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionConfiguration;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.Type;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.utils.AttributeMap;

@SuppressWarnings("unchecked")
public class TestBase {

	/************************************************************************************************************/

	public enum EncryptionType {
		NORMAL, SSE_S3, SSE_C
	}

	/************************************************************************************************************/
	static final int DEFAULT_PART_SIZE = 5 * MainData.MB;
	public static final String SSE_CUSTOMER_ALGORITHM = "AES256";
	static final String SSE_KEY = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
	static final String SSE_KEY_MD5 = "DWygnHRtgiJ77HCm+1rvHw==";
	/************************************************************************************************************/

	private final ArrayList<String> buckets = new ArrayList<>();
	protected final S3Config config;

	protected TestBase() {
		String fileName = System.getProperty("s3tests.ini", S3Config.STR_FILENAME);
		config = new S3Config(fileName);
		config.getConfig();
	}

	// region Create Client

	public S3Client createClient(boolean isSecure, UserData user, boolean useChunkEncoding,
			RequestChecksumCalculation request, ResponseChecksumValidation response) {
		String address = "";
		var httpClient = ApacheHttpClient.builder()
				.connectionTimeout(Duration.ofSeconds(10))
				.socketTimeout(Duration.ofSeconds(10));
		if (isSecure) {
			try {
				SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
				sslContextBuilder.loadTrustMaterial(new TrustAllStrategy());

				var sslSocketFactory = new SSLConnectionSocketFactory(
						sslContextBuilder.build(),
						new String[] { "TLSv1.2", "TLSv1.1", "TLSv1" },
						null,
						NoopHostnameVerifier.INSTANCE);
				httpClient.socketFactory(sslSocketFactory);
			} catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
				e.printStackTrace();
				return null;
			}
			if (config.url.isEmpty())
				address = NetUtils.createRegion2Https(config.regionName);
			else
				address = NetUtils.createURLToHTTPS(config.url, config.sslPort);
		} else {
			if (config.url.isEmpty())
				address = NetUtils.createRegion2Http(config.regionName);
			else
				address = NetUtils.createURLToHTTP(config.url, config.port);
		}
		AwsCredentialsProvider awsCred = null;
		if (user == null)
			awsCred = AnonymousCredentialsProvider.create();
		else
			awsCred = StaticCredentialsProvider.create(AwsBasicCredentials.create(user.accessKey, user.secretKey));

		var s3Config = S3Configuration.builder()
				.chunkedEncodingEnabled(useChunkEncoding)
				.pathStyleAccessEnabled(true)
				.build();

		return S3Client.builder()
				.region(config.regionName != null ? Region.of(config.regionName) : Region.AP_NORTHEAST_2)
				.credentialsProvider(awsCred)
				.httpClientBuilder(httpClient)
				.serviceConfiguration(s3Config)
				.requestChecksumCalculation(request)
				.responseChecksumValidation(response)
				.endpointOverride(URI.create(address)).build();
	}

	public S3Client getOldClient() {
		var url = NetUtils.createURLToHTTP(config.url, config.oldPort);
		var credential = StaticCredentialsProvider.create(AwsBasicCredentials.create(config.mainUser.accessKey,
				config.mainUser.secretKey));
		var httpClient = ApacheHttpClient.builder()
				.connectionTimeout(Duration.ofSeconds(10))
				.socketTimeout(Duration.ofSeconds(10));
		var s3Config = S3Configuration.builder().pathStyleAccessEnabled(true).build();
		return S3Client.builder()
				.region(Region.AP_NORTHEAST_2)
				.credentialsProvider(credential).httpClientBuilder(httpClient)
				.serviceConfiguration(s3Config).endpointOverride(URI.create(url)).build();
	}

	public S3Client getClient() {
		return createClient(config.isSecure, config.mainUser, false, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
	}

	public S3Client getClient(boolean useChunkEncoding) {
		return createClient(config.isSecure, config.mainUser, useChunkEncoding,
				RequestChecksumCalculation.WHEN_REQUIRED, ResponseChecksumValidation.WHEN_REQUIRED);
	}

	public S3Client getClientHttps(boolean useChunkEncoding) {
		return createClient(true, config.mainUser, useChunkEncoding, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
	}

	public S3Client getAltClient() {
		return createClient(config.isSecure, config.altUser, true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
	}

	public S3Client getPublicClient() {
		return createClient(config.isSecure, null, true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
	}

	public S3Client getClient(boolean useChunkEncoding, RequestChecksumCalculation request,
			ResponseChecksumValidation response) {
		return createClient(config.isSecure, config.mainUser, useChunkEncoding, request, response);
	}

	private S3AsyncClient createAsyncClient(boolean isSecure, UserData user, boolean useChunkEncoding,
			RequestChecksumCalculation request, ResponseChecksumValidation response) {
		String address = "";
		var httpClient = NettyNioAsyncHttpClient.builder().writeTimeout(Duration.ofSeconds(30))
				.readTimeout(Duration.ofSeconds(30))
				.connectionTimeout(Duration.ofSeconds(30));

		if (isSecure) {
			if (config.url.isEmpty())
				address = NetUtils.createRegion2Https(config.regionName);
			else {
				address = NetUtils.createURLToHTTPS(config.url, config.sslPort);
				// 인증서 회피
				httpClient.buildWithDefaults(
						AttributeMap.builder().put(
								SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE)
								.build());
			}
		} else {
			if (config.url.isEmpty())
				address = NetUtils.createRegion2Http(config.regionName);
			else
				address = NetUtils.createURLToHTTP(config.url, config.port);
		}
		AwsCredentialsProvider awsCred = null;
		if (user == null)
			awsCred = AnonymousCredentialsProvider.create();
		else
			awsCred = StaticCredentialsProvider.create(AwsBasicCredentials.create(user.accessKey, user.secretKey));

		var s3Config = S3Configuration.builder()
				.chunkedEncodingEnabled(useChunkEncoding)
				.pathStyleAccessEnabled(true)
				.build();

		return S3AsyncClient.builder()
				.region(config.regionName != null ? Region.of(config.regionName) : Region.AP_NORTHEAST_2)
				.httpClient(httpClient.build())
				.credentialsProvider(awsCred)
				.serviceConfiguration(s3Config)
				.requestChecksumCalculation(request)
				.responseChecksumValidation(response)
				.endpointOverride(URI.create(address)).build();
	}

	public S3AsyncClient getAsyncClient(boolean useChunkEncoding, RequestChecksumCalculation request,
			ResponseChecksumValidation response) {
		return createAsyncClient(config.isSecure, config.mainUser, useChunkEncoding, request, response);
	}

	public S3Presigner getS3Presigner() {
		var address = NetUtils.createURLToHTTP(config.url, config.port);
		var s3Config = S3Configuration.builder()
				.pathStyleAccessEnabled(true)
				.build();
		var awsCred = StaticCredentialsProvider
				.create(AwsBasicCredentials.create(config.mainUser.accessKey, config.mainUser.secretKey));
		var presigner = S3Presigner.builder()
				.region(config.regionName != null ? Region.of(config.regionName) : Region.AP_NORTHEAST_2)
				.credentialsProvider(awsCred)
				.serviceConfiguration(s3Config);
		if (!config.url.isEmpty())
			presigner.endpointOverride(URI.create(address));

		return presigner.build();
	}

	public S3Client getBadAuthClient(String accessKey, String secretKey) {
		if (StringUtils.isBlank(accessKey))
			accessKey = "aaaaaaaaaaaaaaa";
		if (StringUtils.isBlank(secretKey))
			secretKey = "bbbbbbbbbbbbbbb";

		var dummyUser = new UserData();
		dummyUser.accessKey = accessKey;
		dummyUser.secretKey = secretKey;

		return createClient(config.isSecure, dummyUser, true, RequestChecksumCalculation.WHEN_REQUIRED,
				ResponseChecksumValidation.WHEN_REQUIRED);
	}

	public S3Client getBackendClient() {
		String address = "";

		if (config.isSecure) {
			if (config.url.isEmpty())
				address = NetUtils.createRegion2Https(config.regionName);
			else
				address = NetUtils.createURLToHTTPS(config.url, config.sslPort);
		} else {
			if (config.url.isEmpty())
				address = NetUtils.createRegion2Http(config.regionName);
			else
				address = NetUtils.createURLToHTTP(config.url, config.port);
		}

		// 공통 헤더 인터셉터
		ClientOverrideConfiguration.Builder configBuilder = ClientOverrideConfiguration.builder();

		ExecutionInterceptor headerInterceptor = new ExecutionInterceptor() {
			@Override
			public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context,
					ExecutionAttributes executionAttributes) {
				return context.httpRequest().toBuilder()
						.putHeader(BackendHeaders.IFS_ADMIN, BackendHeaders.HEADER_DATA)
						.putHeader(BackendHeaders.IFS_BACKEND, BackendHeaders.HEADER_DATA)
						.putHeader(BackendHeaders.KSAN_BACKEND, BackendHeaders.HEADER_DATA)
						.putHeader(BackendHeaders.HEADER_USER_AGENT, BackendHeaders.HEADER_USER_AGENT_VALUE)
						.putHeader(BackendHeaders.HEADER_REPLICATION, BackendHeaders.HEADER_DATA)
						.build();
			}
		};
		configBuilder.addExecutionInterceptor(headerInterceptor);
		configBuilder.retryStrategy(r -> r.maxAttempts(1));

		var awsCred = StaticCredentialsProvider.create(
				AwsBasicCredentials.create(config.backendUser.accessKey, config.backendUser.secretKey));

		var s3Config = S3Configuration.builder()
				.chunkedEncodingEnabled(true)
				.pathStyleAccessEnabled(true)
				.build();

		// HTTP 클라이언트를 지정하지 않음 (기본 클라이언트 사용, mark/reset 문제 없음)
		return S3Client.builder()
				.region(config.regionName != null ? Region.of(config.regionName) : Region.AP_NORTHEAST_2)
				.credentialsProvider(awsCred)
				.serviceConfiguration(s3Config)
				.requestChecksumCalculation(RequestChecksumCalculation.WHEN_SUPPORTED)
				.responseChecksumValidation(ResponseChecksumValidation.WHEN_SUPPORTED)
				.endpointOverride(URI.create(address))
				.overrideConfiguration(configBuilder.build())
				.build();
	}
	// endregion

	// region Create data
	public String getNewBucketName() {
		var bucketName = Utils.getNewBucketName(getPrefix());
		buckets.add(bucketName);
		return bucketName;
	}

	public String getPrefix() {
		return "v2-" + config.bucketPrefix;
	}

	public String getNewBucketNameOnly() {
		return Utils.getNewBucketName(getPrefix());
	}

	public String getNewBucketNameOnly(int length) {
		var bucketName = Utils.getNewBucketName(getPrefix());
		if (bucketName.length() > length)
			bucketName = bucketName.substring(0, length);
		else if (bucketName.length() < length)
			bucketName = bucketName + Utils.randomText(length - bucketName.length());
		return bucketName;
	}

	public String createBucket(S3Client client) {
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName));
		if (config.isOldSystem()) {
			var oldClient = getOldClient();
			oldClient.createBucket(c -> c.bucket(bucketName));
		}
		return bucketName;
	}

	public String createBucket(S3AsyncClient client) {
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName));
		return bucketName;
	}

	public String createBucket(S3Client client, ObjectOwnership ownership) {
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectOwnership(ownership));
		return bucketName;
	}

	public String createBucket(S3Client client, ObjectOwnership ownership, BucketCannedACL acl) {
		var bucketName = createBucket(client, ownership);
		client.putPublicAccessBlock(p -> p.bucket(bucketName).publicAccessBlockConfiguration(c -> c
				.blockPublicAcls(false).ignorePublicAcls(false).blockPublicPolicy(false).restrictPublicBuckets(false)));
		if (acl != null)
			client.putBucketAcl(p -> p.bucket(bucketName).acl(acl));
		return bucketName;
	}

	public String createBucket() {
		return createBucket(getClient());
	}

	public String createBucketCannedAcl(S3Client client) {
		return createBucket(client, ObjectOwnership.OBJECT_WRITER, null);
	}

	public String createBucketCannedAcl(S3Client client, BucketCannedACL acl) {
		return createBucket(client, ObjectOwnership.OBJECT_WRITER, acl);
	}

	public void createObjects(S3Client client, String bucketName, List<String> keys) {
		if (keys != null) {
			for (var key : keys) {
				var body = key.endsWith("/") ? RequestBody.empty() : RequestBody.fromString(key);
				client.putObject(p -> p.bucket(bucketName).key(key), body);
			}
		}
	}

	public String createObjects(S3Client client, String... keys) {
		var bucketName = createBucket(client);
		createObjects(client, bucketName, List.of(keys));
		return bucketName;
	}

	public String createObjects(S3Client client, List<String> keys) {
		var bucketName = createBucket(client);
		createObjects(client, bucketName, keys);
		return bucketName;
	}

	public String createObjects(List<String> keys) {
		var client = getClient();
		return createObjects(client, keys);
	}

	public static Grantee createPublicGrantee() {
		return Grantee.builder().type(Type.GROUP).uri(MainData.ALL_USERS).build();
	}

	public static Grantee createAuthenticatedGrantee() {
		return Grantee.builder().type(Type.GROUP).uri(MainData.AUTHENTICATED_USERS).build();
	}

	public static Grant createPublicGrant(Permission permission) {
		return Grant.builder().grantee(createPublicGrantee()).permission(permission).build();
	}

	public static Grant createAuthenticatedGrant(Permission permission) {
		return Grant.builder().grantee(createAuthenticatedGrantee()).permission(permission).build();
	}

	public static Grant owner2Grant(Owner owner, Permission permission) {
		return Grant.builder()
				.grantee(Grantee.builder().id(owner.id())
						.type(Type.CANONICAL_USER).displayName(owner.displayName()).build())
				.permission(permission).build();
	}

	public static AccessControlPolicy createAcl(Owner owner, Grantee grantee, Permission... permissions) {
		var acl = AccessControlPolicy.builder().owner(owner);
		var grants = new ArrayList<Grant>();
		grants.add(owner2Grant(owner, Permission.FULL_CONTROL));
		for (var permission : permissions)
			grants.add(Grant.builder().grantee(grantee).permission(permission).build());
		return acl.grants(grants).build();
	}

	public AccessControlPolicy createAcl() {
		return createAcl(config.mainUser.toOwnerV2(), config.mainUser.toGranteeV2());
	}

	public AccessControlPolicy createPublicAcl(Permission... permissions) {
		return createAcl(config.mainUser.toOwnerV2(), createPublicGrantee(), permissions);
	}

	public AccessControlPolicy createAuthenticatedAcl(Permission... permissions) {
		return createAcl(config.mainUser.toOwnerV2(), Grantee.builder().type(Type.GROUP)
				.uri(MainData.AUTHENTICATED_USERS)
				.build(), permissions);
	}

	public AccessControlPolicy createAltAcl(Permission... permissions) {
		return createAcl(config.mainUser.toOwnerV2(), config.altUser.toGranteeV2(), permissions);
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

	public static JsonObject makeJsonStatement(String action, String resource, String effect, JsonObject principal,
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

	public static JsonObject makeJsonPolicy(String action, String resource, JsonObject principal,
			JsonObject conditions) {
		var policy = new JsonObject();

		policy.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);

		var statement = new JsonArray();
		statement.add(makeJsonStatement(action, resource, null, principal, conditions));
		policy.add(MainData.POLICY_STATEMENT, statement);

		return policy;
	}

	public static JsonObject makeJsonPolicy(JsonObject... statementList) {
		var policy = new JsonObject();

		policy.addProperty(MainData.POLICY_VERSION, MainData.POLICY_VERSION_DATE);
		var statements = new JsonArray();
		for (var statement : statementList)
			statements.add(statement);
		policy.add(MainData.POLICY_STATEMENT, statements);
		return policy;
	}

	public static List<Tag> makeSimpleTagSet(int count) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(Tag.builder().key(Integer.toString(i)).value(Integer.toString(i)).build());
		return tagSets;
	}

	public static List<Tag> makeDetailTagSet(int count, int keySize, int valueSize) {
		var tagSets = new ArrayList<Tag>();

		for (int i = 0; i < count; i++)
			tagSets.add(Tag.builder().key(Utils.randomTextToLong(keySize)).value(Utils.randomTextToLong(valueSize))
					.build());
		return tagSets;
	}

	public String setupAclBucket(ObjectOwnership ownership, BucketCannedACL acl, List<String> keys) {
		var client = getClient();
		var bucketName = createBucket(client, ownership, acl);
		createObjects(client, bucketName, keys);
		return bucketName;
	}

	public String setupAclBucket(BucketCannedACL acl, List<String> keys) {
		return setupAclBucket(ObjectOwnership.OBJECT_WRITER, acl, keys);
	}

	public String setupAclObjects(ObjectOwnership ownership, BucketCannedACL bucketAcl, ObjectCannedACL objectAcl,
			String... keys) {
		var client = getClient();
		var bucketName = createBucket(client, ownership, bucketAcl);
		for (var key : keys)
			client.putObject(p -> p.bucket(bucketName).key(key).acl(objectAcl), RequestBody.fromString(key));

		return bucketName;
	}

	public String setupAclObjects(BucketCannedACL bucketAcl, ObjectCannedACL objectAcl, String... keys) {
		return setupAclObjects(ObjectOwnership.OBJECT_WRITER, bucketAcl, objectAcl, keys);
	}

	public String setupAclObjectsByAlt(ObjectOwnership ownership, BucketCannedACL bucketAcl, ObjectCannedACL objectAcl,
			String... keys) {
		var altClient = getAltClient();
		var bucketName = createBucket(getClient(), ownership, bucketAcl);
		for (var key : keys)
			altClient.putObject(p -> p.bucket(bucketName).key(key).acl(objectAcl), RequestBody.fromString(key));

		return bucketName;
	}

	public String setupAclObjectsByAlt(BucketCannedACL bucketAcl, ObjectCannedACL objectAcl, String... keys) {
		return setupAclObjectsByAlt(ObjectOwnership.OBJECT_WRITER, bucketAcl, objectAcl, keys);
	}

	public void createKeyWithRandomContent(S3Client client, String key, int size, String bucketName) {
		if (size <= 0)
			size = 7 * MainData.MB;

		var data = Utils.randomTextToLong(size);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));
	}

	public String createKeyWithRandomContent(S3Client client, String key, int size) {
		var bucketName = createBucket(client);
		if (size < 1)
			size = 7 * MainData.MB;

		var data = Utils.randomTextToLong(size);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));

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

	public AccessControlPolicy addBucketUserGrant(String bucketName, Grant grant) {
		var client = getClient();
		var acl = AccessControlPolicy.builder();
		var grants = new ArrayList<Grant>();

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		grants.addAll(response.grants());
		grants.add(grant);

		acl.owner(response.owner());
		acl.grants(grants);

		return acl.build();
	}

	public String setupBucketPermission(Permission permission) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);

		var acl = createAltAcl(permission);
		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl));

		return bucketName;
	}

	public String setupObjectPermission(String key, Permission permission) {
		var client = getClient();
		var bucketName = createBucket(client, ObjectOwnership.OBJECT_WRITER, BucketCannedACL.PUBLIC_READ_WRITE);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));

		var acl = createAltAcl(permission);
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl));

		return bucketName;
	}

	public void createMultipleVersion(S3Client client, String bucketName, String key, int numVersions,
			boolean checkVersion, String body) {
		var versionIds = new ArrayList<String>();
		var contents = new ArrayList<String>();

		for (int i = 0; i < numVersions; i++) {
			var response = client.putObject(p -> p.bucket(bucketName).key(key),
					RequestBody.fromString(body));
			var versionId = response.versionId();

			contents.add(body);
			versionIds.add(versionId);
		}

		if (checkVersion)
			checkObjVersions(client, bucketName, key, versionIds, contents);
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

	public List<ObjectVersion> reverseVersions(List<ObjectVersion> versions) {
		var iterator = versions.listIterator(versions.size());
		var reverseVersions = new ArrayList<ObjectVersion>();

		while (iterator.hasPrevious()) {
			reverseVersions.add(iterator.previous());
		}
		return reverseVersions;
	}

	// endregion

	// region Get data

	public List<String> getObjectList(S3Client client, String bucketName, String prefix) {
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
				return result.toString(StandardCharsets.UTF_8);
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
	public static void checkGetObject(S3Client client, String bucketName, String key, boolean pass) {
		if (pass) {
			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			assertTrue(response != null);
		} else {
			var e = assertThrows(S3Exception.class,
					() -> client.getObject(g -> g.bucket(bucketName).key(key)));
			assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
			assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
		}
	}

	public static void succeedGetObject(S3Client client, String bucketName, String key, String content) {
		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertTrue(content.equals(body), MainData.NOT_MATCHED);
	}

	public static void failedGetObject(S3Client client, String bucketName, String key, int statusCode,
			String errorCode) {
		var e = assertThrows(S3Exception.class, () -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
	}

	public static void succeedPutObject(S3Client client, String bucketName, String key, String content) {
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
	}

	public static void failedPutObject(S3Client client, String bucketName, String key, int statusCode,
			String errorCode) {
		var e = assertThrows(S3Exception.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("")));
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
	}

	public static void succeedListObjects(S3Client client, String bucketName, List<String> keys) {
		var response = client.listObjects(l -> l.bucket(bucketName));
		var keyList = getKeys(response.contents());
		assertEquals(keys, keyList);
	}

	public static void failedListObjects(S3Client client, String bucketName, int statusCode, String errorCode) {
		var e = assertThrows(S3Exception.class, () -> client.listObjects(l -> l.bucket(bucketName)));
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
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
		var versions = new ArrayList<>(response.versions());

		Collections.reverse(versions);

		var index = 0;
		for (var version : versions) {
			assertEquals(version.versionId(), versionIds.get(index));
			if (StringUtils.isNotBlank(key))
				assertEquals(key, version.key());
			checkObjContent(client, bucketName, key, version.versionId(), contents.get(index++));
		}
	}

	public String validateListObject(String bucketName, String prefix, String delimiter, String marker, int maxKeys,
			boolean isTruncated, List<String> checkKeys, List<String> checkPrefixes, String nextMarker) {
		var client = getClient();
		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter)
				.marker(marker).maxKeys(maxKeys).prefix(prefix));

		assertEquals(isTruncated, response.isTruncated());
		assertEquals(nextMarker, response.nextMarker());

		var keys = response.contents().stream().map(S3Object::key).toList();
		var prefixes = response.commonPrefixes().stream().map(CommonPrefix::prefix).toList();

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

		var keys = response.contents().stream().map(S3Object::key).toList();
		var prefixes = response.commonPrefixes().stream().map(CommonPrefix::prefix).toList();

		assertEquals(checkKeys, keys);
		assertEquals(checkPrefixes, prefixes);
		assertEquals(checkKeys.size(), keys.size());
		assertEquals(checkPrefixes.size(), prefixes.size());

		return response.nextContinuationToken();
	}

	public void checkBadBucketName(String bucketName) {
		assertThrows(IllegalArgumentException.class,
				() -> getClient().createBucket(c -> c.bucket(bucketName)));
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
		var bucketName = getNewBucketNameOnly(length);
		buckets.add(bucketName);
		getClient().createBucket(c -> c.bucket(bucketName));
	}

	public void testBucketCreateNamingBadLong(int length) {
		var bucketName = getNewBucketNameOnly(length);
		checkBadBucketName(bucketName);
	}

	public static MultipartUploadV2Data multipartUpload(S3Client client, String bucketName, String key, int size,
			int partSize) {
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key));
		uploadData.uploadId = createResponse.uploadId();

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

		client.completeMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));
		return uploadData;
	}

	public void multipartUpload(S3Client client, String bucketName, String key, ChecksumType checksumType,
			ChecksumAlgorithm checksum) {
		var size = 10 * MainData.MB;
		var partSize = 5 * MainData.MB;
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.checksumType(checksumType)
				.checksumAlgorithm(checksum));
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.checksumAlgorithm(checksum)
					.partNumber(uploadData.nextPartNumber()),
					RequestBody.fromString(Part));
			checksumCompare(checksum, Part, partResponse);
			uploadData.addPart(checksum, partResponse);
		}

		var completeResponse = client.completeMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.uploadId(uploadData.uploadId)
				.checksumType(checksumType)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		checksumCompare(checksum, uploadData, completeResponse);
	}

	public void multipartUpload(S3AsyncClient client, String bucketName, String key, ChecksumType checksumType,
			ChecksumAlgorithm checksum) {
		var size = 10 * MainData.MB;
		var partSize = 5 * MainData.MB;
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.checksumType(checksumType)
				.checksumAlgorithm(checksum)).join();
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.checksumAlgorithm(checksum)
					.partNumber(uploadData.nextPartNumber()),
					AsyncRequestBody.fromString(Part)).join();
			checksumCompare(checksum, Part, partResponse);
			uploadData.addPart(checksum, partResponse);
		}

		var completeResponse = client.completeMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.uploadId(uploadData.uploadId)
				.checksumType(checksumType)
				.multipartUpload(p -> p.parts(uploadData.parts))).join();

		checksumCompare(checksum, uploadData, completeResponse);
	}

	public void multipartCopy(S3Client client, String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey, ChecksumAlgorithm checksum) {
		var size = 10 * MainData.MB;
		var partSize = 5 * MainData.MB;
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(targetBucketName)
				.key(targetKey)
				.checksumAlgorithm(checksum));
		uploadData.uploadId = createResponse.uploadId();

		long index = 0;
		while (index < size) {
			var partNumber = uploadData.nextPartNumber();
			var start = index;
			var end = Math.min(index + partSize, size) - 1;

			var partResponse = client.uploadPartCopy(c -> c.sourceBucket(sourceBucketName)
					.sourceKey(sourceKey).destinationBucket(targetBucketName).destinationKey(targetKey)
					.uploadId(uploadData.uploadId).partNumber(partNumber)
					.copySourceRange("bytes=" + start + "-" + end));
			uploadData.addPart(checksum, partResponse);
			index = end + 1;
		}

		var completeResponse = client.completeMultipartUpload(c -> c
				.bucket(targetBucketName)
				.key(targetKey)
				.uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));
		checksumCompare(checksum, uploadData, completeResponse);
	}

	public void multipartCopy(S3AsyncClient client, String sourceBucketName, String sourceKey, String targetBucketName,
			String targetKey, ChecksumAlgorithm checksum) {
		var size = 10 * MainData.MB;
		var partSize = 5 * MainData.MB;
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(targetBucketName)
				.key(targetKey)
				.checksumAlgorithm(checksum)).join();
		uploadData.uploadId = createResponse.uploadId();

		long index = 0;
		while (index < size) {
			var partNumber = uploadData.nextPartNumber();
			var start = index;
			var end = Math.min(index + partSize, size) - 1;

			var partResponse = client.uploadPartCopy(c -> c.sourceBucket(sourceBucketName)
					.sourceKey(sourceKey).destinationBucket(targetBucketName).destinationKey(targetKey)
					.uploadId(uploadData.uploadId).partNumber(partNumber)
					.copySourceRange("bytes=" + start + "-" + end)).join();
			uploadData.addPart(checksum, partResponse);
			index = end + 1;
		}

		var completeResponse = client.completeMultipartUpload(c -> c
				.bucket(targetBucketName)
				.key(targetKey)
				.uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts))).join();
		checksumCompare(checksum, uploadData, completeResponse);
	}

	static MultipartUploadV2Data multipartUpload(S3Client client, String bucketName, String key,
			int size, int partSize, MultipartUploadV2Data uploadData) {
		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);
			var partResponse = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.partNumber(uploadData.nextPartNumber())
					.contentMD5(Utils.getMD5(Part)),
					RequestBody.fromString(Part));
			uploadData.addPart(partResponse.eTag());
		}

		return uploadData;
	}

	public static MultipartUploadV2Data multipartUpload(S3Client client, String bucketName, String key, int size,
			MultipartUploadV2Data uploadData) {
		return multipartUpload(client, bucketName, key, size, DEFAULT_PART_SIZE, uploadData);
	}

	static MultipartUploadV2Data setupMultipartUpload(S3Client client, String bucketName, String key, int size,
			int partSize, Map<String, String> metadataList) {

		if (partSize < 1)
			partSize = DEFAULT_PART_SIZE;

		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key).metadata(metadataList));
		uploadData.uploadId = createResponse.uploadId();

		return multipartUpload(client, bucketName, key, size, partSize, uploadData);
	}

	public static MultipartUploadV2Data setupMultipartUpload(S3Client client, String bucketName, String key, int size) {
		return setupMultipartUpload(client, bucketName, key, size, 0, null);
	}

	public static MultipartUploadV2Data setupMultipartUpload(S3Client client, String bucketName, String key, int size,
			int partSize) {
		return setupMultipartUpload(client, bucketName, key, size, partSize, null);
	}

	public static MultipartUploadV2Data setupMultipartUpload(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList) {
		return setupMultipartUpload(client, bucketName, key, size, 0, metadataList);
	}

	public MultipartUploadV2Data setupSseCMultipartUpload(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList) {
		var uploadData = new MultipartUploadV2Data();
		var partSize = 5 * MainData.MB;

		var createResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(key)
				.metadata(metadataList)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
				.sseCustomerKey(SSE_KEY)
				.sseCustomerKeyMD5(SSE_KEY_MD5));

		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, partSize);

		for (var Part : parts) {
			uploadData.appendBody(Part);

			var response = client.uploadPart(u -> u
					.bucket(bucketName)
					.key(key)
					.uploadId(uploadData.uploadId)
					.partNumber(uploadData.nextPartNumber())
					.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
					.sseCustomerKey(SSE_KEY)
					.sseCustomerKeyMD5(SSE_KEY_MD5),
					RequestBody.fromString(Part));
			uploadData.addPart(response.eTag());
		}
		return uploadData;
	}

	public MultipartUploadV2Data setupSSEMultipartUpload(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList) {
		var uploadData = new MultipartUploadV2Data();

		var createResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key)
				.metadata(metadataList).serverSideEncryption(ServerSideEncryption.AES256));
		uploadData.uploadId = createResponse.uploadId();
		return multipartUpload(client, bucketName, key, size, uploadData);
	}

	public MultipartUploadV2Data multipartUploadResend(S3Client client, String bucketName, String key, int size,
			Map<String, String> metadataList, List<Integer> resendParts) {
		var uploadData = new MultipartUploadV2Data();
		var contentType = "text/plain";

		var createResponse = client.createMultipartUpload(c -> c.bucket(bucketName).key(key)
				.metadata(metadataList).contentType(contentType));
		uploadData.uploadId = createResponse.uploadId();

		var parts = Utils.generateRandomString(size, DEFAULT_PART_SIZE);

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

	public void checkCopyContent(String sourceBucketName, String sourceKey, String targetBucketName, String targetKey,
			String versionId) {
		var client = getClient();

		var sourceResponse = client.getObject(g -> g.bucket(sourceBucketName).key(sourceKey).versionId(versionId));
		var sourceSize = sourceResponse.response().contentLength();
		var sourceData = getBody(sourceResponse);

		var targetResponse = client
				.getObject(g -> g.bucket(targetBucketName).key(targetKey));
		var targetSize = targetResponse.response().contentLength();
		var targetData = getBody(targetResponse);

		assertEquals(sourceSize, targetSize);
		assertTrue(sourceData.equals(targetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentSseC(S3Client client, String sourceBucket, String sourceKey,
			String targetBucket, String targetKey) {

		var sourceResponse = client.getObject(g -> g.bucket(sourceBucket).key(sourceKey)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY));
		var sourceSize = sourceResponse.response().contentLength();
		var sourceData = getBody(sourceResponse);

		var targetResponse = client.getObject(g -> g.bucket(targetBucket).key(targetKey)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY));
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
			var end = Math.min(start + step, size) - 1;

			var sourceResponse = client.getObject(g -> g.bucket(sourceBucketName).key(sourceKey)
					.range("bytes=" + start + "-" + end));
			var sourceBody = getBody(sourceResponse);

			var targetResponse = client.getObject(g -> g.bucket(targetBucketName).key(targetKey)
					.range("bytes=" + start + "-" + end));
			var targetBody = getBody(targetResponse);

			assertEquals(sourceResponse.response().contentLength(), targetResponse.response().contentLength());
			assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
			index += step;
		}
	}

	public void checkUploadMultipartResend(String bucketName, String key, int size, List<Integer> resendParts) {
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		var client = getClient();
		var uploadData = multipartUploadResend(client, bucketName, key, size, metadata, resendParts);
		client.completeMultipartUpload(c -> c
				.bucket(bucketName).key(key)
				.uploadId(uploadData.uploadId)
				.multipartUpload(uploadData.completedMultipartUpload()));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
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
			String targetBucketName, String targetKey, int size) {
		var data = new MultipartUploadV2Data();
		var partSize = 5 * MainData.MB;

		var request = CreateMultipartUploadRequest.builder().bucket(targetBucketName).key(targetKey)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY).sseCustomerKeyMD5(SSE_KEY_MD5);

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
					.copySourceSSECustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
					.copySourceSSECustomerKey(SSE_KEY)
					.copySourceSSECustomerKeyMD5(SSE_KEY_MD5)
					.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
					.sseCustomerKey(SSE_KEY)
					.sseCustomerKeyMD5(SSE_KEY_MD5)
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
		assertEquals(data.length(), size, bucketName + "/" + key + " : " + data.length() + " != " + size);

		var index = 0L;
		while (index < size) {
			var start = index;
			var end = Math.min(start + step, size - 1L);

			var response = client.getObject(
					g -> g.bucket(bucketName).key(key).range("bytes=" + start + "-" + (end - 1)));
			var body = getBody(response);
			var length = end - start;
			var partBody = data.substring((int) start, (int) end);

			assertEquals(length, response.response().contentLength(),
					bucketName + "/" + key + " : " + length + " != " + response.response().contentLength());
			assertTrue(partBody.equals(body), MainData.NOT_MATCHED);
			index += step;
		}
	}

	public void checkContentUsingRange(String bucketName, String key, String versionId, String data, long step) {
		var client = getClient();
		var headResponse = client.headObject(h -> h.bucket(bucketName).key(key).versionId(versionId));
		var size = headResponse.contentLength();
		assertEquals(data.length(), size, bucketName + "/" + key + " : " + data.length() + " != " + size);

		var index = 0L;
		while (index < size) {
			var start = index;
			var end = Math.min(start + step, size - 1L);

			var response = client.getObject(
					g -> g.bucket(bucketName).key(key).range("bytes=" + start + "-" + (end - 1)).versionId(versionId));
			var body = getBody(response);
			var length = end - start;
			var partBody = data.substring((int) start, (int) end);

			assertEquals(length, response.response().contentLength(),
					bucketName + "/" + key + " : " + length + " != " + response.response().contentLength());
			assertTrue(partBody.equals(body), MainData.NOT_MATCHED);
			index += step;
		}
	}

	public void checkContentUsingRangeEnc(S3Client client, String bucketName, String key, String data, long step) {
		if (client == null)
			client = getClient();
		var headResponse = client
				.headObject(h -> h.bucket(bucketName).key(key).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
						.sseCustomerKey(SSE_KEY));
		var size = headResponse.contentLength();

		var index = 0L;
		while (index < size) {
			long start = index;
			var end = Math.min(index + step, size - 1);

			var response = client.getObject(g -> g.bucket(bucketName).key(key)
					.range("bytes=" + start + "-" + (end - 1))
					.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
					.sseCustomerKey(SSE_KEY));
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

	public void checkContentEnc(String bucketName, String key, String data, int loopCount) {
		var client = getClientHttps(false);

		for (int i = 0; i < loopCount; i++) {
			var response = client
					.getObject(g -> g.bucket(bucketName).key(key).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
							.sseCustomerKey(SSE_KEY));
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
					g -> g.bucket(bucketName).key(key).range("bytes=" + range.start + "-" + (range.end)));
			var body = getBody(response);

			assertEquals(range.length, response.response().contentLength() - 1);
			assertTrue(rangeBody.equals(body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRangeEnc(S3Client client, String bucketName, String key, String data,
			int fileSize, int loopCount) {
		for (int i = 0; i < loopCount; i++) {
			var range = getRandomRange(fileSize);

			var response = client.getObject(g -> g.bucket(bucketName).key(key)
					.range("bytes=" + range.start + "-" + (range.end - 1)).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM)
					.sseCustomerKey(SSE_KEY));
			var body = getBody(response);
			var rangeBody = data.substring(range.start, range.end);

			assertEquals(range.length, response.response().contentLength());
			assertTrue(rangeBody.equals(body), MainData.NOT_MATCHED);
		}
	}

	public boolean errorCheck(Integer statusCode) {
		return statusCode.equals(HttpStatus.SC_BAD_REQUEST) || statusCode.equals(HttpStatus.SC_FORBIDDEN);
	}

	public void testEncryptionCSEWrite(int fileSize) {
		var client = getClient();
		var bucketName = createBucket(client);
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
		var client = getClientHttps(false);
		var bucketName = createBucket(client);
		var key = "test";
		var data = Utils.randomTextToLong(fileSize);

		client.putObject(
				p -> p.bucket(bucketName).key(key).sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY)
						.sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		var response = client.getObject(g -> g.bucket(bucketName).key(key)
				.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY));
		var body = getBody(response);
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSE_KEY_MD5, response.response().sseCustomerKeyMD5());
	}

	public void testEncryptionSseS3Write(int fileSize) {
		var client = getClient();
		var bucketName = createBucket(client);
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
		var client = getClient();
		var bucketName = createBucket(client);
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
		assertEquals(ServerSideEncryption.AES256, sourceResponse.response().serverSideEncryption());

		var targetKey = "foo";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(sourceKey)
				.destinationBucket(bucketName).destinationKey(targetKey));
		var targetResponse = client.getObject(g -> g.bucket(bucketName).key(targetKey));
		assertEquals(ServerSideEncryption.AES256, targetResponse.response().serverSideEncryption());

		var targetBody = getBody(targetResponse);
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(boolean sourceObjectEncryption, boolean sourceBucketEncryption,
			boolean targetBucketEncryption, boolean targetObjectEncryption, int fileSize) {
		var sourceKey = "sourceKey";
		var targetKey = "targetKey";
		var client = getClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);
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
		// Source Object Check
		var sourceResponse = client
				.getObject(g -> g.bucket(sourceBucketName).key(sourceKey));
		var sourceBody = getBody(sourceResponse);
		if (sourceObjectEncryption || sourceBucketEncryption || config.isAWS())
			assertEquals(ServerSideEncryption.AES256, sourceResponse.response().serverSideEncryption());
		else
			assertNull(sourceResponse.response().serverSideEncryption());
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
		if (targetBucketEncryption || targetObjectEncryption || config.isAWS())
			assertEquals(ServerSideEncryption.AES256, targetResponse.response().serverSideEncryption());
		else
			assertNull(targetResponse.response().serverSideEncryption());
		assertTrue(sourceBody.equals(targetBody), MainData.NOT_MATCHED);
	}

	public void testObjectCopy(EncryptionType source, EncryptionType target, int size) {
		var sourceKey = "sourceKey";
		var targetKey = "targetKey";
		var client = getClientHttps(true);
		var bucketName = createBucket(client);
		var data = Utils.randomTextToLong(size);

		// Source Put Object
		var sourcePutRequest = PutObjectRequest.builder().bucket(bucketName).key(sourceKey);
		var sourceGetRequest = GetObjectRequest.builder().bucket(bucketName).key(sourceKey);
		var targetGetRequest = GetObjectRequest.builder().bucket(bucketName).key(targetKey);
		var copyRequest = CopyObjectRequest.builder().sourceBucket(bucketName).sourceKey(sourceKey)
				.destinationBucket(bucketName).destinationKey(targetKey).metadataDirective(MetadataDirective.REPLACE);

		// Source Options
		switch (source) {
			case SSE_S3:
				sourcePutRequest.serverSideEncryption(ServerSideEncryption.AES256);
				break;
			case SSE_C:
				sourcePutRequest.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY)
						.sseCustomerKeyMD5(SSE_KEY_MD5);
				sourceGetRequest.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY);
				copyRequest.copySourceSSECustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).copySourceSSECustomerKey(SSE_KEY)
						.copySourceSSECustomerKeyMD5(SSE_KEY_MD5);
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
				copyRequest.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY)
						.sseCustomerKeyMD5(SSE_KEY_MD5);
				targetGetRequest.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY);
				break;
			case NORMAL:
				copyRequest.metadata(null);
				break;
		}

		// Source Put Object
		client.putObject(sourcePutRequest.build(), RequestBody.fromString(data));
		client.putObject(p -> p.bucket(bucketName).key("temp"), RequestBody.fromString(data));

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
			kk.put(Temp.grantee().id() + Temp.permission().toString(), Temp);
		}

		for (Map.Entry<String, Grant> entry : kk.entrySet()) {
			newList.add(entry.getValue());
		}
		return newList;
	}

	public void checkAcl(AccessControlPolicy expected, GetBucketAclResponse actual) {
		checkAcl(expected,
				AccessControlPolicy.builder().owner(actual.owner()).grants(actual.grants()).build());
	}

	public void checkAcl(AccessControlPolicy expected, GetObjectAclResponse actual) {
		checkAcl(expected,
				AccessControlPolicy.builder().owner(actual.owner()).grants(actual.grants()).build());
	}

	void checkAcl(AccessControlPolicy expected, AccessControlPolicy actual) {
		if (config.isAWS())
			assertEquals(expected.owner().id(), actual.owner().id());
		else
			assertEquals(expected.owner(), actual.owner());

		var expectedGrants = expected.grants();
		var actualGrants = actual.grants();

		assertEquals(expectedGrants.size(), actualGrants.size());

		expectedGrants = grantsSort(expectedGrants);
		actualGrants = grantsSort(actualGrants);

		for (int i = 0; i < expectedGrants.size(); i++) {
			assertEquals(expectedGrants.get(i).permission(), actualGrants.get(i).permission());
			assertEquals(expectedGrants.get(i).grantee().id(), actualGrants.get(i).grantee().id());
			assertEquals(expectedGrants.get(i).grantee().type(), actualGrants.get(i).grantee().type());
		}
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

	public void checkBucketAcl(Permission permission) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		var acl = createAltAcl(permission);

		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl));

		var response = client.getBucketAcl(g -> g.bucket(bucketName));
		checkAcl(acl, response);
	}

	public void checkObjectAcl(Permission permission) {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		var key = "testObjectPermission" + permission;
		var acl = createAcl(config.mainUser.toOwnerV2(), config.mainUser.toGranteeV2(), permission);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).accessControlPolicy(acl));

		var response = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		checkAcl(acl, response);
	}

	public static void checkBucketAclAllowRead(S3Client client, String bucketName) {
		client.headBucket(h -> h.bucket(bucketName));
	}

	public static void checkBucketAclDenyRead(S3Client client, String bucketName) {
		var e = assertThrows(AwsServiceException.class, () -> client.headBucket(h -> h.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	public static void checkBucketAclAllowReadACP(S3Client client, String bucketName) {
		client.getBucketAcl(g -> g.bucket(bucketName));
	}

	public static void checkBucketAclDenyReadACP(S3Client client, String bucketName) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketAcl(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	public static void checkBucketAclAllowWrite(S3Client client, String bucketName) {
		var key = "checkBucketAclAllowWrite";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		client.deleteObject(d -> d.bucket(bucketName).key(key));
	}

	public static void checkBucketAclDenyWrite(S3Client client, String bucketName) {
		var key = "checkBucketAclDenyWrite";
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	public static void checkBucketAclAllowWriteACP(S3Client client, String bucketName) {
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE));
	}

	public static void checkBucketAclDenyWriteACP(S3Client client, String bucketName) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	public static void checkObjectAclAllowRead(S3Client client, String bucketName, String key) {
		client.getObject(g -> g.bucket(bucketName).key(key));
	}

	public static void checkObjectAclDenyRead(S3Client client, String bucketName, String key) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	public static void checkObjectAclAllowReadACP(S3Client client, String bucketName, String key) {
		client.getObjectAcl(g -> g.bucket(bucketName).key(key));
	}

	public static void checkObjectAclDenyReadACP(S3Client client, String bucketName, String key) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObjectAcl(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	public static void checkObjectAclAllowWrite(S3Client client, String bucketName, String key) {
		var tagging = Tag.builder().key("foo").value("bar").build();
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(t -> t.tagSet(tagging)));
	}

	public static void checkObjectAclDenyWrite(S3Client client, String bucketName, String key) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	public static void checkObjectAclAllowWriteACP(S3Client client, String bucketName, String key) {
		client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ_WRITE));
	}

	public static void checkObjectAclDenyWriteACP(S3Client client, String bucketName, String key) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
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
			if (e.abortIncompleteMultipartUpload() != null)
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
		var newList = new ArrayList<Tag>();

		Comparator<String> comparator = (s1, s2) -> s2.compareTo(s1);
		var kk = new TreeMap<String, Tag>(comparator);

		for (var Temp : tags) {
			kk.put(Temp.key() + Temp.value(), Temp);
		}

		for (Map.Entry<String, Tag> entry : kk.entrySet()) {
			newList.add(entry.getValue());
		}
		return newList;
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
			var url = createURL(bucketName);
			if (key != null)
				url = createURL(bucketName, key);

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

	public static void partsETagCompare(List<CompletedPart> expected, List<Part> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).partNumber(), actual.get(i).partNumber());
			assertEquals(expected.get(i).eTag().replace("\"", ""), actual.get(i).eTag().replace("\"", ""));
		}
	}

	public static void versionIDsCompare(List<ObjectVersion> expected, List<ObjectVersion> actual) {
		assertEquals(expected.size(), actual.size());

		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i).versionId(), actual.get(i).versionId());
			assertEquals(expected.get(i).eTag(), actual.get(i).eTag());
			assertEquals(expected.get(i).key(), actual.get(i).key());
			assertEquals(expected.get(i).size(), actual.get(i).size());
		}
	}

	public static void s3eventCompare(List<Event> expected, List<Event> actual) {
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i), actual.get(i));
		}
	}

	public static void checksumCompare(ChecksumAlgorithm algorithm, String content, PutObjectResponse response) {
		String expected = CheckSum.calculateChecksum(algorithm, content);

		String actual = switch (algorithm) {
			case CRC32 -> response.checksumCRC32();
			case CRC32_C -> response.checksumCRC32C();
			case CRC64_NVME -> response.checksumCRC64NVME();
			case SHA1 -> response.checksumSHA1();
			case SHA256 -> response.checksumSHA256();
			default -> throw new IllegalArgumentException("Invalid algorithm: " + algorithm);
		};
		assertEquals(expected, actual);
	}

	public static void checksumCompare(ChecksumAlgorithm algorithm, String content, CopyObjectResponse response) {
		String expected = CheckSum.calculateChecksum(algorithm, content);

		String actual = switch (algorithm) {
			case CRC32 -> response.copyObjectResult().checksumCRC32();
			case CRC32_C -> response.copyObjectResult().checksumCRC32C();
			case CRC64_NVME -> response.copyObjectResult().checksumCRC64NVME();
			case SHA1 -> response.copyObjectResult().checksumSHA1();
			case SHA256 -> response.copyObjectResult().checksumSHA256();
			default -> throw new IllegalArgumentException("Invalid algorithm: " + algorithm);
		};
		assertEquals(expected, actual);
	}

	public static void checksumCompare(ChecksumAlgorithm algorithm, String content, UploadPartResponse response) {
		String expected = CheckSum.calculateChecksum(algorithm, content);

		String actual = switch (algorithm) {
			case CRC32 -> response.checksumCRC32();
			case CRC32_C -> response.checksumCRC32C();
			case CRC64_NVME -> response.checksumCRC64NVME();
			case SHA1 -> response.checksumSHA1();
			case SHA256 -> response.checksumSHA256();
			default -> throw new IllegalArgumentException("Invalid algorithm: " + algorithm);
		};
		assertEquals(expected, actual);
	}

	public static void checksumCompare(ChecksumAlgorithm algorithm, MultipartUploadV2Data uploadData,
			CompleteMultipartUploadResponse response) {
		var contents = uploadData.parts.stream()
				.map(part -> switch (algorithm) {
					case CRC32 -> part.checksumCRC32();
					case CRC32_C -> part.checksumCRC32C();
					case CRC64_NVME -> part.checksumCRC64NVME();
					case SHA1 -> part.checksumSHA1();
					case SHA256 -> part.checksumSHA256();
					default -> null;
				}).toList();

		String expected = response.checksumType() == ChecksumType.COMPOSITE
				? CheckSum.calculateChecksumByBase64(algorithm, contents)
				: CheckSum.combineChecksumByBase64(algorithm, uploadData.partSize, contents);

		String actual = switch (algorithm) {
			case CRC32 -> response.checksumCRC32();
			case CRC32_C -> response.checksumCRC32C();
			case CRC64_NVME -> response.checksumCRC64NVME();
			case SHA1 -> response.checksumSHA1();
			case SHA256 -> response.checksumSHA256();
			default -> null;
		};

		var message = String.format("%s 체크섬 비교 실패", algorithm);
		assertEquals(expected, actual, message);
	}
	// endregion

	// region Backend Utils

	/**
	 * Backend 클라이언트로 다운로드하여 바로 PutObject로 업로드
	 * 
	 * @param client           Backend 클라이언트
	 * @param sourceBucketName 소스 버킷 이름
	 * @param sourceKey        소스 키
	 * @param targetBucketName 타겟 버킷 이름
	 * @param targetKey        타겟 키
	 * @param versionId        버전 ID
	 */
	public static void backendPutObject(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, String versionId) {

		// Backend 클라이언트로 다운로드
		var response = client.getObject(g -> g.bucket(sourceBucketName).key(sourceKey).versionId(versionId));
		var contentLength = response.response().contentLength();
		var backendBody = RequestBody.fromInputStream(response, contentLength);

		// 메타데이터 복사
		var userMetadata = new java.util.HashMap<String, String>();
		java.util.Map<String, java.util.List<String>> headers = null;

		// User 메타데이터 복사
		if (response.response().metadata() != null)
			userMetadata.putAll(response.response().metadata());

		// Header 복사
		if (response.response().sdkHttpResponse().headers() != null)
			headers = response.response().sdkHttpResponse().headers();

		// 리퀘스트 생성
		var putRequestConfig = AwsRequestOverrideConfiguration.builder()
				.putHeader(BackendHeaders.IFS_VERSION_ID, versionId)
				.putHeader(BackendHeaders.KSAN_VERSION_ID, versionId);

		// Header 복사
		if (headers != null) {
			for (var k : headers.entrySet()) {
				var header = k.getKey();
				var value = k.getValue().get(0);
				if (value == null)
					value = "";
				// UTF-8 sign 에러를 배제하기 위해 대문자로 변경
				if (k.getKey().equalsIgnoreCase("content-type"))
					value = value.replace("UTF-8", "utf-8");
				putRequestConfig.putHeader(header, value);
			}
		}

		// Backend 클라이언트로 업로드
		client.putObject(p -> p.bucket(targetBucketName).key(targetKey)
				.overrideConfiguration(putRequestConfig.build()), backendBody);
		try {
			response.close();
		} catch (IOException e) {
			fail("Response Close Failed");
		}
	}

	/**
	 * Backend 클라이언트로 복사
	 * 
	 * @param client           Backend 클라이언트
	 * @param sourceBucketName 소스 버킷 이름
	 * @param sourceKey        소스 키
	 * @param targetBucketName 타겟 버킷 이름
	 * @param targetKey        타겟 키
	 * @param sourceVersionId  소스 버전 ID
	 * @param targetVersionId  타겟 버전 ID
	 */
	public static void backendCopyObject(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, String sourceVersionId, String targetVersionId) {
		// Backend 클라이언트로 복사
		client.copyObject(c -> c
				.sourceBucket(sourceBucketName)
				.sourceKey(sourceKey)
				.destinationBucket(targetBucketName)
				.destinationKey(targetKey)
				.sourceVersionId(sourceVersionId)
				.overrideConfiguration(o -> o.putHeader(BackendHeaders.IFS_VERSION_ID, targetVersionId)
						.putHeader(BackendHeaders.KSAN_VERSION_ID, targetVersionId)));
	}

	/**
	 * Backend 클라이언트로 멀티파트 업로드
	 * 
	 * @param client           Backend 클라이언트
	 * @param sourceBucketName 소스 버킷 이름
	 * @param sourceKey        소스 키
	 * @param targetBucketName 타겟 버킷 이름
	 * @param targetKey        타겟 키
	 * @param versionId        버전 ID
	 * @return
	 */
	public static void backendMultipartUpload(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, String versionId) {
		var partsSize = 5 * MainData.MB;
		// 메타 정보 가져오기
		var metadata = client.headObject(g -> g.bucket(sourceBucketName).key(sourceKey).versionId(versionId));

		var initRequestConfig = AwsRequestOverrideConfiguration.builder();

		// Header 복사
		if (metadata.sdkHttpResponse().headers() != null) {

			for (var k : metadata.sdkHttpResponse().headers().entrySet()) {
				var header = k.getKey();
				var value = k.getValue().get(0);
				if (value == null)
					value = "";

				// UTF-8 sign 에러를 배제하기 위해 대문자로 변경
				if (k.getKey().equalsIgnoreCase("content-type"))
					value = value.replace("UTF-8", "utf-8");
				// content-length는 자동으로 설정되므로 제외
				if (k.getKey().equalsIgnoreCase("content-length"))
					continue;
				initRequestConfig.putHeader(header, value);
			}
		}

		// Multipart 등록
		var initResponse = client.createMultipartUpload(c -> c
				.bucket(targetBucketName)
				.key(targetKey)
				.metadata(metadata.metadata())
				.overrideConfiguration(initRequestConfig.build()));
		var uploadId = initResponse.uploadId();
		final String finalUploadId = uploadId;

		// 오브젝트의 사이즈 확인
		var size = metadata.contentLength();

		// 업로드 시작
		var partList = new ArrayList<CompletedPart>();
		// int partNumber = 1;
		long index = 0;

		while (index < size) {
			var start = index;
			var partNumber = partList.size() + 1;
			var end = Math.min(start + partsSize, size) - 1L;
			var partSize = end - start + 1;

			// 업로드할 내용 가져오기
			var s3Object = client.getObject(
					g -> g.bucket(sourceBucketName).key(sourceKey).versionId(versionId)
							.range("bytes=" + start + "-" + end));
			var body = RequestBody.fromInputStream(s3Object, s3Object.response().contentLength());

			// 업로드 파츠
			var partResponse = client.uploadPart(u -> u.bucket(targetBucketName).key(targetKey)
					.uploadId(finalUploadId).partNumber(partNumber), body);
			partList.add(CompletedPart.builder().partNumber(partNumber).eTag(partResponse.eTag()).build());

			index += partSize;
			try {
				s3Object.close();
			} catch (IOException e) {
				fail("Response Close Failed");
			}
		}

		// 헤더추가
		var compRequestConfig = AwsRequestOverrideConfiguration.builder()
				.putHeader(BackendHeaders.IFS_VERSION_ID, versionId)
				.putHeader(BackendHeaders.KSAN_VERSION_ID, versionId);

		// 업로드 완료 요청
		client.completeMultipartUpload(
				c -> c.bucket(targetBucketName).key(targetKey).uploadId(finalUploadId)
						.multipartUpload(p -> p.parts(partList)).overrideConfiguration(compRequestConfig.build()));
	}

	/**
	 * Backend 클라이언트로 ACL 설정
	 * 
	 * @param client           Backend 클라이언트
	 * @param sourceBucketName 소스 버킷 이름
	 * @param sourceKey        소스 키
	 * @param targetBucketName 타겟 버킷 이름
	 * @param targetKey        타겟 키
	 * @param versionId        버전 ID
	 */
	public static void backendPutObjectAcl(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, String versionId) {
		// ACL 정보 가져오기
		var acl = client.getObjectAcl(g -> g.bucket(sourceBucketName).key(sourceKey).versionId(versionId));

		// ACL 설정
		client.putObjectAcl(p -> p
				.bucket(targetBucketName)
				.key(targetKey)
				.versionId(versionId)
				.accessControlPolicy(a -> a.owner(acl.owner()).grants(acl.grants())));
	}

	/**
	 * Backend 클라이언트로 태그 설정
	 * 
	 * @param client           Backend 클라이언트
	 * @param sourceBucketName 소스 버킷 이름
	 * @param sourceKey        소스 키
	 * @param targetBucketName 타겟 버킷 이름
	 * @param targetKey        타겟 키
	 * @param versionId        버전 ID
	 */
	public static void backendPutObjectTagging(S3Client client, String sourceBucketName, String sourceKey,
			String targetBucketName, String targetKey, String versionId) {
		// 태그 정보 가져오기
		var tagging = client.getObjectTagging(g -> g.bucket(sourceBucketName).key(sourceKey).versionId(versionId));

		// 태그 설정
		client.putObjectTagging(p -> p.bucket(targetBucketName).key(targetKey).versionId(versionId)
				.tagging(Tagging.builder().tagSet(tagging.tagSet()).build()));
	}

	/**
	 * Backend 클라이언트로 삭제
	 * 
	 * @param client     Backend 클라이언트
	 * @param bucketName 버킷 이름
	 * @param key        Object 키
	 * @param versionId  버전 ID
	 */
	public static void backendDeleteObject(S3Client client, String bucketName, String key, String versionId) {
		client.putObject(p -> p
				.bucket(bucketName)
				.key(key)
				.overrideConfiguration(o -> o
						.putHeader(BackendHeaders.DELETE_MARKER_VERSION_ID, versionId)
						.putHeader(BackendHeaders.KSAN_DELETE_MARKER_VERSION_ID, versionId)),
				RequestBody.empty());
	}

	/**
	 * Backend 클라이언트로 태그 삭제
	 * 
	 * @param client     Backend 클라이언트
	 * @param bucketName 버킷 이름
	 * @param key        Object 키
	 * @param versionId  버전 ID
	 */
	public static void backendDeleteObjectTagging(S3Client client, String bucketName, String key, String versionId) {
		client.deleteObjectTagging(d -> d.bucket(bucketName).key(key).versionId(versionId));
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

	public Instant getExpiredDate(Instant day, int days) {
		Instant cal = day.plus(Duration.ofDays(days));

		if (config.isAWS()) {
			cal = cal.plus(Duration.ofDays(1));
			// 시, 분, 초를 0으로 설정
			cal = cal.truncatedTo(ChronoUnit.DAYS);
		}
		return cal;
	}
	// endregion

}
