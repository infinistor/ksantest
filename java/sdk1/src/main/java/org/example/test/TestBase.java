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
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
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
	};

	/************************************************************************************************************/
	private static final int RANDOM_PREFIX_TEXT_LENGTH = 15;
	private static final int RANDOM_SUFFIX_TEXT_LENGTH = 5;
	private static final int BUCKET_MAX_LENGTH = 63;
	private static final String STR_RANDOM = "{random}";
	/************************************************************************************************************/

	private final ArrayList<String> buckets = new ArrayList<String>();
	protected final S3Config config;

	protected TestBase() {
		String fileName = System.getenv(MainData.S3TESTS_INI);
		config = new S3Config(fileName);
		config.GetConfig();
	}

	public AmazonS3 createClient(boolean isSecure, UserData user, Boolean useChunkEncoding, Boolean payloadSigning,
			String signatureVersion) {
		String address = "";
		ClientConfiguration s3Config;

		if (isSecure) {
			address = NetUtils.createURLToHTTPS(config.URL, config.sslPort);
			System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
			s3Config = new ClientConfiguration().withProtocol(Protocol.HTTPS).withSignerOverride(signatureVersion);
		} else {
			address = NetUtils.createURLToHTTP(config.URL, config.port);
			s3Config = new ClientConfiguration().withProtocol(Protocol.HTTP).withSignerOverride(signatureVersion);
		}
		s3Config.setSignerOverride(signatureVersion);
		var clientBuilder = AmazonS3ClientBuilder.standard();

		if (user == null)
			clientBuilder.setCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
		else
			clientBuilder.setCredentials(
					new AWSStaticCredentialsProvider(new BasicAWSCredentials(user.accessKey, user.secretKey)));

		if (StringUtils.isBlank(config.URL))
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
		return createClient(config.isSecure, config.mainUser, true, true, S3Config.STR_SIGNATUREVERSION_V2);
	}

	public AmazonS3 getClientV4(Boolean useChunkEncoding) {
		return createClient(config.isSecure, config.mainUser, useChunkEncoding, true, S3Config.STR_SIGNATUREVERSION_V4);
	}

	public AmazonS3 getClientHttps() {
		return createClient(true, config.mainUser, true, true, config.getSignatureVersion());
	}

	public AmazonS3 getClientHttpsV4(Boolean useChunkEncoding, Boolean payloadSigning) {
		return createClient(true, config.mainUser, useChunkEncoding, payloadSigning, S3Config.STR_SIGNATUREVERSION_V4);
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

	/******************************************
	 * Create Data
	 *******************************************************/

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
		client.createBucket(bucketName);
		return bucketName;
	}

	public String getNewBucket(String prefix) {
		var bucketName = getNewBucketName(prefix);
		var client = getClient();
		client.createBucket(bucketName);
		return bucketName;
	}

	public String createObjects(ArrayList<String> keys) {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null && client != null) {
			for (var key : keys)
				client.putObject(bucketName, key, key);
		}

		return bucketName;
	}

	public String createObjectsToBody(ArrayList<String> keys, String body) {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null && client != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjectsV2(ArrayList<String> keys) {
		var client = getClientV2();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null && client != null) {
			for (var key : keys) {
				var body = key;
				if (key.endsWith("/"))
					body = "";
				client.putObject(bucketName, key, body);
			}
		}

		return bucketName;
	}

	public String createObjectsToBodyV2(ArrayList<String> keys, String body) {
		var client = getClientV2();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null && client != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjectsV4(ArrayList<String> keys, Boolean useChunkEncoding) {
		var client = getClientV4(useChunkEncoding);
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null && client != null) {
			for (var key : keys) {
				var body = key;
				if (key.endsWith("/"))
					body = "";
				client.putObject(bucketName, key, body);
			}
		}

		return bucketName;
	}

	public String createObjectsHttps(ArrayList<String> keys, Boolean useChunkEncoding, Boolean payloadSigning) {
		var client = getClientHttpsV4(useChunkEncoding, payloadSigning);
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null && client != null) {
			for (var key : keys)
				client.putObject(bucketName, key, key);
		}

		return bucketName;
	}

	public String createObjectsToBodyV4(ArrayList<String> keys, String body, Boolean useChunkEncoding) {
		var client = getClientV4(useChunkEncoding);
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		if (keys != null && client != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjectsToBody(String bucketName, ArrayList<String> keys, String body) {
		var client = getClient();

		if (keys != null && client != null) {
			for (var key : keys)
				client.putObject(bucketName, key, body);
		}

		return bucketName;
	}

	public String createObjects(ArrayList<String> keys, String bucketName) {
		var client = getClient();
		if (StringUtils.isBlank(bucketName)) {
			bucketName = getNewBucketName();
			client.createBucket(bucketName);
		}

		if (keys != null && client != null) {
			for (var key : keys)
				client.putObject(bucketName, key, key);
		}

		return bucketName;
	}

	public URL getURL(String bucketName) throws MalformedURLException {
		var protocol = config.isSecure ? MainData.HTTPS : MainData.HTTP;
		var port = config.isSecure ? config.sslPort : config.port;

		return config.isAWS() ? NetUtils.getEndPoint(protocol, config.regionName, bucketName)
				: NetUtils.getEndPoint(protocol, config.URL, port, bucketName);
	}

	public URL getURL(String bucketName, String key) throws MalformedURLException {
		var Protocol = config.isSecure ? MainData.HTTPS : MainData.HTTP;
		var Port = config.isSecure ? config.sslPort : config.port;

		return config.isAWS() ? NetUtils.getEndPoint(Protocol, config.regionName, bucketName, key)
				: NetUtils.getEndPoint(Protocol, config.URL, Port, bucketName, key);
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

		var Response = client.getObject(bucketName, key);
		return Response.getObjectMetadata().getUserMetaDataOf(metadataKey);
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

		var Response = client.getBucketAcl(bucketName);

		var getGrants = Response.getGrantsAsList();
		var myGrants = new ArrayList<Grant>();
		myGrants.add(new Grant(mainUser, Permission.FullControl));
		myGrants.add(new Grant(altUser, permission));
		checkGrants(myGrants, new ArrayList<Grant>(getGrants));

		return bucketName;
	}

	public String setupAccessTest(String key1, String key2, CannedAccessControlList bucketACL,
			CannedAccessControlList objectACL) {
		var bucketName = getNewBucket();
		var client = getClient();

		key1 = "foo";
		key2 = "bar";

		client.setBucketAcl(bucketName, bucketACL);
		client.putObject(bucketName, key1, key1);
		client.setObjectAcl(bucketName, key1, objectACL);
		client.putObject(bucketName, key2, key2);

		return bucketName;
	}

	public void checkObjContent(AmazonS3 client, String bucketName, String key, String versionId, String content) {
		var response = client.getObject(new GetObjectRequest(bucketName, key).withVersionId(versionId));
		if (content != null) {
			var Body = getBody(response.getObjectContent());
			assertTrue(content.equals(Body), MainData.NOT_MATCHED);
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

	public void CreateMultipleVersions(AmazonS3 client, String bucketName, String key, int numVersions,
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

	/*****************************************
	 * Get Data
	 **********************************************************/

	public ArrayList<String> GetObjectList(String bucketName, String Prefix) {
		var client = getClient();
		var Response = client.listObjects(bucketName, Prefix);
		return GetKeys(Response.getObjectSummaries());
	}

	public static ArrayList<String> GetKeys(List<S3ObjectSummary> ObjectList) {
		if (ObjectList != null) {
			var Temp = new ArrayList<String>();

			for (var S3Object : ObjectList)
				Temp.add(S3Object.getKey());

			return Temp;
		}
		return null;
	}

	public static String getBody(S3ObjectInputStream Data) {
		String Body = "";
		if (Data != null) {
			try {
				ByteArrayOutputStream result = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				for (int length; (length = Data.read(buffer)) != -1;) {
					result.write(buffer, 0, length);
				}
				return result.toString("UTF-8");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return Body;
	}

	public static ObjectData GetObjectToKey(String key, ArrayList<ObjectData> KeyList) {
		for (var Object : KeyList) {
			if (Object.key.equals(key))
				return Object;
		}
		return null;
	}

	public ArrayList<String> GetBucketList(List<Bucket> Response) {
		if (Response == null)
			return null;
		var BucketList = new ArrayList<String>();

		for (var Item : Response)
			BucketList.add(Item.getName());

		return BucketList;
	}

	public void checkVersioning(String bucketName, String StatusCode) {
		var client = getClient();

		var Response = client.getBucketVersioningConfiguration(bucketName);
		assertEquals(StatusCode, Response.getStatus());
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

	/*****************************************
	 * Auto
	 **********************************************************/
	public void ACLTest(String bucketName, String key, AmazonS3 client, Boolean Pass) {
		if (Pass) {
			var Response = client.getObject(bucketName, key);
			assertEquals(key, Response.getKey());
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
		var Response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(Delimiter)
				.withMarker(Marker).withMaxKeys(MaxKeys).withPrefix(Prefix));

		assertEquals(IsTruncated, Response.isTruncated());
		assertEquals(NextMarker, Response.getNextMarker());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(CheckKeys.size(), Keys.size());
		assertEquals(checkPrefixes.size(), Prefixes.size());
		assertEquals(CheckKeys, Keys);
		assertEquals(checkPrefixes, Prefixes);

		return Response.getNextMarker();
	}

	public String ValidateListObjectV2(String bucketName, String Prefix, String Delimiter, String ContinuationToken,
			int MaxKeys, boolean IsTruncated, List<String> CheckKeys, List<String> CheckPrefixes, boolean Last) {
		var client = getClient();
		var Response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(Delimiter)
						.withContinuationToken(ContinuationToken).withMaxKeys(MaxKeys).withPrefix(Prefix));

		assertEquals(IsTruncated, Response.isTruncated());
		if (Last)
			assertNull(Response.getNextContinuationToken());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(CheckKeys, Keys);
		assertEquals(CheckPrefixes, Prefixes);
		assertEquals(CheckKeys.size(), Keys.size());
		assertEquals(CheckPrefixes.size(), Prefixes.size());

		return Response.getNextContinuationToken();
	}

	public void CheckBadBucketName(String bucketName) {
		var client = getClient();

		assertThrows(IllegalBucketNameException.class, () -> client.createBucket(bucketName));
	}

	public void CheckGoodBucketName(String Name, String Prefix) {
		if (StringUtils.isBlank(Prefix))
			Prefix = getPrefix();

		var bucketName = String.format("%s%s", Prefix, Name);
		buckets.add(bucketName);

		var client = getClient();
		var Response = client.createBucket(bucketName);
		assertTrue(StringUtils.isNotBlank(Response.getName()));
	}

	public void TestBucketCreateNamingGoodLong(int Length) {
		var bucketName = getPrefix();
		if (bucketName.length() < Length)
			bucketName += Utils.randomText(Length - bucketName.length());
		else
			bucketName = bucketName.substring(0, Length - 1);
		buckets.add(bucketName);

		var client = getClient();
		var Response = client.createBucket(bucketName);
		assertTrue(StringUtils.isNotBlank(Response.getName()));
	}

	public HttpResponse GetObject(URL Address) {
		HttpClient client = HttpClientBuilder.create().build();
		HttpGet getRequest = new HttpGet(Address.toString());
		try {
			HttpResponse httpResponse = client.execute(getRequest);
			return httpResponse;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public HttpResponse PutObject(URL Address, String Body) {
		HttpClient client = HttpClientBuilder.create().build();
		HttpPut getRequest = new HttpPut(Address.toString());
		if (Body != null) {
			StringEntity requestEntity = new StringEntity(Body, "utf-8");
			requestEntity.setContentType(new BasicHeader("Content-Type", "application/txt"));
			getRequest.setEntity(requestEntity);
		}
		try {
			return client.execute(getRequest);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public MultipartUploadData MultipartUpload(AmazonS3 client, String bucketName, String key, int Size,
			MultipartUploadData UploadData) {
		var PartSize = 5 * MainData.MB;
		var Parts = Utils.generateRandomString(Size, PartSize);

		for (var Part : Parts) {
			UploadData.appendBody(Part);
			var PartResPonse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(UploadData.uploadId)
							.withPartNumber(UploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			UploadData.addPart(PartResPonse.getPartETag());
		}

		return UploadData;
	}

	public MultipartUploadData setupMultipartUpload(AmazonS3 client, String bucketName, String key, int Size,
			ObjectMetadata MetadataList) {
		var UploadData = new MultipartUploadData();
		if (MetadataList == null) {
			MetadataList = new ObjectMetadata();
			MetadataList.setContentType("text/plain");
		}
		MetadataList.setContentLength(Size);

		var InitMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, MetadataList));
		UploadData.uploadId = InitMultiPartResponse.getUploadId();

		var Parts = Utils.generateRandomString(Size, UploadData.partSize);

		for (var Part : Parts) {
			UploadData.appendBody(Part);

			var PartResPonse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(UploadData.uploadId)
							.withPartNumber(UploadData.nextPartNumber())
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			UploadData.parts.add(PartResPonse.getPartETag());
		}

		return UploadData;
	}

	public MultipartUploadData MultipartUploadResend(AmazonS3 client, String bucketName, String key, int Size,
			ObjectMetadata MetadataList, ArrayList<Integer> ResendParts) {
		var UploadData = new MultipartUploadData();
		if (MetadataList == null) {
			MetadataList = new ObjectMetadata();
			MetadataList.setContentType("text/plain");
		}
		MetadataList.setContentLength(Size);

		var InitMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, MetadataList));
		UploadData.uploadId = InitMultiPartResponse.getUploadId();

		var Parts = Utils.generateRandomString(Size, UploadData.partSize);

		for (var Part : Parts) {
			UploadData.appendBody(Part);
			int PartNumber = UploadData.nextPartNumber();

			var PartResPonse = client.uploadPart(
					new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key)
							.withUploadId(UploadData.uploadId)
							.withPartNumber(PartNumber)
							.withInputStream(createBody(Part))
							.withPartSize(Part.length()));
			UploadData.parts.add(PartResPonse.getPartETag());

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

		var InitMultiPartResponse = client.initiateMultipartUpload(InitRequest);
		UploadData.uploadId = InitMultiPartResponse.getUploadId();

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

			var Response = client.uploadPart(Request);
			UploadData.parts.add(Response.getPartETag());
		}
		return UploadData;
	}

	public void CheckCopyContent(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey) {
		var client = getClient();

		var Response = client.getObject(new GetObjectRequest(SourceBucketName, SourceKey));
		var SourceSize = Response.getObjectMetadata().getContentLength();
		var SourceData = getBody(Response.getObjectContent());

		Response = client.getObject(TargetBucketName, TargetKey);
		var TargetSize = Response.getObjectMetadata().getContentLength();
		var TargetData = getBody(Response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void CheckCopyContent(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey,
			String versionId) {
		var client = getClient();

		var Response = client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withVersionId(versionId));
		var SourceSize = Response.getObjectMetadata().getContentLength();
		var SourceData = getBody(Response.getObjectContent());

		Response = client.getObject(TargetBucketName, TargetKey);
		var TargetSize = Response.getObjectMetadata().getContentLength();
		var TargetData = getBody(Response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void CheckCopyContentSSE_C(AmazonS3 client, String SourceBucketName, String SourceKey,
			String TargetBucketName, String TargetKey, SSECustomerKey SSE_C) {

		var SourceResponse = client
				.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withSSECustomerKey(SSE_C));
		var SourceSize = SourceResponse.getObjectMetadata().getContentLength();
		var SourceData = getBody(SourceResponse.getObjectContent());

		var TargetResponse = client
				.getObject(new GetObjectRequest(TargetBucketName, TargetKey).withSSECustomerKey(SSE_C));
		var TargetSize = TargetResponse.getObjectMetadata().getContentLength();
		var TargetData = getBody(TargetResponse.getObjectContent());

		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void CheckCopyContentUsingRange(String SourceBucketName, String SourceKey, String TargetBucketName,
			String TargetKey) {
		var client = getClient();

		var Response = client.getObject(new GetObjectRequest(TargetBucketName, TargetKey));
		var TargetSize = Response.getObjectMetadata().getContentLength();
		var TargetData = getBody(Response.getObjectContent());

		Response = client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withRange(0, TargetSize - 1));
		var SourceSize = Response.getObjectMetadata().getContentLength();
		var SourceData = getBody(Response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), MainData.NOT_MATCHED);
	}

	public void checkCopyContentUsingRange(String SourceBucketName, String SourceKey, String TargetBucketName,
			String TargetKey, int Step) {
		var client = getClient();

		var Response = client.getObjectMetadata(SourceBucketName, SourceKey);
		var Size = Response.getContentLength();

		long StartPosition = 0;
		while (StartPosition < Size) {
			var EndPosition = StartPosition + Step;
			if (EndPosition > Size)
				EndPosition = Size - 1;

			var SourceResponse = client.getObject(
					new GetObjectRequest(SourceBucketName, SourceKey).withRange(StartPosition, EndPosition - 1));
			var SourceBody = getBody(SourceResponse.getObjectContent());
			var TargetResponse = client.getObject(
					new GetObjectRequest(SourceBucketName, SourceKey).withRange(StartPosition, EndPosition - 1));
			var TargetBody = getBody(TargetResponse.getObjectContent());

			assertEquals(SourceResponse.getObjectMetadata().getContentLength(),
					TargetResponse.getObjectMetadata().getContentLength());
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

		var Response = client.getObjectMetadata(bucketName, key);
		assertEquals(ContentType, Response.getContentType());
		assertEquals(metadata.getUserMetadata(), Response.getUserMetadata());

		var Body = UploadData.getBody();
		checkContentUsingRange(bucketName, key, Body, MainData.MB);
		checkContentUsingRange(bucketName, key, Body, 10 * MainData.MB);
	}

	public String doTestMultipartUploadContents(String bucketName, String key, int NumParts) {
		var Payload = Utils.randomTextToLong(5 * MainData.MB);
		var client = getClient();

		var InitResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var UploadID = InitResponse.getUploadId();
		var Parts = new ArrayList<PartETag>();
		var AllPayload = "";

		for (int i = 0; i < NumParts; i++) {
			var PartNumber = i + 1;
			var PartResponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
					.withUploadId(UploadID).withPartNumber(PartNumber)
					.withInputStream(new ByteArrayInputStream(Payload.getBytes())).withPartSize(Payload.length()));
			Parts.add(new PartETag(PartNumber, PartResponse.getETag()));
			AllPayload += Payload;
		}
		var LestPayload = Utils.randomTextToLong(MainData.MB);
		var LestPartResponse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
				.withUploadId(UploadID).withPartNumber(NumParts + 1)
				.withInputStream(new ByteArrayInputStream(LestPayload.getBytes())).withPartSize(LestPayload.length()));
		Parts.add(new PartETag(NumParts + 1, LestPartResponse.getETag()));
		AllPayload += LestPayload;

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, UploadID, Parts));

		var Response = client.getObject(bucketName, key);
		var Text = getBody(Response.getObjectContent());

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

		var Response = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey));
		Data.uploadId = Response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartResPonse = client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
					.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
					.withUploadId(Data.uploadId).withPartNumber(UploadCount).withFirstByte(Start).withLastByte(End)
					.withSourceVersionId(versionId));
			Data.parts.add(new PartETag(UploadCount++, PartResPonse.getETag()));

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

		var Response = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey, metadata));
		Data.uploadId = Response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartResPonse = client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
					.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
					.withUploadId(Data.uploadId).withPartNumber(UploadCount).withFirstByte(Start).withLastByte(End));
			Data.parts.add(new PartETag(UploadCount++, PartResPonse.getETag()));

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

		var Response = client.initiateMultipartUpload(
				new InitiateMultipartUploadRequest(TargetBucketName, TargetKey, metadata).withSSECustomerKey(SSE_C));
		Data.uploadId = Response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartResPonse = client.copyPart(new CopyPartRequest()
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
			Data.parts.add(new PartETag(UploadCount++, PartResPonse.getETag()));

			Start = End + 1;
		}

		return Data;
	}

	public static long GetBytesUsed(ListObjectsV2Result Response) {
		if (Response == null)
			return 0;
		if (Response.getObjectSummaries() == null)
			return 0;
		if (Response.getObjectSummaries().size() > 1)
			return 0;

		long Size = 0;

		for (var Obj : Response.getObjectSummaries())
			Size += Obj.getSize();

		return Size;
	}

	public void checkContentUsingRange(String bucketName, String key, String Data, long Step) {
		var client = getClient();
		var GetResponse = client.getObjectMetadata(bucketName, key);
		var Size = GetResponse.getContentLength();
		assertEquals(Data.length(), Size);

		long StartPosition = 0;
		while (StartPosition < Size) {
			var EndPosition = StartPosition + Step;
			if (EndPosition > Size)
				EndPosition = Size - 1;

			var Response = client
					.getObject(new GetObjectRequest(bucketName, key).withRange(StartPosition, EndPosition - 1));
			var Body = getBody(Response.getObjectContent());
			var Length = EndPosition - StartPosition;
			var PartBody = Data.substring((int) StartPosition, (int) EndPosition);

			assertEquals(Length, Response.getObjectMetadata().getContentLength());
			assertTrue(PartBody.equals(Body), MainData.NOT_MATCHED);
			StartPosition += Step;
		}
	}

	public void CheckContentUsingRangeEnc(AmazonS3 client, String bucketName, String key, String Data, long Step,
			SSECustomerKey SSE_C) {
		if (client == null)
			client = getClient();
		var GetResponse = client
				.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key).withSSECustomerKey(SSE_C));
		var Size = GetResponse.getContentLength();

		long StartPosition = 0;
		while (StartPosition < Size) {
			var EndPosition = StartPosition + Step;
			if (EndPosition > Size)
				EndPosition = Size - 1;

			var Response = client.getObject(new GetObjectRequest(bucketName, key)
					.withRange(StartPosition, EndPosition - 1).withSSECustomerKey(SSE_C));
			var Body = getBody(Response.getObjectContent());
			var Length = EndPosition - StartPosition;
			var PartBody = Data.substring((int) StartPosition, (int) EndPosition);

			assertEquals(Length, Response.getObjectMetadata().getContentLength());
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
			var Response = client.getObject(bucketName, key);
			var Body = getBody(Response.getObjectContent());
			assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
		}
	}

	public void CheckContentEnc(String bucketName, String key, String Data, int LoopCount, SSECustomerKey SSE_C) {
		var client = getClientHttps();

		for (int i = 0; i < LoopCount; i++) {
			var Response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
			var Body = getBody(Response.getObjectContent());
			assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRange(String bucketName, String key, String Data, int LoopCount) {
		var client = getClient();
		int FileSize = Data.length();

		for (int i = 0; i < LoopCount; i++) {
			var Range = GetRandomRange(FileSize);
			var RangeBody = Data.substring(Range.start, Range.end + 1);

			var Response = client.getObject(new GetObjectRequest(bucketName, key).withRange(Range.start, Range.end));
			var Body = getBody(Response.getObjectContent());

			assertEquals(Range.length, Response.getObjectMetadata().getContentLength() - 1);
			assertTrue(RangeBody.equals(Body), MainData.NOT_MATCHED);
		}
	}

	public void checkContentUsingRandomRangeEnc(AmazonS3 client, String bucketName, String key, String Data,
			int FileSize, int LoopCount, SSECustomerKey SSE_C) {
		for (int i = 0; i < LoopCount; i++) {
			var Range = GetRandomRange(FileSize);

			var Response = client.getObject(new GetObjectRequest(bucketName, key).withRange(Range.start, Range.end - 1)
					.withSSECustomerKey(SSE_C));
			var Body = getBody(Response.getObjectContent());
			var RangeBody = Data.substring(Range.start, Range.end);

			assertEquals(Range.length, Response.getObjectMetadata().getContentLength());
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

			var Response = client.getObject(bucketName, key);
			var EncodingBody = getBody(Response.getObjectContent());
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
		var Data = Utils.randomTextToLong(FileSize);
		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(FileSize);
		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(Data), metadata).withSSECustomerKey(SSE_C));

		var Response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
		var Body = getBody(Response.getObjectContent());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
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

		var Response = client.getObject(bucketName, key);
		var Body = getBody(Response.getObjectContent());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
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

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSE_S3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var SourceKey = "bar";
		client.putObject(bucketName, SourceKey, Data);

		var SourceResponse = client.getObject(bucketName, SourceKey);
		var SourceBody = getBody(SourceResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), SourceResponse.getObjectMetadata().getSSEAlgorithm());

		var TargetKey = "foo";
		client.copyObject(bucketName, SourceKey, bucketName, TargetKey);
		var TargetResponse = client.getObject(bucketName, TargetKey);
		assertEquals(SSEAlgorithm.AES256.toString(), TargetResponse.getObjectMetadata().getSSEAlgorithm());

		var TargetBody = getBody(TargetResponse.getObjectContent());
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
		var SourceResponse = client.getObject(SourceBucketName, SourceKey);
		var SourceBody = getBody(SourceResponse.getObjectContent());
		if (SourceObjectEncryption)
			assertEquals(SSEAlgorithm.AES256.toString(), SourceResponse.getObjectMetadata().getSSEAlgorithm());
		else
			assertNull(SourceResponse.getObjectMetadata().getSSEAlgorithm());
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

			var EncryptionResponse = client.getBucketEncryption(SourceBucketName);
			assertEquals(SSE_S3Config.getRules(), EncryptionResponse.getServerSideEncryptionConfiguration().getRules());
		}

		// Target Bucket Encryption
		if (TargetBucketEncryption) {
			client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(TargetBucketName)
					.withServerSideEncryptionConfiguration(SSE_S3Config));

			var EncryptionResponse = client.getBucketEncryption(TargetBucketName);
			assertEquals(SSE_S3Config.getRules(), EncryptionResponse.getServerSideEncryptionConfiguration().getRules());
		}

		// Source Copy Object
		if (TargetObjectEncryption)
			client.copyObject(new CopyObjectRequest(SourceBucketName, SourceKey, TargetBucketName, TargetKey)
					.withNewObjectMetadata(metadata));
		else
			client.copyObject(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		// Target Object Check
		var TargetResponse = client.getObject(TargetBucketName, TargetKey);
		var TargetBody = getBody(TargetResponse.getObjectContent());
		if (TargetBucketEncryption || TargetObjectEncryption)
			assertEquals(SSEAlgorithm.AES256.toString(), TargetResponse.getObjectMetadata().getSSEAlgorithm());
		else
			assertNull(TargetResponse.getObjectMetadata().getSSEAlgorithm());
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
		var SourceResponse = client.getObject(SourceGetRequest);
		var SourceBody = getBody(SourceResponse.getObjectContent());
		assertTrue(data.equals(SourceBody), MainData.NOT_MATCHED);

		// Copy Object
		client.copyObject(CopyRequest);

		// Target Object Check
		var TargetResponse = client.getObject(TargetGetRequest);
		var TargetBody = getBody(TargetResponse.getObjectContent());
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
		var Response = client.getObjectAcl(bucketName, key);
		var Policy = new AccessControlList();
		Policy.setOwner(Response.getOwner());
		Policy.grantPermission(Response.getGrantsAsList().get(0).getGrantee(), permission);

		client.setObjectAcl(bucketName, key, Policy);

		Response = client.getObjectAcl(bucketName, key);
		var GetGrants = Response.getGrantsAsList();

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
			var Response = client.putObject(bucketName, key, Body);
			var versionId = Response.getVersionId();

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
			var Response = client.putObject(bucketName, key, Body);
			var versionId = Response.getVersionId();

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
			var Response = client.putObject(bucketName, key, Body);
			var versionId = Response.getVersionId();

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
		var Response = client.listVersions(bucketName, "");

		for (var Version : Response.getVersionSummaries()) {
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

	public void partsETagCompare(List<PartETag> Expected, List<PartSummary> Actual) {
		assertEquals(Expected.size(), Actual.size());

		for (int i = 0; i < Expected.size(); i++) {
			assertEquals(Expected.get(i).getPartNumber(), Actual.get(i).getPartNumber());
			assertEquals(Expected.get(i).getETag(), Actual.get(i).getETag());
		}
	}

	public void VersionIDsCompare(List<S3VersionSummary> Expected, List<S3VersionSummary> Actual) {
		assertEquals(Expected.size(), Actual.size());

		for (int i = 0; i < Expected.size(); i++) {
			assertEquals(Expected.get(i).getVersionId(), Actual.get(i).getVersionId());
			assertEquals(Expected.get(i).getETag(), Actual.get(i).getETag());
			assertEquals(Expected.get(i).getKey(), Actual.get(i).getKey());
			assertEquals(Expected.get(i).getSize(), Actual.get(i).getSize());
		}
	}

	/******************************************
	 * Bucket Clear
	 ******************************************************/
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
			var Result = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withMaxResults(2000));
			var ObjectList = Result.getVersionSummaries();

			for (var Object : ObjectList)
				client.deleteVersion(new DeleteVersionRequest(bucketName, Object.getKey(), Object.getVersionId()));

		} catch (AmazonServiceException e) {
			// System.out.format("Error : Bucket(%s) Clear Failed(%s, %d)\n", bucketName,
			// e.getErrorCode(), e.getStatusCode());
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

	/********************************************
	 * Utility
	 ********************************************************/

	public void delay(int milliseconds) {
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
		}
	}
}
