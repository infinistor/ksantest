package org.example.test;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.example.s3tests.AES256;
import org.example.s3tests.BucketResourceData;
import org.example.s3tests.FormFile;
import org.example.s3tests.MainData;
import org.example.s3tests.MultipartUploadData;
import org.example.s3tests.MyResult;
import org.example.s3tests.ObjectData;
import org.example.s3tests.RangeSet;
import org.example.s3tests.S3Config;
import org.example.s3tests.UserData;
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
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class TestBase {

	/************************************************************************************************************/
	private static final char[] TEXT = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
			'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
	private static final char[] TEXT_String = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
			'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
			'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9' };
	public enum EncryptionType { NORMAL, SSE_S3, SSE_C };
	/************************************************************************************************************/
	private static final int RANDOM_PREFIX_TEXT_LENGTH = 15;
	private static final int RANDOM_SUFFIX_TEXT_LENGTH = 5;
	private static final int BUCKET_MAX_LENGTH = 63;
	private static final String STR_RANDOM = "{random}";
	/************************************************************************************************************/

	private final ArrayList<String> BucketList = new ArrayList<String>();
	protected final S3Config Config;

	protected TestBase() {
		String FileName = System.getenv(MainData.S3TESTS_INI);
		Config = new S3Config(FileName);
		Config.GetConfig();
	}

	public static AmazonS3 CreateClient(String URL, int Port, String RegionName, boolean IsSecure, UserData User, Boolean UseChunkEncoding, Boolean PayloadSigning, String SignatureVersion) {
		String Address = "";
		ClientConfiguration config;

		if (IsSecure)
		{
			Address = CreateURLToHTTPS(URL, 8443);
			System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
			config = new ClientConfiguration().withProtocol(Protocol.HTTPS).withSignerOverride(SignatureVersion);
		}
		else
		{
			Address = CreateURLToHTTP(URL, Port);
			config = new ClientConfiguration().withProtocol(Protocol.HTTP);
		}
		config.setSignerOverride(SignatureVersion);
		var ClientBuilder = AmazonS3ClientBuilder.standard();

		if (User == null)	ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
		else				ClientBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(User.AccessKey, User.SecretKey)));
		
		if (StringUtils.isBlank(URL))	ClientBuilder.setRegion(RegionName);
		else				ClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(Address, ""));

		ClientBuilder.setClientConfiguration(config);
		ClientBuilder.setChunkedEncodingDisabled(UseChunkEncoding);
		ClientBuilder.setPayloadSigningEnabled(PayloadSigning);
		ClientBuilder.setPathStyleAccessEnabled(true);
		return ClientBuilder.build();
	}

	public AmazonS3 GetClient() {
		return CreateClient(Config.URL, Config.Port, Config.RegionName, Config.IsSecure, Config.MainUser, true, true, "AWSS3V4SignerType");
	}
	public AmazonS3 GetClientV2() {
		return CreateClient(Config.URL, Config.Port, Config.RegionName, Config.IsSecure, Config.MainUser, true, true, "S3SignerType");
	}
	public AmazonS3 GetClientV4(Boolean UseChunkEncoding) {
		return CreateClient(Config.URL, Config.Port, Config.RegionName, Config.IsSecure, Config.MainUser, UseChunkEncoding, true, "AWSS3V4SignerType");
	}
	public AmazonS3 GetClientHttps() {
		return CreateClient(Config.URL, Config.Port, Config.RegionName, true, Config.MainUser, true, true, "AWSS3V4SignerType");
	}
	public AmazonS3 GetClientHttpsV4(Boolean UseChunkEncoding, Boolean PayloadSigning) {
		return CreateClient(Config.URL, Config.Port, Config.RegionName, true, Config.MainUser, UseChunkEncoding, PayloadSigning, "AWSS3V4SignerType");
	}
	public AmazonS3 GetAltClient() {
		return CreateClient(Config.URL, Config.Port, Config.RegionName, Config.IsSecure, Config.AltUser, true, true, "AWSS3V4SignerType");
	}
	public AmazonS3 GetUnauthenticatedClient() {
		return CreateClient(Config.URL, Config.Port, Config.RegionName, Config.IsSecure, null, true, true, "AWSS3V4SignerType");
	}
	public AmazonS3 GetBadAuthClient(String AccessKey, String SecretKey) {
		if (StringUtils.isBlank(AccessKey)) AccessKey = "aaaaaaaaaaaaaaa";
		if (StringUtils.isBlank(SecretKey)) SecretKey = "bbbbbbbbbbbbbbb";

		var DummyUser = new UserData();
		DummyUser.AccessKey = AccessKey;
		DummyUser.SecretKey = SecretKey;

		return CreateClient(Config.URL, Config.Port, Config.RegionName, Config.IsSecure, DummyUser, true, true, "AWSS3V4SignerType");
	}

	public AccessControlList GetGrantList(String UserID, Permission[] Perms) {
		Permission[] AllHeaders = new Permission[] { Permission.Read, Permission.Write, Permission.ReadAcp,
				Permission.WriteAcp, Permission.FullControl };

		var accessControlList = new AccessControlList();
		accessControlList.setOwner(new Owner(Config.MainUser.UserID, Config.MainUser.DisplayName));

		if (StringUtils.isBlank(UserID))
			UserID = Config.AltUser.UserID;
		var User = new CanonicalGrantee(UserID);
		if (Perms == null) {
			for (var Perm : AllHeaders)
				accessControlList.grantAllPermissions(new Grant(User, Perm));
		} else {
			for (var Perm : Perms)
				accessControlList.grantAllPermissions(new Grant(User, Perm));
		}
		return accessControlList;
	}

	public ObjectMetadata GetACLHeader(String UserID, String[] Perms) {
		String[] AllHeaders = { "read", "write", "read-acp", "write-acp", "full-control" };

		var Headers = new ObjectMetadata();

		if (StringUtils.isBlank(UserID))
			UserID = Config.AltUser.UserID;
		if (Perms == null) {
			for (var Perm : AllHeaders)
				Headers.setHeader(String.format("x-amz-grant-%s", Perm), String.format("id=%s", UserID));
		} else {
			for (var Perm : Perms)
				Headers.setHeader(String.format("x-amz-grant-%s", Perm), String.format("id=%s", UserID));
		}
		return Headers;
	}

	/******************************************
	 * Create Data
	 *******************************************************/

	
	public String GetPrefix() {
		return Config.BucketPrefix.replace(STR_RANDOM, RandomText(RANDOM_PREFIX_TEXT_LENGTH));
	}

	public String GetNewBucketName() {
		String BucketName = GetPrefix() + RandomText(RANDOM_SUFFIX_TEXT_LENGTH);
		if (BucketName.length() > BUCKET_MAX_LENGTH)
			BucketName = BucketName.substring(0, BUCKET_MAX_LENGTH - 1);
		BucketList.add(BucketName);
		return BucketName;
	}

	public String GetNewBucketName(String Prefix) {
		String BucketName = Prefix + RandomText(RANDOM_PREFIX_TEXT_LENGTH);
		if (BucketName.length() > BUCKET_MAX_LENGTH)
			BucketName = BucketName.substring(0, BUCKET_MAX_LENGTH - 1);
		BucketList.add(BucketName);
		return BucketName;
	}

	public String GetNewBucketName(int Length) {
		String BucketName = GetPrefix() + RandomText(63);
		BucketName = BucketName.substring(0, Length);
		BucketList.add(BucketName);
		return BucketName;
	}

	public String GetNewBucket() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(BucketName);
		return BucketName;
	}

	public String GetNewBucket(String Prefix) {
		var BucketName = GetNewBucketName(Prefix);
		var Client = GetClient();
		Client.createBucket(BucketName);
		return BucketName;
	}

	public String CreateObjects(ArrayList<String> KeyList) {
		var Client = GetClient();
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);

		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Key);
		}

		return BucketName;
	}
	public String CreateObjectsToBody(ArrayList<String> KeyList, String Body) {
		var Client = GetClient();
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);

		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Body);
		}

		return BucketName;
	}
	
	public String CreateObjectsV2(ArrayList<String> KeyList) {
		var Client = GetClientV2();
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);


		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Key);
		}

		return BucketName;
	}
	public String CreateObjectsToBodyV2(ArrayList<String> KeyList, String Body) {
		var Client = GetClientV2();
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);

		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Body);
		}

		return BucketName;
	}
	
	public String CreateObjectsV4(ArrayList<String> KeyList, Boolean UseChunkEncoding) {
		var Client = GetClientV4(UseChunkEncoding);
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);

		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Key);
		}

		return BucketName;
	}
	
	public String CreateObjectsHttps(ArrayList<String> KeyList, Boolean UseChunkEncoding, Boolean PayloadSigning) {
		var Client = GetClientHttpsV4(UseChunkEncoding, PayloadSigning);
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);

		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Key);
		}

		return BucketName;
	}
	public String CreateObjectsToBodyV4(ArrayList<String> KeyList, String Body, Boolean UseChunkEncoding) {
		var Client = GetClientV4(UseChunkEncoding);
		var BucketName = GetNewBucketName();
		Client.createBucket(BucketName);

		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Body);
		}

		return BucketName;
	}
	public String CreateObjectsToBody(String BucketName, ArrayList<String> KeyList, String Body) {
		var Client = GetClient();
		
		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Body);
		}

		return BucketName;
	}
	
	public String CreateObjects(ArrayList<String> KeyList, String BucketName) {
		var Client = GetClient();
		if (StringUtils.isBlank(BucketName)) {
			BucketName = GetNewBucketName();
			Client.createBucket(BucketName);
		}

		if (KeyList != null && Client != null) {
			for (var Key : KeyList)
				Client.putObject(BucketName, Key, Key);
		}

		return BucketName;
	}

	public String GetURL(String BucketName) {
		String Protocol;
		if (Config.IsSecure)
			Protocol = MainData.HTTPS;
		else
			Protocol = MainData.HTTP;

		String Address = GetHost(BucketName);

		return String.format("%s%s", Protocol, Address);
	}

	public String GetHost(String BucketName) {
		if (StringUtils.isBlank(Config.URL))
			return String.format("%s.s3-%s.amazonaws.com", BucketName, Config.RegionName);
		return String.format("%s:%d/%s", Config.URL, Config.Port, BucketName);
	}

	public BucketResourceData GetNewBucketResource() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();

		Client.createBucket(BucketName);

		return new BucketResourceData(Client, BucketName);
	}

	public BucketResourceData GetNewBucketResource(String BucketName) {
		var Client = GetClient();

		Client.createBucket(BucketName);

		return new BucketResourceData(Client, BucketName);
	}

	public boolean BucketIsEmpty(BucketResourceData Resource) {
		if (BucketResourceData.IsEmpty(Resource))
			return true;
		var Response = Resource.Client.listObjects(Resource.BucketName);

		if (Response == null)
			return true;
		if (Response.getObjectSummaries().size() > 0)
			return false;

		return true;
	}

	public String MakeArnResource(String Path) {
		return String.format("arn:aws:s3:::%s", Path);
	}

	public JsonObject MakeJsonStatement(String Action, String Resource, String Effect, JsonObject Principal,
			JsonObject Conditions) {
		if (Principal == null) {
			Principal = new JsonObject();
			Principal.addProperty("AWS", "*");
		}

		if (StringUtils.isBlank(Effect))
			Effect = MainData.PolicyEffectAllow;

		var Statement = new JsonObject();
		Statement.addProperty(MainData.PolicyEffect, Effect);
		Statement.add(MainData.PolicyPrincipal, Principal);
		Statement.addProperty(MainData.PolicyAction, Action);
		Statement.addProperty(MainData.PolicyResource, Resource);

		if (Conditions != null)
			Statement.add(MainData.PolicyCondition, Conditions);

		return Statement;
	}

	public JsonObject MakeJsonPolicy(String Action, String Resource, JsonObject Principal, JsonObject Conditions) {
		var Policy = new JsonObject();

		Policy.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);

		var Statement = new JsonArray();
		Statement.add(MakeJsonStatement(Action, Resource, null, Principal, Conditions));
		Policy.add(MainData.PolicyStatement, Statement);

		return Policy;
	}

	public JsonObject MakeJsonPolicy(JsonObject... StatementList) {
		var Policy = new JsonObject();

		Policy.addProperty(MainData.PolicyVersion, MainData.PolicyVersionDate);
		var Statements = new JsonArray();
		for (var Statement : StatementList)
			Statements.add(Statement);
		Policy.add(MainData.PolicyStatement, Statements);
		return Policy;
	}

	public void CheckConfigureVersioningRetry(String BucketName, String Status) {
		var Client = GetClient();

		Client.setBucketVersioningConfiguration(
				new SetBucketVersioningConfigurationRequest(BucketName, new BucketVersioningConfiguration(Status)));

		String ReadStatus = null;

		for (int i = 0; i < 5; i++) {
			try {
				var Response = Client.getBucketVersioningConfiguration(BucketName);
				ReadStatus = Response.getStatus();

				if (ReadStatus.equals(Status)) break;
				Delay(1000);
			} catch (Exception e) {
				ReadStatus = null;
			}
		}

		assertEquals(Status, ReadStatus);
	}

	public String SetupBucketObjectACL(CannedAccessControlList BucketACL, CannedAccessControlList ObjectACL,
			String Key) {
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(BucketACL));
		Client.putObject(BucketName, Key, "");
		Client.setObjectAcl(BucketName, Key, ObjectACL);

		return BucketName;
	}

	public Date GetTimeToAddSeconds(int Seconds) {
		Calendar today = Calendar.getInstance();
		today.add(Calendar.SECOND, Seconds);
		return new Date(today.getTimeInMillis());
	}

	public String GetTimeToAddMinutes(int Minutes) {
		var myFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		myFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		var time = Calendar.getInstance();
		time.add(Calendar.MINUTE, Minutes);
		return myFormat.format(time.getTime());
	}
	
	public InputStream CreateBody(String Body) {
		return new ByteArrayInputStream(Body.getBytes());
	}

	public AccessControlList AddObjectUserGrant(String BucketName, String Key, Grant myGrant) {
		var Client = GetClient();

		var Response = Client.getObjectAcl(BucketName, Key);
		var Grants = Response.getGrantsAsList();
		Grants.add(myGrant);

		var MyGrants = new AccessControlList();
		for (var Item : Grants)
			MyGrants.grantAllPermissions(Item);

		MyGrants.setOwner(Response.getOwner());

		return MyGrants;
	}

	public AccessControlList AddBucketUserGrant(String BucketName, Grant Grant) {
		var Client = GetClient();

		var Response = Client.getBucketAcl(BucketName);
		var Grants = Response.getGrantsAsList();
		Grants.add(Grant);

		var MyGrants = new AccessControlList();
		for (var Item : Grants)
			MyGrants.grantAllPermissions(Item);

		MyGrants.setOwner(Response.getOwner());

		return MyGrants;
	}

	public String CreateKeyWithRandomContent(String Key, int Size, String BucketName, AmazonS3 Client) {
		if (StringUtils.isBlank(BucketName))
			BucketName = GetNewBucket();
		if (Client == null)
			Client = GetClient();
		if (Size <= 0)
			Size = 7 * MainData.MB;

		var Data = RandomTextToLong(Size);
		Client.putObject(BucketName, Key, Data);

		return BucketName;
	}

	public String SetGetMetadata(String Metadata, String BucketName) {
		if (StringUtils.isBlank(BucketName))
			BucketName = GetNewBucket();

		var Client = GetClient();
		var KeyName = "foo";
		var MetadataKey = "x-amz-meta-meta1";

		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata(MetadataKey, Metadata);
		MetadataList.setContentType("text/plain");
		MetadataList.setContentLength(3);
		Client.putObject(BucketName, KeyName, CreateBody("bar"), MetadataList);

		var Response = Client.getObject(BucketName, KeyName);
		return Response.getObjectMetadata().getUserMetaDataOf(MetadataKey);
	}

	public String BucketACLGrantUserid(Permission permission) {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var MainUser = new CanonicalGrantee(Config.MainUser.UserID);
		MainUser.setDisplayName(Config.MainUser.DisplayName);
		var AltUser = new CanonicalGrantee(Config.AltUser.UserID);
		AltUser.setDisplayName(Config.AltUser.DisplayName);

		var accessControlList = AddBucketUserGrant(BucketName, new Grant(AltUser, permission));
		Client.setBucketAcl(BucketName, accessControlList);

		var Response = Client.getBucketAcl(BucketName);

		var GetGrants = Response.getGrantsAsList();
		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(MainUser, Permission.FullControl));
		MyGrants.add(new Grant(AltUser, permission));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));

		return BucketName;
	}

	public String SetupAccessTest(String Key1, String Key2, String NewKey, CannedAccessControlList BucketACL,
			CannedAccessControlList ObjectACL) {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		Key1 = "foo";
		Key2 = "bar";
		NewKey = "new";

		Client.setBucketAcl(BucketName, BucketACL);
		Client.putObject(BucketName, Key1, "foocontent");
		Client.setObjectAcl(BucketName, Key1, ObjectACL);
		Client.putObject(BucketName, Key2, "barcontent");

		return BucketName;
	}

	public void CheckObjContent(AmazonS3 Client, String BucketName, String Key, String VersionID, String Content) {
		var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withVersionId(VersionID));
		if (Content != null) {
			var Body = GetBody(Response.getObjectContent());
			assertEquals(Content, Body);
		} else
			assertNull(Response);
	}

	public void CheckObjVersions(AmazonS3 Client, String BucketName, String Key, List<String> VersionIDs,
			List<String> Contents) {
		var Response = Client.listVersions(BucketName, "");
		var Versions = GetVersions(Response.getVersionSummaries());

		Collections.reverse(Versions);

		var index = 0;
		for (var version : Versions) {
			assertEquals(version.getVersionId(), VersionIDs.get(index));
			if (StringUtils.isNotBlank(Key))
				assertEquals(Key, version.getKey());
			CheckObjContent(Client, BucketName, Key, version.getVersionId(), Contents.get(index++));
		}
	}

	public void CreateMultipleVersions(AmazonS3 Client, String BucketName, String Key, int NumVersions,
			boolean CheckVersion) {
		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();

		for (int i = 0; i < NumVersions; i++) {
			var Body = String.format("content-%s", i);
			var Response = Client.putObject(BucketName, Key, Body);
			var VersionID = Response.getVersionId();

			Contents.add(Body);
			VersionIDs.add(VersionID);
		}

		if (CheckVersion)
			CheckObjVersions(Client, BucketName, Key, VersionIDs, Contents);

	}

	public List<Tag> CreateSimpleTagset(int Count) {
		var TagSets = new ArrayList<Tag>();

		for (int i = 0; i < Count; i++)
			TagSets.add(new Tag(Integer.toString(i), Integer.toString(i)));
		return TagSets;
	}

	public List<Tag> CreateDetailTagset(int Count, int KeySize, int ValueSize) {
		var TagSets = new ArrayList<Tag>();

		for (int i = 0; i < Count; i++)
			TagSets.add(new Tag(RandomTextToLong(KeySize), RandomTextToLong(ValueSize)));
		return TagSets;
	}

	/*****************************************
	 * Get Data
	 **********************************************************/

	public ArrayList<String> GetObjectList(String BucketName, String Prefix) {
		var Client = GetClient();
		var Response = Client.listObjects(BucketName, Prefix);
		return GetKeys(Response.getObjectSummaries());
	}
	
	public static ArrayList<String> GetKeys(ArrayList<ObjectVersionsData> ObjectList) {
		if (ObjectList != null) {
			var Temp = new ArrayList<String>();

			for (var S3Object : ObjectList)
				Temp.add(S3Object.Key);

			return Temp;
		}
		return null;
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

	public static String GetBody(S3ObjectInputStream Data) {
		String Body = "";
		if (Data != null) {
			try {
				ByteArrayOutputStream result = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				for (int length; (length = Data.read(buffer)) != -1; ) {
					result.write(buffer, 0, length);
				}
				return result.toString("UTF-8");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return Body;
	}

	public static ObjectData GetObjectToKey(String Key, ArrayList<ObjectData> KeyList) {
		for (var Object : KeyList) {
			if (Object.Key.equals(Key))
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

	public void CheckVersioning(String BucketName, String StatusCode) {
		var Client = GetClient();

		var Response = Client.getBucketVersioningConfiguration(BucketName);
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
		for (var Key : KeyList)
			KeyVersions.add(new KeyVersion(Key, null));

		return KeyVersions;
	}

	public List<S3VersionSummary> GetVersions(List<S3VersionSummary> Versions) {
		if (Versions == null)
			return null;

		var Lists = new ArrayList<S3VersionSummary>();
		for (var item : Versions)
			if (!item.isDeleteMarker())
				Lists.add(item);
		return Lists;
	}

	public List<String> GetVersionIDs(List<S3VersionSummary> Versions) {
		if (Versions == null)
			return null;

		var Lists = new ArrayList<String>();
		for (var item : Versions)
			if (!item.isDeleteMarker())
				Lists.add(item.getVersionId());
		return Lists;
	}

	public List<S3VersionSummary> GetDeleteMarkers(List<S3VersionSummary> Versions) {
		if (Versions == null)
			return null;

		var DeleteMarkers = new ArrayList<S3VersionSummary>();

		for (var item : Versions)
			if (item.isDeleteMarker())
				DeleteMarkers.add(item);
		return DeleteMarkers;
	}

	/*****************************************
	 * Auto
	 **********************************************************/
	public void ACLTest(String BucketName, String Key, AmazonS3 Client, Boolean Pass)
	{
		if(Pass){
		   var Response = Client.getObject(BucketName, Key);
		   assertEquals(Key, Response.getKey());
		}
		else{
		   var e = assertThrows(AmazonServiceException.class, ()-> Client.getObject(BucketName, Key));
		   var StatusCode = e.getStatusCode();
		   var ErrorCode = e.getErrorCode();
   
		   assertEquals(403, StatusCode);
		   assertEquals(MainData.AccessDenied, ErrorCode);
		}
	}

	public String ValidateListObjcet(String BucketName, String Prefix, String Delimiter, String Marker, int MaxKeys,
			boolean IsTruncated, ArrayList<String> CheckKeys, ArrayList<String> CheckPrefixs, String NextMarker) {
		var Client = GetClient();
		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter)
				.withMarker(Marker).withMaxKeys(MaxKeys).withPrefix(Prefix));

		assertEquals(IsTruncated, Response.isTruncated());
		assertEquals(NextMarker, Response.getNextMarker());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(CheckKeys.size(), Keys.size());
		assertEquals(CheckPrefixs.size(), Prefixes.size());
		assertEquals(CheckKeys, Keys);
		assertEquals(CheckPrefixs, Prefixes);

		return Response.getNextMarker();
	}

	public String ValidateListObjcetV2(String BucketName, String Prefix, String Delimiter, String ContinuationToken,
			int MaxKeys, boolean IsTruncated, List<String> CheckKeys, List<String> CheckPrefixs, boolean Last) {
		var Client = GetClient();
		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter)
						.withContinuationToken(ContinuationToken).withMaxKeys(MaxKeys).withPrefix(Prefix));

		assertEquals(IsTruncated, Response.isTruncated());
		if (Last)
			assertNull(Response.getNextContinuationToken());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(CheckKeys, Keys);
		assertEquals(CheckPrefixs, Prefixes);
		assertEquals(CheckKeys.size(), Keys.size());
		assertEquals(CheckPrefixs.size(), Prefixes.size());

		return Response.getNextContinuationToken();
	}

	public void CheckBadBucketName(String BucketName) {
		var Client = GetClient();

		assertThrows(IllegalBucketNameException.class, () -> Client.createBucket(BucketName));
	}

	public void CheckGoodBucketName(String Name, String Prefix) {
		if (StringUtils.isBlank(Prefix))
			Prefix = GetPrefix();

		var BucketName = String.format("%s%s", Prefix, Name);
		BucketList.add(BucketName);

		var Client = GetClient();
		var Response = Client.createBucket(BucketName);
		assertTrue(StringUtils.isNotBlank(Response.getName()));
	}

	public void TestBucketCreateNamingGoodLong(int Length) {
		var BucketName = GetPrefix();
		if (BucketName.length() < Length)
			BucketName += RandomText(Length - BucketName.length());
		else
			BucketName = BucketName.substring(0, Length - 1);
		BucketList.add(BucketName);

		var Client = GetClient();
		var Response = Client.createBucket(BucketName);
		assertTrue(StringUtils.isNotBlank(Response.getName()));
	}

	public HttpResponse GetObject(URL Address) {
		HttpClient Client = HttpClientBuilder.create().build();
		HttpGet getRequest = new HttpGet(Address.toString());
		try {
			HttpResponse httpResponse = Client.execute(getRequest);
			return httpResponse;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public HttpResponse PutObject(URL Address, String Body) {
		HttpClient Client = HttpClientBuilder.create().build();
		HttpPut getRequest = new HttpPut(Address.toString());
		if (Body != null) {
			StringEntity requestEntity = new StringEntity(Body, "utf-8");
			requestEntity.setContentType(new BasicHeader("Content-Type", "application/txt"));
			getRequest.setEntity(requestEntity);
		}
		try {
			return Client.execute(getRequest);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public MyResult PostUpload(String BucketName, Map<String, String> Headers, FormFile myFile) {
		var Result = new MyResult();

		try {
			String boundary = Long.toHexString(System.currentTimeMillis());
			String LINE_FEED = "\r\n";
			String url = GetURL(BucketName);
			HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();

			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
			connection.setRequestMethod("POST");
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setConnectTimeout(15000);

			var writer = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));

			for (var Header : Headers.keySet()) {
				writer.append("--" + boundary).append(LINE_FEED);
				writer.append(String.format("Content-Disposition: form-data; name=\"%s\"", Header)).append(LINE_FEED);
				writer.append(LINE_FEED);
				writer.append(Headers.get(Header)).append(LINE_FEED);
			}

			writer.append("--" + boundary).append(LINE_FEED);
			writer.append(String.format("Content-Disposition: form-data; name=\"file\"; filename=\"%s\"", myFile.Name))
					.append(LINE_FEED);
			writer.append(String.format("Content-Type: %s", myFile.ContentType)).append(LINE_FEED);
			writer.append(LINE_FEED);
			writer.append(myFile.Body).append(LINE_FEED);
			writer.append("--" + boundary + "--").append(LINE_FEED);
			writer.close();

			Result.StatusCode = connection.getResponseCode();
			Result.URL = connection.getURL().toString();
			if (Result.StatusCode != HttpURLConnection.HTTP_NO_CONTENT
					&& Result.StatusCode != HttpURLConnection.HTTP_CREATED
					&& Result.StatusCode != HttpURLConnection.HTTP_OK) {
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				Result.Message = response.toString();
			}
		} catch (IOException e) {
			fail(e.getMessage());
		}

		return Result;
	}
	private static void trustAllHttpsCertificates() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[1];
        TrustManager tm = new miTM();
        trustAllCerts[0] = tm;
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, null);
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }
 
    static class miTM implements X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
 
        public boolean isServerTrusted(X509Certificate[] certs) {
            return true;
        }
 
        public boolean isClientTrusted(X509Certificate[] certs) {
            return true;
        }
 
        public void checkServerTrusted(X509Certificate[] certs, String authType)
                throws CertificateException {
            return;
        }
 
        public void checkClientTrusted(X509Certificate[] certs, String authType)
                throws CertificateException {
            return;
        }
    }

	public static void ignoreSsl() throws Exception{
        HostnameVerifier hv = new HostnameVerifier() {
        public boolean verify(String urlHostName, SSLSession session) {
                return true;
            }
        };
        trustAllHttpsCertificates();
        HttpsURLConnection.setDefaultHostnameVerifier(hv);
    }

	public MyResult PostUploadHttps(String BucketName, Map<String, String> Headers, FormFile myFile) {
		var Result = new MyResult();

		try {
			String boundary = Long.toHexString(System.currentTimeMillis());
			String LINE_FEED = "\r\n";
			String url = GetURL(BucketName).replace(MainData.HTTP, MainData.HTTPS);
			url = url.replace("8080", "8443");
			ignoreSsl();
			HttpsURLConnection connection = (HttpsURLConnection) new URL(url).openConnection();

			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
			connection.setRequestMethod("POST");
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setConnectTimeout(15000);

			var writer = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));

			for (var Header : Headers.keySet()) {
				writer.append("--" + boundary).append(LINE_FEED);
				writer.append(String.format("Content-Disposition: form-data; name=\"%s\"", Header)).append(LINE_FEED);
				writer.append(LINE_FEED);
				writer.append(Headers.get(Header)).append(LINE_FEED);
			}

			writer.append("--" + boundary).append(LINE_FEED);
			writer.append(String.format("Content-Disposition: form-data; name=\"file\"; filename=\"%s\"", myFile.Name))
					.append(LINE_FEED);
			writer.append(String.format("Content-Type: %s", myFile.ContentType)).append(LINE_FEED);
			writer.append(LINE_FEED);
			writer.append(myFile.Body).append(LINE_FEED);
			writer.append("--" + boundary + "--").append(LINE_FEED);
			writer.close();

			Result.StatusCode = connection.getResponseCode();
			Result.URL = connection.getURL().toString();
			if (Result.StatusCode != HttpURLConnection.HTTP_NO_CONTENT
					&& Result.StatusCode != HttpURLConnection.HTTP_CREATED
					&& Result.StatusCode != HttpURLConnection.HTTP_OK) {
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				System.out.format("%s\\n", response.toString());
				Result.Message = response.toString();
			}
		} catch (IOException e) {
			fail(e.getMessage());
		} catch (Exception e) {
			fail(e.getMessage());
		}

		return Result;
	}
	
	public MultipartUploadData MultipartUpload(AmazonS3 Client, String BucketName, String Key, int Size, MultipartUploadData UploadData) {
		var	PartSize = 5 * MainData.MB;
		var Parts = GenerateRandomString(Size, PartSize);

		for (var Part : Parts) {
			UploadData.AppendBody(Part);
			var PartResPonse = Client.uploadPart(
					new UploadPartRequest()
						.withBucketName(BucketName)
						.withKey(Key)
						.withUploadId(UploadData.UploadId)
						.withPartNumber(UploadData.NextPartNumber())
						.withInputStream(CreateBody(Part))
						.withPartSize(Part.length()));
			UploadData.AddPart(PartResPonse.getPartETag());
		}

		return UploadData;
	}
	
	public MultipartUploadData SetupMultipartUpload(AmazonS3 Client, String BucketName, String Key, int Size, ObjectMetadata MetadataList) {
		var UploadData = new MultipartUploadData();
		if (MetadataList == null) {
			MetadataList = new ObjectMetadata();
			MetadataList.setContentType("text/plain");
		}
		MetadataList.setContentLength(Size);

		var InitMultiPartResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key, MetadataList));
		UploadData.UploadId = InitMultiPartResponse.getUploadId();

		var Parts = GenerateRandomString(Size, UploadData.PartSize);

		for (var Part : Parts) {
			UploadData.AppendBody(Part);

			var PartResPonse = Client.uploadPart(
					new UploadPartRequest()
						.withBucketName(BucketName)
						.withKey(Key)
						.withUploadId(UploadData.UploadId)
						.withPartNumber(UploadData.NextPartNumber())
						.withInputStream(CreateBody(Part))
						.withPartSize(Part.length()));
			UploadData.Parts.add(PartResPonse.getPartETag());
		}

		return UploadData;
	}
	
	public MultipartUploadData MultipartUploadResend(AmazonS3 Client, String BucketName, String Key, int Size, ObjectMetadata MetadataList, ArrayList<Integer> ResendParts) {
		var UploadData = new MultipartUploadData();
		if (MetadataList == null) {
			MetadataList = new ObjectMetadata();
			MetadataList.setContentType("text/plain");
		}
		MetadataList.setContentLength(Size);

		var InitMultiPartResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key, MetadataList));
		UploadData.UploadId = InitMultiPartResponse.getUploadId();

		var Parts = GenerateRandomString(Size, UploadData.PartSize);

		for (var Part : Parts) {
			UploadData.AppendBody(Part);
			int PartNumber = UploadData.NextPartNumber();

			var PartResPonse = Client.uploadPart(
					new UploadPartRequest()
						.withBucketName(BucketName)
						.withKey(Key)
						.withUploadId(UploadData.UploadId)
						.withPartNumber(PartNumber)
						.withInputStream(CreateBody(Part))
						.withPartSize(Part.length()));
			UploadData.Parts.add(PartResPonse.getPartETag());

			if (ResendParts != null && ResendParts.contains(PartNumber))
				Client.uploadPart(new UploadPartRequest()
						.withBucketName(BucketName)
						.withKey(Key)
						.withUploadId(UploadData.UploadId)
						.withPartNumber(PartNumber)
						.withInputStream(CreateBody(Part))
						.withPartSize(Part.length()));
		}

		return UploadData;
	}
	
	public MultipartUploadData SetupMultipartUpload(AmazonS3 Client, String BucketName, String Key, int Size, int PartSize, ObjectMetadata MetadataList, SSECustomerKey SSEC) {
		var UploadData = new MultipartUploadData();
		if (PartSize <= 0) PartSize = 5 * MainData.MB;
		if (MetadataList == null) {
			MetadataList = new ObjectMetadata();
			MetadataList.setContentType("text/plain");
		}
		MetadataList.setContentLength(Size);

		var InitRequest = new InitiateMultipartUploadRequest(BucketName, Key, MetadataList);
		if (SSEC != null) InitRequest.setSSECustomerKey(SSEC);

		var InitMultiPartResponse = Client.initiateMultipartUpload(InitRequest);
		UploadData.UploadId = InitMultiPartResponse.getUploadId();

		var Parts = GenerateRandomString(Size, PartSize);

		for (var Part : Parts) {
			UploadData.AppendBody(Part);
			var Request = new UploadPartRequest()
						.withBucketName(BucketName)
						.withKey(Key)
						.withUploadId(UploadData.UploadId)
						.withPartNumber(UploadData.NextPartNumber())
						.withInputStream(CreateBody(Part))
						.withPartSize(Part.length());
			if (SSEC != null) Request.setSSECustomerKey(SSEC);

			var Response = Client.uploadPart(Request);
			UploadData.Parts.add(Response.getPartETag());
		}
		return UploadData;
	}

	public void CheckCopyContent(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey) {
		var Client = GetClient();

		var Response = Client.getObject(new GetObjectRequest(SourceBucketName, SourceKey));
		var SourceSize = Response.getObjectMetadata().getContentLength();
		var SourceData = GetBody(Response.getObjectContent());

		 Response = Client.getObject(TargetBucketName, TargetKey);
		var TargetSize = Response.getObjectMetadata().getContentLength();
		var TargetData = GetBody(Response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertEquals(SourceData, TargetData);
	}

	public void CheckCopyContent(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey, String VersionID) {
		var Client = GetClient();

		var Response = Client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withVersionId(VersionID));
		var SourceSize = Response.getObjectMetadata().getContentLength();
		var SourceData = GetBody(Response.getObjectContent());

		 Response = Client.getObject(TargetBucketName, TargetKey);
		var TargetSize = Response.getObjectMetadata().getContentLength();
		var TargetData = GetBody(Response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertEquals(SourceData, TargetData);
	}

	public void CheckCopyContentSSEC(AmazonS3 Client, String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey, SSECustomerKey SSEC) {
		
		var SourceResponse = Client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withSSECustomerKey(SSEC));
		var SourceSize = SourceResponse.getObjectMetadata().getContentLength();
		var SourceData = GetBody(SourceResponse.getObjectContent());

		var TargetResponse = Client.getObject(new GetObjectRequest(TargetBucketName, TargetKey).withSSECustomerKey(SSEC));
		var TargetSize = TargetResponse.getObjectMetadata().getContentLength();
		var TargetData = GetBody(TargetResponse.getObjectContent());

		assertEquals(SourceSize, TargetSize);
		assertTrue(SourceData.equals(TargetData), "Source does not match target");
	}

	public void CheckCopyContentUsingRange(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey) {
		var Client = GetClient();

		var Response = Client.getObject(new GetObjectRequest(TargetBucketName, TargetKey));
		var TargetSize = Response.getObjectMetadata().getContentLength();
		var TargetData = GetBody(Response.getObjectContent());
		
		Response = Client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withRange(0, TargetSize - 1));
		var SourceSize = Response.getObjectMetadata().getContentLength();
		var SourceData = GetBody(Response.getObjectContent());
		assertEquals(SourceSize, TargetSize);
		assertEquals(SourceData, TargetData);
	}
	
	public void CheckCopyContentUsingRange(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey, int Step) {
		var Client = GetClient();

		var Response = Client.getObjectMetadata(SourceBucketName, SourceKey);
		var Size = Response.getContentLength();
		
		long StartpPosition = 0;
		while (StartpPosition < Size) {
			var EndPosition = StartpPosition + Step;
			if (EndPosition > Size) EndPosition = Size - 1;

			var SourceResponse = Client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withRange(StartpPosition, EndPosition - 1));
			var SourceBody = GetBody(SourceResponse.getObjectContent());
			var TargetResponse = Client.getObject(new GetObjectRequest(SourceBucketName, SourceKey).withRange(StartpPosition, EndPosition - 1));
			var TargetBody = GetBody(TargetResponse.getObjectContent());

			assertEquals(SourceResponse.getObjectMetadata().getContentLength(), TargetResponse.getObjectMetadata().getContentLength());
			assertEquals(SourceBody, TargetBody);
			StartpPosition += Step;
		}
	}

	public void CheckUploadMultipartResend(String BucketName, String Key, int Size, ArrayList<Integer> ResendParts) {
		var ContentType = "text/bla";
		var Metadata = new ObjectMetadata();
		Metadata.addUserMetadata("x-amz-meta-foo", "bar");
		Metadata.setContentType(ContentType);
		var Client = GetClient();
		var UploadData = MultipartUploadResend(Client, BucketName, Key, Size, Metadata, ResendParts);
		Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		var Response = Client.getObjectMetadata(BucketName, Key);
		assertEquals(ContentType, Response.getContentType());
		assertEquals(Metadata.getUserMetadata(), Response.getUserMetadata());

		CheckContentUsingRange(BucketName, Key, UploadData.GetBody(), MainData.MB);
		CheckContentUsingRange(BucketName, Key, UploadData.GetBody(), 10 * MainData.MB);
	}

	public String DoTestMultipartUploadContents(String BucketName, String Key, int NumParts) {
		var Payload = RandomTextToLong(5 * MainData.MB);
		var Client = GetClient();

		var InitResponse = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key));
		var UploadID = InitResponse.getUploadId();
		var Parts = new ArrayList<PartETag>();
		var AllPayload = "";

		for (int i = 0; i < NumParts; i++) {
			var PartNumber = i + 1;
			var PartResponse = Client.uploadPart(new UploadPartRequest().withBucketName(BucketName).withKey(Key)
					.withUploadId(UploadID).withPartNumber(PartNumber)
					.withInputStream(new ByteArrayInputStream(Payload.getBytes())).withPartSize(Payload.length()));
			Parts.add(new PartETag(PartNumber, PartResponse.getETag()));
			AllPayload += Payload;
		}
		var LestPayload = RandomTextToLong(MainData.MB);
		var LestPartResponse = Client.uploadPart(new UploadPartRequest().withBucketName(BucketName).withKey(Key)
				.withUploadId(UploadID).withPartNumber(NumParts + 1)
				.withInputStream(new ByteArrayInputStream(LestPayload.getBytes())).withPartSize(LestPayload.length()));
		Parts.add(new PartETag(NumParts + 1, LestPartResponse.getETag()));
		AllPayload += LestPayload;

		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadID, Parts));

		var Response = Client.getObject(BucketName, Key);
		var Text = GetBody(Response.getObjectContent());

		assertEquals(AllPayload, Text);

		return AllPayload;
	}

	public MultipartUploadData MultipartCopy(String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey, int Size, AmazonS3 Client, int PartSize, String VersionID) {
		var Data = new MultipartUploadData();
		if (Client == null) Client = GetClient();
		if (PartSize <= 0) PartSize = 5 * MainData.MB;

		var Response = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey));
		Data.UploadId = Response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartResPonse = Client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
					.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
					.withUploadId(Data.UploadId).withPartNumber(UploadCount).withFirstByte(Start).withLastByte(End)
					.withSourceVersionId(VersionID));
			Data.Parts.add(new PartETag(UploadCount++, PartResPonse.getETag()));

			Start = End + 1;
		}

		return Data;
	}

	public MultipartUploadData MultipartCopy(AmazonS3 Client, String SourceBucketName, String SourceKey, String TargetBucketName, String TargetKey,
											int Size, ObjectMetadata Metadata) {
		var Data = new MultipartUploadData();
		var PartSize = 5 * MainData.MB;
		if (Metadata == null) Metadata = new ObjectMetadata();

		var Response = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey, Metadata));
		Data.UploadId = Response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartResPonse = Client.copyPart(new CopyPartRequest().withSourceBucketName(SourceBucketName)
					.withSourceKey(SourceKey).withDestinationBucketName(TargetBucketName).withDestinationKey(TargetKey)
					.withUploadId(Data.UploadId).withPartNumber(UploadCount).withFirstByte(Start).withLastByte(End));
			Data.Parts.add(new PartETag(UploadCount++, PartResPonse.getETag()));

			Start = End + 1;
		}

		return Data;
	}

	public MultipartUploadData MultipartCopySSEC(AmazonS3 Client, String SourceBucketName, String SourceKey,
										 String TargetBucketName, String TargetKey, int Size, ObjectMetadata Metadata, SSECustomerKey SSEC) {
		var Data = new MultipartUploadData();
		var PartSize = 5 * MainData.MB;
		if (Metadata == null) Metadata = new ObjectMetadata();

		var Response = Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(TargetBucketName, TargetKey, Metadata).withSSECustomerKey(SSEC));
		Data.UploadId = Response.getUploadId();

		int UploadCount = 1;
		long Start = 0;
		while (Start < Size) {
			long End = Math.min(Start + PartSize - 1, Size - 1);

			var PartResPonse = Client.copyPart(new CopyPartRequest()
												.withSourceBucketName(SourceBucketName)
												.withSourceKey(SourceKey)
												.withDestinationBucketName(TargetBucketName)
												.withDestinationKey(TargetKey)
												.withSourceSSECustomerKey(SSEC)
												.withDestinationSSECustomerKey(SSEC)
												.withUploadId(Data.UploadId)
												.withPartNumber(UploadCount)
												.withFirstByte(Start)
												.withLastByte(End));
			Data.Parts.add(new PartETag(UploadCount++, PartResPonse.getETag()));

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

	public void CheckContentUsingRange(String BucketName, String Key, String Data, long Step) {
		var Client = GetClient();
		var GetResponse = Client.getObjectMetadata(BucketName, Key);
		var Size = GetResponse.getContentLength();
		assertEquals(Data.length(), Size);
		
		long StartpPosition = 0;
		while (StartpPosition < Size) {
			var EndPosition = StartpPosition + Step;
			if (EndPosition > Size) EndPosition = Size - 1;

			var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(StartpPosition, EndPosition - 1));
			var Body = GetBody(Response.getObjectContent());
			var Length = EndPosition - StartpPosition;
			var PartBody = Data.substring((int) StartpPosition, (int) EndPosition);

			assertEquals(Length, Response.getObjectMetadata().getContentLength());
			assertTrue(PartBody.equals(Body), "Source does not match target");
			StartpPosition += Step;
		}
	}

	public void CheckContentUsingRangeEnc(AmazonS3 Client, String BucketName, String Key, String Data, long Step, SSECustomerKey SSEC) {
		if (Client == null)
			Client = GetClient();
		var GetResponse = Client.getObjectMetadata(new GetObjectMetadataRequest(BucketName, Key).withSSECustomerKey(SSEC));
		var Size = GetResponse.getContentLength();

		long StartpPosition = 0;
		while (StartpPosition <= Size) {
			var EndPosition = StartpPosition + Step;
			if (EndPosition > Size)
				EndPosition = Size - 1;

			var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(StartpPosition, EndPosition - 1).withSSECustomerKey(SSEC));
			var Body = GetBody(Response.getObjectContent());
			var Length = EndPosition - StartpPosition;
			var PartBody = Data.substring((int) StartpPosition, (int) EndPosition);

			assertEquals(Length, Response.getObjectMetadata().getContentLength());
			assertTrue(PartBody.equals(Body), "Source does not match target");
			StartpPosition += Step;
		}
	}

	public RangeSet GetRandomRange(int FileSize)
	{
		int MAX_LENGTH = 500;
		Random rand = new Random();

		var Start = rand.nextInt(FileSize - MAX_LENGTH * 2);
		var MaxLength = FileSize - Start;
		
		if (MaxLength > MAX_LENGTH) MaxLength = MAX_LENGTH;
		var Length = rand.nextInt(MaxLength) + MAX_LENGTH - 1;
		if(Length <= 0) Length = 1;

		return new RangeSet(Start, Length);

	}

	public void CheckContent(String BucketName, String Key, String Data, int LoopCount) {
		var Client = GetClient();

		for (int i = 0; i < LoopCount; i++)
		{
			var Response = Client.getObject(BucketName, Key);
			var Body = GetBody(Response.getObjectContent());
			assertEquals(Data, Body);
		}
	}

	public void CheckContentEnc(String BucketName, String Key, String Data, int LoopCount, SSECustomerKey SSEC) {
		var Client = GetClientHttps();

		for (int i = 0; i < LoopCount; i++)
		{
			var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SSEC));
			var Body = GetBody(Response.getObjectContent());
			assertEquals(Data, Body);
		}
	}
	
	public void CheckContentUsingRandomRange(String BucketName, String Key, String Data, int LoopCount) {
		var Client = GetClient();
		int FileSize = Data.length();

		for (int i = 0; i < LoopCount; i++)
		{
			var Range = GetRandomRange(FileSize);
			var RangeBody = Data.substring(Range.Start, Range.End + 1);

			var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(Range.Start, Range.End));
			var Body = GetBody(Response.getObjectContent());

			assertEquals(Range.Length, Response.getObjectMetadata().getContentLength() - 1);
			assertTrue(RangeBody.equals(Body), "Source does not match target");
		}
	}

	public void CheckContentUsingRandomRangeEnc(AmazonS3 Client, String BucketName, String Key, String Data, int FileSize, int LoopCount, SSECustomerKey SSEC) {
		for (int i = 0; i < LoopCount; i++)
		{
			var Range = GetRandomRange(FileSize);

			var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withRange(Range.Start, Range.End - 1).withSSECustomerKey(SSEC));
			var Body = GetBody(Response.getObjectContent());
			var RangeBody = Data.substring(Range.Start, Range.End + 1);

			assertEquals(Range.Length, Response.getObjectMetadata().getContentLength());
			assertTrue(RangeBody.equals(Body), "Source does not match target");
		}
	}

	public AmazonServiceException SetGetMetadataUnreadable(String Metadata, String Bucket) {
		if (StringUtils.isBlank(Bucket)) Bucket = GetNewBucket();
		var BucketName = Bucket;
		var Client = GetClient();
		var KeyName = "foo";
		var MetadataKey = "x-amz-meta-meta1";
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata(MetadataKey, Metadata);

		return assertThrows(AmazonServiceException.class,
				() -> Client.putObject(BucketName, KeyName, CreateBody("bar"), MetadataList));
	}

	public boolean ErrorCheck(Integer StatusCode) {
		if (StatusCode.equals(400))
			return true;
		if (StatusCode.equals(403))
			return true;
		return false;
	}

	public void TestEncryptionCSEWrite(int FileSize) {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "testobj";
		var AESKey = RandomTextToLong(32);
		var Data = RandomTextToLong(FileSize);

		try {
			var EncodingData = AES256.encryptAES256(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("x-amz-meta-key", AESKey);
			MetadataList.setContentType("text/plain");
			MetadataList.setContentLength(EncodingData.length());

			Client.putObject(BucketName, Key, CreateBody(EncodingData), MetadataList);

			var Response = Client.getObject(BucketName, Key);
			var EncodingBody = GetBody(Response.getObjectContent());
			var Body = AES256.decryptAES256(EncodingBody, AESKey);
			assertEquals(Data, Body);

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void TestEncryptionSSECustomerWrite(int FileSize) {
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Data = RandomTextToLong(FileSize);
		var Metadata = new ObjectMetadata();
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(FileSize);
		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC));

		var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SSEC));
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
	}

	public void TestEncryptionSSES3ustomerWrite(int FileSize) {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "testobj";
		var Data = RandomTextToLong(FileSize);

		var Metadata = new ObjectMetadata();
		Metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        Metadata.setContentType("text/plain");
        Metadata.setContentLength(FileSize);

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata));

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
	}

	public void TestEncryptionSSES3Copy(int FileSize) {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Data = RandomTextToLong(FileSize);

        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var SourceKey = "bar";
		Client.putObject(BucketName, SourceKey, Data);

        var SourceResponse = Client.getObject(BucketName, SourceKey);
        var SourceBody = GetBody(SourceResponse.getObjectContent());
        assertEquals(SSEAlgorithm.AES256.toString(), SourceResponse.getObjectMetadata().getSSEAlgorithm());

        var TargetKey = "foo";
        Client.copyObject(BucketName, SourceKey, BucketName, TargetKey);
        var TargetResponse = Client.getObject(BucketName, TargetKey);
        assertEquals(SSEAlgorithm.AES256.toString(), TargetResponse.getObjectMetadata().getSSEAlgorithm());

        var TargetBody = GetBody(TargetResponse.getObjectContent());
        assertEquals(SourceBody, TargetBody);
	}

	public void TestObjectCopy(Boolean SourceObjectEncryption, Boolean SourceBucketEncryption, Boolean TargetBucketEncryption, Boolean TargetObjectEncryption, int FileSize)
	{
		var SourceKey = "SourceKey";
		var TargetKey = "TargetKey";
		var SourceBucketName = GetNewBucket();
		var TargetBucketName = GetNewBucket();
		var Client = GetClient();
		var Data = RandomTextToLong(FileSize);

		//SSE-S3 Config
		var Metadata = new ObjectMetadata();
		Metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(FileSize);

		//Source Put Object
		if (SourceObjectEncryption) Client.putObject(new PutObjectRequest(SourceBucketName, SourceKey, CreateBody(Data), Metadata));
		else Client.putObject(SourceBucketName, SourceKey, Data);

		////Source Object Check
		var SourceResponse = Client.getObject(SourceBucketName, SourceKey);
		var SourceBody = GetBody(SourceResponse.getObjectContent());
		if (SourceObjectEncryption) assertEquals(SSEAlgorithm.AES256.toString(), SourceResponse.getObjectMetadata().getSSEAlgorithm());
		else assertNull(SourceResponse.getObjectMetadata().getSSEAlgorithm());
		assertEquals(Data, SourceBody);

	
		var SSES3Config = new ServerSideEncryptionConfiguration()
		.withRules(new ServerSideEncryptionRule()
				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
						withSSEAlgorithm(SSEAlgorithm.AES256)));

		//Source Bucket Encryption
		if (SourceBucketEncryption)
		{
			Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(SourceBucketName).withServerSideEncryptionConfiguration(SSES3Config));

			var EncryptionResponse = Client.getBucketEncryption(SourceBucketName);
			assertEquals(SSES3Config.getRules(), EncryptionResponse.getServerSideEncryptionConfiguration().getRules());
		}

		//Target Bucket Encryption
		if (TargetBucketEncryption)
		{
			Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(TargetBucketName).withServerSideEncryptionConfiguration(SSES3Config));

			var EncryptionResponse = Client.getBucketEncryption(TargetBucketName);
			assertEquals(SSES3Config.getRules(), EncryptionResponse.getServerSideEncryptionConfiguration().getRules());
		}

		//Source Copy Object
		if (TargetObjectEncryption) Client.copyObject(new CopyObjectRequest(SourceBucketName, SourceKey, TargetBucketName, TargetKey).withNewObjectMetadata(Metadata));
		else                      Client.copyObject(SourceBucketName, SourceKey, TargetBucketName, TargetKey);

		//Target Object Check
		var TargetResponse = Client.getObject(TargetBucketName, TargetKey);
		var TargetBody = GetBody(TargetResponse.getObjectContent());
		if (TargetBucketEncryption || TargetObjectEncryption) assertEquals(SSEAlgorithm.AES256.toString(), TargetResponse.getObjectMetadata().getSSEAlgorithm());
		else 											  assertNull(TargetResponse.getObjectMetadata().getSSEAlgorithm());
		assertEquals(SourceBody, TargetBody);
	}

	public void TestObjectCopy(EncryptionType Source, EncryptionType Target, int FileSize)
	{
		var SourceKey = "SourceKey";
		var TargetKey = "TargetKey";
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Data = RandomTextToLong(FileSize);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(FileSize);

		//SSE-S3 Config
		var SSEMetadata = new ObjectMetadata();
		SSEMetadata.setContentType("text/plain");
		SSEMetadata.setContentLength(FileSize);
		SSEMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		//SSE-C Config
		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=").withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
						.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		//Source Put Object
		var SourcePutRequest = new PutObjectRequest(BucketName, SourceKey, CreateBody(Data), Metadata);
		var SourceGetRequest = new GetObjectRequest(BucketName, SourceKey);
		var TargetGetRequest = new GetObjectRequest(BucketName, SourceKey);
		var CopyRequest = new CopyObjectRequest(BucketName, SourceKey, BucketName, TargetKey).withMetadataDirective(MetadataDirective.REPLACE);

		//Source Options
		switch (Source){
			case SSE_S3:
				SourcePutRequest.setMetadata(SSEMetadata);
				break;
			case SSE_C :
				SourcePutRequest.setSSECustomerKey(SSEC);
				SourceGetRequest.setSSECustomerKey(SSEC);
				TargetGetRequest.setSSECustomerKey(SSEC);
				CopyRequest.setSourceSSECustomerKey(SSEC);
				break;
			case NORMAL: break;
		}

		//Target Options
		switch (Target){
			case SSE_S3:
				CopyRequest.setNewObjectMetadata(SSEMetadata);
				break;
			case SSE_C :
				CopyRequest.setDestinationSSECustomerKey(SSEC);
				break;
			case NORMAL:
				CopyRequest.setNewObjectMetadata(Metadata);
				break;
		}

		//Source Put Object
		Client.putObject(SourcePutRequest);

		//Source Get Object
		var SourceResponse = Client.getObject(SourceGetRequest);
		var SourceBody = GetBody(SourceResponse.getObjectContent());
		assertEquals(Data, SourceBody);
		
		//Copy Object
		Client.copyObject(CopyRequest);

		//Target Object Check
		var TargetResponse = Client.getObject(TargetGetRequest);
		var TargetBody = GetBody(TargetResponse.getObjectContent());
		assertEquals(SourceBody, TargetBody);
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
		
		if(PriorityA < PriorityB) return 1;
		if(PriorityA == PriorityB) return 0;
		return -1;

	}
	public int GetGranteeIdentifierPriority(Grant A, Grant B)
	{
		return A.getGrantee().getIdentifier().compareTo(B.getGrantee().getIdentifier());
	}

	public ArrayList<Grant> GrantsSort(ArrayList<Grant> Data) {
		var newList = new ArrayList<Grant>();

		Comparator<String> comparator = (s1, s2)->s2.compareTo(s1);
		TreeMap<String, Grant> kk = new TreeMap<String, Grant>(comparator);
		
		for (var Temp : Data) {
			kk.put(Temp.getGrantee().getIdentifier() + Temp.getPermission(), Temp);
		}
		
		for(Map.Entry<String, Grant> entry : kk.entrySet()) {
			newList.add(entry.getValue());
		}
		return newList;
	}

	public void CheckGrants(ArrayList<Grant> Expected, ArrayList<Grant> Actual) {
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

	public void CheckObjectACL(Permission permission) {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo";

		Client.putObject(BucketName, Key, "");
		var Response = Client.getObjectAcl(BucketName, Key);
		var Policy = new AccessControlList();
		Policy.setOwner(Response.getOwner());
		Policy.grantPermission(Response.getGrantsAsList().get(0).getGrantee(), permission);

		Client.setObjectAcl(BucketName, Key, Policy);

		Response = Client.getObjectAcl(BucketName, Key);
		var GetGrants = Response.getGrantsAsList();

		var User = new CanonicalGrantee(Config.MainUser.UserID);
		User.setDisplayName(Config.MainUser.DisplayName);

		var MyGrants = new ArrayList<Grant>();
		MyGrants.add(new Grant(User, permission));
		CheckGrants(MyGrants, new ArrayList<Grant>(GetGrants));
	}

	public void CheckBucketACLGrantCanRead(String BucketName) {
		var AltClient = GetAltClient();
		AltClient.headBucket(new HeadBucketRequest(BucketName));
	}

	public void CheckBucketACLGrantCantRead(String BucketName) {
		var AltClient = GetAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient.headBucket(new HeadBucketRequest(BucketName)));
	}

	public void CheckBucketACLGrantCanReadACP(String BucketName) {
		var AltClient = GetAltClient();
		AltClient.getBucketAcl(BucketName);
	}

	public void CheckBucketACLGrantCantReadACP(String BucketName) {
		var AltClient = GetAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient.getBucketAcl(BucketName));
	}

	public void CheckBucketACLGrantCanWrite(String BucketName) {
		var AltClient = GetAltClient();
		AltClient.putObject(BucketName, "foo-write", "bar");
	}

	public void CheckBucketACLGrantCantWrite(String BucketName) {
		var AltClient = GetAltClient();
		assertThrows(AmazonServiceException.class, () -> AltClient.putObject(BucketName, "foo-write", "bar"));
	}

	public void CheckBucketACLGrantCanWriteACP(String BucketName) {
		var AltClient = GetAltClient();
		AltClient.setBucketAcl(BucketName, CannedAccessControlList.PublicRead);
	}

	public void CheckBucketACLGrantCantWriteACP(String BucketName) {
		var AltClient = GetAltClient();
		assertThrows(AmazonServiceException.class,
				() -> AltClient.setBucketAcl(BucketName, CannedAccessControlList.PublicRead));
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
			assertEquals(Expected.get(i).getNoncurrentVersionTransitions(), Actual.get(i).getNoncurrentVersionTransitions());
			assertEquals(Expected.get(i).getTransitions(), Actual.get(i).getTransitions());
			if (Expected.get(i).getNoncurrentVersionExpiration() != null) assertEquals(Expected.get(i).getNoncurrentVersionExpiration().getDays(), Actual.get(i).getNoncurrentVersionExpiration().getDays());
			assertEquals(Expected.get(i).getAbortIncompleteMultipartUpload(), Actual.get(i).getAbortIncompleteMultipartUpload());
		}
	}

	public boolean CheckLifecycleExpirationHeader(BucketLifecycleConfiguration Response, Date NowTime, String RuleID,
			int DeltaDays) {
		return true;
	}

	public boolean CheckLifecycleExpirationHeader(ObjectMetadata Response, Date NowTime, String RuleID, int DeltaDays) {
		return true;
	}

	public BucketLifecycleConfiguration SetupLifecycleExpiration(AmazonS3 Client, String BucketName, String RuleID,
			int DeltaDays, String RulePrefix) {
		var Rules = new ArrayList<Rule>();
		Rules.add(new Rule().withId(RuleID).withExpirationInDays(DeltaDays)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(RulePrefix)))
				.withStatus(BucketLifecycleConfiguration.ENABLED));

		var MyLifeCycle = new BucketLifecycleConfiguration(Rules);
		Client.setBucketLifecycleConfiguration(BucketName, MyLifeCycle);

		var Key = RulePrefix + "/foo";
		var Body = "bar";
		Client.putObject(BucketName, Key, Body);

		return Client.getBucketLifecycleConfiguration(BucketName);
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

			for (var Key : subExpected.keySet()) {
				assertEquals(subExpected.get(Key), subActual.get(Key));
			}
		}
	}

	public void TaggingCompare(List<Tag> Expected, List<Tag> Actual) {
		assertEquals(Expected.size(), Actual.size());

		var OrderExpected = TaggingSort(Expected);
		var OrderActual = TaggingSort(Actual);

		for (int i = 0; i < Expected.size(); i++) {
			assertEquals(OrderExpected.get(i).getKey(), OrderActual.get(i).getKey());
			assertEquals(OrderExpected.get(i).getValue(), OrderActual.get(i).getValue());
		}
	}

	public void DeleteSuspendedVersioningObj(AmazonS3 Client, String BucketName, String Key, List<String> VersionIDs,
			List<String> Contents) {
		Client.deleteObject(BucketName, Key);

		assertEquals(VersionIDs.size(), Contents.size());

		for (int i = VersionIDs.size() - 1; i >= 0; i--) {
			if (VersionIDs.get(i).equals("null")) {
				VersionIDs.remove(i);
				Contents.remove(i);
			}
		}
	}

	public void OverwriteSuspendedVersioningObj(AmazonS3 Client, String BucketName, String Key, List<String> VersionIDs,
			List<String> Contents, String Content) {
		Client.putObject(BucketName, Key, Content);

		assertEquals(VersionIDs.size(), Contents.size());

		for (int i = VersionIDs.size() - 1; i >= 0; i--) {
			if (VersionIDs.get(i).equals("null")) {
				VersionIDs.remove(i);
				Contents.remove(i);
			}
		}
		Contents.add(Content);
		VersionIDs.add("null");
	}

	public void RemoveObjVersion(AmazonS3 Client, String BucketName, String Key, List<String> VersionIDs,
			List<String> Contents, int index) {
		assertEquals(VersionIDs.size(), Contents.size());
		var rmVersionID = VersionIDs.get(index);
		VersionIDs.remove(index);
		var rmContent = Contents.get(index);
		Contents.remove(index);

		CheckObjContent(Client, BucketName, Key, rmVersionID, rmContent);

		Client.deleteVersion(BucketName, Key, rmVersionID);

		if (VersionIDs.size() != 0)
			CheckObjVersions(Client, BucketName, Key, VersionIDs, Contents);
	}

	public void CreateMultipleVersions(AmazonS3 Client, String BucketName, String Key, int NumVersions,
			List<String> VersionIDs, List<String> Contents, boolean CheckVersion) {

		for (int i = 0; i < NumVersions; i++) {
			var Body = String.format("content-%s", i);
			var Response = Client.putObject(BucketName, Key, Body);
			var VersionID = Response.getVersionId();

			Contents.add(Body);
			VersionIDs.add(VersionID);
		}

		if (CheckVersion)
			CheckObjVersions(Client, BucketName, Key, VersionIDs, Contents);

	}

	public void CreateMultipleVersion(AmazonS3 Client, String BucketName, String Key, int NumVersions,
			boolean CheckVersion) {
		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();

		for (int i = 0; i < NumVersions; i++) {
			var Body = String.format("content-%s", i);
			var Response = Client.putObject(BucketName, Key, Body);
			var VersionID = Response.getVersionId();

			Contents.add(Body);
			VersionIDs.add(VersionID);
		}

		if (CheckVersion)
			CheckObjVersions(Client, BucketName, Key, VersionIDs, Contents);

	}

	public void DoTestCreateRemoveVersions(AmazonS3 Client, String BucketName, String Key, int Numversions,
			int RemoveStartIdx, int IdxInc) {
		var VersionIDs = new ArrayList<String>();
		var Contents = new ArrayList<String>();
		CreateMultipleVersions(Client, BucketName, Key, Numversions, VersionIDs, Contents, true);
		var Idx = RemoveStartIdx;

		for (int i = 0; i < Numversions; i++) {
			RemoveObjVersion(Client, BucketName, Key, VersionIDs, Contents, Idx);
			Idx += IdxInc;
		}
	}

	public List<Thread> DoCreateVersionedObjConcurrent(AmazonS3 Client, String BucketName, String Key, int Num) {
		var TList = new ArrayList<Thread>();
		for (int i = 0; i < Num; i++) {
			var Item = Integer.toString(i);
			var mThread = new Thread(() -> Client.putObject(BucketName, Key, String.format("Data %s", Item)));
			mThread.start();
			TList.add(mThread);
		}
		return TList;
	}

	public List<Thread> DoClearVersionedBucketConcurrent(AmazonS3 Client, String BucketName) {
		var TList = new ArrayList<Thread>();
		var Response = Client.listVersions(BucketName, "");

		for (var Version : Response.getVersionSummaries()) {
			var mThread = new Thread(() -> Client.deleteVersion(BucketName, Version.getKey(), Version.getVersionId()));
			mThread.start();
			TList.add(mThread);
		}
		return TList;
	}

	public void CorsRequestAndCheck(String Method, String BucketName, Map<String, String> Headers, int StatusCode, String ExpectAllowOrigin, String ExpectAllowMethods, String Key) {
		var url = GetURL(BucketName);
		if (Key != null)
			url += String.format("/%s", Key);

		try {
			System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
			HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
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
	
    public void PartsETagCompare(List<PartETag> Expected, List<PartSummary> Actual)
    {
        assertEquals(Expected.size(), Actual.size());

        for(int i=0;i< Expected.size(); i++)
        {
            assertEquals(Expected.get(i).getPartNumber(), Actual.get(i).getPartNumber());
            assertEquals(Expected.get(i).getETag(), Actual.get(i).getETag());
        }
    }

	public void VersionIDsCompare(List<S3VersionSummary> Expected, List<S3VersionSummary> Actual)
	{
        assertEquals(Expected.size(), Actual.size());

        for(int i=0;i< Expected.size(); i++)
        {
            assertEquals(Expected.get(i).getVersionId(), Actual.get(i).getVersionId());
            assertEquals(Expected.get(i).getETag(), Actual.get(i).getETag());
            assertEquals(Expected.get(i).getKey(), Actual.get(i).getKey());
            assertEquals(Expected.get(i).getSize(), Actual.get(i).getSize());
        }
	}

	/*****************************************
	 * Bucket Clear
	 ******************************************************/
	public void DeleteBucketList(String BucketName)
	{
		BucketList.remove(BucketName);
	}

	@AfterEach
	public void Clear() {
		BucketClear();
	}

	public void BucketClear(AmazonS3 Client, String BucketName) {
		if (Client == null) return;
		if (StringUtils.isBlank(BucketName)) return;

		try {
			var Result = Client.listVersions(new ListVersionsRequest().withBucketName(BucketName).withMaxResults(2000));
			var ObjectList = Result.getVersionSummaries();

			for (var Object : ObjectList)
				Client.deleteVersion(new DeleteVersionRequest(BucketName, Object.getKey(), Object.getVersionId()));

		} catch (AmazonServiceException e) {
			// System.out.format("Error : Bucket(%s) Clear Failed(%s, %d)\n", BucketName, e.getErrorCode(), e.getStatusCode());
		}

		try {
			Client.deleteBucket(BucketName);
		} catch (AmazonServiceException e) {
			System.out.format("Error : Bucket(%s) Delete Failed(%s, %d)\n", BucketName, e.getErrorCode(), e.getStatusCode());
		}
	}

	public void BucketClear() {
		var Client = GetClient();
		if (Client == null)
			return;
		if (BucketList == null)
			return;
		if (BucketList.size() < 1)
			return;

		var iter = BucketList.iterator();
		while (iter.hasNext()) {
			String BucketName = (String) iter.next();
			if (StringUtils.isNotBlank(BucketName)) BucketClear(Client, BucketName);
			iter.remove();
		}

	}

	/*******************************************
	 * Utility
	 ********************************************************/

	public String RandomText(int Length) {
		StringBuffer sb = new StringBuffer();
		Random rand = new Random();

		for (int i = 0; i < Length; i++)
			sb.append(TEXT[rand.nextInt(TEXT.length)]);
		return sb.toString();
	}

	public String RandomTextToLong(int Length) {
		StringBuffer sb = new StringBuffer();
		Random rand = new Random();

		for (int i = 0; i < Length; i++)
			sb.append(TEXT_String[rand.nextInt(TEXT_String.length)]);
		return sb.toString();
	}

	private static String CreateURLToHTTP(String Address, int Port) {
		String URL = MainData.HTTP + Address;

		if (URL.endsWith("/"))
			URL = URL.substring(0, URL.length() - 1);

		return String.format("%s:%d", URL, Port);
	}

	private static String CreateURLToHTTPS(String Address, int Port) {
		String URL = MainData.HTTPS + Address;

		if (URL.endsWith("/"))
			URL = URL.substring(0, URL.length() - 1);

		return String.format("%s:%d", URL, Port);
	}

	public String GetMD5(String str) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes("UTF-8"));

			byte byteData[] = md.digest();

			Encoder encoder = Base64.getEncoder();
			var encodeBytes = encoder.encode(byteData);
			return new String(encodeBytes);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public ArrayList<String> GenerateRandomString(int Size, int PartSize) {
		ArrayList<String> StringList = new ArrayList<String>();

		int RemainSize = Size;
		while (RemainSize > 0) {
			int NowPartSize;
			if (RemainSize > PartSize)
				NowPartSize = PartSize;
			else
				NowPartSize = RemainSize;

			StringList.add(RandomTextToLong(NowPartSize));

			RemainSize -= NowPartSize;
		}

		return StringList;
	}

	public String GetBase64EncodedSHA1Hash(String Policy, String SecretKey) {
		var signingKey = new SecretKeySpec(SecretKey.getBytes(), "HmacSHA1");
		Mac mac;
		try {
			mac = Mac.getInstance("HmacSHA1");
			mac.init(signingKey);
		} catch (NoSuchAlgorithmException e) {
			fail(e.getMessage());
			return "";
		} catch (InvalidKeyException e) {
			fail(e.getMessage());
			return "";
		}

		var encoder = Base64.getEncoder();
		return encoder.encodeToString((mac.doFinal(Policy.getBytes())));
	}

	public void Delay(int milliseconds)
	{
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
		}
	}
}
