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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.HashMap;

import org.example.s3tests.FormFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


public class Post extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Post Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Post End");
	}

	@Test
	@Tag( "Upload")
	//post 방식으로 권한없는 사용자가 파일 업로드할 경우 성공 확인
	public void test_post_object_anonymous_request()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		var Key = "foo.txt";
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var ContentType = "text/plain";
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "acl", "public-read" );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);


		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("Upload")
	//post 방식으로 로그인 정보를 포함한 파일 업로드할 경우 성공 확인
	public void test_post_object_authenticated_request()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var ContentType = "text/plain";
		var Key = "foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("Upload")
	//content-type 헤더 정보 없이  post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_authenticated_no_content_type()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		var ContentType = "text/plain";
		var Key = "foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);
		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("ERROR")
	//[AccessKey 값이 틀린 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_authenticated_request_bad_access_key()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var ContentType = "text/plain";
		var Key = "foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", "foo" );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(403, Result.StatusCode);
	}

	@Test
	@Tag("StatusCode")
	//[성공시 반환상태값을 201로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
	public void test_post_object_set_success_code()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		var Key = "foo.txt";
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var ContentType = "text/plain";
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "acl", "public-read" );
		Payload.put( "Content-Type", ContentType );
		Payload.put( "success_action_status" , "201" );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(201, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("StatusCode")
	//[성공시 반환상태값을 에러코드인 404로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
	public void test_post_object_set_invalid_success_code()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		var Key = "foo.txt";

		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));
		var ContentType = "text/plain";

		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "acl", "public-read" );
		Payload.put( "Content-Type", ContentType );
		Payload.put( "success_action_status" , "404" );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("Upload")
	//post 방식으로 로그인정보를 포함한 대용량 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_upload_larger_than_chunk()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var Size = 5 * 1024 * 1024;
		var Data = RandomTextToLong(Size);

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(Size);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, Data);
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertTrue(Data.equals(Body), "Source does not match target");
	}

	@Test
	@Tag("Upload")
	//[오브젝트 이름을 로그인정보에 포함되어 있는 key값으로 대체할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_set_key_from_filename()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("Upload")
	//post 방식으로 로그인, 헤더 정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_ignored_header()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "x-ignore-foo" , "bar" );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);
	}

	@Test
	@Tag("Upload")
	//[헤더정보에 대소문자를 섞어서 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_case_insensitive_condition_fields()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bUcKeT", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("StArTs-WiTh");
		starts1.add("$KeY");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("AcL", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("StArTs-WiTh");
		starts2.add("$CoNtEnT-TyPe");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "kEy", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "aCl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "pOLICy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);
	}

	@Test
	@Tag("Upload")
	//[오브젝트 이름에 '\'를 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_escaped_field_values()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("Upload")
	//[redirect url설정하여 체크] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_success_redirect_action()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite));

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var RedirectURL = GetURL(BucketName);

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var starts3 = new JsonArray();
		starts3.add("eq");
		starts3.add("$success_action_redirect");
		starts3.add(RedirectURL);
		Conditions.add(starts3);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );
		Payload.put( "success_action_redirect" , RedirectURL );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(200, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		assertEquals(String.format("%s?bucket=%s&key=%s&etag=%s%s%s", RedirectURL, BucketName, Key, "%22", Response.getObjectMetadata().getETag().replace("\"", ""),"%22") , Result.URL);
	}

	@Test
	@Tag("ERROR")
	//[SecretKey Hash 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_invalid_signature()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature.substring(0, Signature.length() - 1) );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(403, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[AccessKey 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_invalid_access_key()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey.substring(0, Config.MainUser.AccessKey.length() - 1) );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(403, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[로그인 정보의 날짜포맷이 다를경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_invalid_date_format()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100).replace("T", " "));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[오브젝트 이름을 입력하지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_no_key_specified()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile("", ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[signature 정보를 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_missing_signature()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy에 버킷 이름을 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_missing_policy_condition()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(403, Result.StatusCode);
	}

	@Test
	@Tag("Metadata")
	//[사용자가 추가 메타데이터를 입력한 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
	public void test_post_object_user_specified_header()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$x-amz-meta-foo");
		starts3.add("bar");
		Conditions.add(starts3);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "x-amz-meta-foo" , "barclamp" );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);

		var Response = Client.getObject(BucketName, Key);
		assertEquals("barclamp", Response.getObjectMetadata().getUserMetadata().get(("foo")));
	}

	@Test
	@Tag("ERROR")
	//[사용자가 추가 메타데이터를 policy에 설정하였으나 오브젝트에 해당 정보가 누락된 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_request_missing_policy_specified_field()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$x-amz-meta-foo");
		starts3.add("bar");
		Conditions.add(starts3);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(403, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy의 condition을 대문자(CONDITIONS)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_condition_is_case_sensitive()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("CONDITIONS", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy의 expiration을 대문자(EXPIRATION)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_expires_is_case_sensitive()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("EXPIRATION", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy의 expiration을 만료된 값으로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_expired_policy()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(-100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(403, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[사용자가 추가 메타데이터를 policy에 설정하였으나 설정정보가 올바르지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_invalid_request_field_value()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var starts3 = new JsonArray();
		starts3.add("eq");
		starts3.add("$x-amz-meta-foo");
		starts3.add("");
		Conditions.add(starts3);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "x-amz-meta-foo" , "barclamp" );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(403, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy의 expiration값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_missing_expires_condition()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";
		var PolicyDocument = new JsonObject();

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );
		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy의 conditions값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_missing_conditions_list()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );
		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy에 설정한 용량보다 큰 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_upload_size_limit_exceeded()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(0);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy에 용량정보 설정을 누락할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_missing_content_length_argument()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy에 용량정보 설정값이 틀렸을 경우(용량값을 음수로 입력) post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_invalid_content_length_argument()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(-1);
		ContentLengthRange.add(0);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy에 설정한 용량보다 작은 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_upload_size_below_minimum()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "\\$foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("\\$foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(512);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("ERROR")
	//[policy의 conditions값이 비어있을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
	public void test_post_object_empty_conditions()
	{
		var BucketName = GetNewBucket();

		var ContentType = "text/plain";
		var Key = "foo.txt";

		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));
		PolicyDocument.add("conditions", new JsonArray());

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );

		var Result = PostUpload(BucketName, Payload, FileData);
		assertEquals(400, Result.StatusCode);
	}

	@Test
	@Tag("PresignedURL")
	//PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_presignedurl_put_get()
	{
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo";

		var PutURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.PUT);
		var PutResponse = PutObject(PutURL, Key);
		assertEquals(200, PutResponse.getStatusLine().getStatusCode());

		var GetURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.GET);
		var GetResponse = GetObject(GetURL);
		assertEquals(200, GetResponse.getStatusLine().getStatusCode());

	}


	@Test
	@Tag("PresignedURL")
	//[SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_presignedurl_put_get_v4()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClientV4(true);
		var Key = "foo";

		Client.createBucket(BucketName);

		var PutURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.PUT);
		var PutResponse = PutObject(PutURL, Key);
		assertEquals(200, PutResponse.getStatusLine().getStatusCode());

		var GetURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.GET);
		var GetResponse = GetObject(GetURL);
		assertEquals(200, GetResponse.getStatusLine().getStatusCode());
	}
}
