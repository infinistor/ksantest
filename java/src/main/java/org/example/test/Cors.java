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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.jupiter.api.Tag;

import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;

public class Cors extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Cors Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Cors End");
	}

	// @Test
	@Tag("Check")
	@Tag("KSAN")
	//버킷의 cors정보 세팅 성공 확인
	public void test_set_cors() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		var allowedMethods = new ArrayList<>(Arrays.asList(new AllowedMethods[] { AllowedMethods.GET, AllowedMethods.PUT }));
		var AllowedOrigins = new ArrayList<>(Arrays.asList(new String[] { "*.get", "*.put" }));

		var CORSConfig = new BucketCrossOriginConfiguration().withRules(new CORSRule().withAllowedMethods(allowedMethods).withAllowedOrigins(AllowedOrigins));

		var Response = Client.getBucketCrossOriginConfiguration(BucketName);
		assertNull(Response);

		Client.setBucketCrossOriginConfiguration(BucketName, CORSConfig);
		Response = Client.getBucketCrossOriginConfiguration(BucketName);
		assertEquals(allowedMethods, Response.getRules().get(0).getAllowedMethods());
		assertEquals(AllowedOrigins, Response.getRules().get(0).getAllowedOrigins());

		Client.deleteBucketCrossOriginConfiguration(BucketName);
		Response = Client.getBucketCrossOriginConfiguration(BucketName);
		assertNull(Response);
	}

	// @Test
	@Tag("Post")
	@Tag("KSAN")
	//버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인
	public void test_cors_origin_response()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicRead));

		var CORSConfig = new BucketCrossOriginConfiguration();
		var Rules = new ArrayList<CORSRule>();
		Rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"*suffix"})));
		Rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"start*end"})));
		Rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"prefix*"})));
		Rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.PUT}))
				.withAllowedOrigins(Arrays.asList(new String[] {"*.put"})));
		CORSConfig.setRules(Rules);

		var Response = Client.getBucketCrossOriginConfiguration(BucketName);
		assertNull(Response);

		Client.setBucketCrossOriginConfiguration(BucketName, CORSConfig);

		var Headers = new HashMap<String, String>();

		CorsRequestAndCheck("GET", BucketName, Headers, 200, null, null, null);
		Headers.clear(); Headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, "foo.suffix", "GET", null);
		Headers.clear(); Headers.put("Origin", "foo.bar");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, null, null, null);
		Headers.clear(); Headers.put("Origin", "foo.suffix.get");
		CorsRequestAndCheck("GET", BucketName, Headers , 200, null, null, null);
		Headers.clear(); Headers.put("Origin", "startend");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, "startend", "GET", null);
		Headers.clear(); Headers.put("Origin", "start1end");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, "start1end", "GET", null);
		Headers.clear(); Headers.put("Origin", "start12end");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, "start12end", "GET", null);
		Headers.clear(); Headers.put("Origin", "0start12end");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, null, null, null);
		Headers.clear(); Headers.put("Origin", "prefix");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, "prefix", "GET", null);
		Headers.clear(); Headers.put("Origin", "prefix.suffix");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, "prefix.suffix", "GET", null);
		Headers.clear(); Headers.put("Origin", "bla.prefix");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, null, null, null);

		Headers.clear(); Headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("GET", BucketName, Headers, 404, "foo.suffix", "GET", "bar");
		Headers.clear(); Headers.put("Origin", "foo.suffix"); Headers.put("Access-Control-Request-Method", "GET"); Headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", BucketName, Headers, 403, "foo.suffix", "GET", "bar");
		Headers.clear(); Headers.put("Origin", "foo.suffix");Headers.put("Access-Control-Request-Method", "PUT"); Headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", BucketName, Headers, 403, null, null, "bar");

		Headers.clear(); Headers.put("Origin", "foo.suffix"); Headers.put("Access-Control-Request-Method", "DELETE"); Headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", BucketName, Headers, 403, null, null, "bar");
		Headers.clear(); Headers.put("Origin", "foo.suffix"); Headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", BucketName, Headers, 403, null, null, "bar");

		Headers.clear(); Headers.put("Origin", "foo.put"); Headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", BucketName, Headers, 403, "foo.put", "PUT", "bar");
		Headers.clear(); Headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("GET", BucketName, Headers, 404, "foo.suffix", "GET", "bar");

		Headers.clear();
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 400, null, null, null);
		Headers.clear(); Headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, null);// 403 => 400
		Headers.clear(); Headers.put("Origin", "foo.bla");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, null);//403 => 400
		Headers.clear(); Headers.put("Origin", "foo.suffix"); Headers.put("Access-Control-Request-Method", "GET"); Headers.put("content-length", "0");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 200, "foo.suffix", "GET", "bar");
		Headers.clear(); Headers.put("Origin", "foo.bar"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, null);
		Headers.clear(); Headers.put("Origin", "foo.suffix.get"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, null);
		Headers.clear(); Headers.put("Origin", "startend"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 200, "startend", "GET", null);
		Headers.clear(); Headers.put("Origin", "start1end"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 200, "start1end", "GET", null);
		Headers.clear(); Headers.put("Origin", "start12end"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 200, "start12end", "GET", null);
		Headers.clear(); Headers.put("Origin", "0start12end"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, null);
		Headers.clear(); Headers.put("Origin", "prefix"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 200, "prefix", "GET", null);
		Headers.clear(); Headers.put("Origin", "prefix.suffix"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 200, "prefix.suffix", "GET", null);
		Headers.clear(); Headers.put("Origin", "bla.prefix"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, null);
		Headers.clear(); Headers.put("Origin", "foo.put"); Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, null);
		Headers.clear(); Headers.put("Origin", "foo.put"); Headers.put("Access-Control-Request-Method", "PUT");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 200, "foo.put", "PUT", null);
	}

	// @Test
	@Tag("Post")
	@Tag("KSAN")
	//와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인
	public void test_cors_origin_wildcard()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicRead));

		var CORSConfig = new BucketCrossOriginConfiguration();
		var Rules = new ArrayList<CORSRule>();
		Rules.add(new CORSRule().withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET})).withAllowedOrigins(Arrays.asList(new String[] {"*"})));
		CORSConfig.setRules(Rules);

		var Response = Client.getBucketCrossOriginConfiguration(BucketName);
		assertNull(Response);

		Client.setBucketCrossOriginConfiguration(BucketName, CORSConfig);

		var Headers = new HashMap<String, String>();
		CorsRequestAndCheck("GET", BucketName, new HashMap<String, String>(), 200, null, null, null);
		Headers.clear(); Headers.put("Origin", "example.origin");
		CorsRequestAndCheck("GET", BucketName, Headers, 200, "*", "GET", null);
	}

	// @Test
	@Tag("Post")
	@Tag("KSAN")
	// @Tag("cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인
	public void test_cors_header_option()
	{
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		Client.createBucket(new CreateBucketRequest(BucketName).withCannedAcl(CannedAccessControlList.PublicRead));

		var CORSConfig = new BucketCrossOriginConfiguration();
		var Rules = new ArrayList<CORSRule>();
		Rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"*"}))
				.withExposedHeaders(Arrays.asList(new String[] {"x-amz-meta-header1"})));
		CORSConfig.setRules(Rules);

		var Response = Client.getBucketCrossOriginConfiguration(BucketName);
		assertNull(Response);

		Client.setBucketCrossOriginConfiguration(BucketName, CORSConfig);

		var Headers = new HashMap<String, String>();
		Headers.clear();
		Headers.put("Origin", "example.origin");
		Headers.put("Access-Control-Request-Headers", "x-amz-meta-header2");
		Headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", BucketName, Headers, 403, null, null, "bar");
	}
}