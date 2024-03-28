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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;

public class Cors extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("Cors SDK V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("Cors SDK V2 End");
	}

	@Test
	@Tag("Check")
	//버킷의 cors정보 세팅 성공 확인
	public void test_set_cors() {
		var bucketName = getNewBucket();
		var client = getClient();

		var allowedMethods = new ArrayList<>(Arrays.asList(new AllowedMethods[] { AllowedMethods.GET, AllowedMethods.PUT }));
		var allowedOrigins = new ArrayList<>(Arrays.asList(new String[] { "*.get", "*.put" }));

		var corsConfig = new BucketCrossOriginConfiguration().withRules(new CORSRule().withAllowedMethods(allowedMethods).withAllowedOrigins(allowedOrigins));

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);
		response = client.getBucketCrossOriginConfiguration(bucketName);
		assertEquals(allowedMethods, response.getRules().get(0).getAllowedMethods());
		assertEquals(allowedOrigins, response.getRules().get(0).getAllowedOrigins());

		client.deleteBucketCrossOriginConfiguration(bucketName);
		response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);
	}

	@Test
	@Ignore
	@Tag("Post")
	//버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인
	public void test_cors_origin_response()
	{
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));

		var corsConfig = new BucketCrossOriginConfiguration();
		var rules = new ArrayList<CORSRule>();
		rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"*suffix"})));
		rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"start*end"})));
		rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"prefix*"})));
		rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.PUT}))
				.withAllowedOrigins(Arrays.asList(new String[] {"*.put"})));
		corsConfig.setRules(rules);

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);

		var headers = new HashMap<String, String>();

		CorsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);
		headers.clear(); headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("GET", bucketName, headers, 200, "foo.suffix", "GET", null);
		headers.clear(); headers.put("Origin", "foo.bar");
		CorsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);
		headers.clear(); headers.put("Origin", "foo.suffix.get");
		CorsRequestAndCheck("GET", bucketName, headers , 200, null, null, null);
		headers.clear(); headers.put("Origin", "startend");
		CorsRequestAndCheck("GET", bucketName, headers, 200, "startend", "GET", null);
		headers.clear(); headers.put("Origin", "start1end");
		CorsRequestAndCheck("GET", bucketName, headers, 200, "start1end", "GET", null);
		headers.clear(); headers.put("Origin", "start12end");
		CorsRequestAndCheck("GET", bucketName, headers, 200, "start12end", "GET", null);
		headers.clear(); headers.put("Origin", "0start12end");
		CorsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);
		headers.clear(); headers.put("Origin", "prefix");
		CorsRequestAndCheck("GET", bucketName, headers, 200, "prefix", "GET", null);
		headers.clear(); headers.put("Origin", "prefix.suffix");
		CorsRequestAndCheck("GET", bucketName, headers, 200, "prefix.suffix", "GET", null);
		headers.clear(); headers.put("Origin", "bla.prefix");
		CorsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);

		headers.clear(); headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("GET", bucketName, headers, 404, "foo.suffix", "GET", "bar");
		headers.clear(); headers.put("Origin", "foo.suffix"); headers.put("Access-Control-Request-Method", "GET"); headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", bucketName, headers, 403, "foo.suffix", "GET", "bar");
		headers.clear(); headers.put("Origin", "foo.suffix");headers.put("Access-Control-Request-Method", "PUT"); headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", bucketName, headers, 403, null, null, "bar");

		headers.clear(); headers.put("Origin", "foo.suffix"); headers.put("Access-Control-Request-Method", "DELETE"); headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", bucketName, headers, 403, null, null, "bar");
		headers.clear(); headers.put("Origin", "foo.suffix"); headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", bucketName, headers, 403, null, null, "bar");

		headers.clear(); headers.put("Origin", "foo.put"); headers.put("content-length", "0");
		CorsRequestAndCheck("PUT", bucketName, headers, 403, "foo.put", "PUT", "bar");
		headers.clear(); headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("GET", bucketName, headers, 404, "foo.suffix", "GET", "bar");

		headers.clear();
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 400, null, null, null);
		headers.clear(); headers.put("Origin", "foo.suffix");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);// 403 => 400
		headers.clear(); headers.put("Origin", "foo.bla");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);//403 => 400
		headers.clear(); headers.put("Origin", "foo.suffix"); headers.put("Access-Control-Request-Method", "GET"); headers.put("content-length", "0");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 200, "foo.suffix", "GET", "bar");
		headers.clear(); headers.put("Origin", "foo.bar"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear(); headers.put("Origin", "foo.suffix.get"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear(); headers.put("Origin", "startend"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 200, "startend", "GET", null);
		headers.clear(); headers.put("Origin", "start1end"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 200, "start1end", "GET", null);
		headers.clear(); headers.put("Origin", "start12end"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 200, "start12end", "GET", null);
		headers.clear(); headers.put("Origin", "0start12end"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear(); headers.put("Origin", "prefix"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 200, "prefix", "GET", null);
		headers.clear(); headers.put("Origin", "prefix.suffix"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 200, "prefix.suffix", "GET", null);
		headers.clear(); headers.put("Origin", "bla.prefix"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear(); headers.put("Origin", "foo.put"); headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear(); headers.put("Origin", "foo.put"); headers.put("Access-Control-Request-Method", "PUT");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 200, "foo.put", "PUT", null);
	}

	@Test
	@Ignore
	@Tag("Post")
	//와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인
	public void test_cors_origin_wildcard()
	{
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));

		var corsConfig = new BucketCrossOriginConfiguration();
		var rules = new ArrayList<CORSRule>();
		rules.add(new CORSRule().withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET})).withAllowedOrigins(Arrays.asList(new String[] {"*"})));
		corsConfig.setRules(rules);

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);

		var headers = new HashMap<String, String>();
		CorsRequestAndCheck("GET", bucketName, new HashMap<String, String>(), 200, null, null, null);
		headers.clear(); headers.put("Origin", "example.origin");
		CorsRequestAndCheck("GET", bucketName, headers, 200, "*", "GET", null);
	}

	@Test
	@Ignore
	@Tag("Post")
	// @Tag("cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인
	public void test_cors_header_option()
	{
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicRead));

		var corsConfig = new BucketCrossOriginConfiguration();
		var rules = new ArrayList<CORSRule>();
		rules.add(new CORSRule()
				.withAllowedMethods(Arrays.asList(new AllowedMethods[] {AllowedMethods.GET}))
				.withAllowedOrigins(Arrays.asList(new String[] {"*"}))
				.withExposedHeaders(Arrays.asList(new String[] {"x-amz-meta-header1"})));
		corsConfig.setRules(rules);

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);

		var headers = new HashMap<String, String>();
		headers.clear();
		headers.put("Origin", "example.origin");
		headers.put("Access-Control-Request-headers", "x-amz-meta-header2");
		headers.put("Access-Control-Request-Method", "GET");
		CorsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, "bar");
	}
}