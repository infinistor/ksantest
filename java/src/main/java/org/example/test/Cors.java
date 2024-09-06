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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;

public class Cors extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Cors Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Cors End");
	}

	@Test
	@Tag("Check")
	public void testSetCors() {
		var client = getClient();
		var bucketName = createBucket(client);

		var allowedMethods = List.of(AllowedMethods.GET, AllowedMethods.PUT);
		var allowedOrigins = List.of("*.get", "*.put");

		var corsConfig = new BucketCrossOriginConfiguration()
				.withRules(new CORSRule().withAllowedMethods(allowedMethods).withAllowedOrigins(allowedOrigins));

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
	@Tag("Post")
	public void testCorsOriginResponse() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var corsConfig = new BucketCrossOriginConfiguration();
		var rules = new ArrayList<CORSRule>();
		rules.add(new CORSRule()
				.withAllowedMethods(List.of(AllowedMethods.GET))
				.withAllowedOrigins(List.of("*suffix")));
		rules.add(new CORSRule()
				.withAllowedMethods(List.of(AllowedMethods.GET))
				.withAllowedOrigins(List.of("start*end")));
		rules.add(new CORSRule()
				.withAllowedMethods(List.of(AllowedMethods.GET))
				.withAllowedOrigins(List.of("prefix*")));
		rules.add(new CORSRule()
				.withAllowedMethods(List.of(AllowedMethods.PUT))
				.withAllowedOrigins(List.of("*.put")));
		corsConfig.setRules(rules);

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);

		var headers = new HashMap<String, String>();

		corsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix");
		corsRequestAndCheck("GET", bucketName, headers, 200, "foo.suffix", "GET", null);
		headers.clear();
		headers.put("Origin", "foo.bar");
		corsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix.get");
		corsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);
		headers.clear();
		headers.put("Origin", "start_end");
		corsRequestAndCheck("GET", bucketName, headers, 200, "start_end", "GET", null);
		headers.clear();
		headers.put("Origin", "start1end");
		corsRequestAndCheck("GET", bucketName, headers, 200, "start1end", "GET", null);
		headers.clear();
		headers.put("Origin", "start12end");
		corsRequestAndCheck("GET", bucketName, headers, 200, "start12end", "GET", null);
		headers.clear();
		headers.put("Origin", "0start12end");
		corsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);
		headers.clear();
		headers.put("Origin", "prefix");
		corsRequestAndCheck("GET", bucketName, headers, 200, "prefix", "GET", null);
		headers.clear();
		headers.put("Origin", "prefix.suffix");
		corsRequestAndCheck("GET", bucketName, headers, 200, "prefix.suffix", "GET", null);
		headers.clear();
		headers.put("Origin", "bla.prefix");
		corsRequestAndCheck("GET", bucketName, headers, 200, null, null, null);

		headers.clear();
		headers.put("Origin", "foo.suffix");
		corsRequestAndCheck("GET", bucketName, headers, 404, "foo.suffix", "GET", "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "GET");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, 403, "foo.suffix", "GET", "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "PUT");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, 403, null, null, "bar");

		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "DELETE");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, 403, null, null, "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, 403, null, null, "bar");

		headers.clear();
		headers.put("Origin", "foo.put");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, 403, "foo.put", "PUT", "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		corsRequestAndCheck("GET", bucketName, headers, 404, "foo.suffix", "GET", "bar");

		headers.clear();
		corsRequestAndCheck("OPTIONS", bucketName, headers, 400, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.bla");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "GET");
		headers.put("content-length", "0");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "foo.suffix", "GET", "bar");
		headers.clear();
		headers.put("Origin", "foo.bar");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix.get");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear();
		headers.put("Origin", "start_end");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "start_end", "GET", null);
		headers.clear();
		headers.put("Origin", "start1end");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "start1end", "GET", null);
		headers.clear();
		headers.put("Origin", "start12end");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "start12end", "GET", null);
		headers.clear();
		headers.put("Origin", "0start12end");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear();
		headers.put("Origin", "prefix");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "prefix", "GET", null);
		headers.clear();
		headers.put("Origin", "prefix.suffix");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "prefix.suffix", "GET", null);
		headers.clear();
		headers.put("Origin", "bla.prefix");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.put");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.put");
		headers.put("Access-Control-Request-Method", "PUT");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "foo.put", "PUT", null);
	}

	@Test
	@Tag("Post")
	public void testCorsOriginWildcard() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var corsConfig = new BucketCrossOriginConfiguration();
		var rules = new ArrayList<CORSRule>();
		rules.add(new CORSRule().withAllowedMethods(List.of(AllowedMethods.GET))
				.withAllowedOrigins(List.of("*")));
		corsConfig.setRules(rules);

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);

		var headers = new HashMap<String, String>();
		corsRequestAndCheck("GET", bucketName, new HashMap<>(), 200, null, null, null);
		headers.clear();
		headers.put("Origin", "example.origin");
		corsRequestAndCheck("GET", bucketName, headers, 200, "*", "GET", null);
	}

	@Test
	@Tag("Post")
	public void testCorsHeaderOption() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var corsConfig = new BucketCrossOriginConfiguration();
		var rules = new ArrayList<CORSRule>();
		rules.add(new CORSRule()
				.withAllowedMethods(List.of(AllowedMethods.GET))
				.withAllowedOrigins(List.of("*"))
				.withExposedHeaders(List.of("x-amz-meta-header1")));
		corsConfig.setRules(rules);

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);

		var headers = new HashMap<String, String>();
		headers.clear();
		headers.put("Origin", "example.origin");
		headers.put("Access-Control-Request-headers", "x-amz-meta-header2");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 403, null, null, "bar");
	}
}