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

import org.apache.hc.core5.http.HttpStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;

public class Cors extends TestBase {
	@Test
	@Tag("Check")
	public void testSetCors() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

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
		var bucketName = createBucketCannedAcl(client, 2);
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
		waitForBucketCrossOriginConfiguration(client, bucketName);

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
		corsRequestAndCheck("GET", bucketName, headers, HttpStatus.SC_NOT_FOUND, "foo.suffix", "GET", "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "GET");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, HttpStatus.SC_FORBIDDEN, "foo.suffix", "GET", "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "PUT");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, "bar");

		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "DELETE");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, "bar");

		headers.clear();
		headers.put("Origin", "foo.put");
		headers.put("content-length", "0");
		corsRequestAndCheck("PUT", bucketName, headers, HttpStatus.SC_FORBIDDEN, "foo.put", "PUT", "bar");
		headers.clear();
		headers.put("Origin", "foo.suffix");
		corsRequestAndCheck("GET", bucketName, headers, HttpStatus.SC_NOT_FOUND, "foo.suffix", "GET", "bar");

		headers.clear();
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_BAD_REQUEST, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix");
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.bla");
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix");
		headers.put("Access-Control-Request-Method", "GET");
		headers.put("content-length", "0");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "foo.suffix", "GET", "bar");
		headers.clear();
		headers.put("Origin", "foo.bar");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.suffix.get");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, null);
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
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, null);
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
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.put");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, null);
		headers.clear();
		headers.put("Origin", "foo.put");
		headers.put("Access-Control-Request-Method", "PUT");
		corsRequestAndCheck("OPTIONS", bucketName, headers, 200, "foo.put", "PUT", null);
	}

	@Test
	@Tag("Post")
	public void testCorsOriginWildcard() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 3);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var corsConfig = new BucketCrossOriginConfiguration();
		var rules = new ArrayList<CORSRule>();
		rules.add(new CORSRule().withAllowedMethods(List.of(AllowedMethods.GET))
				.withAllowedOrigins(List.of("*")));
		corsConfig.setRules(rules);

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertNull(response);

		client.setBucketCrossOriginConfiguration(bucketName, corsConfig);
		waitForBucketCrossOriginConfiguration(client, bucketName);

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
		var bucketName = createBucketCannedAcl(client, 4);
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
		waitForBucketCrossOriginConfiguration(client, bucketName);

		var headers = new HashMap<String, String>();
		headers.clear();
		headers.put("Origin", "example.origin");
		headers.put("Access-Control-Request-headers", "x-amz-meta-header2");
		headers.put("Access-Control-Request-Method", "GET");
		corsRequestAndCheck("OPTIONS", bucketName, headers, HttpStatus.SC_FORBIDDEN, null, null, "bar");
	}

	@Test
	@Tag("Check")
	public void testCorsAllFields() {
		var client = getClient();
		var bucketName = createBucket(client, 5);
		var rules = List.of(
				new CORSRule().withId("rule-1")
						.withAllowedMethods(List.of(AllowedMethods.GET, AllowedMethods.PUT))
						.withAllowedOrigins(List.of("http://a.example.com"))
						.withAllowedHeaders(List.of("x-amz-*", "content-type"))
						.withExposedHeaders(List.of("ETag", "x-amz-meta-foo"))
						.withMaxAgeSeconds(600),
				new CORSRule().withId("rule-2")
						.withAllowedMethods(List.of(AllowedMethods.GET))
						.withAllowedOrigins(List.of("*"))
						.withMaxAgeSeconds(20));
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(rules));

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertEquals(2, response.getRules().size());
		var first = response.getRules().get(0);
		var second = response.getRules().get(1);
		assertEquals("rule-1", first.getId());
		assertEquals(List.of(AllowedMethods.GET, AllowedMethods.PUT), first.getAllowedMethods());
		assertEquals(List.of("http://a.example.com"), first.getAllowedOrigins());
		assertEquals(List.of("x-amz-*", "content-type"), first.getAllowedHeaders());
		assertEquals(List.of("ETag", "x-amz-meta-foo"), first.getExposedHeaders());
		assertEquals(600, first.getMaxAgeSeconds());
		assertEquals("rule-2", second.getId());
		assertEquals(List.of(AllowedMethods.GET), second.getAllowedMethods());
		assertEquals(List.of("*"), second.getAllowedOrigins());
		assertEquals(20, second.getMaxAgeSeconds());
	}

	@Test
	@Tag("Preflight")
	public void testCorsPreflightHeaders() {
		var client = getClient();
		var bucketName = createBucket(client, 6);
		var rules = List.of(new CORSRule()
				.withAllowedMethods(List.of(AllowedMethods.GET, AllowedMethods.PUT))
				.withAllowedOrigins(List.of("http://a.example.com"))
				.withAllowedHeaders(List.of("x-amz-*", "content-type"))
				.withExposedHeaders(List.of("ETag", "x-amz-meta-foo"))
				.withMaxAgeSeconds(600));
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(rules));
		waitForBucketCrossOriginConfiguration(client, bucketName);

		// 허용된 헤더(와일드카드, 대소문자 무시)로 preflight 요청
		var headers = new HashMap<String, String>();
		headers.put("Origin", "http://a.example.com");
		headers.put("Access-Control-Request-Method", "PUT");
		headers.put("Access-Control-Request-Headers", "X-Amz-Meta-A, Content-Type");
		var response = corsRequest("OPTIONS", bucketName, headers, null);
		assertEquals(200, response.statusCode());
		assertEquals("http://a.example.com", response.header("access-control-allow-origin"));
		assertEquals("GET, PUT", response.header("access-control-allow-methods"));
		assertEquals("x-amz-meta-a, content-type", response.header("access-control-allow-headers"));
		assertEquals("ETag, x-amz-meta-foo", response.header("access-control-expose-headers"));
		assertEquals("600", response.header("access-control-max-age"));
		assertEquals("true", response.header("access-control-allow-credentials"));

		// 허용되지 않은 헤더를 요청하면 거부된다
		headers.clear();
		headers.put("Origin", "http://a.example.com");
		headers.put("Access-Control-Request-Method", "PUT");
		headers.put("Access-Control-Request-Headers", "authorization");
		response = corsRequest("OPTIONS", bucketName, headers, null);
		assertEquals(HttpStatus.SC_FORBIDDEN, response.statusCode());
		assertNull(response.header("access-control-allow-origin"));
	}

	@Test
	@Tag("Preflight")
	public void testCorsRuleOrder() {
		var client = getClient();
		var bucketName = createBucket(client, 7);
		var rules = List.of(
				new CORSRule().withAllowedMethods(List.of(AllowedMethods.GET, AllowedMethods.PUT))
						.withAllowedOrigins(List.of("http://a.example.com")).withMaxAgeSeconds(600),
				new CORSRule().withAllowedMethods(List.of(AllowedMethods.GET))
						.withAllowedOrigins(List.of("*")).withMaxAgeSeconds(20));
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(rules));
		waitForBucketCrossOriginConfiguration(client, bucketName);

		// 두 규칙 모두에 맞으면 먼저 선언된 규칙이 적용된다
		var headers = new HashMap<String, String>();
		headers.put("Origin", "http://a.example.com");
		headers.put("Access-Control-Request-Method", "GET");
		var response = corsRequest("OPTIONS", bucketName, headers, null);
		assertEquals(200, response.statusCode());
		assertEquals("http://a.example.com", response.header("access-control-allow-origin"));
		assertEquals("GET, PUT", response.header("access-control-allow-methods"));
		assertEquals("600", response.header("access-control-max-age"));

		// 첫 규칙에 맞지 않으면 다음 규칙이 적용된다
		headers.clear();
		headers.put("Origin", "http://b.example.com");
		headers.put("Access-Control-Request-Method", "GET");
		response = corsRequest("OPTIONS", bucketName, headers, null);
		assertEquals(200, response.statusCode());
		assertEquals("*", response.header("access-control-allow-origin"));
		assertEquals("GET", response.header("access-control-allow-methods"));
		assertEquals("20", response.header("access-control-max-age"));
		assertNull(response.header("access-control-allow-credentials"));

		// 어느 규칙에도 맞지 않으면 거부된다
		headers.clear();
		headers.put("Origin", "http://b.example.com");
		headers.put("Access-Control-Request-Method", "PUT");
		response = corsRequest("OPTIONS", bucketName, headers, null);
		assertEquals(HttpStatus.SC_FORBIDDEN, response.statusCode());
		assertNull(response.header("access-control-allow-origin"));
	}

	@Test
	@Tag("Object")
	public void testCorsObjectRequest() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 8);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
		var key = "testCorsObjectRequest";
		client.putObject(new PutObjectRequest(bucketName, key, createBody("cors"), new ObjectMetadata())
				.withCannedAcl(CannedAccessControlList.PublicRead));
		var rules = List.of(new CORSRule()
				.withAllowedMethods(List.of(AllowedMethods.GET, AllowedMethods.HEAD))
				.withAllowedOrigins(List.of("http://a.example.com"))
				.withExposedHeaders(List.of("ETag")));
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(rules));
		waitForBucketCrossOriginConfiguration(client, bucketName);

		var headers = new HashMap<String, String>();
		for (var method : List.of("GET", "HEAD")) {
			headers.clear();
			headers.put("Origin", "http://a.example.com");
			var response = corsRequest(method, bucketName, headers, key);
			assertEquals(200, response.statusCode());
			assertEquals("http://a.example.com", response.header("access-control-allow-origin"));
			assertEquals("GET, HEAD", response.header("access-control-allow-methods"));
			assertEquals("ETag", response.header("access-control-expose-headers"));

			// 규칙에 맞지 않는 Origin 이면 요청은 성공하지만 CORS 헤더가 없다
			headers.clear();
			headers.put("Origin", "http://b.example.com");
			response = corsRequest(method, bucketName, headers, key);
			assertEquals(200, response.statusCode());
			assertNull(response.header("access-control-allow-origin"));
		}

		// 오브젝트 키에 대한 preflight
		headers.clear();
		headers.put("Origin", "http://a.example.com");
		headers.put("Access-Control-Request-Method", "GET");
		var response = corsRequest("OPTIONS", bucketName, headers, key);
		assertEquals(200, response.statusCode());
		assertEquals("http://a.example.com", response.header("access-control-allow-origin"));
	}

	@Test
	@Tag("Error")
	public void testCorsInvalidOriginWildcard() {
		var client = getClient();
		var bucketName = createBucket(client, 10);
		assertInvalidCors(client, bucketName, List.of(corsRule(AllowedMethods.GET, "*a*")));
	}

	@Test
	@Tag("Error")
	public void testCorsInvalidHeaderWildcard() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		assertInvalidCors(client, bucketName,
				List.of(corsRule(AllowedMethods.GET, "*").withAllowedHeaders(List.of("*a*"))));
	}

	@Test
	@Tag("Error")
	public void testCorsExposeHeaderWildcard() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		assertInvalidCors(client, bucketName,
				List.of(corsRule(AllowedMethods.GET, "*").withExposedHeaders(List.of("x-amz-*"))));
	}

	@Test
	@Tag("Error")
	public void testCorsRuleLimit() {
		var client = getClient();
		var bucketName = createBucket(client, 13);
		var rules = new ArrayList<CORSRule>();
		for (int i = 0; i < 100; i++)
			rules.add(corsRule(AllowedMethods.GET, "http://" + i + ".example.com"));
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(rules));
		assertEquals(100, client.getBucketCrossOriginConfiguration(bucketName).getRules().size());

		rules.add(corsRule(AllowedMethods.GET, "http://100.example.com"));
		assertInvalidCors(client, bucketName, rules);
	}

	@Test
	@Tag("Check")
	public void testCorsOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(List.of(
				corsRule(AllowedMethods.GET, "http://a.example.com"),
				corsRule(AllowedMethods.PUT, "http://b.example.com"))));
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(List.of(
				corsRule(AllowedMethods.HEAD, "http://z.example.com"))));

		var response = client.getBucketCrossOriginConfiguration(bucketName);
		assertEquals(1, response.getRules().size());
		assertEquals(List.of(AllowedMethods.HEAD), response.getRules().get(0).getAllowedMethods());
		assertEquals(List.of("http://z.example.com"), response.getRules().get(0).getAllowedOrigins());
	}

	@Test
	@Tag("Delete")
	public void testCorsDeleteNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 15);
		// 설정이 없어도 삭제는 성공한다
		client.deleteBucketCrossOriginConfiguration(bucketName);
		assertNull(client.getBucketCrossOriginConfiguration(bucketName));
	}

	@Test
	@Tag("Error")
	public void testCorsBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(16);
		var config = new BucketCrossOriginConfiguration(List.of(corsRule(AllowedMethods.GET, "*")));

		// SDK v1 은 조회의 404 응답을 예외 대신 null 로 돌려주므로 에러 코드를 확인할 수 없다
		assertNull(client.getBucketCrossOriginConfiguration(bucketName));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketCrossOriginConfiguration(bucketName, config));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> client.deleteBucketCrossOriginConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Permission")
	public void testCorsAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 17);
		var config = new BucketCrossOriginConfiguration(List.of(corsRule(AllowedMethods.GET, "*")));
		client.setBucketCrossOriginConfiguration(bucketName, config);

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.getBucketCrossOriginConfiguration(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.setBucketCrossOriginConfiguration(bucketName, config));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> altClient.deleteBucketCrossOriginConfiguration(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	private static CORSRule corsRule(AllowedMethods method, String origin) {
		return new CORSRule().withAllowedMethods(List.of(method)).withAllowedOrigins(List.of(origin));
	}

	/** 잘못된 CORS 설정이 400 InvalidRequest 로 거부되는지 확인한다. */
	private void assertInvalidCors(AmazonS3 client, String bucketName, List<CORSRule> rules) {
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration(rules)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}
}
