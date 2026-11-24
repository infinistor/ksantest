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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.CORSConfiguration;
import software.amazon.awssdk.services.s3.model.CORSRule;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

public class Cors extends TestBase {
	@Test
	@Tag("Check")
	public void testSetCors() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var allowedMethods = List.of("GET", "PUT");
		var allowedOrigins = List.of("*.get", "*.put");

		var corsConfig = CORSConfiguration.builder()
				.corsRules(CORSRule.builder()
						.allowedMethods(allowedMethods)
						.allowedOrigins(allowedOrigins)
						.build())
				.build();

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CORS_CONFIGURATION, e.awsErrorDetails().errorCode());

		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(corsConfig));

		var response = client.getBucketCors(g -> g.bucket(bucketName));

		assertEquals(allowedMethods, response.corsRules().get(0).allowedMethods());
		assertEquals(allowedOrigins, response.corsRules().get(0).allowedOrigins());

		client.deleteBucketCors(d -> d.bucket(bucketName));
		e = assertThrows(AwsServiceException.class, () -> client.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CORS_CONFIGURATION, e.awsErrorDetails().errorCode());

	}

	@Test
	@Tag("Post")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 CORS 설정이 있는 버킷의
	// OPTIONS 는 Origin 이 없거나 규칙에 맞지 않아도 200 을 준다(2026-04 InfiniStor 와 같게 맞춤).
	// AWS 는 Origin 이 없으면 400, 규칙에 맞지 않으면 403 이다.
	public void testCorsOriginResponse() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 2);
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));

		var corsConfig = CORSConfiguration.builder();
		var rules = new ArrayList<CORSRule>();
		rules.add(CORSRule.builder()
				.allowedMethods(List.of("GET"))
				.allowedOrigins(List.of("*suffix")).build());
		rules.add(CORSRule.builder()
				.allowedMethods(List.of("GET"))
				.allowedOrigins(List.of("start*end")).build());
		rules.add(CORSRule.builder()
				.allowedMethods(List.of("GET"))
				.allowedOrigins(List.of("prefix*")).build());
		rules.add(CORSRule.builder()
				.allowedMethods(List.of("PUT"))
				.allowedOrigins(List.of("*.put")).build());
		corsConfig.corsRules(rules);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CORS_CONFIGURATION, e.awsErrorDetails().errorCode());

		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(corsConfig.build()));

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
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));

		var corsConfig = CORSConfiguration.builder();
		var rules = new ArrayList<CORSRule>();
		rules.add(CORSRule.builder().allowedMethods(List.of("GET"))
				.allowedOrigins(List.of("*")).build());
		corsConfig.corsRules(rules);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CORS_CONFIGURATION, e.awsErrorDetails().errorCode());

		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(corsConfig.build()));

		var headers = new HashMap<String, String>();
		corsRequestAndCheck("GET", bucketName, new HashMap<>(), 200, null, null, null);
		headers.clear();
		headers.put("Origin", "example.origin");
		corsRequestAndCheck("GET", bucketName, headers, 200, "*", "GET", null);
	}

	@Test
	@Tag("Post")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 허용하지 않은
	// Access-Control-Request-Headers 로 preflight 해도 200 을 준다(InfiniStor 와 같음). AWS 는 403 이다.
	public void testCorsHeaderOption() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 4);
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));

		var corsConfig = CORSConfiguration.builder();
		var rules = new ArrayList<CORSRule>();
		rules.add(CORSRule.builder()
				.allowedMethods(List.of("GET"))
				.allowedOrigins(List.of("*"))
				.exposeHeaders(List.of("x-amz-meta-header1"))
				.build());
		corsConfig.corsRules(rules);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CORS_CONFIGURATION, e.awsErrorDetails().errorCode());

		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(corsConfig.build()));

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
				CORSRule.builder().id("rule-1")
						.allowedMethods(List.of("GET", "PUT"))
						.allowedOrigins(List.of("http://a.example.com"))
						.allowedHeaders(List.of("x-amz-*", "content-type"))
						.exposeHeaders(List.of("ETag", "x-amz-meta-foo"))
						.maxAgeSeconds(600).build(),
				CORSRule.builder().id("rule-2")
						.allowedMethods(List.of("GET"))
						.allowedOrigins(List.of("*"))
						.maxAgeSeconds(20).build());
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules)));

		var response = client.getBucketCors(g -> g.bucket(bucketName));
		assertEquals(2, response.corsRules().size());
		var first = response.corsRules().get(0);
		var second = response.corsRules().get(1);
		assertEquals("rule-1", first.id());
		assertEquals(List.of("GET", "PUT"), first.allowedMethods());
		assertEquals(List.of("http://a.example.com"), first.allowedOrigins());
		assertEquals(List.of("x-amz-*", "content-type"), first.allowedHeaders());
		assertEquals(List.of("ETag", "x-amz-meta-foo"), first.exposeHeaders());
		assertEquals(600, first.maxAgeSeconds());
		assertEquals("rule-2", second.id());
		assertEquals(List.of("GET"), second.allowedMethods());
		assertEquals(List.of("*"), second.allowedOrigins());
		assertEquals(20, second.maxAgeSeconds());
	}

	@Test
	@Tag("Preflight")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 OPTIONS 는 Origin 만 보고 규칙을 고르고 항상 200 을 준다.
	// Access-Control-Allow-Methods 도 GET,PUT 처럼 공백 없이 잇는다. AWS 는 GET, PUT 형식이고 허용하지 않은 헤더는 403 이다.
	public void testCorsPreflightHeaders() {
		var client = getClient();
		var bucketName = createBucket(client, 6);
		var rules = List.of(CORSRule.builder()
				.allowedMethods(List.of("GET", "PUT"))
				.allowedOrigins(List.of("http://a.example.com"))
				.allowedHeaders(List.of("x-amz-*", "content-type"))
				.exposeHeaders(List.of("ETag", "x-amz-meta-foo"))
				.maxAgeSeconds(600).build());
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules)));

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
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 OPTIONS 는 Origin 만 보고 첫 규칙을 고르고 항상 200 을 준다.
	// AWS 는 메서드까지 맞는 규칙을 순서대로 찾고, 맞는 규칙이 없으면 403 이다.
	public void testCorsRuleOrder() {
		var client = getClient();
		var bucketName = createBucket(client, 7);
		var rules = List.of(
				CORSRule.builder().allowedMethods(List.of("GET", "PUT"))
						.allowedOrigins(List.of("http://a.example.com")).maxAgeSeconds(600).build(),
				CORSRule.builder().allowedMethods(List.of("GET"))
						.allowedOrigins(List.of("*")).maxAgeSeconds(20).build());
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules)));

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
		client.putBucketAcl(p -> p.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ));
		var key = "testCorsObjectRequest";
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString("cors"));
		var rules = List.of(CORSRule.builder()
				.allowedMethods(List.of("GET", "HEAD"))
				.allowedOrigins(List.of("http://a.example.com"))
				.exposeHeaders(List.of("ETag")).build());
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules)));

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
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 AllowedMethod 가 GET·PUT·POST·DELETE·HEAD(대문자)가 아니면 400 InvalidRequest 다.
	public void testCorsInvalidMethod() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		assertInvalidCors(client, bucketName, List.of(corsRule(List.of("PATCH"), List.of("*"))));
		// 메서드 이름은 대문자만 허용된다
		assertInvalidCors(client, bucketName, List.of(corsRule(List.of("get"), List.of("*"))));
	}

	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 AllowedOrigin 에 * 가 둘 이상이면 400 InvalidRequest 다.
	public void testCorsInvalidOriginWildcard() {
		var client = getClient();
		var bucketName = createBucket(client, 10);
		assertInvalidCors(client, bucketName, List.of(corsRule(List.of("GET"), List.of("*a*"))));
	}

	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 AllowedHeader 에 * 가 둘 이상이면 400 InvalidRequest 다.
	public void testCorsInvalidHeaderWildcard() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		assertInvalidCors(client, bucketName, List.of(CORSRule.builder()
				.allowedMethods(List.of("GET")).allowedOrigins(List.of("*")).allowedHeaders(List.of("*a*")).build()));
	}

	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 ExposeHeader 에 * 가 있으면 400 InvalidRequest 다.
	public void testCorsExposeHeaderWildcard() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		assertInvalidCors(client, bucketName, List.of(CORSRule.builder()
				.allowedMethods(List.of("GET")).allowedOrigins(List.of("*")).exposeHeaders(List.of("x-amz-*")).build()));
	}

	@Test
	@Tag("Error")
	// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
	// AWS 는 규칙이 101개 이상이면 400 InvalidRequest 다.
	public void testCorsRuleLimit() {
		var client = getClient();
		var bucketName = createBucket(client, 13);
		var rules = new ArrayList<CORSRule>();
		for (int i = 0; i < 100; i++)
			rules.add(corsRule(List.of("GET"), List.of("http://" + i + ".example.com")));
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules)));
		assertEquals(100, client.getBucketCors(g -> g.bucket(bucketName)).corsRules().size());

		rules.add(corsRule(List.of("GET"), List.of("http://100.example.com")));
		assertInvalidCors(client, bucketName, rules);
	}

	@Test
	@Tag("Check")
	public void testCorsOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(
				corsRule(List.of("GET"), List.of("http://a.example.com")),
				corsRule(List.of("PUT"), List.of("http://b.example.com")))));
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(
				corsRule(List.of("HEAD"), List.of("http://z.example.com")))));

		var response = client.getBucketCors(g -> g.bucket(bucketName));
		assertEquals(1, response.corsRules().size());
		assertEquals(List.of("HEAD"), response.corsRules().get(0).allowedMethods());
		assertEquals(List.of("http://z.example.com"), response.corsRules().get(0).allowedOrigins());
	}

	@Test
	@Tag("Delete")
	public void testCorsDeleteNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 15);
		// 설정이 없어도 삭제는 성공한다
		client.deleteBucketCors(d -> d.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CORS_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testCorsBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(16);
		var rules = List.of(corsRule(List.of("GET"), List.of("*")));

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules))));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> client.deleteBucketCors(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Permission")
	public void testCorsAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 17);
		var rules = List.of(corsRule(List.of("GET"), List.of("*")));
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules)));

		var e = assertThrows(AwsServiceException.class, () -> altClient.getBucketCors(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> altClient.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules))));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> altClient.deleteBucketCors(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	private static CORSRule corsRule(List<String> methods, List<String> origins) {
		return CORSRule.builder().allowedMethods(methods).allowedOrigins(origins).build();
	}

	/** 잘못된 CORS 설정이 400 InvalidRequest 로 거부되는지 확인한다. */
	private void assertInvalidCors(S3Client client, String bucketName, List<CORSRule> rules) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(c -> c.corsRules(rules))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}
}
