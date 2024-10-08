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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.CORSConfiguration;
import software.amazon.awssdk.services.s3.model.CORSRule;

public class Cors extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Cors V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Cors V2 End");
	}

	@Test
	@Tag("Check")
	public void testSetCors() {
		var client = getClient();
		var bucketName = createBucket(client);

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
	@Disabled
	@Tag("Post")
	public void testCorsOriginResponse() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
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
	@Disabled
	@Tag("Post")
	public void testCorsOriginWildcard() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
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
	@Disabled
	@Tag("Post")
	public void testCorsHeaderOption() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
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
}