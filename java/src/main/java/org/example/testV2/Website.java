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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Protocol;
import software.amazon.awssdk.services.s3.model.RoutingRule;
import software.amazon.awssdk.services.s3.model.WebsiteConfiguration;

import org.junit.jupiter.api.Tag;

public class Website extends TestBase {
	@Test
	@Tag("Check")
	public void testWebsiteGetBuckets() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketWebsite(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_WEBSITE_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testWebsitePutBuckets() {
		var client = getClient();
		var bucketName = createBucket(client, 2);

		var webConfig = WebsiteConfiguration.builder()
				.errorDocument(d -> d.key("HttpStatus.SC_BAD_REQUEST"))
				.indexDocument(d -> d.suffix("a"))
				.build();

		client.putBucketWebsite(g -> g.bucket(bucketName).websiteConfiguration(webConfig));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals(webConfig.errorDocument(), response.errorDocument());
		assertEquals(webConfig.indexDocument(), response.indexDocument());
	}

	@Test
	@Tag("Delete")
	public void testWebsiteDeleteBuckets() {
		var client = getClient();
		var bucketName = createBucket(client, 3);

		var webConfig = WebsiteConfiguration.builder()
				.errorDocument(d -> d.key("HttpStatus.SC_BAD_REQUEST"))
				.indexDocument(d -> d.suffix("a"))
				.build();

		client.putBucketWebsite(g -> g.bucket(bucketName).websiteConfiguration(webConfig));
		client.deleteBucketWebsite(d -> d.bucket(bucketName));
	}

	@Test
	@Tag("Delete")
	public void testWebsiteGetAfterDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 4);

		var webConfig = WebsiteConfiguration.builder()
				.errorDocument(d -> d.key("error.html"))
				.indexDocument(d -> d.suffix("index.html"))
				.build();

		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(webConfig));
		client.getBucketWebsite(g -> g.bucket(bucketName));
		client.deleteBucketWebsite(d -> d.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class, () -> client.getBucketWebsite(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_WEBSITE_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Delete")
	public void testWebsiteDeleteNotConfigured() {
		var client = getClient();
		var bucketName = createBucket(client, 5);

		var response = client.deleteBucketWebsite(d -> d.bucket(bucketName));
		assertEquals(HttpStatus.SC_NO_CONTENT, response.sdkHttpResponse().statusCode());
	}

	@Test
	@Tag("Check")
	public void testWebsiteIndexDocumentOnly() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		client.putBucketWebsite(p -> p.bucket(bucketName)
				.websiteConfiguration(w -> w.indexDocument(d -> d.suffix("index.html"))));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals("index.html", response.indexDocument().suffix());
		assertNull(response.errorDocument());
		assertNull(response.redirectAllRequestsTo());
		assertEquals(0, response.routingRules().size());
	}

	@Test
	@Tag("Overwrite")
	public void testWebsiteOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(w -> w
				.indexDocument(d -> d.suffix("index.html"))
				.errorDocument(d -> d.key("error.html"))));
		client.putBucketWebsite(p -> p.bucket(bucketName)
				.websiteConfiguration(w -> w.indexDocument(d -> d.suffix("main.html"))));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals("main.html", response.indexDocument().suffix());
		assertNull(response.errorDocument());
	}

	@Test
	@Tag("Redirect")
	public void testWebsiteRedirectAllRequests() {
		var client = getClient();
		var bucketName = createBucket(client, 8);

		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(
				w -> w.redirectAllRequestsTo(r -> r.hostName("example.com").protocol(Protocol.HTTPS))));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals("example.com", response.redirectAllRequestsTo().hostName());
		assertEquals(Protocol.HTTPS, response.redirectAllRequestsTo().protocol());
		assertNull(response.indexDocument());
		assertNull(response.errorDocument());
	}

	@Test
	@Tag("Redirect")
	public void testWebsiteRedirectAllRequestsHostOnly() {
		var client = getClient();
		var bucketName = createBucket(client, 9);

		client.putBucketWebsite(p -> p.bucket(bucketName)
				.websiteConfiguration(w -> w.redirectAllRequestsTo(r -> r.hostName("example.com"))));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals("example.com", response.redirectAllRequestsTo().hostName());
		assertNull(response.redirectAllRequestsTo().protocol());
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteRoutingRules() {
		var client = getClient();
		var bucketName = createBucket(client, 10);

		var rules = List.of(
				RoutingRule.builder()
						.condition(c -> c.keyPrefixEquals("docs/"))
						.redirect(r -> r.replaceKeyPrefixWith("documents/"))
						.build(),
				RoutingRule.builder()
						.condition(c -> c.httpErrorCodeReturnedEquals("404"))
						.redirect(r -> r.hostName("example.com").protocol(Protocol.HTTPS)
								.httpRedirectCode("301").replaceKeyWith("error.html"))
						.build(),
				RoutingRule.builder()
						.redirect(r -> r.hostName("example.com"))
						.build());

		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(w -> w
				.indexDocument(d -> d.suffix("index.html"))
				.errorDocument(d -> d.key("error.html"))
				.routingRules(rules)));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals("index.html", response.indexDocument().suffix());
		assertEquals("error.html", response.errorDocument().key());
		assertEquals(rules, response.routingRules());
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteRoutingRulesMax() {
		var client = getClient();
		var bucketName = createBucket(client, 11);

		var rules = createRoutingRules(50);
		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(w -> w
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(rules)));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals(rules, response.routingRules());
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRulesOverMax() {
		var client = getClient();
		var bucketName = createBucket(client, 12);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(createRoutingRules(51))
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteNoIndexDocument() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

		var webConfig = WebsiteConfiguration.builder()
				.errorDocument(d -> d.key("error.html"))
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteEmptyIndexSuffix() {
		var client = getClient();
		var bucketName = createBucket(client, 14);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix(""))
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteIndexSuffixWithSlash() {
		var client = getClient();
		var bucketName = createBucket(client, 15);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix("dir/index.html"))
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRedirectAllWithIndexDocument() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		var webConfig = WebsiteConfiguration.builder()
				.redirectAllRequestsTo(r -> r.hostName("example.com"))
				.indexDocument(d -> d.suffix("index.html"))
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRedirectAllWithRoutingRules() {
		var client = getClient();
		var bucketName = createBucket(client, 17);

		var webConfig = WebsiteConfiguration.builder()
				.redirectAllRequestsTo(r -> r.hostName("example.com"))
				.routingRules(createRoutingRules(1))
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRedirectAllInvalidProtocol() {
		var client = getClient();
		var bucketName = createBucket(client, 18);

		var webConfig = WebsiteConfiguration.builder()
				.redirectAllRequestsTo(r -> r.hostName("example.com").protocol("ftp"))
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRuleReplaceKeyAndPrefix() {
		var client = getClient();
		var bucketName = createBucket(client, 19);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(RoutingRule.builder()
						.condition(c -> c.keyPrefixEquals("docs/"))
						.redirect(r -> r.replaceKeyWith("index.html").replaceKeyPrefixWith("documents/"))
						.build())
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRuleInvalidRedirectCode() {
		var client = getClient();
		var bucketName = createBucket(client, 20);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(RoutingRule.builder()
						.redirect(r -> r.hostName("example.com").httpRedirectCode("200"))
						.build())
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRuleInvalidErrorCode() {
		var client = getClient();
		var bucketName = createBucket(client, 21);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(RoutingRule.builder()
						.condition(c -> c.httpErrorCodeReturnedEquals("200"))
						.redirect(r -> r.hostName("example.com"))
						.build())
				.build();

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteRoutingRuleEmptyRedirect() {
		var client = getClient();
		var bucketName = createBucket(client, 22);

		var rule = RoutingRule.builder()
				.condition(c -> c.keyPrefixEquals("docs/"))
				.redirect(r -> r.build())
				.build();

		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(w -> w
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(rule)));

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals(List.of(rule), response.routingRules());
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteBucketNotFound() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(23);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix("index.html"))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(webConfig)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> client.getBucketWebsite(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> client.deleteBucketWebsite(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Access")
	public void testWebsiteAccessDenied() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 24);

		var webConfig = WebsiteConfiguration.builder()
				.indexDocument(d -> d.suffix("index.html"))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(webConfig)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(webConfig));

		e = assertThrows(AwsServiceException.class, () -> altClient.getBucketWebsite(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> altClient.deleteBucketWebsite(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		var response = client.getBucketWebsite(g -> g.bucket(bucketName));
		assertEquals("index.html", response.indexDocument().suffix());
	}

	@Test
	@Tag("Access")
	public void testWebsiteAccessIndexDocument() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createPublicWebsiteBucket(client, 25, w -> w.indexDocument(d -> d.suffix("index.html")));
		putHtml(client, bucketName, "index.html", "root index");
		putHtml(client, bucketName, "dir/index.html", "dir index");

		var response = websiteGet(bucketName, "");
		assertEquals(HttpStatus.SC_OK, response.statusCode());
		assertEquals("root index", response.body());

		response = websiteGet(bucketName, "dir/");
		assertEquals(HttpStatus.SC_OK, response.statusCode());
		assertEquals("dir index", response.body());

		response = websiteGet(bucketName, "index.html");
		assertEquals(HttpStatus.SC_OK, response.statusCode());
		assertEquals("root index", response.body());

		// 슬래시 없이 디렉터리를 요청하면 슬래시를 붙인 주소로 보낸다
		response = websiteGet(bucketName, "dir");
		assertEquals(HttpStatus.SC_MOVED_TEMPORARILY, response.statusCode());
		assertEquals("/dir/", response.location());
	}

	@Test
	@Tag("Access")
	public void testWebsiteAccessErrorDocument() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createPublicWebsiteBucket(client, 26, w -> w
				.indexDocument(d -> d.suffix("index.html"))
				.errorDocument(d -> d.key("error.html")));
		putHtml(client, bucketName, "error.html", "custom error");

		var response = websiteGet(bucketName, "not-exist.html");
		assertEquals(HttpStatus.SC_NOT_FOUND, response.statusCode());
		assertEquals("custom error", response.body());
	}

	@Test
	@Tag("Access")
	public void testWebsiteAccessNoErrorDocument() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createPublicWebsiteBucket(client, 27, w -> w.indexDocument(d -> d.suffix("index.html")));

		var response = websiteGet(bucketName, "not-exist.html");
		assertEquals(HttpStatus.SC_NOT_FOUND, response.statusCode());
		assertTrue(response.body().contains(MainData.NO_SUCH_KEY), response.body());
	}

	@Test
	@Tag("Redirect")
	public void testWebsiteAccessRedirectAllRequests() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createPublicWebsiteBucket(client, 28,
				w -> w.redirectAllRequestsTo(r -> r.hostName("example.com").protocol(Protocol.HTTPS)));

		var response = websiteGet(bucketName, "docs/a.html");
		assertEquals(HttpStatus.SC_MOVED_PERMANENTLY, response.statusCode());
		assertEquals("https://example.com/docs/a.html", response.location());
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteAccessRoutingRuleKeyPrefix() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createPublicWebsiteBucket(client, 29, w -> w
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(RoutingRule.builder()
						.condition(c -> c.keyPrefixEquals("docs/"))
						.redirect(r -> r.replaceKeyPrefixWith("documents/"))
						.build()));

		var response = websiteGet(bucketName, "docs/a.html");
		assertEquals(HttpStatus.SC_MOVED_PERMANENTLY, response.statusCode());
		assertTrue(response.location().endsWith("/documents/a.html"), response.location());
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteAccessRoutingRuleErrorCode() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createPublicWebsiteBucket(client, 30, w -> w
				.indexDocument(d -> d.suffix("index.html"))
				.routingRules(RoutingRule.builder()
						.condition(c -> c.httpErrorCodeReturnedEquals("404"))
						.redirect(r -> r.hostName("example.com").protocol(Protocol.HTTPS)
								.httpRedirectCode("302").replaceKeyWith("error.html"))
						.build()));
		putHtml(client, bucketName, "exist.html", "exist");

		var response = websiteGet(bucketName, "exist.html");
		assertEquals(HttpStatus.SC_OK, response.statusCode());
		assertEquals("exist", response.body());

		response = websiteGet(bucketName, "not-exist.html");
		assertEquals(HttpStatus.SC_MOVED_TEMPORARILY, response.statusCode());
		assertEquals("https://example.com/error.html", response.location());
	}

	@Test
	@Tag("Redirect")
	public void testWebsiteAccessObjectRedirectLocation() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createPublicWebsiteBucket(client, 31, w -> w.indexDocument(d -> d.suffix("index.html")));
		client.putObject(p -> p.bucket(bucketName).key("old.html").websiteRedirectLocation("/new.html"),
				RequestBody.empty());

		var response = websiteGet(bucketName, "old.html");
		assertEquals(HttpStatus.SC_MOVED_PERMANENTLY, response.statusCode());
		assertEquals("/new.html", response.location());
	}

	@Test
	@Tag("Access")
	public void testWebsiteAccessNotPublic() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createBucket(client, 32);
		client.putBucketWebsite(p -> p.bucket(bucketName)
				.websiteConfiguration(w -> w.indexDocument(d -> d.suffix("index.html"))));
		putHtml(client, bucketName, "index.html", "root index");

		var response = websiteGet(bucketName, "index.html");
		assertEquals(HttpStatus.SC_FORBIDDEN, response.statusCode());
		assertTrue(response.body().contains(MainData.ACCESS_DENIED), response.body());
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteAccessNoConfiguration() {
		assumeTrue(config.hasWebsiteUrl());
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, 33);
		setPublicReadPolicy(client, bucketName);
		putHtml(client, bucketName, "index.html", "root index");

		var response = websiteGet(bucketName, "index.html");
		assertEquals(HttpStatus.SC_NOT_FOUND, response.statusCode());
		assertTrue(response.body().contains(MainData.NO_SUCH_WEBSITE_CONFIGURATION), response.body());
	}

	// 익명 읽기를 허용하고 웹사이트 설정을 건 버킷을 만든다
	private String createPublicWebsiteBucket(S3Client client, int testId,
			Consumer<WebsiteConfiguration.Builder> webConfig) {
		var bucketName = createBucketCannedAcl(client, testId);
		setPublicReadPolicy(client, bucketName);
		client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(webConfig));
		return bucketName;
	}

	private void putHtml(S3Client client, String bucketName, String key, String body) {
		client.putObject(p -> p.bucket(bucketName).key(key).contentType("text/html"), RequestBody.fromString(body));
	}

	private List<RoutingRule> createRoutingRules(int count) {
		var rules = new ArrayList<RoutingRule>();
		for (int i = 0; i < count; i++) {
			var index = i;
			rules.add(RoutingRule.builder()
					.condition(c -> c.keyPrefixEquals("prefix" + index + "/"))
					.redirect(r -> r.replaceKeyPrefixWith("target" + index + "/"))
					.build());
		}
		return rules;
	}

	// PutBucketWebsite가 400으로 실패하고 설정이 남지 않았는지 확인
	private void checkPutWebsiteFailed(S3Client client, String bucketName, WebsiteConfiguration webConfig,
			String errorCode) {
		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketWebsite(p -> p.bucket(bucketName).websiteConfiguration(webConfig)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class, () -> client.getBucketWebsite(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_WEBSITE_CONFIGURATION, e.awsErrorDetails().errorCode());
	}
}
