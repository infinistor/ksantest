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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.RedirectRule;
import com.amazonaws.services.s3.model.RoutingRule;
import com.amazonaws.services.s3.model.RoutingRuleCondition;

public class Website extends TestBase
{
	@Test
	@Tag("Check")
	public void testWebsiteGetBuckets()
	{
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertNull(response);
	}

	@Test
	@Tag("Check")
	public void testWebsitePutBuckets()
	{
		var client = getClient();
		var bucketName = createBucket(client, 2);

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setErrorDocument("HttpStatus.SC_BAD_REQUEST");
		webConfig.setIndexDocumentSuffix("a");

		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals(webConfig.getErrorDocument(), response.getErrorDocument());
	}

	@Test
	@Tag("Delete")
	public void testWebsiteDeleteBuckets()
	{
		var client = getClient();
		var bucketName = createBucket(client, 3);

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setErrorDocument("HttpStatus.SC_BAD_REQUEST");
		webConfig.setIndexDocumentSuffix("a");

		client.setBucketWebsiteConfiguration(bucketName, webConfig);
		client.deleteBucketWebsiteConfiguration(bucketName);
	}

	// v1 SDK의 getBucketWebsiteConfiguration은 404를 null로 돌려주므로 NoSuchWebsiteConfiguration은 null로 확인한다
	@Test
	@Tag("Delete")
	public void testWebsiteGetAfterDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 4);

		var webConfig = new BucketWebsiteConfiguration("index.html", "error.html");

		client.setBucketWebsiteConfiguration(bucketName, webConfig);
		client.getBucketWebsiteConfiguration(bucketName);
		client.deleteBucketWebsiteConfiguration(bucketName);

		assertNull(client.getBucketWebsiteConfiguration(bucketName));
	}

	@Test
	@Tag("Delete")
	public void testWebsiteDeleteNotConfigured() {
		var client = getClient();
		var bucketName = createBucket(client, 5);

		client.deleteBucketWebsiteConfiguration(bucketName);
	}

	@Test
	@Tag("Check")
	public void testWebsiteIndexDocumentOnly() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		client.setBucketWebsiteConfiguration(bucketName, new BucketWebsiteConfiguration("index.html"));

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals("index.html", response.getIndexDocumentSuffix());
		assertNull(response.getErrorDocument());
		assertNull(response.getRedirectAllRequestsTo());
		assertEquals(0, response.getRoutingRules().size());
	}

	@Test
	@Tag("Overwrite")
	public void testWebsiteOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client, 7);

		client.setBucketWebsiteConfiguration(bucketName, new BucketWebsiteConfiguration("index.html", "error.html"));
		client.setBucketWebsiteConfiguration(bucketName, new BucketWebsiteConfiguration("main.html"));

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals("main.html", response.getIndexDocumentSuffix());
		assertNull(response.getErrorDocument());
	}

	@Test
	@Tag("Redirect")
	public void testWebsiteRedirectAllRequests() {
		var client = getClient();
		var bucketName = createBucket(client, 8);

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setRedirectAllRequestsTo(new RedirectRule().withHostName("example.com").withProtocol("https"));
		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals("example.com", response.getRedirectAllRequestsTo().getHostName());
		assertEquals("https", response.getRedirectAllRequestsTo().getprotocol());
		assertNull(response.getIndexDocumentSuffix());
		assertNull(response.getErrorDocument());
	}

	@Test
	@Tag("Redirect")
	public void testWebsiteRedirectAllRequestsHostOnly() {
		var client = getClient();
		var bucketName = createBucket(client, 9);

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setRedirectAllRequestsTo(new RedirectRule().withHostName("example.com"));
		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals("example.com", response.getRedirectAllRequestsTo().getHostName());
		assertNull(response.getRedirectAllRequestsTo().getprotocol());
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteRoutingRules() {
		var client = getClient();
		var bucketName = createBucket(client, 10);

		var rules = List.of(
				new RoutingRule()
						.withCondition(new RoutingRuleCondition().withKeyPrefixEquals("docs/"))
						.withRedirect(new RedirectRule().withReplaceKeyPrefixWith("documents/")),
				new RoutingRule()
						.withCondition(new RoutingRuleCondition().withHttpErrorCodeReturnedEquals("404"))
						.withRedirect(new RedirectRule().withHostName("example.com").withProtocol("https")
								.withHttpRedirectCode("301").withReplaceKeyWith("error.html")),
				new RoutingRule()
						.withRedirect(new RedirectRule().withHostName("example.com")));

		var webConfig = new BucketWebsiteConfiguration("index.html", "error.html");
		webConfig.setRoutingRules(rules);
		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals("index.html", response.getIndexDocumentSuffix());
		assertEquals("error.html", response.getErrorDocument());
		checkRoutingRules(rules, response.getRoutingRules());
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteRoutingRulesMax() {
		var client = getClient();
		var bucketName = createBucket(client, 11);

		var rules = createRoutingRules(50);
		var webConfig = new BucketWebsiteConfiguration("index.html");
		webConfig.setRoutingRules(rules);
		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		checkRoutingRules(rules, response.getRoutingRules());
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRulesOverMax() {
		var client = getClient();
		var bucketName = createBucket(client, 12);

		var webConfig = new BucketWebsiteConfiguration("index.html");
		webConfig.setRoutingRules(createRoutingRules(51));

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteEmptyIndexSuffix() {
		var client = getClient();
		var bucketName = createBucket(client, 14);

		checkPutWebsiteFailed(client, bucketName, new BucketWebsiteConfiguration(""), MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteIndexSuffixWithSlash() {
		var client = getClient();
		var bucketName = createBucket(client, 15);

		checkPutWebsiteFailed(client, bucketName, new BucketWebsiteConfiguration("dir/index.html"),
				MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRedirectAllWithIndexDocument() {
		var client = getClient();
		var bucketName = createBucket(client, 16);

		var webConfig = new BucketWebsiteConfiguration("index.html");
		webConfig.setRedirectAllRequestsTo(new RedirectRule().withHostName("example.com"));

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRedirectAllWithRoutingRules() {
		var client = getClient();
		var bucketName = createBucket(client, 17);

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setRedirectAllRequestsTo(new RedirectRule().withHostName("example.com"));
		webConfig.setRoutingRules(createRoutingRules(1));

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_ARGUMENT);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRedirectAllInvalidProtocol() {
		var client = getClient();
		var bucketName = createBucket(client, 18);

		var webConfig = new BucketWebsiteConfiguration();
		webConfig.setRedirectAllRequestsTo(new RedirectRule().withHostName("example.com").withProtocol("ftp"));

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRuleReplaceKeyAndPrefix() {
		var client = getClient();
		var bucketName = createBucket(client, 19);

		var webConfig = new BucketWebsiteConfiguration("index.html");
		webConfig.setRoutingRules(List.of(new RoutingRule()
				.withCondition(new RoutingRuleCondition().withKeyPrefixEquals("docs/"))
				.withRedirect(new RedirectRule().withReplaceKeyWith("index.html")
						.withReplaceKeyPrefixWith("documents/"))));

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRuleInvalidRedirectCode() {
		var client = getClient();
		var bucketName = createBucket(client, 20);

		var webConfig = new BucketWebsiteConfiguration("index.html");
		webConfig.setRoutingRules(List.of(new RoutingRule()
				.withRedirect(new RedirectRule().withHostName("example.com").withHttpRedirectCode("200"))));

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteRoutingRuleInvalidErrorCode() {
		var client = getClient();
		var bucketName = createBucket(client, 21);

		var webConfig = new BucketWebsiteConfiguration("index.html");
		webConfig.setRoutingRules(List.of(new RoutingRule()
				.withCondition(new RoutingRuleCondition().withHttpErrorCodeReturnedEquals("200"))
				.withRedirect(new RedirectRule().withHostName("example.com"))));

		checkPutWebsiteFailed(client, bucketName, webConfig, MainData.INVALID_REQUEST);
	}

	@Test
	@Tag("RoutingRule")
	public void testWebsiteRoutingRuleEmptyRedirect() {
		var client = getClient();
		var bucketName = createBucket(client, 22);

		var rules = List.of(new RoutingRule()
				.withCondition(new RoutingRuleCondition().withKeyPrefixEquals("docs/"))
				.withRedirect(new RedirectRule()));

		var webConfig = new BucketWebsiteConfiguration("index.html");
		webConfig.setRoutingRules(rules);
		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		var response = client.getBucketWebsiteConfiguration(bucketName);
		checkRoutingRules(rules, response.getRoutingRules());
	}

	@Test
	@Tag("ERROR")
	public void testWebsiteBucketNotFound() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(23);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketWebsiteConfiguration(bucketName, new BucketWebsiteConfiguration("index.html")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> client.deleteBucketWebsiteConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Access")
	public void testWebsiteAccessDenied() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 24);

		var webConfig = new BucketWebsiteConfiguration("index.html");

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.setBucketWebsiteConfiguration(bucketName, webConfig));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.setBucketWebsiteConfiguration(bucketName, webConfig);

		e = assertThrows(AmazonServiceException.class, () -> altClient.getBucketWebsiteConfiguration(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class, () -> altClient.deleteBucketWebsiteConfiguration(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		var response = client.getBucketWebsiteConfiguration(bucketName);
		assertEquals("index.html", response.getIndexDocumentSuffix());
	}

	private List<RoutingRule> createRoutingRules(int count) {
		var rules = new ArrayList<RoutingRule>();
		for (int i = 0; i < count; i++) {
			rules.add(new RoutingRule()
					.withCondition(new RoutingRuleCondition().withKeyPrefixEquals("prefix" + i + "/"))
					.withRedirect(new RedirectRule().withReplaceKeyPrefixWith("target" + i + "/")));
		}
		return rules;
	}

	// v1 모델은 equals를 구현하지 않아 필드 단위로 비교
	// v1 SDK는 Condition의 빈 값을 ""로 주고받으므로 null과 ""를 같게 본다
	private void checkRoutingRules(List<RoutingRule> expected, List<RoutingRule> actual) {
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			var expectedCondition = expected.get(i).getCondition();
			var actualCondition = actual.get(i).getCondition();
			if (expectedCondition == null) {
				assertNull(actualCondition);
			} else {
				assertEquals(Objects.toString(expectedCondition.getKeyPrefixEquals(), ""),
						Objects.toString(actualCondition.getKeyPrefixEquals(), ""));
				assertEquals(Objects.toString(expectedCondition.getHttpErrorCodeReturnedEquals(), ""),
						Objects.toString(actualCondition.getHttpErrorCodeReturnedEquals(), ""));
			}

			var expectedRedirect = expected.get(i).getRedirect();
			var actualRedirect = actual.get(i).getRedirect();
			assertEquals(expectedRedirect.getHostName(), actualRedirect.getHostName());
			assertEquals(expectedRedirect.getprotocol(), actualRedirect.getprotocol());
			assertEquals(expectedRedirect.getHttpRedirectCode(), actualRedirect.getHttpRedirectCode());
			assertEquals(expectedRedirect.getReplaceKeyWith(), actualRedirect.getReplaceKeyWith());
			assertEquals(expectedRedirect.getReplaceKeyPrefixWith(), actualRedirect.getReplaceKeyPrefixWith());
		}
	}

	// setBucketWebsiteConfiguration이 400으로 실패하고 설정이 남지 않았는지 확인
	private void checkPutWebsiteFailed(AmazonS3 client, String bucketName, BucketWebsiteConfiguration webConfig,
			String errorCode) {
		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketWebsiteConfiguration(bucketName, webConfig));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());

		assertNull(client.getBucketWebsiteConfiguration(bucketName));
	}
}
