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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Request;

public class ListObjectsV2 extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("ListObjectsV2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ListObjectsV2 End");
	}

	@Test
	@Tag("Check")
	public void testBucketListV2Many() {
		var client = getClient();
		var bucketName = createObjects(client, "foo", "bar", "baz");

		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2));
		assertEquals(List.of("bar", "baz"),
				getKeys(response.getObjectSummaries()));
		assertEquals(2, response.getObjectSummaries().size());
		assertTrue(response.isTruncated());

		response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withStartAfter("baz").withMaxKeys(2));
		assertEquals(List.of("foo"), getKeys(response.getObjectSummaries()));
		assertEquals(1, response.getObjectSummaries().size());
		assertFalse(response.isTruncated());
	}

	@Test
	@Tag("KeyCount")
	public void testBasicKeyCount() {
		var client = getClient();
		var bucketName = createBucket(client);

		for (int i = 0; i < 5; i++)
			client.putObject(bucketName, Integer.toString(i), "");

		var response = client.listObjectsV2(bucketName);

		assertEquals(5, response.getKeyCount());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterBasic() {
		var client = getClient();
		var bucketName = createObjects(client, "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf");

		String delimiter = "/";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(List.of("asdf"), getKeys(response.getObjectSummaries()));

		var prefixes = response.getCommonPrefixes();
		assertEquals(2, prefixes.size());
		assertEquals(List.of("foo/", "quux/"), prefixes);
	}

	@Test
	@Tag("Encoding")
	public void testBucketListV2EncodingBasic() {
		var client = getClient();
		var bucketName = createObjects(client, "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b");

		String delimiter = "/";

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter).withEncodingType("URL"));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(List.of("asdf%2Bb"), getKeys(response.getObjectSummaries()));

		var prefixes = response.getCommonPrefixes();
		assertEquals(3, prefixes.size());
		assertLinesMatch(List.of("foo%2B1/", "foo/", "quux+ab/"), prefixes);
	}

	@Test
	@Tag("Filtering")
	public void testBucketListV2DelimiterPrefix() {
		var bucketName = createObjects(List.of("asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"));

		String delimiter = "/";
		String continuationToken = "";
		String prefix = "";

		continuationToken = validateListObjectV2(bucketName, prefix, delimiter, null, 1, true,
				List.of("asdf"), new ArrayList<>(), false);
		continuationToken = validateListObjectV2(bucketName, prefix, delimiter, continuationToken, 1, true,
				new ArrayList<>(), List.of("boo/"), false);
		validateListObjectV2(bucketName, prefix, delimiter, continuationToken, 1, false, new ArrayList<>(),
				List.of("cquux/"), true);

		continuationToken = validateListObjectV2(bucketName, prefix, delimiter, null, 2, true, List.of("asdf"),
				List.of("boo/"), false);
		validateListObjectV2(bucketName, prefix, delimiter, continuationToken, 2, false, new ArrayList<>(),
				List.of("cquux/"), true);

		prefix = "boo/";

		continuationToken = validateListObjectV2(bucketName, prefix, delimiter, null, 1, true,
				List.of("boo/bar"), new ArrayList<>(), false);
		validateListObjectV2(bucketName, prefix, delimiter, continuationToken, 1, false,
				new ArrayList<>(), List.of("boo/baz/"), true);

		validateListObjectV2(bucketName, prefix, delimiter, null, 2, false, List.of("boo/bar"), List.of("boo/baz/"),
				true);
	}

	@Test
	@Tag("Filtering")
	public void testBucketListV2DelimiterPrefixEndsWithDelimiter() {
		var client = getClient();
		var bucketName = createObjects(client, "asdf/");
		validateListObjectV2(bucketName, "asdf/", "/", null, 1000, false,
				List.of("asdf/"), new ArrayList<>(), true);
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterAlt() {
		var client = getClient();
		var bucketName = createObjects(client, "bar", "baz", "cab", "foo");

		String delimiter = "a";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertEquals(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();
		assertEquals(2, prefixes.size());
		assertEquals(List.of("ba", "ca"), prefixes);
	}

	@Test
	@Tag("Filtering")
	public void testBucketListV2DelimiterPrefixUnderscore() {
		var bucketName = createObjects(List.of("Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"));

		String delim = "/";
		String continuationToken = "";
		String prefix = "";

		continuationToken = validateListObjectV2(bucketName, prefix, delim, null, 1, true,
				List.of("Obj1_"), new ArrayList<>(), false);
		continuationToken = validateListObjectV2(bucketName, prefix, delim, continuationToken, 1, true,
				new ArrayList<>(), List.of("Under1/"), false);
		validateListObjectV2(bucketName, prefix, delim, continuationToken, 1, false,
				new ArrayList<>(), List.of("Under2/"), true);

		continuationToken = validateListObjectV2(bucketName, prefix, delim, null, 2, true,
				List.of("Obj1_"),
				List.of("Under1/"), false);
		validateListObjectV2(bucketName, prefix, delim, continuationToken, 2, false,
				new ArrayList<>(), List.of("Under2/"), true);

		prefix = "Under1/";

		continuationToken = validateListObjectV2(bucketName, prefix, delim, null, 1, true,
				List.of("Under1/bar"), new ArrayList<>(), false);
		validateListObjectV2(bucketName, prefix, delim, continuationToken, 1, false,
				new ArrayList<>(), List.of("Under1/baz/"), true);

		validateListObjectV2(bucketName, prefix, delim, null, 2, false,
				List.of("Under1/bar"),
				List.of("Under1/baz/"), true);
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterPercentage() {
		var client = getClient();
		var bucketName = createObjects(client, "b%ar", "b%az", "c%ab", "foo");

		String delimiter = "%";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertEquals(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();
		assertEquals(2, prefixes.size());
		assertEquals(List.of("b%", "c%"), prefixes);
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterWhitespace() {
		var client = getClient();
		var bucketName = createObjects(client, "b ar", "b az", "c ab", "foo");

		String delimiter = " ";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertEquals(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();
		assertEquals(2, prefixes.size());
		assertEquals(List.of("b ", "c "), prefixes);
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterDot() {
		var client = getClient();
		var bucketName = createObjects(client, "b.ar", "b.az", "c.ab", "foo");

		String delimiter = ".";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertEquals(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();
		assertEquals(2, prefixes.size());
		assertEquals(List.of("b.", "c."), prefixes);
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterUnreadable() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "\n";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterEmpty() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertNull(response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterNone() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectsV2(bucketName);
		assertNull(response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("FetchOwner")
	public void testBucketListV2FetchOwnerNotEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withFetchOwner(true));
		var objectList = response.getObjectSummaries();

		assertNotNull(objectList.get(0).getOwner());
	}

	@Test
	@Tag("FetchOwner")
	public void testBucketListV2FetchOwnerDefaultEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectsV2(bucketName);
		var objectList = response.getObjectSummaries();

		assertNull(objectList.get(0).getOwner());
	}

	@Test
	@Tag("FetchOwner")
	public void testBucketListV2FetchOwnerEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withFetchOwner(false));
		var objectList = response.getObjectSummaries();

		assertNull(objectList.get(0).getOwner());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListV2DelimiterNotExist() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "/";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Prefix")
	public void testBucketListV2PrefixBasic() {
		var client = getClient();
		var bucketName = createObjects(client, "foo/bar", "foo/baz", "quux");

		String prefix = "foo/";
		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(List.of("foo/bar", "foo/baz"), keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Prefix")
	public void testBucketListV2PrefixAlt() {
		var client = getClient();
		var bucketName = createObjects(client, "bar", "baz", "foo");

		String prefix = "ba";
		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(List.of("bar", "baz"), keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Prefix")
	public void testBucketListV2PrefixEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "";
		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix));
		assertNull(response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Prefix")
	public void testBucketListV2PrefixNone() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectsV2(bucketName);
		assertNull(response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Prefix")
	public void testBucketListV2PrefixNotExist() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "d";
		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Prefix")
	public void testBucketListV2PrefixUnreadable() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "\n";
		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListV2PrefixDelimiterBasic() {
		var keyNames = List.of("foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "foo/";
		String delimiter = "/";
		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(List.of("foo/bar"), keys);
		assertEquals(List.of("foo/baz/"), prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListV2PrefixDelimiterAlt() {
		var keyNames = List.of("bar", "bazar", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "a";
		String prefix = "ba";

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(List.of("bar"), keys);
		assertEquals(List.of("baza"), prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListV2PrefixDelimiterPrefixNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, "b/a/r", "b/a/c", "b/a/g", "g");

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("d").withPrefix("/"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListV2PrefixDelimiterDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, "b/a/c", "b/a/g", "b/a/r", "g");

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("z").withPrefix("b"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(List.of("b/a/c", "b/a/g", "b/a/r"), keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListV2PrefixDelimiterPrefixDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, "b/a/r", "b/a/c", "b/a/g", "g");

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("z").withPrefix("y"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("maxKeys")
	public void testBucketListV2MaxKeysOne() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(1));
		assertTrue(response.isTruncated());

		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames.subList(0, 1), keys);

		response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withStartAfter(keyNames.get(0)));
		assertFalse(response.isTruncated());

		keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames.subList(1, keyNames.size()), keys);
	}

	@Test
	@Tag("maxKeys")
	public void testBucketListV2MaxKeysZero() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(0));

		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("maxKeys")
	public void testBucketListV2MaxKeysNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectsV2(bucketName);
		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames, keys);
		assertEquals(1000, response.getMaxKeys());
	}

	@Test
	@Tag("ContinuationToken")
	public void testBucketListV2ContinuationToken() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response1 = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(1));
		var nextContinuationToken = response1.getNextContinuationToken();

		var response2 = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withContinuationToken(nextContinuationToken));
		assertEquals(nextContinuationToken, response2.getContinuationToken());
		assertFalse(response2.isTruncated());
		var keyNames2 = List.of("baz", "foo", "quxx");
		var keys = getKeys(response2.getObjectSummaries());
		assertEquals(keyNames2, keys);
	}

	@Test
	@Tag("ContinuationTokenAndStartAfter")
	public void testBucketListV2BothContinuationTokenStartAfter() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var startAfter = "bar";

		var response1 = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withStartAfter(startAfter).withMaxKeys(1));
		var nextContinuationToken = response1.getNextContinuationToken();

		var response2 = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName)
				.withStartAfter(startAfter).withContinuationToken(nextContinuationToken));
		assertEquals(nextContinuationToken, response2.getContinuationToken());
		assertFalse(response2.isTruncated());
		var keyNames2 = List.of("foo", "quxx");
		var keys = getKeys(response2.getObjectSummaries());
		assertEquals(keyNames2, keys);
	}

	@Test
	@Tag("startAfter")
	public void testBucketListV2StartAfterUnreadable() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var startAfter = "\n";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withStartAfter(startAfter));
		assertTrue(response.getStartAfter().isBlank());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames, keys);
	}

	@Test
	@Tag("startAfter")
	public void testBucketListV2StartAfterNotInList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var startAfter = "blah";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withStartAfter(startAfter));
		assertEquals(startAfter, response.getStartAfter());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(List.of("foo", "quxx"), keys);
	}

	@Test
	@Tag("startAfter")
	public void testBucketListV2StartAfterAfterList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var startAfter = "zzz";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withStartAfter(startAfter));
		assertEquals(startAfter, response.getStartAfter());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("ACL")
	public void testBucketListV2ObjectsAnonymous() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client);
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.listObjectsV2(bucketName);
	}

	@Test
	@Tag("ACL")
	public void testBucketListV2ObjectsAnonymousFail() {
		var bucketName = createBucket();
		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AmazonServiceException.class, () -> unauthenticatedClient.listObjectsV2(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketV2NotExist() {
		var bucketName = getNewBucketNameOnly();
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class, () -> client.listObjectsV2(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Filtering")
	public void testBucketListV2FilteringAll() {
		var keyNames = List.of("test1/f1", "test2/f2", "test3", "test4/f3", "testF4");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var delimiter = "/";
		var maxKeys = 3;

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter).withMaxKeys(maxKeys));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(true, response.isTruncated());
		assertEquals(maxKeys, response.getKeyCount());

		var token = response.getNextContinuationToken();

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("test3"), keys);
		assertLinesMatch(List.of("test1/", "test2/"), prefixes);

		response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter)
				.withMaxKeys(maxKeys).withContinuationToken(token));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(false, response.isTruncated());
	}

	@Test
	@Tag("Versioning")
	public void testBucketListV2Versioning() {
		var keyNames = List.of("aaa", "bbb", "ccc");
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		for (var key : keyNames) {
			for (int i = 0; i < 3; i++) {
				client.putObject(bucketName, key, key + i);
			}
		}

		var response = client.listObjectsV2(bucketName);
		assertEquals(3, response.getObjectSummaries().size());
		assertLinesMatch(List.of("aaa", "bbb", "ccc"), getKeys(response.getObjectSummaries()));
	}
}
