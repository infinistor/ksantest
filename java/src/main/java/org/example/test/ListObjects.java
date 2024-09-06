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
import org.example.Data.ObjectData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;

public class ListObjects extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("ListObjects Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ListObjects End");
	}

	@Test
	@Tag("Check")
	public void testBucketListMany() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo", "bar", "baz"));

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(2));
		assertLinesMatch(List.of("bar", "baz"),

				getKeys(response.getObjectSummaries()));
		assertEquals(2, response.getObjectSummaries().size());
		assertTrue(response.isTruncated());

		response = client
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMarker("baz").withMaxKeys(2));
		assertLinesMatch(List.of("foo"),

				getKeys(response.getObjectSummaries()));
		assertEquals(1, response.getObjectSummaries().size());
		assertFalse(response.isTruncated());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterBasic() {
		var bucketName = createObjects(
				List.of("foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"));
		var client = getClient();

		String delimiter = "/";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());
		assertLinesMatch(List.of("asdf"),

				getKeys(response.getObjectSummaries()));

		var prefixes = response.getCommonPrefixes();
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("foo/", "quux/"), prefixes);

	}

	@Test
	@Tag("Encoding")
	public void testBucketListEncodingBasic() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"));

		String delimiter = "/";

		var response = client.listObjects(
				new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter).withEncodingType("url"));
		assertEquals(delimiter, response.getDelimiter());
		assertLinesMatch(List.of("asdf%2Bb"), getKeys(response.getObjectSummaries()));

		var prefixes = response.getCommonPrefixes();
		assertEquals(3, prefixes.size());
		assertLinesMatch(List.of("foo%2B1/", "foo/", "quux+ab/"), prefixes);

	}

	@Test
	@Tag("Filtering")
	public void testBucketListDelimiterPrefix() {
		var bucketName = createObjects(List.of("asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"));

		String delimiter = "/";
		String marker = "";
		String prefix = "";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("asdf"),
				new ArrayList<>(), "asdf");
		marker = validateListObject(bucketName, prefix, delimiter, marker, 1, true, new ArrayList<>(),
				List.of("boo/"), "boo/");

		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(),
				List.of("cquux/"), null);

		marker = validateListObject(bucketName, prefix, delimiter, "", 2, true, List.of("asdf"), List.of("boo/"),
				"boo/");
		validateListObject(bucketName, prefix, delimiter, marker, 2, false, new ArrayList<>(),
				List.of("cquux/"), null);

		prefix = "boo/";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("boo/bar"),
				new ArrayList<>(), "boo/bar");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(),
				List.of("boo/baz/"), null);

		validateListObject(bucketName, prefix, delimiter, "", 2, false, List.of("boo/bar"),
				List.of("boo/baz/"), null);
	}

	@Test
	@Tag("Filtering")
	public void testBucketListDelimiterPrefixEndsWithDelimiter() {
		var client = getClient();
		var bucketName = createEmptyObjects(client, List.of("asdf/"));

		validateListObject(bucketName, "asdf/", "/", "", 1000, false,
				List.of("asdf/"), new ArrayList<>(), null);

	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterAlt() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("bar", "baz", "cab", "foo"));

		String delimiter = "a";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("ba", "ca"), prefixes);

	}

	@Test
	@Tag("Filtering")
	public void testBucketListDelimiterPrefixUnderscore() {
		var bucketName = createObjects(List.of("Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"));

		String delimiter = "/";
		String marker = "";
		String prefix = "";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("Obj1_"),
				new ArrayList<>(), "Obj1_");
		marker = validateListObject(bucketName, prefix, delimiter, marker, 1, true, new ArrayList<>(),
				List.of("Under1/"), "Under1/");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(),
				List.of("Under2/"), null);

		marker =

				validateListObject(bucketName, prefix, delimiter, "", 2, true, List.of("Obj1_"), List.of("Under1/"

				), "Under1/");
		validateListObject(bucketName, prefix, delimiter, marker, 2, false, new ArrayList<>(), List.of("Under2/"),
				null);

		prefix = "Under1/";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("Under1/bar"),
				new ArrayList<>(), "Under1/bar");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(),
				List.of("Under1/baz/"), null);

		validateListObject(bucketName, prefix, delimiter, "", 2, false, List.of("Under1/bar"),
				List.of("Under1/baz/"), null);
	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterPercentage() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b%ar", "b%az", "c%ab", "foo"));

		String delimiter = "%";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b%", "c%"), prefixes);

	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterWhitespace() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b ar", "b az", "c ab", "foo"));

		String delimiter = " ";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b ", "c "), prefixes);

	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterDot() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b.ar", "b.az", "c.ab", "foo"));

		String delimiter = ".";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b.", "c."), prefixes);

	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterUnreadable() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "\n";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterEmpty() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertNull(response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterNone() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(bucketName);

		assertNull(response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterNotExist() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "/";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	public void testBucketListDelimiterNotSkipSpecial() {
		var keyNames = new ArrayList<String>();
		for (int i = 1000; i < 1999; i++)
			keyNames.add("0/" + Integer.toString(i));
		var keyNames2 = List.of("1999", "1999#", "1999+", "2000");
		keyNames.addAll(keyNames2);

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "/";

		var response = client.listObjects(
				new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames2, keys);
		assertLinesMatch(List.of("0/"), prefixes);

	}

	@Test
	@Tag("prefix")
	public void testBucketListPrefixBasic() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo/bar", "foo/baz", "quux"));

		String prefix = "foo/";
		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("foo/bar", "foo/baz"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListPrefixAlt() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("bar", "baz", "foo"));

		String prefix = "ba";
		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));
		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("bar", "baz"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListPrefixEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "";
		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertNull(response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListPrefixNone() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(bucketName);

		assertNull(response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListPrefixNotExist() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "d";
		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListPrefixUnreadable() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "\n";
		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListPrefixDelimiterBasic() {
		var keyNames = List.of("foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "foo/";
		String delimiter = "/";
		var response = client.listObjects(
				new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("foo/bar"), keys);

		assertLinesMatch(List.of("foo/baz/"), prefixes);

	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListPrefixDelimiterAlt() {
		var keyNames = List.of("bar", "bazar", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "a";
		String prefix = "ba";

		var response = client.listObjects(
				new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("bar"), keys);

		assertLinesMatch(List.of("baza"), prefixes);

	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListPrefixDelimiterPrefixNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var response = client
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter("d").withPrefix("/"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListPrefixDelimiterDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/c", "b/a/g", "b/a/r", "g"));

		var response = client
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter("z").withPrefix("b"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("b/a/c", "b/a/g", "b/a/r"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListPrefixDelimiterPrefixDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var response = client
				.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter("z").withPrefix("y"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListMaxKeysOne() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(1));

		assertTrue(response.isTruncated());

		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames.subList(0, 1), keys);

		response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMarker(keyNames.get(0)));
		assertFalse(response.isTruncated());

		keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames.subList(1, keyNames.size()), keys);
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListMaxKeysZero() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(0));

		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListMaxKeysNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(bucketName);

		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames, keys);
		assertEquals(1000, response.getMaxKeys());
	}

	@Test
	@Tag("marker")
	public void testBucketListMarkerNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMarker(""));

		assertNull(response.getNextMarker());
	}

	@Test
	@Tag("marker")
	public void testBucketListMarkerEmpty() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMarker(""));

		assertNull(response.getNextMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames, keys);
	}

	@Test
	@Tag("marker")
	public void testBucketListMarkerUnreadable() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "\n";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMarker(marker));

		assertNull(response.getNextMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames, keys);
	}

	@Test
	@Tag("marker")
	public void testBucketListMarkerNotInList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "blah";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMarker(marker));

		assertEquals(marker, response.getMarker());
		var keys = getKeys(response.getObjectSummaries());
		assertLinesMatch(List.of("foo", "quxx"), keys);

	}

	@Test
	@Tag("marker")
	public void testBucketListMarkerAfterList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "zzz";

		var response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withMarker(marker));

		assertEquals(marker, response.getMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("Metadata")
	public void testBucketListReturnData() {
		var keyNames = List.of("bar", "baz", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var data = new ArrayList<ObjectData>();

		for (var Key : keyNames) {
			var objResponse = client.getObjectMetadata(bucketName, Key);
			var aclResponse = client.getObjectAcl(bucketName, Key);

			data.add(ObjectData.builder().key(Key).displayName(aclResponse.getOwner().getDisplayName())
					.id(aclResponse.getOwner().getId()).eTag(objResponse.getETag())
					.lastModified(objResponse.getLastModified()).contentLength(objResponse.getContentLength()).build());
		}

		var response = client.listObjects(bucketName);
		var objList = response.getObjectSummaries();

		for (var Object : objList) {
			var keyName = Object.getKey();
			var keyData = getObjectToKey(keyName, data);

			assertNotNull(keyData);
			assertEquals(keyData.eTag, Object.getETag());
			assertEquals(keyData.contentLength, Object.getSize());
			assertEquals(keyData.displayName, Object.getOwner().getDisplayName());
			assertEquals(keyData.id, Object.getOwner().getId());
			assertEquals(keyData.lastModified, Object.getLastModified());
		}

	}

	@Test
	@Tag("ACL")
	public void testBucketListObjectsAnonymous() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		client.setBucketAcl(new SetBucketAclRequest(bucketName, CannedAccessControlList.PublicRead));

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.listObjects(bucketName);
	}

	@Test
	@Tag("ACL")
	public void testBucketListObjectsAnonymousFail() {
		var bucketName = createBucket();
		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AmazonServiceException.class, () -> unauthenticatedClient.listObjects(bucketName));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(HttpStatus.SC_FORBIDDEN, statusCode);
		assertEquals(MainData.ACCESS_DENIED, errorCode);
	}

	@Test
	@Tag("ERROR")
	public void testBucketNotExist() {
		var bucketName = getNewBucketNameOnly();
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class, () -> client.listObjects(bucketName));

		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(HttpStatus.SC_NOT_FOUND, statusCode);
		assertEquals(MainData.NO_SUCH_BUCKET, errorCode);
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Filtering")
	public void testBucketListFilteringAll() {
		var keyNames = List.of("test1/f1", "test2/f2", "test3", "test4/f3", "testF4");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "test3";
		var delimiter = "/";
		var maxKeys = 3;

		var response = client.listObjects(
				new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter).withMaxKeys(maxKeys));

		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(marker, response.getNextMarker());
		assertEquals(true, response.isTruncated());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("test3"), keys);

		assertLinesMatch(List.of("test1/", "test2/"), prefixes);

		response = client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter)
				.withMaxKeys(maxKeys).withMarker(marker));

		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(false, response.isTruncated());
	}
}
