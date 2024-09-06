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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Data.ObjectData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;

public class ListObjectsVersions extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("ListObjectsVersions Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ListObjectsVersions End");
	}

	@Test
	@Tag("Check")
	public void testBucketListVersionsMany() {
		var bucketName = createObjects(List.of("foo", "bar", "baz"));
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withMaxResults(2));
		assertLinesMatch(List.of("bar", "baz"), getKeys2(response.getVersionSummaries()));
		assertEquals(2, response.getVersionSummaries().size());
		assertTrue(response.isTruncated());

		response = client
				.listVersions(
						new ListVersionsRequest().withBucketName(bucketName).withKeyMarker("baz").withMaxResults(2));
		assertLinesMatch(List.of("foo"), getKeys2(response.getVersionSummaries()));
		assertEquals(1, response.getVersionSummaries().size());
		assertFalse(response.isTruncated());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterBasic() {
		var bucketName = createObjects(List.of("foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"));
		var client = getClient();

		String delimiter = "/";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());
		assertLinesMatch(List.of("asdf"), getKeys2(response.getVersionSummaries()));

		var prefixes = response.getCommonPrefixes();
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("foo/", "quux/"), prefixes);
	}

	@Test
	@Tag("Encoding")
	public void testBucketListVersionsEncodingBasic() {
		var bucketName = createObjects(List.of("foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"));
		var client = getClient();

		String delimiter = "/";

		var response = client.listVersions(
				new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter).withEncodingType("url"));
		assertEquals(delimiter, response.getDelimiter());
		assertLinesMatch(List.of("asdf%2Bb"), getKeys2(response.getVersionSummaries()));

		var prefixes = response.getCommonPrefixes();
		assertEquals(3, prefixes.size());
		assertLinesMatch(List.of("foo%2B1/", "foo/", "quux+ab/"), prefixes);

	}

	@Test
	@Tag("Filtering")
	public void testBucketListVersionsDelimiterPrefix() {
		var bucketName = createObjects(List.of("asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"));

		String delimiter = "/";
		String marker = "";
		String prefix = "";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("asdf"), new ArrayList<>(),
				"asdf");
		marker = validateListObject(bucketName, prefix, delimiter, marker, 1, true, new ArrayList<>(), List.of("boo/"),
				"boo/");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(), List.of("cquux/"), null);

		marker = validateListObject(bucketName, prefix, delimiter, "", 2, true, List.of("asdf"), List.of("boo/"),
				"boo/");
		validateListObject(bucketName, prefix, delimiter, marker, 2, false, new ArrayList<>(), List.of("cquux/"), null);

		prefix = "boo/";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("boo/bar"), new ArrayList<>(),
				"boo/bar");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(), List.of("boo/baz/"),
				null);

		validateListObject(bucketName, prefix, delimiter, "", 2, false, List.of("boo/bar"), List.of("boo/baz/"), null);
	}

	@Test
	@Tag("Filtering")
	public void testBucketListVersionsDelimiterPrefixEndsWithDelimiter() {
		var client = getClient();
		var bucketName = createEmptyObjects(client, List.of("asdf/"));

		validateListObject(bucketName, "asdf/", "/", "", 1000, false,
				List.of("asdf/"), new ArrayList<>(), null);

	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterAlt() {
		var bucketName = createObjects(List.of("bar", "baz", "cab", "foo"));

		var client = getClient();

		String delimiter = "a";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("ba", "ca"), prefixes);

	}

	@Test
	@Tag("Filtering")
	public void testBucketListVersionsDelimiterPrefixUnderscore() {
		var bucketName = createObjects(List.of("Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"));

		String delimiter = "/";
		String marker = "";
		String prefix = "";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true,
				List.of("Obj1_"), new ArrayList<>(), "Obj1_");
		marker = validateListObject(bucketName, prefix, delimiter, marker, 1, true, new ArrayList<>(),
				List.of("Under1/"), "Under1/");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(),
				List.of("Under2/"), null);

		marker = validateListObject(bucketName, prefix, delimiter, "", 2, true, List.of("Obj1_"), List.of("Under1/"),
				"Under1/");
		validateListObject(bucketName, prefix, delimiter, marker, 2, false, new ArrayList<>(), List.of("Under2/"),
				null);

		prefix = "Under1/";

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("Under1/bar"),
				new ArrayList<>(), "Under1/bar");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(), List.of("Under1/baz/"),
				null);

		validateListObject(bucketName, prefix, delimiter, "", 2, false, List.of("Under1/bar"), List.of("Under1/baz/"),
				null);
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterPercentage() {
		var bucketName = createObjects(List.of("b%ar", "b%az", "c%ab", "foo"));

		var client = getClient();

		String delimiter = "%";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b%", "c%"), prefixes);

	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterWhitespace() {
		var bucketName = createObjects(List.of("b ar", "b az", "c ab", "foo"));

		var client = getClient();

		String delimiter = " ";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b ", "c "), prefixes);

	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterDot() {
		var bucketName = createObjects(List.of("b.ar", "b.az", "c.ab", "foo"));

		var client = getClient();

		String delimiter = ".";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = response.getCommonPrefixes();

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b.", "c."), prefixes);

	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterUnreadable() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String delimiter = "\n";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterEmpty() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String delimiter = "";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertNull(response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterNone() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));

		assertNull(response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterNotExist() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String delimiter = "/";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterNotSkipSpecial() {
		var keyNames = new ArrayList<String>();
		for (int i = 1000; i < 1999; i++)
			keyNames.add("0/" + Integer.toString(i));
		var keyNames2 = List.of("1999", "1999#", "1999+", "2000");
		keyNames.addAll(keyNames2);

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String delimiter = "/";

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter));

		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames2, keys);
		assertLinesMatch(List.of("0/"), prefixes);

	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixBasic() {
		var bucketName = createObjects(List.of("foo/bar", "foo/baz", "quux"));

		var client = getClient();

		String prefix = "foo/";
		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("foo/bar", "foo/baz"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixAlt() {
		var bucketName = createObjects(List.of("bar", "baz", "foo"));

		var client = getClient();

		String prefix = "ba";
		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("bar", "baz"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String prefix = "";
		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertNull(response.getPrefix());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixNone() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));

		assertNull(response.getPrefix());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixNotExist() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String prefix = "d";
		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixUnreadable() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String prefix = "\n";
		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterBasic() {
		var keyNames = List.of("foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String prefix = "foo/";
		String delimiter = "/";
		var response = client.listVersions(
				new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("foo/bar"), keys);

		assertLinesMatch(List.of("foo/baz/"), prefixes);

	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterAlt() {
		var keyNames = List.of("bar", "bazar", "cab", "foo");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		String delimiter = "a";
		String prefix = "ba";

		var response = client.listVersions(
				new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter).withPrefix(prefix));

		assertEquals(prefix, response.getPrefix());
		assertEquals(delimiter, response.getDelimiter());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("bar"), keys);

		assertLinesMatch(List.of("baza"), prefixes);

	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterPrefixNotExist() {
		var bucketName = createObjects(List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var client = getClient();

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter("d").withPrefix("/"));

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterDelimiterNotExist() {
		var bucketName = createObjects(List.of("b/a/c", "b/a/g", "b/a/r", "g"));

		var client = getClient();

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter("z").withPrefix("b"));

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertLinesMatch(List.of("b/a/c", "b/a/g", "b/a/r"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist() {
		var bucketName = createObjects(List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var client = getClient();

		var response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter("z").withPrefix("y"));

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListVersionsMaxKeysOne() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withMaxResults(1));

		assertTrue(response.isTruncated());

		var keys = getKeys2(response.getVersionSummaries());
		assertEquals(keyNames.subList(0, 1), keys);

		response = client
				.listVersions(new ListVersionsRequest().withBucketName(bucketName).withKeyMarker(keyNames.get(0)));
		assertFalse(response.isTruncated());

		keys = getKeys2(response.getVersionSummaries());
		assertEquals(keyNames.subList(1, keyNames.size()), keys);
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListVersionsMaxKeysZero() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withMaxResults(0));

		assertFalse(response.isTruncated());
		var keys = getKeys2(response.getVersionSummaries());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListVersionsMaxKeysNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));

		assertFalse(response.isTruncated());
		var keys = getKeys2(response.getVersionSummaries());
		assertEquals(keyNames, keys);
		assertEquals(1000, response.getMaxKeys());
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withKeyMarker(""));

		assertNull(response.getNextKeyMarker());
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerEmpty() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withKeyMarker(""));

		assertNull(response.getNextKeyMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys2(response.getVersionSummaries());
		assertEquals(keyNames, keys);
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerUnreadable() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var marker = "\n";

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withKeyMarker(marker));

		assertNull(response.getNextKeyMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys2(response.getVersionSummaries());
		assertEquals(keyNames, keys);
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerNotInList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var marker = "blah";

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withKeyMarker(marker));

		assertEquals(marker, response.getKeyMarker());
		var keys = getKeys2(response.getVersionSummaries());
		assertLinesMatch(List.of("foo", "quxx"), keys);

	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerAfterList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var marker = "zzz";

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withKeyMarker(marker));

		assertEquals(marker, response.getKeyMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys2(response.getVersionSummaries());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("Metadata")
	public void testBucketListVersionsReturnData() {
		var keys = List.of("bar", "baz", "foo");
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		createObjects(client, bucketName, keys);

		var dataList = new ArrayList<ObjectData>();

		for (var key : keys) {
			var objResponse = client.getObjectMetadata(bucketName, key);
			var aclResponse = client.getObjectAcl(bucketName, key);

			dataList.add(ObjectData.builder().key(key).displayName(aclResponse.getOwner().getDisplayName())
					.id(aclResponse.getOwner().getId()).eTag(objResponse.getETag())
					.versionId(objResponse.getVersionId())
					.lastModified(objResponse.getLastModified()).contentLength(objResponse.getContentLength()).build());
		}

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
		var objects = response.getVersionSummaries();

		for (var object : objects) {
			var key = object.getKey();
			var data = getObjectToKey(key, dataList);

			assertNotNull(data);
			assertEquals(data.eTag, object.getETag());
			assertEquals(data.contentLength, object.getSize());
			assertEquals(data.displayName, object.getOwner().getDisplayName());
			assertEquals(data.id, object.getOwner().getId());
			assertEquals(data.versionId, object.getVersionId());
			assertEquals(data.lastModified, object.getLastModified());
		}
	}

	@Test
	@Tag("ACL")
	public void testBucketListVersionsObjectsAnonymous() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client);
		client.setBucketAcl(new SetBucketAclRequest(bucketName, CannedAccessControlList.PublicRead));

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.listVersions(new ListVersionsRequest().withBucketName(bucketName));
	}

	@Test
	@Tag("ACL")
	public void testBucketListVersionsObjectsAnonymousFail() {
		var bucketName = createBucket();
		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AmazonServiceException.class,
				() -> unauthenticatedClient.listVersions(new ListVersionsRequest().withBucketName(bucketName)));

		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketListVersionsNotExist() {
		var bucketName = getNewBucketNameOnly();
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class,
				() -> client.listVersions(new ListVersionsRequest().withBucketName(bucketName)));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Filtering")
	public void testVersioningBucketListFilteringAll() {
		var keyNames = List.of("test1/f1", "test2/f2", "test3", "test4/f3", "testF4");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var marker = "test3";
		var delimiter = "/";
		var maxKeys = 3;

		var response = client.listVersions(
				new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter).withMaxResults(maxKeys));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(marker, response.getNextKeyMarker());
		assertEquals(true, response.isTruncated());

		var keys = getKeys2(response.getVersionSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("test3"), keys);

		assertLinesMatch(List.of("test1/", "test2/"), prefixes);

		response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withDelimiter(delimiter)
				.withMaxResults(maxKeys).withKeyMarker(marker));

		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(false, response.isTruncated());
	}
}
