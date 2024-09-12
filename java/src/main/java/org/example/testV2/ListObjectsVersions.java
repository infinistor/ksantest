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
import org.example.Data.ObjectDataV2;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

public class ListObjectsVersions extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("ListObjectsVersions V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ListObjectsVersions V2 End");
	}

	@Test
	@Tag("Check")
	public void testBucketListVersionsMany() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo", "bar", "baz"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName).maxKeys(2));
		assertLinesMatch(List.of("bar", "baz"), getKeys2(response.versions()));
		assertEquals(2, response.versions().size());
		assertTrue(response.isTruncated());

		response = client.listObjectVersions(l -> l.bucket(bucketName).keyMarker("baz").maxKeys(2));
		assertLinesMatch(List.of("foo"), getKeys2(response.versions()));
		assertEquals(1, response.versions().size());
		assertFalse(response.isTruncated());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterBasic() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"));

		String delimiter = "/";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());
		assertLinesMatch(List.of("asdf"), getKeys2(response.versions()));

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("foo/", "quux/"), prefixes);
	}

	@Test
	@Tag("Encoding")
	public void testBucketListVersionsEncodingBasic() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"));

		String delimiter = "/";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter).encodingType("url"));
		assertEquals(delimiter, response.delimiter());
		assertLinesMatch(List.of("asdf+b"), getKeys2(response.versions()));

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(3, prefixes.size());
		assertLinesMatch(List.of("foo+1/", "foo/", "quux ab/"), prefixes);

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
		String delimiter = "a";
		var client = getClient();
		var bucketName = createObjects(client, List.of("bar", "baz", "cab", "foo"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());

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
		String delimiter = "%";
		var client = getClient();
		var bucketName = createObjects(client, List.of("b%ar", "b%az", "c%ab", "foo"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b%", "c%"), prefixes);

	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterWhitespace() {
		String delimiter = " ";
		var client = getClient();
		var bucketName = createObjects(client, List.of("b ar", "b az", "c ab", "foo"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b ", "c "), prefixes);

	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterDot() {
		String delimiter = ".";
		var client = getClient();
		var bucketName = createObjects(client, List.of("b.ar", "b.az", "c.ab", "foo"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b.", "c."), prefixes);

	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterUnreadable() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "\n";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterEmpty() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals("", response.delimiter());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterNone() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectVersions(l -> l.bucket(bucketName));

		assertNull(response.delimiter());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	public void testBucketListVersionsDelimiterNotExist() {
		var keyNames = List.of("bar", "baz", "cab", "foo");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "/";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

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

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "/";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(keyNames2, keys);
		assertLinesMatch(List.of("0/"), prefixes);

	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixBasic() {
		String prefix = "foo/";
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo/bar", "foo/baz", "quux"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName).prefix(prefix));

		assertEquals(prefix, response.prefix());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("foo/bar", "foo/baz"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixAlt() {
		String prefix = "ba";
		var client = getClient();
		var bucketName = createObjects(client, List.of("bar", "baz", "foo"));

		var response = client.listObjectVersions(l -> l.bucket(bucketName).prefix(prefix));

		assertEquals(prefix, response.prefix());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("bar", "baz"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "";
		var response = client.listObjectVersions(l -> l.bucket(bucketName).prefix(prefix));

		assertEquals("", response.prefix());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixNone() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectVersions(l -> l.bucket(bucketName));

		assertEquals("", response.prefix());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixNotExist() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "d";
		var response = client.listObjectVersions(l -> l.bucket(bucketName).prefix(prefix));

		assertEquals(prefix, response.prefix());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	public void testBucketListVersionsPrefixUnreadable() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "\n";
		var response = client.listObjectVersions(l -> l.bucket(bucketName).prefix(prefix));

		assertEquals(prefix, response.prefix());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterBasic() {
		var keyNames = List.of("foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "foo/";
		String delimiter = "/";
		var response = client.listObjectVersions(
				l -> l.bucket(bucketName).delimiter(delimiter).prefix(prefix));

		assertEquals(prefix, response.prefix());
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("foo/bar"), keys);

		assertLinesMatch(List.of("foo/baz/"), prefixes);

	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterAlt() {
		var keyNames = List.of("bar", "bazar", "cab", "foo");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "a";
		String prefix = "ba";

		var response = client.listObjectVersions(
				l -> l.bucket(bucketName).delimiter(delimiter).prefix(prefix));

		assertEquals(prefix, response.prefix());
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("bar"), keys);

		assertLinesMatch(List.of("baza"), prefixes);

	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterPrefixNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var response = client
				.listObjectVersions(l -> l.bucket(bucketName).delimiter("d").prefix("/"));

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/c", "b/a/g", "b/a/r", "g"));

		var response = client
				.listObjectVersions(l -> l.bucket(bucketName).delimiter("z").prefix("b"));

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertLinesMatch(List.of("b/a/c", "b/a/g", "b/a/r"), keys);

		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	public void testBucketListVersionsPrefixDelimiterPrefixDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var response = client
				.listObjectVersions(l -> l.bucket(bucketName).delimiter("z").prefix("y"));

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListVersionsMaxKeysOne() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectVersions(l -> l.bucket(bucketName).maxKeys(1));

		assertTrue(response.isTruncated());

		var keys = getKeys2(response.versions());
		assertEquals(keyNames.subList(0, 1), keys);

		response = client
				.listObjectVersions(l -> l.bucket(bucketName).keyMarker(keyNames.get(0)));
		assertFalse(response.isTruncated());

		keys = getKeys2(response.versions());
		assertEquals(keyNames.subList(1, keyNames.size()), keys);
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListVersionsMaxKeysZero() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectVersions(l -> l.bucket(bucketName).maxKeys(0));

		assertFalse(response.isTruncated());
		var keys = getKeys2(response.versions());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("MaxKeys")
	public void testBucketListVersionsMaxKeysNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectVersions(l -> l.bucket(bucketName));

		assertFalse(response.isTruncated());
		var keys = getKeys2(response.versions());
		assertEquals(keyNames, keys);
		assertEquals(1000, response.maxKeys());
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectVersions(l -> l.bucket(bucketName).keyMarker(""));

		assertNull(response.nextKeyMarker());
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerEmpty() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjectVersions(l -> l.bucket(bucketName).keyMarker(""));

		assertNull(response.nextKeyMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys2(response.versions());
		assertEquals(keyNames, keys);
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerUnreadable() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "\n";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).keyMarker(marker));

		assertNull(response.nextKeyMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys2(response.versions());
		assertEquals(keyNames, keys);
	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerNotInList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "blah";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).keyMarker(marker));

		assertEquals(marker, response.keyMarker());
		var keys = getKeys2(response.versions());
		assertLinesMatch(List.of("foo", "quxx"), keys);

	}

	@Test
	@Tag("marker")
	public void testBucketListVersionsMarkerAfterList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "zzz";

		var response = client.listObjectVersions(l -> l.bucket(bucketName).keyMarker(marker));

		assertEquals(marker, response.keyMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys2(response.versions());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("Metadata")
	public void testBucketListVersionsReturnData() {
		var keys = List.of("bar", "baz", "foo");
		var client = getClient();
		var bucketName = createBucket(client);

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		createObjects(client, bucketName, keys);

		var dataList = new ArrayList<ObjectDataV2>();

		for (var key : keys) {
			var objResponse = client.headObject(h -> h.bucket(bucketName).key(key));
			var aclResponse = client.getObjectAcl(g -> g.bucket(bucketName).key(key));

			dataList.add(ObjectDataV2.builder().key(key).displayName(aclResponse.owner().displayName())
					.id(aclResponse.owner().id()).eTag(objResponse.eTag())
					.lastModified(objResponse.lastModified()).contentLength(objResponse.contentLength())
					.versionId(objResponse.versionId()).build());
		}

		var response = client.listObjectVersions(l -> l.bucket(bucketName));
		var objects = response.versions();

		for (var object : objects) {
			var key = object.key();
			var data = getObjectToKey(key, dataList);

			assertNotNull(data);
			assertEquals(data.eTag, object.eTag());
			assertEquals(data.contentLength, object.size());
			assertEquals(data.displayName, object.owner().displayName());
			assertEquals(data.id, object.owner().id());
			assertEquals(data.versionId, object.versionId());
			assertEquals(data.lastModified, object.lastModified());
		}
	}

	@Test
	@Tag("ACL")
	public void testBucketListVersionsObjectsAnonymous() {
		var client = getClient();
		var bucketName = createBucketCannedAcl(client, BucketCannedACL.PUBLIC_READ);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.listObjectVersions(l -> l.bucket(bucketName));
	}

	@Test
	@Tag("ACL")
	public void testBucketListVersionsObjectsAnonymousFail() {
		var bucketName = createBucket();
		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AwsServiceException.class,
				() -> unauthenticatedClient.listObjectVersions(l -> l.bucket(bucketName)));

		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketListVersionsNotExist() {
		var bucketName = getNewBucketNameOnly();
		var client = getClient();

		var e = assertThrows(AwsServiceException.class,
				() -> client.listObjectVersions(l -> l.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Filtering")
	public void testVersioningBucketListFilteringAll() {
		var keyNames = List.of("test1/f1", "test2/f2", "test3", "test4/f3", "testF4");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "test3";
		var delimiter = "/";
		var maxKeys = 3;

		var response = client.listObjectVersions(
				l -> l.bucket(bucketName).delimiter(delimiter).maxKeys(maxKeys));
		assertEquals(delimiter, response.delimiter());
		assertEquals(maxKeys, response.maxKeys());
		assertEquals(marker, response.nextKeyMarker());
		assertEquals(true, response.isTruncated());

		var keys = getKeys2(response.versions());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("test3"), keys);

		assertLinesMatch(List.of("test1/", "test2/"), prefixes);

		response = client.listObjectVersions(l -> l.bucket(bucketName).delimiter(delimiter)
				.maxKeys(maxKeys).keyMarker(marker));

		assertEquals(delimiter, response.delimiter());
		assertEquals(maxKeys, response.maxKeys());
		assertEquals(false, response.isTruncated());
	}
}
