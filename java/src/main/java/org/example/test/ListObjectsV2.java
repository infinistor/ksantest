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

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
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
	// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)
	public void testBucketListV2Many() {
		var bucketName = createObjects(List.of("foo", "bar", "baz"));
		var client = getClient();

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
	// ListObjectsV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인
	public void testBasicKeyCount() {
		var bucketName = getNewBucket();
		var client = getClient();

		for (int i = 0; i < 5; i++)
			client.putObject(bucketName, Integer.toString(i), "");

		var response = client.listObjectsV2(bucketName);

		assertEquals(5, response.getKeyCount());
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterBasic() {
		var bucketName = createObjects(
				List.of("foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"));
		var client = getClient();

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
	// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)
	public void testBucketListV2EncodingBasic() {
		var bucketName = createObjects(List.of("foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"));
		var client = getClient();

		String delimiter = "/";

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter).withEncodingType("URL"));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(List.of("asdf%2Bb"),
				getKeys(response.getObjectSummaries()));

		var prefixes = response.getCommonPrefixes();
		assertEquals(3, prefixes.size());
		assertEquals(List.of("foo%2B1/", "foo/", "quux+ab/"), prefixes);
	}

	@Test
	@Tag("Filtering")
	// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
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
	// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterPrefixEndsWithDelimiter() {
		var bucketName = createObjectsToBody(List.of("asdf/"), "");
		validateListObjectV2(bucketName, "asdf/", "/", null, 1000, false,
				List.of("asdf/"), new ArrayList<>(), true);
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterAlt() {
		var bucketName = createObjects(List.of("bar", "baz", "cab", "foo"));
		var client = getClient();

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
	// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
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
	// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterPercentage() {
		var bucketName = createObjects(List.of("b%ar", "b%az", "c%ab", "foo"));
		var client = getClient();

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
	// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterWhitespace() {
		var bucketName = createObjects(List.of("b ar", "b az", "c ab", "foo"));
		var client = getClient();

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
	// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterDot() {
		var bucketName = createObjects(List.of("b.ar", "b.az", "c.ab", "foo"));
		var client = getClient();

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
	// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterUnreadable() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterEmpty() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterNone() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listObjectsV2(bucketName);
		assertNull(response.getDelimiter());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();

		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Fetchowner")
	// [권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	public void testBucketListV2FetchownerNotempty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withFetchOwner(true));
		var objectList = response.getObjectSummaries();

		assertNotNull(objectList.get(0).getOwner());
	}

	@Test
	@Tag("Fetchowner")
	// @Tag( "[default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지
	// 확인(ListObjectsV2)
	public void testBucketListV2FetchownerDefaultempty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listObjectsV2(bucketName);
		var objectList = response.getObjectSummaries();

		assertNull(objectList.get(0).getOwner());
	}

	@Test
	@Tag("Fetchowner")
	// [권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	public void testBucketListV2FetchownerEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withFetchOwner(false));
		var objectList = response.getObjectSummaries();

		assertNull(objectList.get(0).getOwner());
	}

	@Test
	@Tag("delimiter")
	// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	public void testBucketListV2DelimiterNotExist() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixBasic() {
		var bucketName = createObjects(List.of("foo/bar", "foo/baz", "quux"));
		var client = getClient();

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
	// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixAlt() {
		var bucketName = createObjects(List.of("bar", "baz", "foo"));
		var client = getClient();

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
	// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixNone() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listObjectsV2(bucketName);
		assertNull(response.getPrefix());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("Prefix")
	// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixNotExist() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixUnreadable() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixDelimiterBasic() {
		var keyNames = List.of("foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixDelimiterAlt() {
		var keyNames = List.of("bar", "bazar", "cab", "foo");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixDelimiterPrefixNotExist() {
		var bucketName = createObjects(List.of("b/a/r", "b/a/c", "b/a/g", "g"));
		var client = getClient();

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("d").withPrefix("/"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void testBucketListV2PrefixDelimiterDelimiterNotExist() {
		var bucketName = createObjects(List.of("b/a/c", "b/a/g", "b/a/r", "g"));
		var client = getClient();

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("z").withPrefix("b"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(List.of("b/a/c", "b/a/g", "b/a/r"), keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지
	// 확인(ListObjectsV2)
	public void testBucketListV2PrefixDelimiterPrefixDelimiterNotExist() {
		var bucketName = createObjects(List.of("b/a/r", "b/a/c", "b/a/g", "g"));
		var client = getClient();

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter("z").withPrefix("y"));

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("maxKeys")
	// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	public void testBucketListV2MaxKeysOne() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)
	public void testBucketListV2MaxKeysZero() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(0));

		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("maxKeys")
	// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	public void testBucketListV2MaxKeysNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listObjectsV2(bucketName);
		assertFalse(response.isTruncated());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(keyNames, keys);
		assertEquals(1000, response.getMaxKeys());
	}

	@Test
	@Tag("ContinuationToken")
	// 오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인
	public void testBucketListV2ContinuationToken() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 오브젝트 목록을 가져올때 StartAfter와 토큰이 재대로 동작하는지 확인
	public void testBucketListV2BothContinuationTokenStartAfter() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// startafter에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	public void testBucketListV2StartafterUnreadable() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// [startafter와 일치하는 오브젝트가 존재하지 않는 환경 해당 startafter보다 정렬순서가 낮은 오브젝트는 존재하는 환경]
	// startafter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	public void testBucketListV2StartafterNotInList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var startAfter = "blah";

		var response = client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withStartAfter(startAfter));
		assertEquals(startAfter, response.getStartAfter());
		var keys = getKeys(response.getObjectSummaries());
		assertEquals(List.of("foo", "quxx"), keys);
	}

	@Test
	@Tag("startAfter")
	// [startafter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] startafter를 설정하고 오브젝트 목록을
	// 불러올때 재대로 가져오는지 확인
	public void testBucketListV2StartafterAfterList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var bucketName = createObjects(keyNames);
		var client = getClient();

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
	// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)
	public void testBucketListV2ObjectsAnonymous() {
		var bucketName = getNewBucket();
		var client = getClient();
		client.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.listObjectsV2(bucketName);
	}

	@Test
	@Tag("ACL")
	// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)
	public void testBucketListV2ObjectsAnonymousFail() {
		var bucketName = getNewBucket();

		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AmazonServiceException.class, () -> unauthenticatedClient.listObjectsV2(bucketName));

		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(403, statusCode);
		assertEquals(MainData.AccessDenied, errorCode);
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)
	public void testBucketv2Notexist() {
		var bucketName = getNewBucketNameOnly();
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class, () -> client.listObjectsV2(bucketName));

		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(404, statusCode);
		assertEquals(MainData.NoSuchBucket, errorCode);
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Filtering")
	// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	public void testBucketListV2FilteringAll() {
		var keyNames = List.of("test1/f1", "test2/f2", "test3", "test4/f3", "testF4");
		var bucketName = createObjects(keyNames);
		var client = getClient();

		var marker = "test3";
		var delimiter = "/";
		var maxKeys = 3;

		var response = client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter).withMaxKeys(maxKeys));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(marker, response.getNextContinuationToken());
		assertEquals(true, response.isTruncated());
		assertEquals(maxKeys, response.getKeyCount());

		var keys = getKeys(response.getObjectSummaries());
		var prefixes = response.getCommonPrefixes();
		assertLinesMatch(List.of("test3"), keys);
		assertLinesMatch(List.of("test1/", "test2/"), prefixes);

		response = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withDelimiter(delimiter)
				.withMaxKeys(maxKeys).withContinuationToken(marker));
		assertEquals(delimiter, response.getDelimiter());
		assertEquals(maxKeys, response.getMaxKeys());
		assertEquals(false, response.isTruncated());
	}
}
