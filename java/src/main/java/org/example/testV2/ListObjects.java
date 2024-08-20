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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.example.Data.MainData;
import org.example.Data.ObjectDataV2;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;

public class ListObjects extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("ListObjects V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ListObjects V2 End");
	}

	@Test
	@Tag("Check")
	// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인
	public void testBucketListMany() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo", "bar", "baz"));

		var response = client.listObjects(l -> l.bucket(bucketName).maxKeys(2));
		assertLinesMatch(List.of("bar", "baz"), getKeys(response.contents()));
		assertEquals(2, response.contents().size());
		assertTrue(response.isTruncated());

		response = client.listObjects(l -> l.bucket(bucketName).marker("baz").maxKeys(2));
		assertLinesMatch(List.of("foo"), getKeys(response.contents()));
		assertEquals(1, response.contents().size());
		assertFalse(response.isTruncated());
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	public void testBucketListDelimiterBasic() {
		var client = getClient();
		var bucketName = createObjects(client,
				List.of("foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf"));

		String delimiter = "/";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));

		assertEquals(delimiter, response.delimiter());
		assertLinesMatch(List.of("asdf"), getKeys(response.contents()));

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("foo/", "quux/"), prefixes);
	}

	@Test
	@Tag("Encoding")
	// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
	public void testBucketListEncodingBasic() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"));

		String delimiter = "/";

		var response = client.listObjects(
				l -> l.bucket(bucketName).delimiter(delimiter).encodingType("url"));
		assertEquals(delimiter, response.delimiter());
		assertLinesMatch(List.of("asdf+b"), getKeys(response.contents()));

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(3, prefixes.size());
		assertLinesMatch(List.of("foo+1/", "foo/", "quux ab/"), prefixes);
	}

	@Test
	@Tag("Filtering")
	// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	public void testBucketListDelimiterPrefix() {
		String delimiter = "/";
		String marker = "";
		String prefix = "";
		var bucketName = createObjects(List.of("asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"));

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("asdf"), new ArrayList<>(),
				"asdf");
		marker = validateListObject(bucketName, prefix, delimiter, marker, 1, true, new ArrayList<>(), List.of("boo/"),
				"boo/");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(), List.of("cquux/"), null);

		marker = validateListObject(bucketName, prefix, delimiter, "", 2, true, List.of("asdf"), List.of("boo/"),
				"boo/");
		validateListObject(bucketName, prefix, delimiter, marker, 2, false, new ArrayList<>(), List.of("cquux/"), null);

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
	// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
	public void testBucketListDelimiterPrefixEndsWithDelimiter() {
		var client = getClient();
		var bucketName = createEmptyObjects(client, List.of("asdf/"));
		validateListObject(bucketName, "asdf/", "/", "", 1000, false,
				List.of("asdf/"), new ArrayList<>(), null);
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
	public void testBucketListDelimiterAlt() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("bar", "baz", "cab", "foo"));

		String delimiter = "a";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("ba", "ca"), prefixes);
	}

	@Test
	@Tag("Filtering")
	// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	public void testBucketListDelimiterPrefixUnderscore() {
		String delimiter = "/";
		String marker = "";
		String prefix = "";
		var bucketName = createObjects(List.of("Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"));

		marker = validateListObject(bucketName, prefix, delimiter, "", 1, true, List.of("Obj1_"), new ArrayList<>(),
				"Obj1_");
		marker = validateListObject(bucketName, prefix, delimiter, marker, 1, true, new ArrayList<>(),
				List.of("Under1/"), "Under1/");
		validateListObject(bucketName, prefix, delimiter, marker, 1, false, new ArrayList<>(), List.of("Under2/"),
				null);

		marker = validateListObject(bucketName, prefix, delimiter, "", 2, true, List.of("Obj1_"), List.of("Under1/"),
				"Under1/");
		validateListObject(bucketName, prefix, delimiter, marker, 2, false, new ArrayList<>(),
				List.of("Under2/"), null);

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
	// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
	public void testBucketListDelimiterPercentage() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b%ar", "b%az", "c%ab", "foo"));

		String delimiter = "%";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b%", "c%"), prefixes);
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
	public void testBucketListDelimiterWhitespace() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b ar", "b az", "c ab", "foo"));

		String delimiter = " ";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b ", "c "), prefixes);
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
	public void testBucketListDelimiterDot() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b.ar", "b.az", "c.ab", "foo"));

		String delimiter = ".";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		assertLinesMatch(List.of("foo"), keys);

		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(2, prefixes.size());
		assertLinesMatch(List.of("b.", "c."), prefixes);
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
	public void testBucketListDelimiterUnreadable() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "\n";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertLinesMatch(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
	public void testBucketListDelimiterEmpty() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertNull(response.delimiter());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertLinesMatch(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
	public void testBucketListDelimiterNone() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(l -> l.bucket(bucketName));
		assertNull(response.delimiter());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertLinesMatch(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	public void testBucketListDelimiterNotExist() {
		var keyNames = List.of("bar", "baz", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "/";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertLinesMatch(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("delimiter")
	// 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
	public void testBucketListDelimiterNotSkipSpecial() {
		var keyNames = new ArrayList<String>();
		for (int i = 1000; i < 1999; i++)
			keyNames.add("0/" + Integer.toString(i));
		var keyNames2 = List.of("1999", "1999#", "1999+", "2000");
		keyNames.addAll(keyNames2);
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "/";

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter));
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());

		assertLinesMatch(keyNames2, keys);
		assertLinesMatch(List.of("0/"), prefixes);
	}

	@Test
	@Tag("prefix")
	// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
	public void testBucketListPrefixBasic() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("foo/bar", "foo/baz", "quux"));

		String prefix = "foo/";
		var response = client.listObjects(l -> l.bucket(bucketName).prefix(prefix));
		assertEquals(prefix, response.prefix());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("foo/bar", "foo/baz"), keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
	public void testBucketListPrefixAlt() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("bar", "baz", "foo"));

		String prefix = "ba";
		var response = client.listObjects(l -> l.bucket(bucketName).prefix(prefix));
		assertEquals(prefix, response.prefix());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("bar", "baz"), keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
	public void testBucketListPrefixEmpty() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "";
		var response = client.listObjects(l -> l.bucket(bucketName).prefix(prefix));
		assertEquals("", response.prefix());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
	public void testBucketListPrefixNone() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(l -> l.bucket(bucketName));
		assertEquals("", response.prefix());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(keyNames, keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	public void testBucketListPrefixNotExist() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "d";
		var response = client.listObjects(l -> l.bucket(bucketName).prefix(prefix));
		assertEquals(prefix, response.prefix());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("prefix")
	// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	public void testBucketListPrefixUnreadable() {
		var keyNames = List.of("foo/bar", "foo/baz", "quux");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "\n";
		var response = client.listObjects(l -> l.bucket(bucketName).prefix(prefix));
		assertEquals(prefix, response.prefix());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	public void testBucketListPrefixDelimiterBasic() {
		var keyNames = List.of("foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String prefix = "foo/";
		String delimiter = "/";
		var response = client.listObjects(
				l -> l.bucket(bucketName).delimiter(delimiter).prefix(prefix));
		assertEquals(prefix, response.prefix());
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("foo/bar"), keys);
		assertLinesMatch(List.of("foo/baz/"), prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	public void testBucketListPrefixDelimiterAlt() {
		var keyNames = List.of("bar", "bazar", "cab", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		String delimiter = "a";
		String prefix = "ba";

		var response = client.listObjects(
				l -> l.bucket(bucketName).delimiter(delimiter).prefix(prefix));
		assertEquals(prefix, response.prefix());
		assertEquals(delimiter, response.delimiter());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("bar"), keys);
		assertLinesMatch(List.of("baza"), prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	public void testBucketListPrefixDelimiterPrefixNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var response = client.listObjects(l -> l.bucket(bucketName).delimiter("d").prefix("/"));

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	public void testBucketListPrefixDelimiterDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/c", "b/a/g", "b/a/r", "g"));

		var response = client
				.listObjects(l -> l.bucket(bucketName).delimiter("z").prefix("b"));

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("b/a/c", "b/a/g", "b/a/r"), keys);
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지
	// 확인
	public void testBucketListPrefixDelimiterPrefixDelimiterNotExist() {
		var client = getClient();
		var bucketName = createObjects(client, List.of("b/a/r", "b/a/c", "b/a/g", "g"));

		var response = client
				.listObjects(l -> l.bucket(bucketName).delimiter("z").prefix("y"));

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertEquals(0, keys.size());
		assertEquals(0, prefixes.size());
	}

	@Test
	@Tag("maxKeys")
	// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
	public void testBucketListMaxKeysOne() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(l -> l.bucket(bucketName).maxKeys(1));
		assertTrue(response.isTruncated());

		var keys = getKeys(response.contents());
		assertLinesMatch(keyNames.subList(0, 1), keys);

		response = client.listObjects(l -> l.bucket(bucketName).marker(keyNames.get(0)));
		assertFalse(response.isTruncated());

		keys = getKeys(response.contents());
		assertLinesMatch(keyNames.subList(1, keyNames.size()), keys);
	}

	@Test
	@Tag("maxKeys")
	// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
	public void testBucketListMaxKeysZero() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(l -> l.bucket(bucketName).maxKeys(0));

		assertFalse(response.isTruncated());
		var keys = getKeys(response.contents());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("maxKeys")
	// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
	public void testBucketListMaxKeysNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(l -> l.bucket(bucketName));
		assertFalse(response.isTruncated());
		var keys = getKeys(response.contents());
		assertLinesMatch(keyNames, keys);
		assertEquals(1000, response.maxKeys());
	}

	@Test
	@Tag("marker")
	// 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
	public void testBucketListMarkerNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(l -> l.bucket(bucketName).marker(""));
		assertNull(response.nextMarker());
	}

	@Test
	@Tag("marker")
	// 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
	public void testBucketListMarkerEmpty() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var response = client.listObjects(l -> l.bucket(bucketName).marker(""));
		assertNull(response.nextMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.contents());
		assertLinesMatch(keyNames, keys);
	}

	@Test
	@Tag("marker")
	// 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	public void testBucketListMarkerUnreadable() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "\n";

		var response = client.listObjects(l -> l.bucket(bucketName).marker(marker));
		assertNull(response.nextMarker());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.contents());
		assertLinesMatch(keyNames, keys);
	}

	@Test
	@Tag("marker")
	// [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을
	// 불러올때 재대로 가져오는지 확인
	public void testBucketListMarkerNotInList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "blah";

		var response = client.listObjects(l -> l.bucket(bucketName).marker(marker));
		assertEquals(marker, response.marker());
		var keys = getKeys(response.contents());
		assertLinesMatch(List.of("foo", "quxx"), keys);
	}

	@Test
	@Tag("marker")
	// [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지
	// 확인
	public void testBucketListMarkerAfterList() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "zzz";

		var response = client.listObjects(l -> l.bucket(bucketName).marker(marker));
		assertEquals(marker, response.marker());
		assertFalse(response.isTruncated());
		var keys = getKeys(response.contents());
		assertEquals(0, keys.size());
	}

	@Test
	@Tag("Metadata")
	// ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	public void testBucketListReturnData() {
		var keyNames = List.of("bar", "baz", "foo");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var data = new ArrayList<ObjectDataV2>();

		for (var Key : keyNames) {
			var headResponse = client.headObject(h -> h.bucket(bucketName).key(Key));
			var aclResponse = client.getObjectAcl(g -> g.bucket(bucketName).key(Key));

			data.add(ObjectDataV2.builder().key(Key).displayName(aclResponse.owner().displayName())
					.id(aclResponse.owner().id()).eTag(headResponse.eTag())
					.lastModified(headResponse.lastModified()).contentLength(headResponse.contentLength()).build());
		}

		var response = client.listObjects(l -> l.bucket(bucketName));
		var objList = response.contents();

		for (var Object : objList) {
			var keyName = Object.key();
			var keyData = getObjectToKey(keyName, data);

			assertNotNull(keyData);
			assertEquals(keyData.eTag, Object.eTag());
			assertEquals(keyData.contentLength, Object.size());
			assertEquals(keyData.displayName, Object.owner().displayName());
			assertEquals(keyData.id, Object.owner().id());
			assertEquals(keyData.lastModified, Object.lastModified());
		}
	}

	@Test
	@Tag("ACL")
	// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
	public void testBucketListObjectsAnonymous() {
		var client = getClient();
		var bucketName = createBucketCannedACL(client, BucketCannedACL.PUBLIC_READ);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.listObjects(l -> l.bucket(bucketName));
	}

	@Test
	@Tag("ACL")
	// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
	public void testBucketListObjectsAnonymousFail() {
		var bucketName = createBucket();
		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AwsServiceException.class,
				() -> unauthenticatedClient.listObjects(l -> l.bucket(bucketName)));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();

		assertEquals(403, statusCode);
		assertEquals(MainData.ACCESS_DENIED, errorCode);
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
	public void testBucketNotExist() {
		var bucketName = getNewBucketNameOnly();
		var client = getClient();

		var e = assertThrows(AwsServiceException.class, () -> client.listObjects(l -> l.bucket(bucketName)));

		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();

		assertEquals(404, statusCode);
		assertEquals(MainData.NO_SUCH_BUCKET, errorCode);
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Filtering")
	// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	public void testBucketListFilteringAll() {
		var keyNames = List.of("test1/f1", "test2/f2", "test3", "test4/f3", "testF4");
		var client = getClient();
		var bucketName = createObjects(client, keyNames);

		var marker = "test3";
		var delimiter = "/";
		var maxKeys = 3;

		var response = client.listObjects(
				l -> l.bucket(bucketName).delimiter(delimiter).maxKeys(maxKeys));
		assertEquals(delimiter, response.delimiter());
		assertEquals(maxKeys, response.maxKeys());
		assertEquals(marker, response.nextMarker());
		assertEquals(true, response.isTruncated());

		var keys = getKeys(response.contents());
		var prefixes = getPrefixList(response.commonPrefixes());
		assertLinesMatch(List.of("test3"), keys);
		assertLinesMatch(List.of("test1/", "test2/"), prefixes);

		response = client.listObjects(l -> l.bucket(bucketName).delimiter(delimiter)
				.maxKeys(maxKeys).marker(marker));
		assertEquals(delimiter, response.delimiter());
		assertEquals(maxKeys, response.maxKeys());
		assertEquals(false, response.isTruncated());
	}
}
