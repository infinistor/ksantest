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
	// 버킷의 오브젝트 목록을 올바르게 가져오는지 확인
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
	// 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
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
	// 오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
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
	// 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
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
	// 비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
	public void testBucketListVersionsDelimiterPrefixEndsWithDelimiter() {
		var bucketName = createObjectsToBody(List.of("asdf/"), "");

		validateListObject(bucketName, "asdf/", "/", "", 1000, false,
				List.of("asdf/"), new ArrayList<>(), null);

	}

	@Test
	@Tag("Delimiter")
	// 오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
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
	// [폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
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
	// 오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
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
	// 오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
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
	// 오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
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
	// 오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
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
	// 오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
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
	// 오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
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
	// [폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
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
	// 오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
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
	// [접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
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
	// 접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
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
	// 접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
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
	// 접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
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
	// [접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
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
	// 읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
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
	// 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
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
	// [구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
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
	// [입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
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
	// [구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
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
	// [구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지
	// 확인
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
	// 오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
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
	// 오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
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
	// [default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
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
	// 오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
	public void testBucketListVersionsMarkerNone() {
		var keyNames = List.of("bar", "baz", "foo", "quxx");

		var bucketName = createObjects(keyNames);
		var client = getClient();

		var response = client.listVersions(new ListVersionsRequest().withBucketName(bucketName).withKeyMarker(""));

		assertNull(response.getNextKeyMarker());
	}

	@Test
	@Tag("marker")
	// 빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
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
	// 마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
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
	// [마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을
	// 불러올때 재대로 가져오는지 확인
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
	// [마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지
	// 확인
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
	// ListObjects으로 가져온 Metadata와 HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	public void testBucketListVersionsReturnData() {
		var bucketName = getNewBucket();
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var keys = List.of("bar", "baz", "foo");
		bucketName =

				createObjects(keys, bucketName);

		var client = getClient();
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
	// 권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
	public void testBucketListVersionsObjectsAnonymous() {
		var bucketName = getNewBucket();
		var client = getClient();
		client.setBucketAcl(new SetBucketAclRequest(bucketName, CannedAccessControlList.PublicRead));

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.listVersions(new ListVersionsRequest().withBucketName(bucketName));
	}

	@Test
	@Tag("ACL")
	// 권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
	public void testBucketListVersionsObjectsAnonymousFail() {
		var bucketName = getNewBucket();
		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AmazonServiceException.class,
				() -> unauthenticatedClient.listVersions(new ListVersionsRequest().withBucketName(bucketName)));

		assertEquals(403, e.getStatusCode());
		assertEquals(MainData.AccessDenied, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
	public void testBucketListVersionsNotExist() {
		var bucketName = getNewBucketNameOnly();
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class,
				() -> client.listVersions(new ListVersionsRequest().withBucketName(bucketName)));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchBucket, e.getErrorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Filtering")
	// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
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
