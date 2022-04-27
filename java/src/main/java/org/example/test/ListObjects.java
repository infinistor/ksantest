/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
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
import java.util.Arrays;

import org.example.s3tests.MainData;
import org.example.s3tests.ObjectData;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;

public class ListObjects extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("ListObjects Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("ListObjects End");
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	//버킷의 오브젝트 목록을 올바르게 가져오는지 확인
	public void test_bucket_list_many() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "foo", "bar", "baz" })));
		var Client = GetClient();

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMaxKeys(2));
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz" })), GetKeys(Response.getObjectSummaries()));
		assertEquals(2, Response.getObjectSummaries().size());
		assertTrue(Response.isTruncated());

		Response = Client
				.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMarker("baz").withMaxKeys(2));
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo" })), GetKeys(Response.getObjectSummaries()));
		assertEquals(1, Response.getObjectSummaries().size());
		assertFalse(Response.isTruncated());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	public void test_bucket_list_delimiter_basic() {
		var BucketName = CreateObjects(
				new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf" })));
		var Client = GetClient();

		String Delimiter = "/";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));

		assertEquals(Delimiter, Response.getDelimiter());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "asdf" })), GetKeys(Response.getObjectSummaries()));

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo/", "quux/" })), Prefixes);
	}

	@Test
	@Tag("Encoding")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인
	public void test_bucket_list_encoding_basic() {
		var BucketName = CreateObjects(new ArrayList<>(
				Arrays.asList(new String[] { "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b" })));
		var Client = GetClient();

		String Delimiter = "/";

		var Response = Client.listObjects(
				new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter).withEncodingType("url"));
		assertEquals(Delimiter, Response.getDelimiter());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "asdf%2Bb" })), GetKeys(Response.getObjectSummaries()));

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(3, Prefixes.size());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo%2B1/", "foo/", "quux+ab/" })), Prefixes);
	}

	@Test
	@Tag("DelimiterandPrefix")
	@Tag("KSAN")
	//조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	public void test_bucket_list_delimiter_prefix() {
		var BucketName = CreateObjects(new ArrayList<>(
				Arrays.asList(new String[] { "asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla" })));

		String Delimiter = "/";
		String Marker = "";
		String Prefix = "";

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new ArrayList<>(Arrays.asList(new String[] { "asdf" })), new ArrayList<String>(), "asdf");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, true, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "boo/" })), "boo/");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "cquux/" })), null);

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, true, new ArrayList<>(Arrays.asList(new String[] { "asdf" })), new ArrayList<>(Arrays.asList(new String[] { "boo/" })), "boo/");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 2, false, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "cquux/" })), null);

		Prefix = "boo/";

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new ArrayList<>(Arrays.asList(new String[] { "boo/bar" })), new ArrayList<String>(), "boo/bar");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "boo/baz/" })), null);

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, false, new ArrayList<>(Arrays.asList(new String[] { "boo/bar" })), new ArrayList<>(Arrays.asList(new String[] { "boo/baz/" })), null);
	}

	@Test
	@Tag("DelimiterandPrefix")
	@Tag("KSAN")
	//비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인
	public void test_bucket_list_delimiter_prefix_ends_with_delimiter() {
		var BucketName = CreateObjectsToBody(new ArrayList<>(Arrays.asList(new String[] { "asdf/" })), "");
		ValidateListObjcet(BucketName, "asdf/", "/", "", 1000, false,
				new ArrayList<>(Arrays.asList(new String[] { "asdf/" })), new ArrayList<String>(), null);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인
	public void test_bucket_list_delimiter_alt() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" })));
		var Client = GetClient();

		String Delimiter = "a";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Profixes = Response.getCommonPrefixes();
		assertEquals(2, Profixes.size());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "ba", "ca" })), Profixes);
	}

	@Test
	@Tag("DelimiterandPrefix")
	@Tag("KSAN")
	//[폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인
	public void test_bucket_list_delimiter_prefix_underscore() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays
				.asList(new String[] { "_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla" })));

		String Delimiter = "/";
		String Marker = "";
		String Prefix = "";

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new ArrayList<>(Arrays.asList(new String[] { "_obj1_" })), new ArrayList<String>(), "_obj1_");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, true, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under1/" })), "_under1/");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under2/" })), null);

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, true, new ArrayList<>(Arrays.asList(new String[] { "_obj1_" })), new ArrayList<>(Arrays.asList(new String[] { "_under1/" })), "_under1/");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 2, false, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under2/" })), null);

		Prefix = "_under1/";

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 1, true, new ArrayList<>(Arrays.asList(new String[] { "_under1/bar" })), new ArrayList<String>(), "_under1/bar");
		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, Marker, 1, false, new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under1/baz/" })), null);

		Marker = ValidateListObjcet(BucketName, Prefix, Delimiter, "", 2, false, new ArrayList<>(Arrays.asList(new String[] { "_under1/bar" })), new ArrayList<>(Arrays.asList(new String[] { "_under1/baz/" })), null);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인
	public void test_bucket_list_delimiter_percentage() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b%ar", "b%az", "c%ab", "foo" })));
		var Client = GetClient();

		String Delimiter = "%";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "b%", "c%" })), Prefixes);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인
	public void test_bucket_list_delimiter_whitespace() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b ar", "b az", "c ab", "foo" })));
		var Client = GetClient();

		String Delimiter = " ";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "b ", "c " })), Prefixes);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인
	public void test_bucket_list_delimiter_dot() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b.ar", "b.az", "c.ab", "foo" })));
		var Client = GetClient();

		String Delimiter = ".";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "b.", "c." })), Prefixes);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인
	public void test_bucket_list_delimiter_unreadable() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "\n";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인
	public void test_bucket_list_delimiter_empty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));
		assertNull(Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인
	public void test_bucket_list_delimiter_none() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjects(BucketName);
		assertNull(Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//[폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인
	public void test_bucket_list_delimiter_not_exist() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "/";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 특수문자가 생략되는지 확인
	public void test_bucket_list_delimiter_not_skip_special() {
		var KeyNames = new ArrayList<String>();
		for (int i = 1000; i < 1999; i++)
			KeyNames.add("0/" + Integer.toString(i));
		var KeyNames2 = new ArrayList<>(Arrays.asList(new String[] { "1999", "1999#", "1999+", "2000" }));
		KeyNames.addAll(KeyNames2);
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "/";

		var Response = Client.listObjects(
				new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter).withMaxKeys(2000));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames2, Keys);
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "0/" })), Prefixes);
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//[접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인
	public void test_bucket_list_prefix_basic() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" })));
		var Client = GetClient();

		String Prefix = "foo/";
		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz" })), Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인
	public void test_bucket_list_prefix_alt() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo" })));
		var Client = GetClient();

		String Prefix = "ba";
		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz" })), Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인
	public void test_bucket_list_prefix_empty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "";
		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withPrefix(Prefix));
		assertNull(Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인
	public void test_bucket_list_prefix_none() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjects(BucketName);
		assertNull(Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//[접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	public void test_bucket_list_prefix_not_exist() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "d";
		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인
	public void test_bucket_list_prefix_unreadable() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "\n";
		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	public void test_bucket_list_prefix_delimiter_basic() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "foo/";
		String Delimiter = "/";
		var Response = Client.listObjects(
				new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo/bar" })), Keys);
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo/baz/" })), Prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	public void test_bucket_list_prefix_delimiter_alt() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "bazar", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "a";
		String Prefix = "ba";

		var Response = Client.listObjects(
				new ListObjectsRequest().withBucketName(BucketName).withDelimiter(Delimiter).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "bar" })), Keys);
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "baza" })), Prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	public void test_bucket_list_prefix_delimiter_prefix_not_exist() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b/a/r", "b/a/c", "b/a/g", "g" })));
		var Client = GetClient();

		var Response = Client
				.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter("d").withPrefix("/"));

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인
	public void test_bucket_list_prefix_delimiter_delimiter_not_exist() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b/a/c", "b/a/g", "b/a/r", "g" })));
		var Client = GetClient();

		var Response = Client
				.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter("z").withPrefix("b"));

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "b/a/c", "b/a/g", "b/a/r" })), Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인
	public void test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b/a/r", "b/a/c", "b/a/g", "g" })));
		var Client = GetClient();

		var Response = Client
				.listObjects(new ListObjectsRequest().withBucketName(BucketName).withDelimiter("z").withPrefix("y"));

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("MaxKeys")
	@Tag("KSAN")
	//오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인
	public void test_bucket_list_maxkeys_one() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMaxKeys(1));
		assertTrue(Response.isTruncated());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames.subList(0, 1), Keys);

		Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMarker(KeyNames.get(0)));
		assertFalse(Response.isTruncated());

		Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames.subList(1, KeyNames.size()), Keys);
	}

	@Test
	@Tag("MaxKeys")
	@Tag("KSAN")
	//오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인
	public void test_bucket_list_maxkeys_zero() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMaxKeys(0));

		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(0, Keys.size());
	}

	@Test
	@Tag("MaxKeys")
	@Tag("KSAN")
	//[default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인
	public void test_bucket_list_maxkeys_none() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjects(BucketName);
		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames, Keys);
		assertEquals(1000, Response.getMaxKeys());
	}

	@Test
	@Disabled("JAVA에서는 BeforeRequestEvent 사용이 불가능하여 테스트 하지 못함")
	@Tag("MaxKeys")
	@Tag("KSAN")
	//[함수가 호출되기 전에 URL에 유효하지 않은 최대목록갯수를 추가할 경우] 오브젝트 목록 조회 실패 확인
	public void test_bucket_list_maxkeys_invalid() {
//	            var KeyNames = new ArrayList<>(Arrays.asList(new String[]{ "bar", "baz", "foo", "quxx" }));
//	            var BucketName = CreateObjects(KeyNames);
//	            var Client = GetClient();
//
//	            Client.BeforeRequestEvent += BeforeCallS3ListObjectsMaxKeys;
//
//	            var e = Assert.Throws<AggregateException>(() => Client.listObjects(BucketName));
//
//	            var StatusCode = e.getStatusCode();
//	            var ErrorCode = GetErrorCode(e);
//
//	            assertEquals(HttpStatusCode.BadRequest, StatusCode);
//	            assertEquals(MainData.InvalidArgument, ErrorCode);
	}

//	private void BeforeCallS3ListObjectsMaxKeys(object sender, RequestEventArgs e)
//	        {
//	            var requestEvent = e as WebServiceRequestEventArgs;
//	            requestEvent.ParameterCollection.Add("max-keys", "blah");
//	            //requestEvent.Headers.Add("max-keys", "blah");
//	        }

	@Test
	@Tag("Marker")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 모든 목록을 가져왓을 경우 마커가 비어있는지 확인
	public void test_bucket_list_marker_none() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMarker(""));
		assertNull(Response.getNextMarker());
	}

	@Test
	@Tag("Marker")
	@Tag("KSAN")
	//빈 마커를 입력하고 오브젝트 목록을 불러올때 올바르게 가져오는지 확인
	public void test_bucket_list_marker_empty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMarker(""));
		assertNull(Response.getNextMarker());
		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Marker")
	@Tag("KSAN")
	//마커에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	public void test_bucket_list_marker_unreadable() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Marker = "\n";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMarker(Marker));
		assertNull(Response.getNextMarker());
		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("Marker")
	@Tag("KSAN")
	//[마커와 일치하는 오브젝트가 존재하지 않지만 해당 마커보다 정렬순서가 낮은 오브젝트는 존재하는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	public void test_bucket_list_marker_not_in_list() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Marker = "blah";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMarker(Marker));
		assertEquals(Marker, Response.getMarker());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertLinesMatch(new ArrayList<>(Arrays.asList(new String[] { "foo", "quxx" })), Keys);
	}

	@Test
	@Tag("Marker")
	@Tag("KSAN")
	//[마커와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] 마커를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	public void test_bucket_list_marker_after_list() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Marker = "zzz";

		var Response = Client.listObjects(new ListObjectsRequest().withBucketName(BucketName).withMarker(Marker));
		assertEquals(Marker, Response.getMarker());
		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(0, Keys.size());
	}

	@Test
	@Tag("Metadata")
	@Tag("KSAN")
	//ListObjcets으로 가져온 Metadata와  HeadObject, GetObjectAcl로 가져온 Metadata 일치 확인
	public void test_bucket_list_return_data() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Data = new ArrayList<ObjectData>();

		for (var Key : KeyNames) {
			var ObjResponse = Client.getObjectMetadata(BucketName, Key);
			var ACLResponse = Client.getObjectAcl(BucketName, Key);

			Data.add(new ObjectData().withKey(Key).withDisplayName(ACLResponse.getOwner().getDisplayName())
					.withID(ACLResponse.getOwner().getId()).withETag(ObjResponse.getETag())
					.withLastModified(ObjResponse.getLastModified()).withContentLength(ObjResponse.getContentLength()));
		}

		var Response = Client.listObjects(BucketName);
		var ObjList = Response.getObjectSummaries();

		for (var Object : ObjList) {
			var KeyName = Object.getKey();
			var KeyData = GetObjectToKey(KeyName, Data);

			assertNotNull(KeyData);
			assertEquals(KeyData.ETag, Object.getETag());
			assertEquals(KeyData.ContentLength, Object.getSize());
			assertEquals(KeyData.DisplayName, Object.getOwner().getDisplayName());
			assertEquals(KeyData.ID, Object.getOwner().getId());
			assertEquals(KeyData.LastModified, Object.getLastModified());
		}
	}

	@Test
	@Tag("ACL")
	@Tag("KSAN")
	//권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인
	public void test_bucket_list_objects_anonymous() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		Client.setBucketAcl(new SetBucketAclRequest(BucketName, CannedAccessControlList.PublicRead));

		var UnauthenticatedClient = GetUnauthenticatedClient();
		UnauthenticatedClient.listObjects(BucketName);
	}

	@Test
	@Tag("ACL")
	@Tag("KSAN")
	//권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인
	public void test_bucket_list_objects_anonymous_fail() {
		var BucketName = GetNewBucket();
		var UnauthenticatedClient = GetUnauthenticatedClient();

		var e = assertThrows(AmazonServiceException.class, () -> UnauthenticatedClient.listObjects(BucketName));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(403, StatusCode);
		assertEquals(MainData.AccessDenied, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인
	public void test_bucket_notexist() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();

		var e = assertThrows(AmazonServiceException.class, () -> Client.listObjects(BucketName));

		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchBucket, ErrorCode);
		DeleteBucketList(BucketName);
	}
}
