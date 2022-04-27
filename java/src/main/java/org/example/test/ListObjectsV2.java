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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.s3tests.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Request;

public class ListObjectsV2 extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("ListObjectsV2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("ListObjectsV2 End");
	}

	@Test
	@Tag("Check")
	@Tag("KSAN")
	//버킷의 오브젝트 목록을 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_many() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "foo", "bar", "baz" })));
		var Client = GetClient();

		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withMaxKeys(2));
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz" })),
				GetKeys(Response.getObjectSummaries()));
		assertEquals(2, Response.getObjectSummaries().size());
		assertTrue(Response.isTruncated());

		Response = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withStartAfter("baz").withMaxKeys(2));
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo" })), GetKeys(Response.getObjectSummaries()));
		assertEquals(1, Response.getObjectSummaries().size());
		assertFalse(Response.isTruncated());
	}

	@Test
	@Tag("KeyCount")
	@Tag("KSAN")
	//ListObjectsV2로 오브젝트 목록을 가져올때 Key Count 값을 올바르게 가져오는지 확인
	public void test_basic_key_count() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		for (int i = 0; i < 5; i++)
			Client.putObject(BucketName, Integer.toString(i), "");

		var Response = Client.listObjectsV2(BucketName);

		assertEquals(5, Response.getKeyCount());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_basic() {
		var BucketName = CreateObjects(
				new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf" })));
		var Client = GetClient();

		String MyDelimiter = "/";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(MyDelimiter));
		assertEquals(MyDelimiter, Response.getDelimiter());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "asdf" })), GetKeys(Response.getObjectSummaries()));

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo/", "quux/" })), Prefixes);
	}

	@Test
	@Tag("Encoding")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 인코딩이 올바르게 동작하는지 확인(ListObjectsV2)
	public void test_bucket_listv2_encoding_basic() {
		var BucketName = CreateObjects(new ArrayList<>(
				Arrays.asList(new String[] { "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b" })));
		var Client = GetClient();

		String Delimiter = "/";

		var Response = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter).withEncodingType("URL"));
		assertEquals(Delimiter, Response.getDelimiter());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "asdf%2Bb" })),
				GetKeys(Response.getObjectSummaries()));

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(3, Prefixes.size());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo%2B1/", "foo/", "quux+ab/" })), Prefixes);
	}

	@Test
	@Tag("DelimiterandPrefix")
	@Tag("KSAN")
	//조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_prefix() {
		var BucketName = CreateObjects(new ArrayList<>(
				Arrays.asList(new String[] { "asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla" })));

		String Delimiter = "/";
		String ContinuationToken = "";
		String prefix = "";

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 1, true,
				new ArrayList<>(Arrays.asList(new String[] { "asdf" })), new ArrayList<String>(), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 1, true,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "boo/" })), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 1, false,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "cquux/" })), true);

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 2, true,
				new ArrayList<>(Arrays.asList(new String[] { "asdf" })),
				new ArrayList<>(Arrays.asList(new String[] { "boo/" })), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 2, false,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "cquux/" })), true);

		prefix = "boo/";

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 1, true,
				new ArrayList<>(Arrays.asList(new String[] { "boo/bar" })), new ArrayList<String>(), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, ContinuationToken, 1, false,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "boo/baz/" })), true);

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, Delimiter, null, 2, false,
				new ArrayList<>(Arrays.asList(new String[] { "boo/bar" })),
				new ArrayList<>(Arrays.asList(new String[] { "boo/baz/" })), true);
	}

	@Test
	@Tag("DelimiterandPrefix")
	@Tag("KSAN")
	//비어있는 폴더의 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_prefix_ends_with_delimiter() {
		var BucketName = CreateObjectsToBody(new ArrayList<>(Arrays.asList(new String[] { "asdf/" })), "");
		ValidateListObjcetV2(BucketName, "asdf/", "/", null, 1000, false,
				new ArrayList<>(Arrays.asList(new String[] { "asdf/" })), new ArrayList<String>(), true);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 문자 구분자[a]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_alt() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" })));
		var Client = GetClient();

		String Delimiter = "a";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Profixes = Response.getCommonPrefixes();
		assertEquals(2, Profixes.size());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "ba", "ca" })), Profixes);
	}

	@Test
	@Tag("DelimiterandPrefix")
	@Tag("KSAN")
	//[폴더명 앞에 _가 포함되어 있는 환경] 조건에 맞는 오브젝트 목록을 가져올 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_prefix_underscore() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays
				.asList(new String[] { "_obj1_", "_under1/bar", "_under1/baz/xyzzy", "_under2/thud", "_under2/bla" })));

		String delim = "/";
		String ContinuationToken = "";
		String prefix = "";

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 1, true,
				new ArrayList<>(Arrays.asList(new String[] { "_obj1_" })), new ArrayList<String>(), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 1, true,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under1/" })), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 1, false,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under2/" })), true);

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 2, true,
				new ArrayList<>(Arrays.asList(new String[] { "_obj1_" })),
				new ArrayList<>(Arrays.asList(new String[] { "_under1/" })), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 2, false,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under2/" })), true);

		prefix = "_under1/";

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 1, true,
				new ArrayList<>(Arrays.asList(new String[] { "_under1/bar" })), new ArrayList<String>(), false);
		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, ContinuationToken, 1, false,
				new ArrayList<String>(), new ArrayList<>(Arrays.asList(new String[] { "_under1/baz/" })), true);

		ContinuationToken = ValidateListObjcetV2(BucketName, prefix, delim, null, 2, false,
				new ArrayList<>(Arrays.asList(new String[] { "_under1/bar" })),
				new ArrayList<>(Arrays.asList(new String[] { "_under1/baz/" })), true);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 특수문자 구분자[%]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_percentage() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b%ar", "b%az", "c%ab", "foo" })));
		var Client = GetClient();

		String Delimiter = "%";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "b%", "c%" })), Prefixes);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 공백문자 구분자[ ]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_whitespace() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b ar", "b az", "c ab", "foo" })));
		var Client = GetClient();

		String Delimiter = " ";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "b ", "c " })), Prefixes);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 구분자[.]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_dot() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b.ar", "b.az", "c.ab", "foo" })));
		var Client = GetClient();

		String Delimiter = ".";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo" })), Keys);

		var Prefixes = Response.getCommonPrefixes();
		assertEquals(2, Prefixes.size());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "b.", "c." })), Prefixes);
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 읽을수 없는 구분자[\n]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_unreadable() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "\n";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 구분자가 빈문자일때 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_empty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter));
		assertNull(Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 구분자를 입력하지 않아도 문제없는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_none() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjectsV2(BucketName);
		assertNull(Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Fetchowner")
	@Tag("KSAN")
	//[권한정보를 가져오도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_fetchowner_notempty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withFetchOwner(true));
		var ObjectList = Response.getObjectSummaries();

		assertNotNull(ObjectList.get(0).getOwner());
	}

	@Test
	@Tag("Fetchowner")
	@Tag("KSAN")
	//@Tag(	"[default = 권한정보를 가져오지 않음] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_fetchowner_defaultempty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjectsV2(BucketName);
		var ObjectList = Response.getObjectSummaries();

		assertNull(ObjectList.get(0).getOwner());
	}

	@Test
	@Tag("Fetchowner")
	@Tag("KSAN")
	//[권한정보를 가져오지 않도록 설정] 오브젝트 목록을 가져올때 권한정보를를 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_fetchowner_empty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withFetchOwner(false));
		var ObjectList = Response.getObjectSummaries();

		assertNull(ObjectList.get(0).getOwner());
	}

	@Test
	@Tag("Delimiter")
	@Tag("KSAN")
	//[폴더가 존재하지 않는 환경] 오브젝트 목록을 가져올때 폴더 구분자[/]로 필터링 되는지 확인(ListObjectsV2)
	public void test_bucket_listv2_delimiter_not_exist() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "/";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter));
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();

		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//[접두어에 '/'가 포함] 오브젝트 목록을 가져올때 선택한 폴더 목록만 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_basic() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" })));
		var Client = GetClient();

		String Prefix = "foo/";
		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz" })), Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//접두어가 [/]가 아닌 경우 구분기호와 접두사 논리를 수행할 수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_alt() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo" })));
		var Client = GetClient();

		String Prefix = "ba";
		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "bar", "baz" })), Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//접두어를 빈문자로 입력할 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_empty() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "";
		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withPrefix(Prefix));
		assertNull(Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//접두어를 입력하지 않을 경우 모든 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_none() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjectsV2(BucketName);
		assertNull(Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(KeyNames, Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//[접두어와 일치하는 오브젝트가 없는 경우] 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_not_exist() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "d";
		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("Prefix")
	@Tag("KSAN")
	//읽을수 없는 접두어를 입력할 경우 빈 오브젝트 목록을 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_unreadable() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz", "quux" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "\n";
		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_basic() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Prefix = "foo/";
		String Delimiter = "/";
		var Response = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo/bar" })), Keys);
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo/baz/" })), Prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[구분자가 '/' 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_alt() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "bazar", "cab", "foo" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		String Delimiter = "a";
		String Prefix = "ba";

		var Response = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withDelimiter(Delimiter).withPrefix(Prefix));
		assertEquals(Prefix, Response.getPrefix());
		assertEquals(Delimiter, Response.getDelimiter());

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "bar" })), Keys);
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "baza" })), Prefixes);
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[입력한 접두어와 일치하는 오브젝트가 없을 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_prefix_not_exist() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b/a/r", "b/a/c", "b/a/g", "g" })));
		var Client = GetClient();

		var Response = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withDelimiter("d").withPrefix("/"));

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[구분자가 '/'가 아닐 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록을 올바르게 받아오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_delimiter_not_exist() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b/a/c", "b/a/g", "b/a/r", "g" })));
		var Client = GetClient();

		var Response = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withDelimiter("z").withPrefix("b"));

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "b/a/c", "b/a/g", "b/a/r" })), Keys);
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("PrefixAndDelimiter")
	@Tag("KSAN")
	//[구분자가 '/'가 아니며, 접두어와 일치하는 오브젝트가 존재하지 않는 경우] 접두어와 구분자를 입력할 경우 오브젝트 목록이 비어있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "b/a/r", "b/a/c", "b/a/g", "g" })));
		var Client = GetClient();

		var Response = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withDelimiter("z").withPrefix("y"));

		var Keys = GetKeys(Response.getObjectSummaries());
		var Prefixes = Response.getCommonPrefixes();
		assertEquals(0, Keys.size());
		assertEquals(0, Prefixes.size());
	}

	@Test
	@Tag("MaxKeys")
	@Tag("KSAN")
	//오브젝트 목록의 최대갯수를 1로 지정하고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_maxkeys_one() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withMaxKeys(1));
		assertTrue(Response.isTruncated());

		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames.subList(0, 1), Keys);

		Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withStartAfter(KeyNames.get(0)));
		assertFalse(Response.isTruncated());

		Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames.subList(1, KeyNames.size()), Keys);
	}

	@Test
	@Tag("MaxKeys")
	@Tag("KSAN")
	//오브젝트 목록의 최대갯수를 0으로 지정하고 불러올때 목록이 비어있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_maxkeys_zero() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withMaxKeys(0));

		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(0, Keys.size());
	}

	@Test
	@Tag("MaxKeys")
	@Tag("KSAN")
	//[default = 1000] 오브젝트 목록의 최대갯수를 지정하지않고 불러올때 올바르게 가져오는지 확인(ListObjectsV2)
	public void test_bucket_listv2_maxkeys_none() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response = Client.listObjectsV2(BucketName);
		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames, Keys);
		assertEquals(1000, Response.getMaxKeys());
	}

	@Test
	@Tag("Continuationtoken")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 다음 토큰값을 올바르게 가져오는지 확인
	public void test_bucket_listv2_continuationtoken() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var Response1 = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withMaxKeys(1));
		var NextContinuationToken = Response1.getNextContinuationToken();

		var Response2 = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withContinuationToken(NextContinuationToken));
		assertEquals(NextContinuationToken, Response2.getContinuationToken());
		assertFalse(Response2.isTruncated());
		var KeyNames2 = new ArrayList<>(Arrays.asList(new String[] { "baz", "foo", "quxx" }));
		var Keys = GetKeys(Response2.getObjectSummaries());
		assertEquals(KeyNames2, Keys);
	}

	@Test
	@Tag("ContinuationtokenAndStartAfter")
	@Tag("KSAN")
	//오브젝트 목록을 가져올때 Startafter와 토큰이 재대로 동작하는지 확인
	public void test_bucket_listv2_both_continuationtoken_startafter() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var StartAfter = "bar";

		var Response1 = Client.listObjectsV2(
				new ListObjectsV2Request().withBucketName(BucketName).withStartAfter(StartAfter).withMaxKeys(1));
		var NextContinuationToken = Response1.getNextContinuationToken();

		var Response2 = Client.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName)
				.withStartAfter(StartAfter).withContinuationToken(NextContinuationToken));
		assertEquals(NextContinuationToken, Response2.getContinuationToken());
		//assertEquals(StartAfter, Response2.getStartAfter());
		assertFalse(Response2.isTruncated());
		var KeyNames2 = new ArrayList<>(Arrays.asList(new String[] { "foo", "quxx" }));
		var Keys = GetKeys(Response2.getObjectSummaries());
		assertEquals(KeyNames2, Keys);
	}

	@Test
	@Tag("StartAfter")
	@Tag("KSAN")
	//startafter에 읽을수 없는 값[\n]을 설정한 경우 오브젝트 목록을 올바르게 가져오는지 확인
	public void test_bucket_listv2_startafter_unreadable() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var StartAfter = "\n";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withStartAfter(StartAfter));
		assertTrue(Response.getStartAfter().isBlank());
		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(KeyNames, Keys);
	}

	@Test
	@Tag("StartAfter")
	@Tag("KSAN")
	//[startafter와 일치하는 오브젝트가 존재하지 않는 환경 해당 startafter보다 정렬순서가 낮은 오브젝트는 존재하는 환경] startafter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	public void test_bucket_listv2_startafter_not_in_list() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var StartAfter = "blah";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withStartAfter(StartAfter));
		assertEquals(StartAfter, Response.getStartAfter());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(new ArrayList<>(Arrays.asList(new String[] { "foo", "quxx" })), Keys);
	}

	@Test
	@Tag("StartAfter")
	@Tag("KSAN")
	//[startafter와 일치하는 오브젝트도 정렬순서가 같은 오브젝트도 존재하지 않는 환경] startafter를 설정하고 오브젝트 목록을 불러올때 재대로 가져오는지 확인
	public void test_bucket_listv2_startafter_after_list() {
		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "bar", "baz", "foo", "quxx" }));
		var BucketName = CreateObjects(KeyNames);
		var Client = GetClient();

		var StartAfter = "zzz";

		var Response = Client
				.listObjectsV2(new ListObjectsV2Request().withBucketName(BucketName).withStartAfter(StartAfter));
		assertEquals(StartAfter, Response.getStartAfter());
		assertFalse(Response.isTruncated());
		var Keys = GetKeys(Response.getObjectSummaries());
		assertEquals(0, Keys.size());
	}

	@Test
	@Tag("ACL")
	@Tag("KSAN")
	//권한없는 사용자가 공용읽기설정된 버킷의 오브젝트 목록을 읽을수 있는지 확인(ListObjectsV2)
	public void test_bucket_listv2_objects_anonymous() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		Client.setBucketAcl(BucketName, CannedAccessControlList.PublicRead);

		var UnauthenticatedClient = GetUnauthenticatedClient();
		UnauthenticatedClient.listObjectsV2(BucketName);
	}

	@Test
	@Tag("ACL")
	@Tag("KSAN")
	//권한없는 사용자가 버킷의 오브젝트 목록을 읽지 못하는지 확인(ListObjectsV2)
	public void test_bucket_listv2_objects_anonymous_fail() {
		var BucketName = GetNewBucket();

		var UnauthenticatedClient = GetUnauthenticatedClient();

		var e = assertThrows(AmazonServiceException.class, () -> UnauthenticatedClient.listObjectsV2(BucketName));

		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(403, StatusCode);
		assertEquals(MainData.AccessDenied, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//존재하지 않는 버킷 내 오브젝트들을 가져오려 했을 경우 실패 확인(ListObjectsV2)
	public void test_bucketv2_notexist() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();

		var e = assertThrows(AmazonServiceException.class, () -> Client.listObjectsV2(BucketName));

		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchBucket, ErrorCode);
		DeleteBucketList(BucketName);
	}
}
