package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.example.s3tests.MainData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import com.amazonaws.AmazonServiceException;

public class PutBucket extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("PutBucket Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("PutBucket End");
	}

	@Test
	@DisplayName("test_bucket_list_empty")
	@Tag("PUT")
	@Tag("KSAN")
	//@Tag("생성한 버킷이 비어있는지 확인")
	public void test_bucket_list_empty() {
		var Bucket = GetNewBucketResource();
		var IsEmpty = BucketIsEmpty(Bucket);

		assertTrue(IsEmpty);
	}

	@Test
	@DisplayName("test_bucket_create_naming_bad_starts_nonalpha")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_starts_nonalpha() {
		var BucketName = GetNewBucketName();
		CheckBadBucketName("_" + BucketName);
		DeleteBucketList(BucketName);
	}

	@Test
	@DisplayName("test_bucket_create_naming_bad_short_one")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_short_one() {
		CheckBadBucketName("a");
	}

	@Test
	@DisplayName("test_bucket_create_naming_bad_short_two")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_short_two() {
		CheckBadBucketName("aa");
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_long_60")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 60자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_60() {
		TestBucketCreateNamingGoodLong(60);
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_long_61")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 61자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_61() {
		TestBucketCreateNamingGoodLong(61);
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_long_62")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 62자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_62() {
		TestBucketCreateNamingGoodLong(62);
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_long_63")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 63자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_63() {
		TestBucketCreateNamingGoodLong(63);
	}

	@Test
	@DisplayName("test_bucket_list_long_name")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("버킷이름의 길이 긴 경우 버킷 목록을 읽어올 수 있는지 확인")
	public void test_bucket_list_long_name() {
		var BucketName = GetNewBucketName(61);
		var Bucket = GetNewBucketResource(BucketName);
		var IsEmpty = BucketIsEmpty(Bucket);
		assertTrue(IsEmpty);
	}

	@Test
	@DisplayName("test_bucket_create_naming_bad_ip")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_ip() {
		CheckBadBucketName("192.168.11.123");
	}

	@Test
	@DisplayName("test_bucket_create_naming_dns_underscore")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_underscore() {
		CheckBadBucketName("foo_bar");
	}

	@Test
	@DisplayName("test_bucket_create_naming_dns_long")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인")
	public void test_bucket_create_naming_dns_long() {
		var Prefix = GetPrefix();
		var AddLength = 63 - Prefix.length();
		Prefix = RandomText(AddLength);
		CheckGoodBucketName(Prefix, null);
	}

	@Test
	@DisplayName("test_bucket_create_naming_dns_dash_at_end")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dash_at_end() {
		CheckBadBucketName("foo-");
	}

	@Test
	@DisplayName("test_bucket_create_naming_dns_dot_dot")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dot_dot() {
		CheckBadBucketName("foo..bar");
	}

	@Test
	@DisplayName("test_bucket_create_naming_dns_dot_dash")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dot_dash() {
		CheckBadBucketName("foo.-bar");
	}

	@Test
	@DisplayName("test_bucket_create_naming_dns_dash_dot")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dash_dot() {
		CheckBadBucketName("foo-.bar");
	}

	@Test
	@DisplayName("test_bucket_create_exists")
	@Tag("Duplicate")
	@Tag("KSAN")
	//@Tag("버킷 중복 생성시 실패 확인")
	public void test_bucket_create_exists() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();

		Client.createBucket(BucketName);

		var e = assertThrows(AmazonServiceException.class, () -> Client.createBucket(BucketName));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(409, StatusCode);
		assertEquals(MainData.BucketAlreadyOwnedByYou, ErrorCode);
	}

	@Test
	@DisplayName("test_bucket_create_exists_nonowner")
	@Tag("Duplicate")
	@Tag("KSAN")
	//@Tag("[다른 2명의 사용자가 버킷 생성하려고 할 경우] 메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인")
	public void test_bucket_create_exists_nonowner() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();
		var AltClient = GetAltClient();

		Client.createBucket(BucketName);

		var e = assertThrows(AmazonServiceException.class, () -> AltClient.createBucket(BucketName));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(409, StatusCode);
		assertEquals(MainData.BucketAlreadyExists, ErrorCode);
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_starts_alpha")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_starts_alpha() {
		CheckGoodBucketName("foo", "a" + GetPrefix());
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_starts_digit")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름이 숫자로 시작할 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_starts_digit() {
		CheckGoodBucketName("foo", "0" + GetPrefix());
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_contains_period")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_contains_period() {
		CheckGoodBucketName("aaa.111", null);
	}

	@Test
	@DisplayName("test_bucket_create_naming_good_contains_hyphen")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_contains_hyphen() {
		CheckGoodBucketName("aaa-111", null);
	}

	@Test
	@DisplayName("test_bucket_recreate_not_overriding")
	@Tag("Duplicate")
	//@Tag("버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인 (버킷은 중복 생성 할 수 없음을 확인)")
	public void test_bucket_recreate_not_overriding() {
		var KeyNames = new ArrayList<String>();
		KeyNames.add("mykey1");
		KeyNames.add("mykey2");

		var BucketName = CreateObjects(KeyNames);

		var ObjectList = GetObjectList(BucketName, null);
		assertEquals(KeyNames, ObjectList);

		var Client = GetClient();
		assertThrows(AmazonServiceException.class, () -> Client.createBucket(BucketName));

		ObjectList = GetObjectList(BucketName, null);
		assertEquals(KeyNames, ObjectList);
	}
}
