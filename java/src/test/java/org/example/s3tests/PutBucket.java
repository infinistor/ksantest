package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class PutBucket {

    org.example.test.PutBucket Test = new org.example.test.PutBucket();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@DisplayName("test_bucket_list_empty")
	@Tag("PUT")
	@Tag("KSAN")
	//@Tag("생성한 버킷이 비어있는지 확인")
	public void test_bucket_list_empty()
    {
        Test.test_bucket_list_empty();
    }

	@Test
	@DisplayName("test_bucket_create_naming_bad_starts_nonalpha")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_starts_nonalpha()
    {
        Test.test_bucket_create_naming_bad_starts_nonalpha();
    }

	@Test
	@DisplayName("test_bucket_create_naming_bad_short_one")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_short_one()
    {
        Test.test_bucket_create_naming_bad_short_one();
    }

	@Test
	@DisplayName("test_bucket_create_naming_bad_short_two")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_short_two()
    {
        Test.test_bucket_create_naming_bad_short_two();
    }

	@Test
	@DisplayName("test_bucket_create_naming_good_long_60")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 60자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_60()
    {
        Test.test_bucket_create_naming_good_long_60();
    }

	@Test
	@DisplayName("test_bucket_create_naming_good_long_61")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 61자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_61()
    {
        Test.test_bucket_create_naming_good_long_61();
    }

	@Test
	@DisplayName("test_bucket_create_naming_good_long_62")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 62자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_62()
    {
        Test.test_bucket_create_naming_good_long_62();
    }

	@Test
	@DisplayName("test_bucket_create_naming_good_long_63")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 63자인 경우 버킷 생성 확인")
	public void test_bucket_create_naming_good_long_63()
    {
        Test.test_bucket_create_naming_good_long_63();
    }

	@Test
	@DisplayName("test_bucket_list_long_name")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("버킷이름의 길이 긴 경우 버킷 목록을 읽어올 수 있는지 확인")
	public void test_bucket_list_long_name()
    {
        Test.test_bucket_list_long_name();
    }

	@Test
	@DisplayName("test_bucket_create_naming_bad_ip")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_bad_ip()
    {
        Test.test_bucket_create_naming_bad_ip();
    }

	@Test
	@DisplayName("test_bucket_create_naming_dns_underscore")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_underscore()
    {
        Test.test_bucket_create_naming_dns_underscore();
    }

	@Test
	@DisplayName("test_bucket_create_naming_dns_long")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인")
	public void test_bucket_create_naming_dns_long()
    {
        Test.test_bucket_create_naming_dns_long();
    }

	@Test
	@DisplayName("test_bucket_create_naming_dns_dash_at_end")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dash_at_end()
    {
        Test.test_bucket_create_naming_dns_dash_at_end();
    }

	@Test
	@DisplayName("test_bucket_create_naming_dns_dot_dot")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dot_dot()
    {
        Test.test_bucket_create_naming_dns_dot_dot();
    }

	@Test
	@DisplayName("test_bucket_create_naming_dns_dot_dash")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dot_dash()
    {
        Test.test_bucket_create_naming_dns_dot_dash();
    }

	@Test
	@DisplayName("test_bucket_create_naming_dns_dash_dot")
	@Tag("CreationRules")
	@Tag("KSAN")
	//@Tag("생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인")
	public void test_bucket_create_naming_dns_dash_dot()
    {
        Test.test_bucket_create_naming_dns_dash_dot();
    }

	@Test
	@DisplayName("test_bucket_create_exists")
	@Tag("Duplicate")
	@Tag("KSAN")
	//@Tag("버킷 중복 생성시 실패 확인")
	public void test_bucket_create_exists()
    {
        Test.test_bucket_create_exists();
    }

	@Test
	@DisplayName("test_bucket_create_exists_nonowner")
	@Tag("Duplicate")
	@Tag("KSAN")
	//@Tag("[다른 2명의 사용자가 버킷 생성하려고 할 경우] 메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인")
	public void test_bucket_create_exists_nonowner()
    {
        Test.test_bucket_create_exists_nonowner();
    }


	@Test
	@DisplayName("test_bucket_create_naming_good_starts_alpha")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_starts_alpha()
    {
        Test.test_bucket_create_naming_good_starts_alpha();
    }


	@Test
	@DisplayName("test_bucket_create_naming_good_starts_digit")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름이 숫자로 시작할 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_starts_digit()
    {
        Test.test_bucket_create_naming_good_starts_digit();
    }


	@Test
	@DisplayName("test_bucket_create_naming_good_contains_period")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_contains_period()
    {
        Test.test_bucket_create_naming_good_contains_period();
    }


	@Test
	@DisplayName("test_bucket_create_naming_good_contains_hyphen")
	@Tag("CreationRules")
	//@Tag("생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인")
	public void test_bucket_create_naming_good_contains_hyphen()
    {
        Test.test_bucket_create_naming_good_contains_hyphen();
    }


	@Test
	@DisplayName("test_bucket_recreate_not_overriding")
	@Tag("Duplicate")
	//@Tag("버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인 (버킷은 중복 생성 할 수 없음을 확인)")
	public void test_bucket_recreate_not_overriding()
    {
        Test.test_bucket_recreate_not_overriding();
    }

}
