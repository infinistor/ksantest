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
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class PutBucket {

	org.example.test.PutBucket Test = new org.example.test.PutBucket();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("PUT")
	// 생성한 버킷이 비어있는지 확인
	public void test_bucket_list_empty() {
		Test.test_bucket_list_empty();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_bad_starts_nonalpha() {
		Test.test_bucket_create_naming_bad_starts_nonalpha();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_bad_short_one() {
		Test.test_bucket_create_naming_bad_short_one();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_bad_short_two() {
		Test.test_bucket_create_naming_bad_short_two();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 60자인 경우 버킷 생성 확인
	public void test_bucket_create_naming_good_long_60() {
		Test.test_bucket_create_naming_good_long_60();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 61자인 경우 버킷 생성 확인
	public void test_bucket_create_naming_good_long_61() {
		Test.test_bucket_create_naming_good_long_61();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 62자인 경우 버킷 생성 확인
	public void test_bucket_create_naming_good_long_62() {
		Test.test_bucket_create_naming_good_long_62();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 63자인 경우 버킷 생성 확인
	public void test_bucket_create_naming_good_long_63() {
		Test.test_bucket_create_naming_good_long_63();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 버킷이름의 길이 긴 경우 버킷 목록을 읽어올 수 있는지 확인
	public void test_bucket_list_long_name() {
		Test.test_bucket_list_long_name();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_bad_ip() {
		Test.test_bucket_create_naming_bad_ip();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_dns_underscore() {
		Test.test_bucket_create_naming_dns_underscore();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인
	public void test_bucket_create_naming_dns_long() {
		Test.test_bucket_create_naming_dns_long();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_dns_dash_at_end() {
		Test.test_bucket_create_naming_dns_dash_at_end();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_dns_dot_dot() {
		Test.test_bucket_create_naming_dns_dot_dot();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_dns_dot_dash() {
		Test.test_bucket_create_naming_dns_dot_dash();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인
	public void test_bucket_create_naming_dns_dash_dot() {
		Test.test_bucket_create_naming_dns_dash_dot();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// 버킷 중복 생성시 실패 확인
	public void test_bucket_create_exists() {
		Test.test_bucket_create_exists();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// [다른 2명의 사용자가 버킷 생성하려고 할 경우] 메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인
	public void test_bucket_create_exists_nonowner() {
		Test.test_bucket_create_exists_nonowner();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인
	public void test_bucket_create_naming_good_starts_alpha() {
		Test.test_bucket_create_naming_good_starts_alpha();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름이 숫자로 시작할 경우 생성되는지 확인
	public void test_bucket_create_naming_good_starts_digit() {
		Test.test_bucket_create_naming_good_starts_digit();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인
	public void test_bucket_create_naming_good_contains_period() {
		Test.test_bucket_create_naming_good_contains_period();
	}

	@Test
	@Tag("KSAN")
	@Tag("CreationRules")
	// 생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인
	public void test_bucket_create_naming_good_contains_hyphen() {
		Test.test_bucket_create_naming_good_contains_hyphen();
	}

	@Test
	@Tag("KSAN")
	@Tag("Duplicate")
	// 버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인 (버킷은 중복 생성 할 수 없음을 확인)
	public void test_bucket_recreate_not_overriding() {
		Test.test_bucket_recreate_not_overriding();
	}

}
