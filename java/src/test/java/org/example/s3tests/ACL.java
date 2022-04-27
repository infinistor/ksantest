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

public class ACL
{
	org.example.test.ACL Test = new org.example.test.ACL();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 오브젝트에 접근 가능한지 확인
	public void test_object_raw_get()
	{
		Test.test_object_raw_get();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인
	public void test_object_raw_get_bucket_gone()
	{
		Test.test_object_raw_get_bucket_gone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Delete")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된 오브젝트를 삭제할때 에러 확인
	public void test_object_delete_key_bucket_gone()
	{
		Test.test_object_delete_key_bucket_gone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 오브젝트에 접근할때 에러 확인
	public void test_object_raw_get_object_gone()
	{
		Test.test_object_raw_get_object_gone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = private, Object_ACL = public-read] 권한없는 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인
	public void test_object_raw_get_bucket_acl()
	{
		Test.test_object_raw_get_bucket_acl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = private] 권한없는 사용자가 공용버킷의 개인 오브젝트에 접근할때 에러확인
	public void test_object_raw_get_object_acl()
	{
		Test.test_object_raw_get_object_acl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 공용 버킷의 공용 오브젝트에 접근 가능한지 확인
	public void test_object_raw_authenticated()
	{
		Test.test_object_raw_authenticated();
	}

	@Test
	@Tag("KSAN")
	@Tag("Header")
	//[Bucket_ACL = priavte, Object_ACL = priavte] 로그인한 사용자가 GetObject의 반환헤더값을 설정하고 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인
	public void test_object_raw_response_headers()
	{
		Test.test_object_raw_response_headers();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = private, Object_ACL = public-read] 로그인한 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인
	public void test_object_raw_authenticated_bucket_acl()
	{
		Test.test_object_raw_authenticated_bucket_acl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = private] 로그인한 사용자가 공용버킷의 개인 오브젝트에 접근 가능한지 확인
	public void test_object_raw_authenticated_object_acl()
	{
		Test.test_object_raw_authenticated_object_acl();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 버킷의 삭제된 오브젝트에 접근할때 에러 확인
	public void test_object_raw_authenticated_bucket_gone()
	{
		Test.test_object_raw_authenticated_bucket_gone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 오브젝트에 접근할때 에러 확인
	public void test_object_raw_authenticated_object_gone()
	{
		Test.test_object_raw_authenticated_object_gone();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료되지 않은 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 가능한지 확인
	public void test_object_raw_get_x_amz_expires_not_expired()
	{
		Test.test_object_raw_get_x_amz_expires_not_expired();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인
	public void test_object_raw_get_x_amz_expires_out_range_zero()
	{
		Test.test_object_raw_get_x_amz_expires_out_range_zero();
	}

	@Test
	@Tag("KSAN")
	@Tag("Post")
	//[Bucket_ACL = public-read, Object_ACL = public-read] 로그인 유효주기가 만료된 사용자가 공용 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인
	public void test_object_raw_get_x_amz_expires_out_positive_range()
	{
		Test.test_object_raw_get_x_amz_expires_out_positive_range();
	}

	@Test
	@Tag("KSAN")
	@Tag("Update")
	//[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트하려고 할때 실패 확인
	public void test_object_anon_put()
	{
		Test.test_object_anon_put();
	}

	@Test
	@Tag("KSAN")
	@Tag("Update")
	//[Bucket_ACL = public-read-write] 로그인한 사용자가 공용버켓(w/r)을 만들고 업로드한 오브젝트를 권한없는 사용자가 업데이트했을때 올바르게 적용 되는지 확인
	public void test_object_anon_put_write_access()
	{
		Test.test_object_anon_put_write_access();
	}

	@Test
	@Tag("KSAN")
	@Tag("Default")
	//[Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드
	public void test_object_put_authenticated()
	{
		Test.test_object_put_authenticated();
	}

	@Test
	@Tag("KSAN")
	@Tag("Default")
	//[Bucket_ACL = Default, Object_ACL = Default] Post방식으로 만료된 로그인 정보를 설정하여 오브젝트 업데이트 실패 확인
	public void test_object_raw_put_authenticated_expired()
	{
		Test.test_object_raw_put_authenticated_expired();
	}

	@Test
	@Tag("KSAN")
	@Tag("Get")
	//[Bucket_ACL = private, Object_ACL = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근 가능한지 확인
	public void test_acl_private_bucket_public_read_object()
	{
		Test.test_acl_private_bucket_public_read_object();
	}
}
