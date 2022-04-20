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

public class Grants {

    org.example.test.Grants Test = new org.example.test.Grants();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @Tag("KSAN")
    @Tag("Bucket")
    // @Tag("[bucket_acl : default] 권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인")
    public void test_bucket_acl_default()
    {
        Test.test_bucket_acl_default();
    }

    @Test
    @Tag("KSAN")
    @Tag("Bucket")
    // @Tag("[bucket_acl : public-read] 권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인")
    public void test_bucket_acl_canned_during_create()
    {
        Test.test_bucket_acl_canned_during_create();
    }

    @Test
    @Tag("KSAN")
    @Tag("Bucket")
    // @Tag("[bucket_acl : public-read => bucket_acl : private] 권한을 public-read로 생성한
    // 버킷을 private로 변경할경우 올바르게 적용되는지 확인")
    public void test_bucket_acl_canned()
    {
        Test.test_bucket_acl_canned();
    }

    @Test
    @Tag("KSAN")
    @Tag("Bucket")
    // @Tag("[bucket_acl : public-read-write] 권한을 public-read-write로 생성한 버킷의 acl정보가
    // 올바른지 확인")
    public void test_bucket_acl_canned_publicreadwrite()
    {
        Test.test_bucket_acl_canned_publicreadwrite();
    }

    @Test
    @Tag("KSAN")
    @Tag("Bucket")
    // @Tag("[bucket_acl : authenticated-read] 권한을 authenticated-read로 생성한 버킷의
    // acl정보가 올바른지 확인")
    public void test_bucket_acl_canned_authenticatedread()
    {
        Test.test_bucket_acl_canned_authenticatedread();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[object_acl : default] 권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인")
    public void test_object_acl_default()
    {
        Test.test_object_acl_default();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[object_acl : public-read] 권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인")
    public void test_object_acl_canned_during_create()
    {
        Test.test_object_acl_canned_during_create();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[object_acl : public-read => object_acl : private] 권한을 public-read로 생성한
    // 오브젝트를 private로 변경할경우 올바르게 적용되는지 확인")
    public void test_object_acl_canned()
    {
        Test.test_object_acl_canned();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[object_acl : public-read-write] 권한을 public-read-write로 생성한 오브젝트의
    // acl정보가 올바른지 확인")
    public void test_object_acl_canned_publicreadwrite()
    {
        Test.test_object_acl_canned_publicreadwrite();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[object_acl : authenticated-read] 권한을 authenticated-read로 생성한 오브젝트의
    // acl정보가 올바른지 확인")
    public void test_object_acl_canned_authenticatedread()
    {
        Test.test_object_acl_canned_authenticatedread();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[bucket_acl: public-read-write] [object_acl : public-read-write =>
    // object_acl : bucket-owner-read]" +
    // "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
    // bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인")
    public void test_object_acl_canned_bucketownerread()
    {
        Test.test_object_acl_canned_bucketownerread();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[bucket_acl: public-read-write] [object_acl : public-read-write =>
    // object_acl : bucket-owner-full-control] " +
    // "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를 서브 유저가 권한을
    // bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인")
    public void test_object_acl_canned_bucketownerfullcontrol()
    {
        Test.test_object_acl_canned_bucketownerfullcontrol();
    }

    @Test
    @Tag("KSAN")
    @Tag("Object")
    // @Tag("[bucket_acl: public-read-write] " +
    // "메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트의 권한을 서브유저에게
    // FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때 오브젝트의
    // 소유자가 유지되는지 확인")
    public void test_object_acl_full_control_verify_owner()
    {
        Test.test_object_acl_full_control_verify_owner();
    }

    @Test
    @Tag("KSAN")
    @Tag("ETag")
    // @Tag("[bucket_acl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인")
    public void test_object_acl_full_control_verify_attributes()
    {
        Test.test_object_acl_full_control_verify_attributes();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("[bucket_acl:private] 기본생성한 버킷에 priavte 설정이 가능한지 확인")
    public void test_bucket_acl_canned_private_to_private()
    {
        Test.test_bucket_acl_canned_private_to_private();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL")
    public void test_object_acl()
    {
        Test.test_object_acl();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE")
    public void test_object_acl_write()
    {
        Test.test_object_acl_write();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP")
    public void test_object_acl_writeacp()
    {
        Test.test_object_acl_writeacp();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ")
    public void test_object_acl_read()
    {
        Test.test_object_acl_read();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP")
    public void test_object_acl_readacp()
    {
        Test.test_object_acl_readacp();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL")
    public void test_bucket_acl_grant_userid_fullcontrol()
    {
        Test.test_bucket_acl_grant_userid_fullcontrol();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ")
    public void test_bucket_acl_grant_userid_read()
    {
        Test.test_bucket_acl_grant_userid_read();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP")
    public void test_bucket_acl_grant_userid_readacp()
    {
        Test.test_bucket_acl_grant_userid_readacp();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE")
    public void test_bucket_acl_grant_userid_write()
    {
        Test.test_bucket_acl_grant_userid_write();
    }

    @Test
    @Tag("KSAN")
    @Tag("Permission")
    // @Tag("메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP")
    public void test_bucket_acl_grant_userid_writeacp()
    {
        Test.test_bucket_acl_grant_userid_writeacp();
    }

    @Test
    @Tag("KSAN")
    @Tag("ERROR")
    // @Tag("버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인")
    public void test_bucket_acl_grant_nonexist_user()
    {
        Test.test_bucket_acl_grant_nonexist_user();
    }

    @Test
    @Tag("KSAN")
    @Tag("ERROR")
    // @Tag("버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인")
    public void test_bucket_acl_no_grants()
    {
        Test.test_bucket_acl_no_grants();
    }

    @Test
    @Tag("KSAN")
    @Tag("Header")
    // @Tag("오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인")
    public void test_object_header_acl_grants()
    {
        Test.test_object_header_acl_grants();
    }

    @Test
    @Tag("KSAN")
    @Tag("Header")
    // @Tag("버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인")
    public void test_bucket_header_acl_grants()
    {
        Test.test_bucket_header_acl_grants();
    }

    @Test
    @Tag("KSAN")
    @Tag("Delete")
    // @Tag("버킷의 소유자정보를 포함한 모든 acl정보를 삭제할 경우 올바르게 적용되는지 확인")
    public void test_bucket_acl_revoke_all()
    {
        Test.test_bucket_acl_revoke_all();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:private, object_acl:private] 메인유저가 pirvate권한으로 생성한 버킷과
    // 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인")
    public void test_access_bucket_private_object_private()
    {
        Test.test_access_bucket_private_object_private();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:private, object_acl:private] 메인유저가 pirvate권한으로 생성한 버킷과
    // 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjects_v2)")
    public void test_access_bucket_private_objectv2_private()
    {
        Test.test_access_bucket_private_objectv2_private();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:private, object_acl:private, public-read] 메인유저가 pirvate권한으로
    // 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
    // 확인")
    public void test_access_bucket_private_object_publicread()
    {
        Test.test_access_bucket_private_object_publicread();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:private, object_acl:private, public-read] 메인유저가 pirvate권한으로
    // 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read로 설정한 오브젝트는 다운로드 할 수 있음을
    // 확인(ListObjects_v2)")
    public void test_access_bucket_private_objectv2_publicread()
    {
        Test.test_access_bucket_private_objectv2_publicread();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:private, object_acl:private, public-read-write] 메인유저가
    // pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한
    // 오브젝트는 다운로드만 할 수 있음을 확인 (버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로
    // 설정되어있어도 업로드불가)")
    public void test_access_bucket_private_object_publicreadwrite()
    {
        Test.test_access_bucket_private_object_publicreadwrite();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:private, object_acl:private, public-read-write] 메인유저가
    // pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 public-read-write로 설정한
    // 오브젝트는 다운로드만 할 수 있음을 확인(ListObjects_v2) (버킷의 권한이 private이기 때문에 오브젝트의 권한이
    // public-read-write로 설정되어있어도 업로드불가)")
    public void test_access_bucket_private_objectv2_publicreadwrite()
    {
        Test.test_access_bucket_private_objectv2_publicreadwrite();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:public-read, object_acl:private] 메인유저가 public-read권한으로 생성한
    // 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록만 볼 수 있음을 확인")
    public void test_access_bucket_publicread_object_private()
    {
        Test.test_access_bucket_publicread_object_private();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:public-read, object_acl:public-read, private] 메인유저가
    // public-read권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 보거나 다운로드
    // 할 수 있음을 확인")
    public void test_access_bucket_publicread_object_publicread()
    {
        Test.test_access_bucket_publicread_object_publicread();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:public-read, object_acl:public-read-wirte, private] 메인유저가
    // public-read권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을
    // 보거나 다운로드 할 수 있음을 확인 (버킷의 권한이 public-read이기 때문에 오브젝트의 권한이 public-read-write로
    // 설정되어있어도 수정불가)")
    public void test_access_bucket_publicread_object_publicreadwrite()
    {
        Test.test_access_bucket_publicread_object_publicreadwrite();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:public-read-write, object_acl:private] 메인유저가
    // public-read-write권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을 읽거나
    // 업로드는 가능하지만 다운로드 할 수 없음을 확인")
    public void test_access_bucket_publicreadwrite_object_private()
    {
        Test.test_access_bucket_publicreadwrite_object_private();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:public-read-write, object_acl:public-read, private] 메인유저가
    // public-read-write권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 서브유저는 오브젝트 목록을
    // 읽거나 업로드, 다운로드 모두 가능함을 확인")
    public void test_access_bucket_publicreadwrite_object_publicread()
    {
        Test.test_access_bucket_publicreadwrite_object_publicread();
    }

    @Test
    @Tag("KSAN")
    @Tag("Access")
    // @Tag("[bucket_acl:public-read-write, object_acl:public-read-write, private]
    // 메인유저가 public-read-write권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 서브유저는
    // 오브젝트 목록을 읽거나 업로드, 다운로드 모두 가능함을 확인")
    public void test_access_bucket_publicreadwrite_object_publicreadwrite()
    {
        Test.test_access_bucket_publicreadwrite_object_publicreadwrite();
    }
}
