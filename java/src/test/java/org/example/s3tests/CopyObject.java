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

public class CopyObject
{
    org.example.test.CopyObject Test = new org.example.test.CopyObject();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("오브젝트의 크기가 0일때 복사가 가능한지 확인")
    public void test_object_copy_zero_size()
    {
        Test.test_object_copy_zero_size();
    }

    @Test
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("동일한 버킷에서 오브젝트 복사가 가능한지 확인")
    public void test_object_copy_same_bucket()
    {
        Test.test_object_copy_same_bucket();
    }

    @Test
    @Tag("ContentType")
    @Tag("KSAN")
    //@Tag("ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인")
    public void test_object_copy_verify_contenttype()
    {
        Test.test_object_copy_verify_contenttype();
    }

    @Test
    @Tag("OverWrite")
    @Tag("KSAN")
    //@Tag("복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인")
    public void test_object_copy_to_itself()
    {
        Test.test_object_copy_to_itself();
    }

    @Test
    @Tag("OverWrite")
    @Tag("KSAN")
    //@Tag("복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인")
    public void test_object_copy_to_itself_with_metadata()
    {
        Test.test_object_copy_to_itself_with_metadata();
    }

    @Test
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("다른 버킷으로 오브젝트 복사가 가능한지 확인")
    public void test_object_copy_diff_bucket()
    {
        Test.test_object_copy_diff_bucket();
    }

    @Test
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("[bucket1:created main user, object:created main user / bucket2:created sub user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인")
    public void test_object_copy_not_owned_bucket()
    {
        Test.test_object_copy_not_owned_bucket();
    }

    @Test
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("[bucket_acl = main:full control,sub : full control | object_acl = default] 서브유저가 접근권한이 있는 버킷에 들어있는 접근권한이 있는 오브젝트를 복사가 가능한지 확인")
    public void test_object_copy_not_owned_object_bucket()
    {
        Test.test_object_copy_not_owned_object_bucket();
    }

    @Test
    @Tag("OverWrite")
    @Tag("KSAN")
    //@Tag("권한정보를 포함하여 복사할때 올바르게 적용되는지 확인 메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인")
    public void test_object_copy_canned_acl()
    {
        Test.test_object_copy_canned_acl();
    }

    @Test
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("크고 작은 용량의 오브젝트가 복사되는지 확인")
    public void test_object_copy_retaining_metadata()
    {
        Test.test_object_copy_retaining_metadata();
    }

    @Test
    @Tag("Check")
    @Tag("KSAN")
    //@Tag("크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인")
    public void test_object_copy_replacing_metadata()
    {
        Test.test_object_copy_replacing_metadata();
    }

    @Test
    @Tag("ERROR")
    @Tag("KSAN")
    //@Tag("존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인")
    public void test_object_copy_bucket_not_found()
    {
        Test.test_object_copy_bucket_not_found();
    }

    @Test
    @Tag("ERROR")
    @Tag("KSAN")
    //@Tag("존재하지않는 오브젝트 복사 실패 확인")
    public void test_object_copy_key_not_found()
    {
        Test.test_object_copy_key_not_found();
    }

    @Test
    @Tag("Version")
    @Tag("KSAN")
    //@Tag("버저닝된 오브젝트 복사 확인")
    public void test_object_copy_versioned_bucket()
    {
        Test.test_object_copy_versioned_bucket();
    }

    @Test
    @Tag("Version")
    @Tag("KSAN")
    //@Tag("[버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인")
    public void test_object_copy_versioned_url_encoding()
    {
        Test.test_object_copy_versioned_url_encoding();
    }

    @Test
    @Tag("Multipart")
    @Tag("KSAN")
    //@Tag("[버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인")
    public void test_object_copy_versioning_multipart_upload()
    {
        Test.test_object_copy_versioning_multipart_upload();
    }

    @Test
    @Tag("Imatch")
    //@Tag("ifmatch 값을 추가하여 오브젝트를 복사할 경우 성공확인")
    public void test_copy_object_ifmatch_good()
    {
        Test.test_copy_object_ifmatch_good();
    }

    @Test
    @Tag("Imatch")
    //@Tag("ifmatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인")
    public void test_copy_object_ifmatch_failed()
    {
        Test.test_copy_object_ifmatch_failed();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_nor_src_to_nor_bucket_and_obj()
    {
        Test.test_copy_nor_src_to_nor_bucket_and_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_nor_src_to_nor_bucket_encryption_obj()
    {
        Test.test_copy_nor_src_to_nor_bucket_encryption_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_nor_src_to_encryption_bucket_nor_obj()
    {
        Test.test_copy_nor_src_to_encryption_bucket_nor_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_nor_src_to_encryption_bucket_and_obj()
    {
        Test.test_copy_nor_src_to_encryption_bucket_and_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_src_to_nor_bucket_and_obj()
    {
        Test.test_copy_encryption_src_to_nor_bucket_and_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_src_to_nor_bucket_encryption_obj()
    {
        Test.test_copy_encryption_src_to_nor_bucket_encryption_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_src_to_encryption_bucket_nor_obj()
    {
        Test.test_copy_encryption_src_to_encryption_bucket_nor_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_src_to_encryption_bucket_and_obj()
    {
        Test.test_copy_encryption_src_to_encryption_bucket_and_obj();
    }


    @Test
    @Tag("encryption")
    //@Tag("[source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj()
    {
        Test.test_copy_encryption_bucket_nor_obj_to_nor_bucket_and_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj()
    {
        Test.test_copy_encryption_bucket_nor_obj_to_nor_bucket_encryption_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj()
    {
        Test.test_copy_encryption_bucket_nor_obj_to_encryption_bucket_nor_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj()
    {
        Test.test_copy_encryption_bucket_nor_obj_to_encryption_bucket_and_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj()
    {
        Test.test_copy_encryption_bucket_and_obj_to_nor_bucket_and_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj()
    {
        Test.test_copy_encryption_bucket_and_obj_to_nor_bucket_encryption_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj()
    {
        Test.test_copy_encryption_bucket_and_obj_to_encryption_bucket_nor_obj();
    }

    @Test
    @Tag("encryption")
    //@Tag("[source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공 확인")
    public void test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj()
    {
        Test.test_copy_encryption_bucket_and_obj_to_encryption_bucket_and_obj();
    }
    
    @Test
    @Tag("encryption")
    //@Tag("일반 오브젝트에서 다양한 방식으로 복사 성공 확인")
    public void test_copy_to_normal_source()
    {
        Test.test_copy_to_normal_source();
    }
    
    @Test
    @Tag("encryption")
    //@Tag("SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인")
    public void test_copy_to_sse_s3_source()
    {
        Test.test_copy_to_sse_s3_source();
    }

    @Test
    @Tag("encryption")
    //@Tag("SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인")
    public void test_copy_to_sse_c_source()
    {
        Test.test_copy_to_sse_c_source();
    }
}
