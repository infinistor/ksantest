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

public class CSE {

    org.example.test.CSE Test = new org.example.test.CSE();

    @AfterEach
	public void Clear() {
		Test.Clear();
	}

    @Test
    @Tag("KSAN")
    @Tag("PutGet")
    // @Tag("[AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_1b()
    {
        Test.test_cse_encrypted_transfer_1b();
    }

    @Test
    @Tag("KSAN")
    @Tag("PutGet")
    // @Tag("[AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_1kb()
    {
        Test.test_cse_encrypted_transfer_1kb();
    }

    @Test
    @Tag("KSAN")
    @Tag("PutGet")
    // @Tag("[AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_1MB()
    {
        Test.test_cse_encrypted_transfer_1MB();
    }

    @Test
    @Tag("KSAN")
    @Tag("PutGet")
    // @Tag("[AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인")
    public void test_cse_encrypted_transfer_13b()
    {
        Test.test_cse_encrypted_transfer_13b();
    }

    @Test
    @Tag("KSAN")
    @Tag("Metadata")
    // @Tag("[AES256] 암호화하고 메타데이터에 키값을 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인 ")
    public void test_cse_encryption_method_head()
    {
        Test.test_cse_encryption_method_head();
    }

    @Test
    @Tag("KSAN")
    @Tag("ERROR")
    // @Tag("[AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치")
    public void test_cse_encryption_non_decryption()
    {
        Test.test_cse_encryption_non_decryption();
    }

    @Test
    @Tag("KSAN")
    @Tag("ERROR")
    // @Tag("[AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인")
    public void test_cse_non_encryption_decryption()
    {
        Test.test_cse_non_encryption_decryption();
    }

    @Test
    @Tag("KSAN")
    @Tag("RangeRead")
    // @Tag("[AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공")
    public void test_cse_encryption_range_read()
    {
        Test.test_cse_encryption_range_read();
    }

    @Test
    @Tag("KSAN")
    @Tag("Multipart")
    // @Tag("[AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인")
    public void test_cse_encryption_multipart_upload()
    {
        Test.test_cse_encryption_multipart_upload();
    }

    @Test
    @Tag("KSAN")
    @Tag("Get")
    // @Tag("CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")
    public void test_cse_get_object_many()
    {
        Test.test_cse_get_object_many();
    }

    @Test
    @Tag("KSAN")
    @Tag("Get")
    // @Tag("CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")
    public void test_cse_range_object_many()
    {
        Test.test_cse_range_object_many();
    }
}
