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
package org.example.testV2;

import org.junit.Test;
import org.junit.jupiter.api.Tag;

public class KMS {
	
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("KMS V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("KMS V2 End");
	}

	@Test
	@Tag("PutGet")
	//1Byte 오브젝트를 SSE-KMS 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void testSseKmsEncryptedTransfer1b(){
		
	}
	
}
