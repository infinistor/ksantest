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

public class ListObjectsVersions {

	org.example.test.ListObjectsVersions Test = new org.example.test.ListObjectsVersions();

	@AfterEach
	public void Clear() {
		Test.Clear();
	}

	@Test
	@Tag("KSAN")
	@Tag("Metadata")
	// Version정보를 가질 수 있는 버킷에서 ListObjectsVersions로 가져온 Metadata와 HeadObject,
	// GetObjectAcl로 가져온 Metadata 일치 확인
	public void test_bucket_list_return_data_versioning() {
		Test.test_bucket_list_return_data_versioning();
	}
	
	@Test
	@Tag("Filtering")
	// delimiter, prefix, max-keys, marker를 조합하여 오브젝트 목록을 가져올때 올바르게 가져오는지 확인
	public void test_versioning_bucket_list_filtering_all() {
		Test.test_versioning_bucket_list_filtering_all();
	}
}
