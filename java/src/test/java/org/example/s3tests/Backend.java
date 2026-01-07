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
package org.example.s3tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class Backend {
	org.example.testV2.Backend testV2 = new org.example.testV2.Backend();

	/**
	 * 테스트 완료 후 정리 작업을 수행합니다.
	 * 
	 * @param testInfo 테스트 정보
	 */
	@AfterEach
	void clear(TestInfo testInfo) {
		testV2.clear(testInfo);
	}

	// /**
	// * PutObject가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("PUT")
	// void testPutObject() {
	// testV2.testPutObject();
	// }

	// /**
	// * GetObject가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("GET")
	// void testGetObject() {
	// testV2.testGetObject();
	// }

	// /**
	// * GetObject => PutObject가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("GET")
	// void testGetPut() {
	// testV2.testGetPut();
	// }

	// /**
	// * DeleteObject가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("DELETE")
	// void testDeleteObject() {
	// testV2.testDeleteObject();
	// }

	// /**
	// * CopyObject가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("COPY")
	// void testCopyObject() {
	// testV2.testCopyObject();
	// }

	// /**
	// * MultipartUpload가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("MULTIPART")
	// void testMultipartUpload() {
	// testV2.testMultipartUpload();
	// }

	// /**
	// * PutObjectAcl가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("ACL")
	// void testPutObjectAcl() {
	// testV2.testPutObjectAcl();
	// }

	// /**
	// * GetObjectAcl가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("ACL")
	// void testGetObjectAcl() {
	// testV2.testGetObjectAcl();
	// }

	// /**
	// * PutObjectTagging가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("TAGGING")
	// void testPutObjectTagging() {
	// testV2.testPutObjectTagging();
	// }

	// /**
	// * GetObjectTagging가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("TAGGING")
	// void testGetObjectTagging() {
	// testV2.testGetObjectTagging();
	// }

	// /**
	// * DeleteObjectTagging가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("TAGGING")
	// void testDeleteObjectTagging() {
	// testV2.testDeleteObjectTagging();
	// }

	// /**
	// * PutObjectRetention가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("RETENTION")
	// void testPutObjectRetention() {
	// testV2.testPutObjectRetention();
	// }

	// /**
	// * GetObjectRetention가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("RETENTION")
	// void testGetObjectRetention() {
	// testV2.testGetObjectRetention();
	// }

	// /**
	// * DeleteObjectRetention가 정상 동작하는지 확인
	// */
	// @Test
	// @Tag("RETENTION")
	// void testDeleteObjectRetention() {
	// testV2.testDeleteObjectRetention();
	// }

	/**
	 * [Versioning] PutObject가 정상 동작하는지 확인
	 */
	@Test
	@Tag("PUT")
	void testPutObjectVersioning() {
		testV2.testPutObjectVersioning();
	}

	/**
	 * [Versioning] PutObject 버전 정보 추가시 정상 동작 확인
	 */
	@Test
	@Tag("PUT")
	void testPutObjectVersioningWithVersionId() {
		testV2.testPutObjectVersioningWithVersionId();
	}

	/**
	 * [Versioning] GetObject가 정상 동작하는지 확인
	 */
	@Test
	@Tag("GET")
	void testGetObjectVersioning() {
		testV2.testGetObjectVersioning();
	}

	/**
	 * [Versioning] DeleteObject가 정상 동작하는지 확인
	 */
	@Test
	@Tag("DELETE")
	void testDeleteObjectVersioning() {
		testV2.testDeleteObjectVersioning();
	}

	/**
	 * [Versioning] HeadObject가 정상 동작하는지 확인
	 */
	@Test
	@Tag("HEAD")
	void testHeadObjectVersioning() {
		testV2.testHeadObjectVersioning();
	}

	/**
	 * [Versioning] CopyObject가 정상 동작하는지 확인
	 */
	@Test
	@Tag("COPY")
	void testCopyObjectVersioning() {
		testV2.testCopyObjectVersioning();
	}

	/**
	 * [Versioning] MultipartUpload가 정상 동작하는지 확인
	 */
	@Test
	@Tag("MULTIPART")
	void testMultipartUploadVersioning() {
		testV2.testMultipartUploadVersioning();
	}

	/**
	 * [Versioning] PutObjectAcl가 정상 동작하는지 확인
	 */
	@Test
	@Tag("ACL")
	void testPutObjectAclVersioning() {
		testV2.testPutObjectAclVersioning();
	}

	/**
	 * [Versioning] GetObjectAcl가 정상 동작하는지 확인
	 */
	@Test
	@Tag("ACL")
	void testGetObjectAclVersioning() {
		testV2.testGetObjectAclVersioning();
	}

	/**
	 * [Versioning] PutObjectTagging가 정상 동작하는지 확인
	 */
	@Test
	@Tag("TAGGING")
	void testPutObjectTaggingVersioning() {
		testV2.testPutObjectTaggingVersioning();
	}

	/**
	 * [Versioning] GetObjectTagging가 정상 동작하는지 확인
	 */
	@Test
	@Tag("TAGGING")
	void testGetObjectTaggingVersioning() {
		testV2.testGetObjectTaggingVersioning();
	}

	/**
	 * [Versioning] DeleteObjectTagging가 정상 동작하는지 확인
	 */
	@Test
	@Tag("TAGGING")
	void testDeleteObjectTaggingVersioning() {
		testV2.testDeleteObjectTaggingVersioning();
	}

	/**
	 * [Versioning] PutObjectRetention가 정상 동작하는지 확인
	 */
	@Test
	@Tag("RETENTION")
	void testPutObjectRetentionVersioning() {
		testV2.testPutObjectRetentionVersioning();
	}

	/**
	 * [Versioning] GetObjectRetention가 정상 동작하는지 확인
	 */
	@Test
	@Tag("RETENTION")
	void testGetObjectRetentionVersioning() {
		testV2.testGetObjectRetentionVersioning();
	}

	/**
	 * PutObject 복제가 정상 동작하는지 확인
	 */
	@Test
	@Tag("Replication")
	void testPutObjectReplication() {
		testV2.testPutObjectReplication();
	}

	/**
	 * PutObject 태그가 복제되는지 확인
	 */
	@Test
	@Tag("Replication")
	void testPutObjectTaggingReplication() {
		testV2.testPutObjectTaggingReplication();
	}

	/**
	 * PutObject 헤더와 메타데이터가 복제되는지 확인
	 */
	@Test
	@Tag("Replication")
	void testPutObjectMetadataReplication() {
		testV2.testPutObjectMetadataReplication();
	}

	/**
	 * CopyObject 복제가 정상 동작하는지 확인
	 */
	@Test
	@Tag("Replication")
	void testCopyObjectReplication() {
		testV2.testCopyObjectReplication();
	}

	/**
	 * CopyObject 태그가 복제되는지 확인
	 */
	@Test
	@Tag("Replication")
	void testCopyObjectTaggingReplication() {
		testV2.testCopyObjectTaggingReplication();
	}

	/**
	 * CopyObject 헤더와 메타데이터가 복제되는지 확인
	 */
	@Test
	@Tag("Replication")
	void testCopyObjectMetadataReplication() {
		testV2.testCopyObjectMetadataReplication();
	}

	/**
	 * CopyObject 메타데이터가 Replace되었을 경우 복제되는지 확인
	 */
	@Test
	@Tag("Replication")
	void testCopyObjectMetadataReplicationReplace() {
		testV2.testCopyObjectMetadataReplicationReplace();
	}

	/**
	 * MultipartUpload 복제가 정상 동작하는지 확인
	 */
	@Test
	@Tag("Replication")
	void testMultipartUploadReplication() {
		testV2.testMultipartUploadReplication();
	}

	/**
	 * MultipartUpload 태그가 복제되는지 확인
	 */
	@Test
	@Tag("Replication")
	void testMultipartUploadTaggingReplication() {
		testV2.testMultipartUploadTaggingReplication();
	}

	/**
	 * MultipartUpload 헤더와 메타데이터가 복제되는지 확인
	 */
	@Test
	@Tag("Replication")
	void testMultipartUploadMetadataReplication() {
		testV2.testMultipartUploadMetadataReplication();
	}
}
