# Java(testV2) ↔ Go 테스트 정합표

기준 스위트: **`java/src/main/java/org/example/testV2`** (베이스). Go는 aws-sdk-go-v2, 단일 `s3tests` 패키지.

## 요약
- Java testV2 시나리오 **770**개 ↔ Go 클래스 테스트 **740**개 구현 + Go 전용 헬퍼(`backend`/`s3`) 41개.
- 미구현 **30**개 = 의도적 비활성 **24** + go-v2 표현 불가 **6**. 전부 사유가 문서/주석에 남아 있음.
- 명명 규칙: Java `testXxx` ↔ Go `TestXxx` 1:1. Java가 서로 다른 클래스에서 같은 메서드명을 쓰는 경우만 충돌한 쪽에 클래스 접두사(예: `TestListObjectsVersionsVersioningObjListMarker`).

## 클래스별 1:1 대조

| Java 클래스 (testV2) | Java | Go 파일 | Go | 상태 |
|---|---:|---|---:|---|
| ACL | 46 | acl_test.go | 46 | ✅ 일치 |
| Accelerate | 4 | accelerate_test.go | 0 | ③ 의도적 전체 주석 |
| Access | 6 | access_test.go | 6 | ✅ 일치 |
| Analytics | 6 | analytics_test.go | 0 | ③ 의도적 전체 주석 |
| CSE | 11 | cse_test.go | 11 | ✅ 일치 |
| CopyObject | 62 | copy_object_test.go | 62 | ✅ 일치 |
| Cors | 4 | cors_test.go | 0 | ③ 의도적 전체 주석 |
| DeleteBucket | 3 | delete_bucket_test.go | 3 | ✅ 일치 |
| DeleteObjects | 21 | delete_objects_test.go | 21 | ✅ 일치 (② 명명 정리) |
| GetObject | 35 | get_object_test.go | 35 | ✅ 일치 |
| GetObjectAttributes | 15 | get_object_attributes_test.go | 13 | ④ async 2개 주석 |
| Grants | 35 | grants_test.go | 35 | ✅ 일치 |
| Inventory | 17 | inventory_test.go | 17 | ✅ 일치 |
| KMS | 0 | kms_test.go | 0 | ✅ 양쪽 의도적 비움 |
| LifeCycle | 20 | lifecycle_test.go | 20 | ✅ 일치 |
| ListBuckets | 7 | list_buckets_test.go | 7 | ✅ 일치 |
| ListObjects | 40 | list_objects_test.go | 40 | ✅ 일치 |
| ListObjectsV2 | 42 | list_objects_v2_test.go | 42 | ✅ 일치 |
| ListObjectsVersions | 40 | list_objects_versions_test.go | 40 | ✅ 일치 (② 명명 정리) |
| Lock | 36 | lock_test.go | 36 | ✅ 일치 |
| Logging | 8 | logging_test.go | 8 | ✅ 일치 |
| Metrics | 14 | metrics_test.go | 14 | ✅ 일치 |
| Multipart | 48 | multipart_test.go | 48 | ✅ 일치 |
| Notification | 4 | notification_test.go | 4 | ✅ 일치 |
| Ownership | 7 | ownership_test.go | 7 | ✅ 일치 |
| Payment | 3 | payment_test.go | 0 | ③ 의도적 전체 주석 |
| Policy | 21 | policy_test.go | 21 | ✅ 일치 |
| Post | 36 | post_test.go | 36 | ✅ 일치 |
| PutBucket | 24 | put_bucket_test.go | 24 | ✅ 일치 |
| PutObject | 48 | put_object_test.go | 45 | ④ 2개 구현·3개 주석 |
| Replication | 6 | replication_test.go | 6 | ✅ 일치 |
| SSE_C | 20 | sse_c_test.go | 20 | ✅ 일치 |
| SSE_S3 | 25 | sse_s3_test.go | 24 | ④ 1개 구현·1개 주석 |
| SelectObjectContent | 7 | select_object_content_test.go | 0 | ③ 의도적 전체 주석(사유 명시) |
| Taggings | 13 | taggings_test.go | 13 | ✅ 일치 |
| Versioning | 33 | versioning_test.go | 33 | ✅ 일치 (② 밑줄 제거) |
| Website | 3 | website_test.go | 3 | ✅ 일치 |
| — | — | backend_test.go / s3_test.go | 41 | Go 전용 백엔드·공용 헬퍼 |

## ② 명명 정리 (밑줄 제거 → PascalCase)
Java는 서로 다른 클래스에서 동일 메서드명을 (다른 내용으로) 정의하지만, Go는 단일 패키지라 함수명이 유일해야 한다. 이전에는 `versioning_test.go` 쪽에 밑줄을 붙여 회피했으나, 원 클래스(Versioning)가 깨끗한 이름을 갖도록 하고 충돌한 복사본에 클래스 접두사를 붙였다.

| Java 메서드 (클래스) | 이전 Go | 현재 Go |
|---|---|---|
| `testVersioningObjListMarker` (Versioning) | `TestVersioning_ObjListMarker` | `TestVersioningObjListMarker` |
| `testVersioningObjListMarker` (ListObjectsVersions) | `TestVersioningObjListMarker` | `TestListObjectsVersionsVersioningObjListMarker` |
| `testVersioningMultiObjectDeleteWithMarker` (Versioning) | `TestVersioning_MultiObjectDeleteWithMarker` | `TestVersioningMultiObjectDeleteWithMarker` |
| `testVersioningMultiObjectDeleteWithMarker` (DeleteObjects) | `TestVersioningMultiObjectDeleteWithMarker` | `TestDeleteObjectsVersioningMultiObjectDeleteWithMarker` |
| `testVersioningMultiObjectDeleteWithMarkerCreate` (Versioning) | `TestVersioning_MultiObjectDeleteWithMarkerCreate` | `TestVersioningMultiObjectDeleteWithMarkerCreate` |
| `testVersioningMultiObjectDeleteWithMarkerCreate` (DeleteObjects) | `TestVersioningMultiObjectDeleteWithMarkerCreate` | `TestDeleteObjectsVersioningMultiObjectDeleteWithMarkerCreate` |
| `testVersioningMultiObjectDeleteWithMarkerCreateObjects` (DeleteObjects) | `TestVersioningMultiObjectDeleteWithMarkerCreateObjects` | `TestDeleteObjectsVersioningMultiObjectDeleteWithMarkerCreateObjects` |

## ③ 의도적 비활성 (미변경)
서버/정책상 의도적으로 전체 주석 처리된 클래스. 사용자 지시에 따라 손대지 않음.
`Accelerate`(4), `Analytics`(6), `Cors`(4), `Payment`(3), `SelectObjectContent`(7). `KMS`는 양쪽 모두 빈 상태(의도적).

## ④ go-v2 표현 불가 처리
근거: aws-sdk-go-v2 `s3.Options`에 `chunkedEncodingEnabled`·`disablePayloadSigning` 옵션이 없고, 별도 `S3AsyncClient`도 없다. 반면 presign은 항상 SigV4, `RequestChecksumCalculation`/`ResponseChecksumValidation`는 지원한다.

### 구현 (3)
| Java 테스트 | Go | 방식 |
|---|---|---|
| `testPutObjectChecksumUseChunkEncoding` | `TestPutObjectChecksumUseChunkEncoding` | UseChunkEncoding=go 기본 전송. 동기 경로만(async 미지원 주석) |
| `testPutObjectSpecialCharactersUseChunkEncoding` | `TestPutObjectSpecialCharactersUseChunkEncoding` | 특수문자 업로드+List 검증(청크=기본) |
| `testSseS3BucketPresignedUrlPutGetV4` | `TestSseS3BucketPresignedUrlPutGetV4` | go presign은 항상 SigV4 → 기본 presign과 동치 |

### 사유 주석만 (6)
| Java 테스트 | Go 파일 | 사유 |
|---|---|---|
| `testPutObjectSpecialCharactersNotChunkEncoding` | put_object_test.go | chunkedEncodingEnabled(false) 옵션 없음 |
| `testPutObjectSpecialCharactersNotChunkEncodingAndDisablePayloadSigning` | put_object_test.go | 위 + disablePayloadSigning 없음 |
| `testPutObjectUseSpecialCharactersChunkEncodingAndDisablePayloadSigning` | put_object_test.go | disablePayloadSigning 없음 |
| `testSseS3BucketPutGetNotChunkEncoding` | sse_s3_test.go | chunkedEncodingEnabled(false) 옵션 없음 |
| `testGetObjectAttributesAsync` | get_object_attributes_test.go | 별도 S3AsyncClient 없음 |
| `testGetObjectAttributesAsyncError` | get_object_attributes_test.go | 별도 S3AsyncClient 없음 |
