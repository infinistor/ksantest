# dotnet s3tests — AWS 실행 실패 108건 수정 계획

원본: `xunit-to-html/Result_dotnet.xml` (AWS `ap-northeast-2`, 계정 `635518764071`)
결과: **794건 중 108 실패 / 2 스킵 / 684 통과**

## 0. 가장 중요한 사실 — Java가 이미 정답을 갖고 있다

동일 AWS 계정으로 돌린 다른 언어 결과와 교차 대조한 결과:

| 스위트 | tests | failures |
|---|---|---|
| **java** | 790 | **4** |
| go | 800 | 14 (skip 30) |
| python | 778 | 14 |
| **dotnet** | **794** | **108** |

.NET 실패 108건을 Java 테스트명(camelCase)으로 매핑하면:

- **104건 → Java에서 PASS**
- 4건 → Java에 대응 테스트 없음 (Cors 4건, Java는 `@Disabled`)

즉 **AWS 서버 문제가 아니라 .NET 포팅이 Java testV2를 따라가지 못한 것**이다.
Java의 남은 실패 4건(`testObjectAclRevokeAll`, `testBucketAclRevokeAll`, `testPutObjectKeyTooLong`,
`testPutObjectKeyUnicodeCharactersTooLong`)은 구 `s3tests` 래퍼 소속이며 .NET 실패 목록에 없다.

→ **작업 원칙: 추측하지 말고 `java/src/main/java/org/example/testV2/` 의 대응 구현을 1:1로 이식한다.**

---

## 1. 원인별 요약

| # | 원인 | 건수 | 수정 위치 | 난이도 |
|---|---|---|---|---|
| A | 버킷 Public Access Block 미해제 | 25 | 각 테스트의 `GetNewBucket` → `GetNewBucketCannedAcl` | 낮음 |
| B | ObjectOwnership 기본값 `BucketOwnerEnforced` → ACL 거부 | 9 | `CreateBucketWithAcl(client, ObjectWriter, acl)` 사용 | 낮음 |
| C | POST 폼이 SigV2 (`AWSAccessKeyId`/`Signature`) | 15 | 기존 `SignPostPolicy` 헬퍼로 교체 | 중간(반복) |
| D | `MyHttpClient` 리전 하드코딩 `us-west-2` | 3 | `Config.S3.RegionName` 주입 | 낮음 |
| E | AWS 신규 버킷 SSE-C 기본 차단 | 20 | `UnblockSseC` 헬퍼 신설 | 중간 |
| F | AWS 기본 SSE-S3(AES256) → `Assert.Null` 실패 | 10 | `TestObjectCopy` 단언에 `\|\| Config.S3.IsAWS` | 낮음 |
| G | 체크섬 헤더만 전송(값/트레일러 없음) | 9 | 체크섬 값 선계산 or chunk-encoding 유지 | 중간 |
| H | 메타데이터 비교(순서/prefix) | 2 | 순서 무시 + `x-amz-meta-` 정규화 | 낮음 |
| I | 개별 이슈 15건 | 15 | 아래 §2.I | 혼재 |
| | **합계** | **108** | | |

---

## 2. 세션(클래스)별 상세

### A. Public Access Block 미해제 — 25건

에러: `... is not authorized to perform: s3:PutBucketAcl / s3:PutBucketPolicy ... because public ACLs(policies) are prevented by the BlockPublicAcls(BlockPublicPolicy) setting`

AWS는 2023-04부터 신규 버킷에 BPA 4종을 기본 ON으로 만든다.
`TestBase.cs`에는 이미 해제 헬퍼가 있는데(`GetNewBucketCannedAcl` [TestBase.cs:1077](Test/TestBase.cs:1077),
`CreateBucketWithAcl` [TestBase.cs:1171](Test/TestBase.cs:1171)) 해당 테스트들이 맨 `GetNewBucket()`을 쓰고 있다.

Java 대조 — `Policy.testBucketPolicy`는 `createBucketCannedAcl(client, 1)` 사용
([Policy.java:53](../java/src/main/java/org/example/testV2/Policy.java)), .NET은 `GetNewBucket(client)`
([Policy.cs:34](Test/Policy.cs:34)). Java `createBucketCannedAcl`은 `ObjectOwnership=OBJECT_WRITER` +
BPA 4종 false와 정확히 동일하다.

**수정: `var bucketName = GetNewBucket(client);` → `GetNewBucketCannedAcl(client);`**

- **Policy (19)** — `Policy.java`가 `createBucketCannedAcl`을 24곳에서 쓴다. 전 테스트 일괄 교체.
  `TestBucketPolicy`, `TestBucketV2Policy`, `TestBucketPolicyAcl`, `TestBucketV2PolicyAcl`,
  `TestBucketPolicyPutObjAcl`, `TestBucketPolicyPutObjGrant`, `TestBucketPolicyPutObjCopySource`,
  `TestBucketPolicyPutObjCopySourceMeta`, `TestBucketPolicyGetObjExistingTag`,
  `TestBucketPolicyGetObjAclExistingTag`, `TestBucketPolicyGetObjTaggingExistingTag`,
  `TestBucketPolicyPutObjTaggingExistingTag`, `TestBucketPolicyStatusWithAllUser`,
  `TestBucketPolicyStatusWithTagCondition`, `TestBucketPolicyStatusWithTimeCondition`,
  `TestBucketPolicyStatusWithWideIPRange`, `TestPutTagsAclPublic`, `TestGetTagsAclPublic`,
  `TestDeleteTagsObjPublic`
- **Access (1)** — `TestIgnorePublicAcls`
- **ListObjectsVersions (1)** — `TestBucketListVersionsObjectsAnonymous`
- **Versioning (1)** — `TestVersionedObjectAclNoVersionSpecified` (`s3:PutObjectAcl` 차단)
- **Cors (3)** — `TestCorsHeaderOption`, `TestCorsOriginWildcard`, `TestCorsOriginResponse`
  → **주의: 이 3건은 Java에서 `@Disabled`, Go에서도 `t.Skip`.**
  KSAN의 OPTIONS 응답 기대치가 AWS/Ceph와 갈려서 의도적으로 꺼둔 것이므로
  BPA만 풀어도 통과하지 않는다. **다른 언어와 동일하게 `[Fact(Skip=...)]` 처리**가 정답.

### B. ObjectOwnership BucketOwnerEnforced — 9건

에러: `The bucket does not allow ACLs` / `Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting`

AWS 신규 버킷 기본값이 `BucketOwnerEnforced`라 ACL 자체가 금지된다.
`PutBucket(bucketName, acl: ...)` 형태를 `CreateBucketWithAcl(client, ObjectOwnership.ObjectWriter, acl)`로 교체.

- **Post (6)** — `TestPostObjectAnonymousRequest`([Post.cs:38](Test/Post.cs:38)),
  `TestPostObjectAuthenticatedRequestBadAccessKey`, `TestPostObjectAuthenticatedNoContentType`,
  `TestPostObjectSetSuccessCode`, `TestPostObjectSetInvalidSuccessCode`, `TestPostObjectSuccessRedirectAction`
- **CopyObject (2)** — `TestObjectCopyCannedAcl`, `TestObjectCopyNotOwnedObjectBucket`
- **PutObject (1)** — `TestBucketCreateSpecialKeyNames` (`SetupObjects` 후 `PutObjectACL` 호출)

### C. POST 폼 서명 SigV2 → SigV4 — 15건

에러: `The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.` → 400

`ap-northeast-2`를 포함한 2014년 이후 리전은 POST 폼의 SigV2를 지원하지 않는다.
**헬퍼는 이미 있다**: `PostPolicyV4` / `SignPostPolicy` ([TestBase.cs:166–214](Test/TestBase.cs:166)).
`TestPostObjectAuthenticatedRequest` 한 건만 이미 이식되어 통과 중이고 ([Post.cs:83](Test/Post.cs:83)),
나머지는 여전히 `{ "AWSAccessKeyId", ... }` + `{ "Signature", ... }` 폼을 만든다.

**수정 패턴** (기존 통과 테스트와 동일하게):
```csharp
var sign = SignPostPolicy(policyDocument);   // 또는 SignPostPolicy(policyDocument, user)
var payload = new Dictionary<string, object>() { { "key", key }, /* ... */ };
sign.Apply(payload);                          // policy/x-amz-* 5개 필드 주입
```

- **Post (13)** — `TestPostObjectExpiredPolicy`, `TestPostObjectInvalidAccessKey`,
  `TestPostObjectInvalidSignature`, `TestPostObjectMissingPolicyCondition`,
  `TestPostObjectRequestMissingPolicySpecifiedField`, `TestPostObjectInvalidRequestFieldValue`,
  `TestPostObjectUserSpecifiedHeader`, `TestPostObjectIgnoredHeader`,
  `TestPostObjectCaseInsensitiveConditionFields`, `TestPostObjectEscapedFieldValues`,
  `TestPostObjectSetKeyFromFilename`, `TestPostObjectUploadLargerThanChunk`, `TestPostObjectWrongBucket`
- **SSE_C (1)** — `TestEncryptionSseCPostObjectAuthenticatedRequest` ([SSE_C.cs:374](Test/SSE_C.cs:374))
- **Taggings (1)** — `TestPostObjectTagsAuthenticatedRequest` ([Taggings.cs:303](Test/Taggings.cs:303))

> 실패 케이스(`InvalidAccessKey`, `InvalidSignature`, `ExpiredPolicy` 등)는 **SigV4로 서명한 뒤
> 해당 필드만 망가뜨려야** 원래 의도한 403/404가 나온다. 지금은 서명 방식 자체가 거부돼 전부 400이다.

### D. MyHttpClient 리전 하드코딩 — 3건

에러: `The authorization header is malformed; the region 'us-west-2' is wrong; expecting 'ap-northeast-2'`

[Client/MyHttpClient.cs:56, 79, 124](Client/MyHttpClient.cs:56) 세 곳에 `Region = "us-west-2"` 고정.

**수정**: 생성자에 `region` 파라미터 추가 → `Config.S3.RegionName`(빈 값이면 `us-east-1`) 주입.
`SignPostPolicy`가 쓰는 리전 결정 로직([TestBase.cs:195](Test/TestBase.cs:195))과 동일하게 맞춘다.

- **Post (3)** — `TestPutObjectV4`, `TestPutObjectChunkedV4`, `TestGetObjectV4`

### E. AWS SSE-C 기본 차단 해제 — 20건

에러: `... because this bucket has blocked upload requests that specify server-side encryption with customer-provided keys`

AWS가 2026-04부터 신규 버킷에 SSE-C를 기본 차단한다. 해제하려면 `PutBucketEncryption`에
`BlockedEncryptionTypes = [NONE]`을 넣어야 한다.

**Java/Python/Go는 이미 헬퍼가 있다** — Java `unblockSseC`
([TestBase.java:1020](../java/src/main/java/org/example/testV2/TestBase.java)), Go `unblockSseC`
(`go/sse_c_test.go`). .NET에만 없다.

**좋은 소식**: Go는 SDK에 필드가 없어 raw 서명 요청을 직접 짰지만,
**AWSSDK.S3 4.0.101.3에는 `ServerSideEncryptionRule.BlockedEncryptionTypes`가 이미 있다**
(어셈블리 확인 완료: `Amazon.S3.Model.BlockedEncryptionTypes.EncryptionType`, `Amazon.S3.EncryptionType.NONE/SSEC`).
→ **SDK 그대로 쓰면 되고 raw HTTP 서명 불필요.**

**신설: `TestBase.UnblockSseC(string bucketName)`**
```csharp
public void UnblockSseC(string bucketName)
{
    if (!Config.S3.IsAWS) return;
    var client = GetClient();
    client.PutBucketEncryption(bucketName, new ServerSideEncryptionConfiguration()
    {
        ServerSideEncryptionRules = [ new() {
            ServerSideEncryptionByDefault = new() {
                ServerSideEncryptionAlgorithm = ServerSideEncryptionMethod.AES256 },
            BlockedEncryptionTypes = new BlockedEncryptionTypes {
                EncryptionType = [EncryptionType.NONE] },
        }]
    });
    // Java와 동일: 반영까지 최대 5회 * 1s 재확인 (GetBucketEncryption에 SSE-C가 남아있는지)
}
```
`S3Client.PutBucketEncryption` 래퍼가 `BlockedEncryptionTypes`를 통과시키는지 함께 확인 필요.

**호출 지점** (Java 호출 위치와 1:1):
- **SSE_C (16)** — `TestEncryptedTransfer1b`, `TestEncryptedTransfer1kb`, `TestEncryptedTransfer13b`,
  `TestEncryptedTransfer1MB`, `TestEncryptionSseCPresent`, `TestEncryptionSseCOtherKey`,
  `TestEncryptionSseCNoMd5`, `TestEncryptionSseCMethodHead`, `TestEncryptionSseCGetObjectMany`,
  `TestEncryptionSseCRangeObjectMany`, `TestEncryptionSseCMultipartUpload`,
  `TestEncryptionSseCMultipartBadDownload`, `TestEncryptionSseCMultipartUploadOverwriteExistingObject`,
  `TestEncryptionSseCPutObjectOverwriteMultipartUpload`, `TestSseCEncryptionMultipartCopyPartUpload`,
  `TestSseCEncryptionMultipartCopyMany`
  → `TestBase.TestEncryptionSSECustomerWrite` ([TestBase.cs:1535](Test/TestBase.cs:1535))에도 추가
- **CopyObject (4)** — `TestCopyToSseCSource`, `TestCopyRevokeSseAlgorithm`,
  `TestCopyToSseS3Source`, `TestCopyToNormalSource`
  → 후자 둘은 `The encryption parameters are not applicable to this object` — `EncryptionType` 조합
  루프가 SSE_C 타깃을 포함하므로, Java `testObjectCopy(testId, prefix, EncryptionType, EncryptionType, size)`
  ([TestBase.java:2013–2020](../java/src/main/java/org/example/testV2/TestBase.java))처럼
  **`source == SSE_C || target == SSE_C`일 때 `UnblockSseC(bucketName)`** 호출.
  .NET 대응은 `TestObjectCopy(EncryptionType, EncryptionType, int)` [TestBase.cs:1672](Test/TestBase.cs:1672).

### F. AWS 기본 SSE-S3(AES256) 단언 — 10건

에러: `Assert.Null() Failure: Value is not null / Actual: AES256`

AWS는 2023-01부터 모든 오브젝트에 SSE-S3를 기본 적용하므로 `ServerSideEncryptionMethod`가 절대 null이 아니다.
Java는 이미 `|| config.isAWS()` 조건을 넣어놨다
([TestBase.java:1990, 2007](../java/src/main/java/org/example/testV2/TestBase.java)).

**수정: `TestBase.TestObjectCopy(bool,bool,bool,bool,int)` [TestBase.cs:1623, 1666](Test/TestBase.cs:1607)**
```csharp
if (sourceObjectEncryption || sourceBucketEncryption || Config.S3.IsAWS)
    Assert.Equal(ServerSideEncryptionMethod.AES256, sourceResponse.ServerSideEncryptionMethod);
else Assert.Null(...);
// dest 쪽도 동일하게 destBucketEncryption || destObjectEncryption || Config.S3.IsAWS
```
헬퍼 한 곳만 고치면 **10건 동시 해결**:
`TestCopyNorSrcToNorBucketAndObj`, `TestCopyNorSrcToNorBucketEncryptionObj`,
`TestCopyNorSrcToEncryptionBucketNorObj`, `TestCopyNorSrcToEncryptionBucketAndObj`,
`TestCopyEncryptionSrcToNorBucketAndObj`, `TestCopyEncryptionBucketAndObjToNorBucketAndObj`,
`TestCopyEncryptionBucketNorObjToNorBucketAndObj`, `TestCopyEncryptionBucketNorObjToNorBucketEncryptionObj`,
`TestCopyEncryptionBucketNorObjToEncryptionBucketNorObj`, `TestCopyEncryptionBucketNorObjToEncryptionBucketAndObj`

> 부가: Java는 `PutBucketEncryption` 비교 시 응답에 `BlockedEncryptionTypes`가 섞여 나오므로
> **설정할 때도 명시**해서 비교한다. .NET도 `GetBucketEncryption` 결과를 통째로 비교한다면 동일 처리 필요.

### G. 체크섬 — 9건

에러: `x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found.`

`RequestChecksumCalculation.WHEN_REQUIRED` + `useChunkEncoding: false` 조합에서 SDK v4가
알고리즘 헤더만 붙이고 체크섬 값도 트레일러도 안 만든다(트레일러는 aws-chunked 필요).
Java는 `useChunkEncoding`을 건드리지 않고, SDK가 못 만드는 알고리즘(MD5)만 값을 선계산한다
(`CheckSum.applyChecksum`, [CheckSum:131](../java/src/main/java/org/example/testV2/)).

**방향 (택1, Java 의미 보존 우선순위대로)**
1. `S3Client.PutObject/UploadPart` 래퍼에서 `checksumAlgorithm != null && useChunkEncoding == false`이면
   `CheckSum.CalculateChecksum`으로 값을 선계산해 `SetChecksum` — 검증 대상(최종 체크섬 값)은 그대로 유지
2. 또는 `useChunkEncoding: false`를 빼고 SDK 트레일러 경로를 쓰게 둔다
   (단 `...UseChunkEncoding` 계열 테스트는 chunk 경로 자체가 대상이므로 1번을 써야 함)

- **PutObject (2)** — `TestPutObjectChecksum` ([PutObject.cs:595](Test/PutObject.cs:595)), `TestPutObjectChecksumUseChunkEncoding`
- **Multipart (3)** — `TestMultipartUploadChecksum`, `TestMultipartUploadChecksumUseChunkEncoding`,
  `TestcreateMultipartUploadEmptyChecksumType` (모두 `S3Client.UploadPart` [S3Client.cs:1594](Test/S3Client.cs:1594) 경유)
- **GetObjectAttributes (2)** — `TestGetObjectAttributesWithChecksum`, `TestGetObjectAttributesAllAttributes`
- **CopyObject (1)** — `TestCopyObjectChecksumUseChunkEncoding`
- **Lock (1)** — `TestObjectLockMultipart`:
  `Content-MD5 OR x-amz-checksum-* HTTP header is required for Put Part requests with Object Lock parameters`
  → Object Lock 버킷의 UploadPart는 반드시 무결성 헤더가 필요. 위 1번 방식으로 함께 해결.

### H. 메타데이터 비교 — 2건

- **CopyObject `TestObjectCopyRetainingMetadata`** — 내용은 같고 **순서만 다름**
  (`Expected [key1, key2]` vs `Actual [key2, key1]`). `Assert.Equal`이 순서 민감.
  → 키 기준 조회 비교 또는 정렬 후 비교로 변경.
- **GetObjectAttributes `TestGetObjectAttributesWithMetadata`** — `Expected ["custom-key1"]` vs
  `Actual ["x-amz-meta-custom-key1"]`. .NET `MetadataCollection`은 `x-amz-meta-` prefix를 유지하고
  Java SDK v2는 벗긴다. → 비교 전에 prefix 제거 정규화. `TestBase.GetMetaData` [TestBase.cs:929](Test/TestBase.cs:929) 확장 권장.

### I. 개별 이슈 — 15건

| 테스트 | 현상 | 수정 방향 |
|---|---|---|
| **CSE** `TestCseEncryptedTransfer1b`, `TestCseEncryptedTransfer13b` | 복호 결과가 `""` | **로컬 버그.** [AES256.cs `AESDecrypt`](Test/AES256.cs)가 `CryptoStream` 종료(final block flush) 전에 `ms.ToArray()`를 호출한다. `using` 선언문이라 dispose가 메서드 끝이라 마지막 블록이 안 써짐 → 1블록 미만 입력(1B/13B)만 빈 문자열. `cs.FlushFinalBlock()` 후 `ToArray()` 호출. AWS 무관 |
| **Cors** `TestSetCors` | `GetCORSConfiguration`이 `The CORS configuration does not exist` 예외 | AWS는 미설정 시 **404 `NoSuchCORSConfiguration`**을 던진다. Go가 `assertNoCors`로 404를 기대해 통과 중(`go/cors_test.go:71`). `Assert.Null(response.Configuration)` → 404+에러코드 검증으로 교체 (호출 2곳) |
| **Multipart** `TestUploadPartCopyIfMatchAndIfNoneMatch`, `...Any` | 501 기대, 412 수신 | `CopyPartRequest.ETagToMatch/ETagsToNotMatch`는 `x-amz-copy-source-if-*`(소스 조건)로 나가 412가 된다. Java는 **raw `If-Match`/`If-None-Match`**를 대상 오브젝트에 붙여 501을 받는다([Multipart.java:1063](../java/src/main/java/org/example/testV2/Multipart.java)). `S3Client.CopyPart`에 헤더 직접 주입 경로 추가 |
| **Multipart** `TestMultipartCopyChecksum` | CRC 값 불일치 (`iX/wHw==` vs `maNqkA==`) | `CrcCombine`(파트 결합) 또는 파트 분할 크기 계산 문제. Java `crc32(List<byte[]>)` 결과와 대조해 [Utils/CrcCombine.cs](Utils/CrcCombine.cs) 검증 필요 |
| **Grants** `TestBucketAclNoGrants` | `PutBucketACL(Grants=[])` → MalformedXML | 빈 Grants를 AWS가 거부. Java 대응 구현 확인 후 정렬 ([Grants.cs:514](Test/Grants.cs:514)) |
| **Inventory** `TestPutBucketInventoryIdNotExist` | 400 기대, `response.HttpStatusCode == 0` | 예외가 안 나고 응답 객체의 상태코드가 미설정. `CheckErrorResponse` [TestBase.cs:895](Test/TestBase.cs:895)의 v4 대응 분기가 실제 동작과 안 맞음. **로컬 재현 후 판정 필요** |
| **Inventory** `TestPutBucketInventoryInvalidCase` | 예외 미발생 | 동상. Java는 `IncludedObjectVersions("CUrrENT")` 문자열을 그대로 전송해 400을 받는다. .NET은 `InventoryIncludedObjectVersions` 타입이라 잘못된 값이 전송되지 않을 가능성 → raw 값 전송 경로 필요 |
| **Metrics** `TestPutMetricsEmptyId` | 400 기대, `0` 수신 | Inventory와 동일 패턴. 함께 처리 |
| **Replication** `TestReplicationSet` | MalformedXML | `Filter = null` + `DeleteMarkerReplication` 조합을 AWS가 거부(V2 스키마는 Filter 필수). Java 구성과 대조 |
| **Replication** `TestReplicationBucketVersioningSuspend` | `iam:PassRole` 권한 없음 | **테스트 코드가 아닌 계정 권한 문제.** `arn:aws:iam::635518764071:role/awsreplicationtest`에 대한 `iam:PassRole`을 `user/Main`에 부여해야 함. 별도 인프라 작업 |
| **SSE_S3** `TestSseS3NotRetroactive` | `NoSuchKey` | 멀티파트(`SetupMultipartUpload`) 오브젝트 조회 실패. 완료 호출 누락 또는 키 불일치 가능성 — [SSE_S3.cs:624](Test/SSE_S3.cs:624) 라인 기준 추적 |
| **PutObject** `TestPutObjectKeyWithConsecutiveSlashes` | SignatureDoesNotMatch | `//` 연속 슬래시 키에서 .NET SDK v4의 URI 정규화가 서명 대상 경로를 바꿔버림. Java는 통과. SDK 옵션(`ForcePathStyle` 등) 조합 실측 필요 |
| **PutObject** `TestPutObjectKeySpecialCharactersAtStart` | SignatureDoesNotMatch | 동상 (선두 특수문자 인코딩). 위와 함께 처리 |

---

## 3. 실행 순서

효과/비용 비로 정렬. 각 단계 후 해당 클래스만 재실행해 확인한다.

| 단계 | 작업 | 해결 | 누적 |
|---|---|---|---|
| 1 | **F** — `TestObjectCopy` 단언에 `\|\| Config.S3.IsAWS` (헬퍼 1곳) | 10 | 10 |
| 2 | **A** — `GetNewBucket` → `GetNewBucketCannedAcl` 일괄 (Cors 3건은 Skip 처리) | 25 | 35 |
| 3 | **B** — ACL 버킷 생성을 `CreateBucketWithAcl(ObjectWriter, acl)`로 | 9 | 44 |
| 4 | **E** — `UnblockSseC` 헬퍼 신설 + 20곳 호출 | 20 | 64 |
| 5 | **C** — POST 폼 SigV4 이식 (Post 13 + SSE_C 1 + Taggings 1) | 15 | 79 |
| 6 | **D** — `MyHttpClient` 리전 주입 | 3 | 82 |
| 7 | **G** — 체크섬 값 선계산 경로 | 9 | 91 |
| 8 | **H** — 메타데이터 비교 정규화 | 2 | 93 |
| 9 | **I** — 개별 15건 (CSE/Cors/Multipart 우선, Replication PassRole은 인프라) | 15 | 108 |

**1~4단계(헬퍼 중심)만으로 64건, 전체의 59%가 해결된다.**

## 4. 유의사항

- **`Config.S3.IsAWS` 가드를 반드시 넣을 것.** KSAN/GW 대상 실행이 깨지면 안 된다.
  Java `unblockSseC`도 `if (!config.isAWS()) return;`로 시작한다.
- **Cors 3건은 "고치기"가 아니라 "다른 언어와 동일하게 Skip"이 정답이다.**
  Java `@Disabled` / Go `t.Skip` 근거를 주석에 남긴다.
- **`TestReplicationBucketVersioningSuspend`는 코드로 해결 불가.** IAM `iam:PassRole` 부여가 선행돼야 하며,
  이것 없이 "모든 테스트 통과"는 달성되지 않는다.
- 이식할 때는 반드시 `java/src/main/java/org/example/testV2/` 쪽을 본다.
  `java/src/test/java/org/example/s3tests/`는 구 래퍼이고 남은 실패 4건이 거기 있다.
