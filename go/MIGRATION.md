# Go migration status

Java SDK V2 is the primary source. Python ports are used to capture fixes and expected behavior. A class advances only after implementation, compile-only verification, resolver verification, and this document are all updated.

Final static audit: 38 classes, 811 Java scenarios, 808 Python scenarios, and 810 executable/skip Go subtests are accounted for. The three-scenario Python difference is the intentionally commented Cors migration, and the one-scenario Go difference is the intentionally empty KMS type. All 1,617 applicable Java camelCase and Python snake_case method inputs resolve through `start-function` without missing or ambiguous targets.

`go vet ./...`, `go test -run '^$' ./...`, and `git diff --check -- go` pass. Accelerate (4), Analytics (6), Payment (3), and SelectObjectContent (7) report every scenario as an individual SKIP. Backend reports 30 scenarios (Java wrapper-aligned; basic 10 disabled) as individual SKIPs with the supplied empty-endpoint AWS configuration. Compatible-S3 execution and the Python/Go live-result comparison remain pending until credentials and an endpoint are supplied.

The final source scan found no TODO, FIXME, placeholder, panic-only, or unconditional-success tests. Both PowerShell launchers parse without errors, the batch launchers use the same config/resolver contract, and all six launchers preserve the test process exit code. Shell parsing could not be executed on this Windows host because neither WSL bash nor Git Bash is installed; both shell scripts were reviewed directly.

| Class | Java | Python | Go | Status | Notes |
|---|---:|---:|---:|---|---|
| PutBucket | 24 | 24 | 24 | 구현 완료 | Actual S3 run pending |
| DeleteBucket | 3 | 3 | 3 | 구현 완료 | Actual S3 run pending |
| ListBuckets | 7 | 7 | 7 | 구현 완료 | Actual S3 run pending |
| KMS | 1 | 1 | 0 | 의도적 미구현 | Empty `KMS` type only |
| Accelerate | 4 | 4 | 4 | SKIP 구현 완료 | Request/assertion code compiled; always SKIP |
| Access | 6 | 6 | 6 | 구현 완료 | Actual S3 run pending |
| ACL | 46 | 46 | 46 | 구현 완료 | Access matrix, list, bucket/object permission cases; actual S3 run pending |
| Analytics | 6 | 6 | 6 | SKIP 구현 완료 | Request/assertion code compiled; always SKIP |
| Backend | 30 | 40 | 30 | 구현 완료 | Java 래퍼가 basic 10개 주석 처리 → Go도 동일(30). version/delete-marker 헤더 미들웨어 이름 분리 |
| CopyObject | 62 | 62 | 62 | 구현 완료 | Conditions, metadata/tags, versioning and encryption matrices; actual S3 run pending |
| Cors | 4 | 1 | 4 | 구현 완료 | All disabled Java cases are active; actual S3 run pending |
| CSE | 11 | 11 | 11 | 구현 완료 | Java/Python-compatible PBKDF2/AES-256-CBC format, raw/decrypt errors, multipart and repeated/range reads; actual S3 run pending |
| DeleteObjects | 21 | 21 | 21 | 구현 완료 | Quiet, version/delete-marker and conditional-delete coverage; actual S3 run pending |
| GetObject | 35 | 35 | 35 | 구현 완료 | Conditional GET/HEAD, range, response overrides and delete markers; actual S3 run pending |
| GetObjectAttributes | 15 | 15 | 15 | 구현 완료 | Selective/all attributes, versions, multipart and CRC64NVME; actual S3 run pending |
| Grants | 35 | 35 | 35 | 구현 완료 | Canned, ownership, explicit/multi grant and revoke cases; actual S3 run pending |
| Inventory | 17 | 17 | 17 | 구현 완료 | Disabled missing-target case active; CRUD, validation, prefix and optional fields; actual S3 run pending |
| LifeCycle | 20 | 20 | 20 | 구현 완료 | Disabled Expires case active; rule validation, version/delete-marker, noncurrent and multipart cases; actual S3 run pending |
| ListObjects | 40 | 40 | 40 | 구현 완료 | V1 delimiter/prefix/marker/pagination/ACL coverage; actual S3 run pending |
| ListObjectsV2 | 42 | 42 | 42 | 구현 완료 | KeyCount, owner, continuation token and StartAfter coverage; actual S3 run pending |
| ListObjectsVersions | 40 | 40 | 40 | 구현 완료 | Version/key markers, latest ordering and V1-style filtering; actual S3 run pending |
| Lock | 36 | 36 | 36 | 구현 완료 | Object Lock configuration, default/object retention, governance bypass, legal hold, copy/multipart and MD5 requirements; actual S3 run pending |
| Logging | 8 | 8 | 8 | 구현 완료 | Empty/get/set/prefix, versioning/encryption sources and missing source/target errors; actual S3 run pending |
| Metrics | 14 | 14 | 14 | 구현 완료 | CRUD, missing bucket/configuration, ID replacement and prefix/tag/AND filters; actual S3 run pending |
| Multipart | 48 | 48 | 48 | 구현 완료 | Upload/copy, lifecycle, checksum, conditional completion and PutObject overwrite of multipart; actual S3 run pending |
| Notification | 4 | 4 | 4 | 구현 완료 | AWS skips Lambda-dependent cases; actual compatible-S3 run pending |
| Ownership | 7 | 7 | 7 | 구현 완료 | Actual S3 run pending |
| Payment | 3 | 3 | 3 | SKIP 구현 완료 | Request/assertion code compiled; always SKIP |
| Policy | 21 | 21 | 21 | 구현 완료 | Basic, tagging, copy-source, ACL/grant, existing-tag and policy-status cases; actual S3 run pending |
| Post | 36 | 36 | 36 | 구현 완료 | Anonymous/SigV2/SigV4 multipart forms, policy failures, redirect/status, presigned and chunked requests; source AWS skips preserved; actual S3 run pending |
| PutObject | 48 | 48 | 48 | 구현 완료 | Disabled Expires/invalid-metadata cases active; actual S3 run pending |
| Replication | 6 | 6 | 6 | 구현 완료 | Versioning prerequisites, configuration round trip/delete and invalid source/target/state errors; actual S3 run pending |
| SelectObjectContent | 7 | 7 | 7 | SKIP 구현 완료 | Full event-stream request/assertion code compiled; every subtest always SKIP |
| SSE_C | 20 | 20 | 20 | 구현 완료 | SSE-C key/header errors, multipart/copy, signed POST, repeated/range reads and multipart/PutObject overwrite; POST skips only on AWS; actual S3 run pending |
| SSE_S3 | 25 | 25 | 25 | 구현 완료 | Explicit/default AES256, multipart/copy, chunked body, presigned URL, repeated/range reads, non-retroactive and multipart/PutObject overwrite cases; actual S3 run pending |
| Taggings | 13 | 13 | 13 | 구현 완료 | Bucket/object CRUD, validation limits and signed POST; actual S3 run pending |
| Versioning | 33 | 33 | 33 | 구현 완료 | Disabled delete-marker HEAD case active; mixed Put/MPU version ordering; actual S3 run pending |
| Website | 3 | 3 | 3 | 구현 완료 | Actual S3 run pending |

## Completed mappings

### DeleteBucket

| Java / Python | Go target |
|---|---|
| `testBucketDeleteNotExist` / `test_bucket_delete_not_exist` | `TestDeleteBucket/test_bucket_delete_not_exist` |
| `testBucketDeleteNonempty` / `test_bucket_delete_nonempty` | `TestDeleteBucket/test_bucket_delete_nonempty` |
| `testBucketCreateDelete` / `test_bucket_create_delete` | `TestDeleteBucket/test_bucket_create_delete` |

### ListBuckets

All seven Python snake-case methods map to same-named subtests under `TestListBuckets`.

### PutBucket

All 24 Python snake-case methods map to same-named subtests under `TestPutBucket`.

### 권한과 소유권

| Class | Mapping | Static verification | Runtime verification |
|---|---|---|---|
| `ACL` | Java/Python 46 methods → same-named subtests under `TestACL` | `go vet ./...`, compile-only passed | Pending actual S3 run |
| `Grants` | Java/Python 35 methods → same-named subtests under `TestGrants` | `go vet ./...`, compile-only passed | Pending actual S3 run |
| `Ownership` | Java/Python 7 methods → same-named subtests under `TestOwnership` | `go vet ./...`, compile-only passed | Pending actual S3 run |
| `Access` | Java/Python 6 methods → same-named subtests under `TestAccess` | `go vet ./...`, compile-only passed | Pending actual S3 run |
| `Policy` | Java/Python 21 methods → same-named subtests under `TestPolicy` | `go vet ./...`, compile-only passed | Pending actual S3 run |

The fourth migration milestone contains 115 mapped scenarios in total. Java camelCase and Python snake_case names resolve through `start-function` to their corresponding Go subtest.

### SSE_S3

All 25 Java/Python methods map to same-named subtests under `TestSSES3`. The Go SDK implementation preserves the source sizes and 5 MiB multipart boundaries. The Java HTTPS chunk-encoding variants are represented by unknown-length and known-length Go request bodies. Multipart-overwrite and PutObject-overwrite-of-multipart cases validate AES256 content replacement under default bucket encryption. Static and compile-only verification passed; actual S3 execution is pending.

### SSE_C

All 20 Java/Python methods map to same-named subtests under `TestSSEC`. Customer algorithm, key and MD5 headers are applied to PUT/GET/HEAD, multipart upload and multipart copy requests. The missing-MD5 case removes the SDK-generated header through middleware. The authenticated POST case uses the source-compatible SigV2 multipart form and skips only on AWS. Multipart-overwrite and PutObject-overwrite-of-multipart cases reuse the same SSE-C customer headers. Static and compile-only verification passed; actual S3 execution is pending.

### CSE

All 11 Java/Python methods map to same-named subtests under `TestCSE`. The Go implementation preserves the Python migration format: a 20-byte salt, 16-byte IV and AES-256-CBC ciphertext are Base64 encoded, with the AES key derived using PBKDF2-HMAC-SHA1 for 70,000 iterations. Multipart sizes, encrypted-byte range reads, repeated reads, metadata and invalid plaintext decryption are implemented. Static and compile-only verification passed; actual S3 execution is pending.

### Post

All 36 Java/Python methods map to same-named subtests under `TestPost`. Multipart form upload, anonymous ACL access, SigV2 and SigV4 policy signing, policy field and expiration failures, content-length conditions, redirect/status behavior, metadata, presigned URL and known/unknown-length SigV4 requests are implemented. Compatibility-specific SigV2 cases preserve the source AWS skip conditions; SigV4 and anonymous cases remain active on AWS. Static and compile-only verification passed; actual S3 execution is pending.

### LifeCycle

All 20 Java/Python methods map to same-named subtests under `TestLifeCycle`. Rule round trips, generated IDs, invalid IDs/status/dates, current and noncurrent version expiration, delete markers, filters, incomplete multipart uploads and deletion are implemented. The formerly disabled `test_lifecycle_set_expiration` is active and validates the `Expires` value on both HEAD and GET responses. Static and compile-only verification passed; actual S3 execution is pending.

### Inventory

All 17 Java/Python methods map to same-named subtests under `TestInventory`. Empty/list/get/put/delete behavior, missing configurations and buckets, ID replacement, format/frequency/version validation, destination prefixes and optional fields are implemented. The formerly disabled missing-target-bucket case is active and expects the source-defined 404 `NoSuchBucket` response even on AWS. Static and compile-only verification passed; actual S3 execution is pending.

### Logging

All eight Java/Python methods map to same-named subtests under `TestLogging`. Empty logging state, set/get round trips, target prefixes, versioned and AES256-encrypted source buckets, and missing source/target bucket failures are implemented. Error cases validate both HTTP status and S3 error code. Static and compile-only verification passed; actual S3 execution is pending.

### Metrics

All 14 Java/Python methods map to same-named subtests under `TestMetrics`. Empty/list/get/put/delete behavior, missing configurations and buckets, empty and omitted IDs, duplicate-ID replacement, and prefix/tag/AND filters are implemented. Go SDK filter unions are type-checked after round trips. Static and compile-only verification passed; actual S3 execution is pending.

### Replication

All six Java/Python methods map to same-named subtests under `TestReplication`. Source and target versioning prerequisites, role/destination/prefix/priority/delete-marker configuration round trips, deletion, missing buckets, unversioned targets and prohibited source-versioning suspension are implemented. Static and compile-only verification passed; actual S3 execution is pending.

### SelectObjectContent

All seven Java methods and seven intentionally commented Python methods map to same-named subtests under `TestSelectObjectContent`. CSV header, WHERE, LIMIT, JSON Lines, empty rows, missing bucket and missing object requests are fully implemented. The collector validates Records, Stats and End event-stream messages. Every subtest reports an explicit unconditional SKIP according to the special-class policy; the individual SKIP report was verified locally.

### Backend

Java 래퍼(`java/src/test/.../Backend.java`)가 basic 10개(`Put/Get/Delete/Copy/Multipart` + ACL/Tagging)를 주석 처리하므로 실행 대상은 **30**이다. Go `TestBackend`도 동일하게 versioning 16 + replication 14만 등록한다. basic 케이스 이름은 소스에 주석으로 남겨 두었다.

Backend 클라이언트는 `Backend User` 자격 증명과 IFS/KSAN admin/backend/replication 헤더를 붙인다. versioned PUT/COPY/multipart 및 delete-marker 요청은 클라이언트에 이미 등록된 `backend-headers`와 이름이 겹치지 않도록 `backend-version-headers` / `backend-delete-marker-headers` 미들웨어로 주입한다. AWS(`Config.Endpoint()==""`)에서는 각 subtest SKIP.

### PutObject

All 48 Java/Python methods map to same-named subtests under `TestPutObject`. The implementation covers bucket isolation and missing buckets, zero-byte/ETag/cache/expiry headers, overwrite and metadata replacement, special and Unicode keys, spaces/directories, object-lock headers, conditional PUT requests, key byte-length boundaries and CRC32/CRC32C/SHA1/SHA256 checksum success and failure paths.

The four source-disabled scenarios (`test_object_write_expires` and the three invalid newline-metadata cases) are active. Invalid metadata must fail before or during transmission, while expiry is checked through `HeadObject`. Static, compile-only and all 48 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### GetObject

All 35 Java/Python methods map to same-named subtests under `TestGetObject`. Conditional GET and HEAD requests cover ETag and modified/unmodified date precedence with 200, 304 and 412 outcomes. Range tests validate returned bytes and `Content-Range`, including an 8 MiB object, open and suffix ranges, empty objects and 416 `InvalidRange` failures.

The implementation also performs 50 repeated full and deterministic random-range reads of a 15 MiB object, response-header query overrides, multipart part retrieval, normal and versioned deletion behavior, and explicit delete-marker 405 handling followed by retrieval of the retained object version. Static, compile-only and all 35 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### ListObjects

All 40 Java/Python methods map to same-named subtests under `TestListObjects`. The V1 implementation covers delimiter and prefix combinations, URL encoding, empty and control-character parameters, marker and `NextMarker` behavior, `MaxKeys`, common-prefix pagination, object metadata, anonymous access, missing buckets and versioned objects.

The 1,003-key delimiter boundary scenario is retained, including the `#` and `+` keys immediately following a 999-key common prefix. Shared cleanup now paginates through every object version and delete marker so tests containing more than 1,000 entries do not leave a bucket behind. Static, compile-only and all 40 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### ListObjectsV2

All 42 Java/Python methods map to same-named subtests under `TestListObjectsV2`. V2 delimiter, prefix, encoding and pagination requests are issued independently of the V1 implementation. The tests validate `KeyCount`, optional owners, `MaxKeys`, `StartAfter`, opaque continuation-token round trips and continuation-token precedence when `StartAfter` is also supplied.

Common-prefix continuation pages, anonymous access, missing buckets, combined filtering and versioned buckets are included. Static, compile-only and all 42 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### ListObjectsVersions

All 40 Java/Python methods map to same-named subtests under `TestListObjectsVersions`. Version listing covers delimiter, prefix, URL encoding, common-prefix pagination, `MaxKeys`, key markers, metadata, anonymous access and missing buckets. The 1,003-key delimiter boundary is retained for the version API as well.

Version-specific assertions include version IDs and sizes, latest-version flags, combined filtering through `NextKeyMarker`, and exact newest-to-oldest ordering for ten versions of the same key. Static, compile-only and all 40 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### CopyObject

All 62 Java/Python methods map to same-named subtests under `TestCopyObject`. Basic same/cross-bucket copy, zero-length objects, copy-to-self rules, ACLs, content type, metadata replacement, missing sources, URL-encoded version sources and multipart-created versioned sources are implemented. Source and destination ETag/date preconditions validate successful copies and 412/501 failures.

The source-object, source-bucket-default, destination-bucket-default and destination-object encryption matrix uses separate source and target buckets. Normal, SSE-S3 and SSE-C source/target combinations supply the required copy-source and destination customer headers. Deleted sources, delete markers, checksum-bearing copies and metadata/tag preservation are also covered. Static, compile-only and all 62 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### DeleteObjects

All 21 Java/Python methods map to same-named subtests under `TestDeleteObjects`. Single and 100-object batches, repeated deletion, quiet responses, directory-shaped keys, versioned objects and repeated delete-marker creation are implemented. Mixed current/noncurrent version deletion verifies both remaining versions and markers before explicit final cleanup.

Conditional single deletes use SDK `IfMatch`; conditional multi-delete uses per-object ETags and validates mixed `Deleted`/`PreconditionFailed` results. Combined `If-Match` and `If-None-Match` requests inject the otherwise unsupported header before signing and validate the source-defined 501 response. Static, compile-only and all 21 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### Multipart

All 48 Java/Python methods map to same-named subtests under `TestMultipart`. Multipart creation, small and multi-size parts, resent parts, overwrite atomicity, abort/list lifecycle, missing parts, incorrect ETags, undersized non-final parts, versioned and special-name copy sources, and copy ranges are implemented. `test_put_object_overwrite_multipart_upload` completes a 10 MiB multipart object then replaces it with a 1 MiB `PutObject` and validates Head/Get/range reads.

The implementation also covers paginated `ListParts`, CRC32 upload and multipart-copy checksums, checksum type validation, source ETag/date conditions for `UploadPartCopy`, and destination ETag conditions for completion including 412 and 501 outcomes. Static, compile-only and all 48 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### Versioning

All 33 Java methods and 33 Python methods map to same-named subtests under `TestVersioning`. Enabled/suspended transitions, null-version replacement and deletion, current/noncurrent versions, delete markers, multipart version IDs, copy by version, ACLs, concurrent creation/deletion and key/version marker pagination are implemented. `test_versioning_obj_mix_put_and_multipart` mixes PutObject and multipart completes (1 KiB → 50 MiB → 1 MiB → 10 MiB) and checks newest-first listing plus versioned GetObject bodies.

The formerly disabled `test_versioning_obj_create_read_remove_head` is active and validates the delete response's marker version ID, current-object failure, marker-version HEAD behavior and access to the retained object version. Off → enabled → suspended sequences cover both one key and multiple keys with exact null-version contents and version counts. Static, compile-only and all 33 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### Taggings

All 13 Java/Python methods map to same-named subtests under `TestTaggings`. Bucket and object tagging CRUD, `TagCount`, replacement/deletion, inline `PutObject` tags and untagged objects are implemented. Boundary tests cover ten tags, 128-character keys and 256-character values; count, key and value overflow paths validate the expected S3 errors and unchanged empty tag state.

The authenticated POST scenario builds the source-compatible SigV2 policy and XML tagging form, verifies the uploaded body and retrieves both tags. It preserves the source AWS-only SKIP while remaining active for compatible endpoints. Static, compile-only and all 13 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### GetObjectAttributes

All 15 Java/Python methods map to same-named subtests under `TestGetObjectAttributes`. Selective and combined size, storage class, ETag, checksum and object-parts requests are implemented, together with missing object/bucket, empty attributes and invalid version failures. Version-specific requests compare distinct object sizes and returned version IDs.

Multipart coverage includes two parts, the source-compatible 100 MiB/20-part object, and an all-attributes CRC64NVME full-object checksum upload. Metadata and SSE-S3 are cross-checked with `HeadObject`; the source async cases use the same Go SDK operation and retain their separate success/error subtests. Static, compile-only and all 15 camelCase/snake_case resolver checks passed; actual S3 execution is pending.

### Lock

All 36 Java/Python methods map to same-named subtests under `TestLock`. Object Lock enablement and configuration validation, automatic versioning, default governance/compliance retention, explicit object retention, version-specific retention, retention extension/shortening and governance bypass are implemented. Invalid bucket, mode, status, day/year combinations and versioning suspension validate both HTTP status and S3 error code.

Put, copy and multipart paths verify inherited default retention and required Content-MD5 behavior. Legal holds cover put/get, invalid inputs, blocked deletion and removal before cleanup; single and multi-object retained deletion cover both denied and bypassed requests. Static, compile-only and all 36 camelCase/snake_case resolver checks passed; actual S3 execution is pending.
