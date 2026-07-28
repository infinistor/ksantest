# Go migration status

Java SDK V2 is the primary source. Each Java `@Test` method maps to a top-level Go `TestXxx` function in the matching `*_test.go` file (1:1). Python ports remain reference-only for behavior.

**Naming**

| Java | Go |
|---|---|
| `Grants.testBucketAclDefault` | `TestBucketAclDefault` in `grants_test.go` |
| `DeleteBucket.testBucketDeleteNotExist` | `TestBucketDeleteNotExist` |

Package-level collisions (same method name in two Java classes) use a class prefix, e.g. `Versioning.testVersioningObjListMarker` → `TestVersioning_ObjListMarker` while `ListObjectsVersions` keeps `TestVersioningObjListMarker`.

Final static audit: 38 classes, 811 Java scenarios, 808 Python scenarios, and 810 executable/skip Go tests. Cors Python gap (3) and empty KMS (1) are intentional. `start-function` accepts Java camelCase and Python snake_case.

`go vet ./...` and `go test -run '^$' ./...` pass. Accelerate (4), Analytics (6), Payment (3), and SelectObjectContent (7) SKIP every scenario. Backend reports 30 scenarios (Java wrapper-aligned; basic 10 disabled). Live S3 comparison remains pending.

| Class | Java | Python | Go | Status | Notes |
|---|---:|---:|---:|---|---|
| PutBucket | 24 | 24 | 24 | 구현 완료 | Actual S3 run pending |
| DeleteBucket | 3 | 3 | 3 | 구현 완료 | Actual S3 run pending |
| ListBuckets | 7 | 7 | 7 | 구현 완료 | Actual S3 run pending |
| KMS | 1 | 1 | 0 | 의도적 미구현 | Empty `KMS` type only |
| Accelerate | 4 | 4 | 4 | SKIP 구현 완료 | Always SKIP |
| Access | 6 | 6 | 6 | 구현 완료 | Actual S3 run pending |
| ACL | 46 | 46 | 46 | 구현 완료 | Actual S3 run pending |
| Analytics | 6 | 6 | 6 | SKIP 구현 완료 | Always SKIP |
| Backend | 30 | 40 | 30 | 구현 완료 | Wrapper-aligned 30 |
| CopyObject | 62 | 62 | 62 | 구현 완료 | Actual S3 run pending |
| Cors | 4 | 1 | 4 | 구현 완료 | Actual S3 run pending |
| CSE | 11 | 11 | 11 | 구현 완료 | Actual S3 run pending |
| DeleteObjects | 21 | 21 | 21 | 구현 완료 | Actual S3 run pending |
| GetObject | 35 | 35 | 35 | 구현 완료 | Actual S3 run pending |
| GetObjectAttributes | 15 | 15 | 15 | 구현 완료 | Actual S3 run pending |
| Grants | 35 | 35 | 35 | 구현 완료 | Actual S3 run pending |
| Inventory | 17 | 17 | 17 | 구현 완료 | Actual S3 run pending |
| LifeCycle | 20 | 20 | 20 | 구현 완료 | Actual S3 run pending |
| ListObjects | 40 | 40 | 40 | 구현 완료 | Actual S3 run pending |
| ListObjectsV2 | 42 | 42 | 42 | 구현 완료 | Actual S3 run pending |
| ListObjectsVersions | 40 | 40 | 40 | 구현 완료 | Actual S3 run pending |
| Lock | 36 | 36 | 36 | 구현 완료 | Actual S3 run pending |
| Logging | 8 | 8 | 8 | 구현 완료 | Actual S3 run pending |
| Metrics | 14 | 14 | 14 | 구현 완료 | Actual S3 run pending |
| Multipart | 48 | 48 | 48 | 구현 완료 | Actual S3 run pending |
| Notification | 4 | 4 | 4 | 구현 완료 | Actual S3 run pending |
| Ownership | 7 | 7 | 7 | 구현 완료 | Actual S3 run pending |
| Payment | 3 | 3 | 3 | SKIP 구현 완료 | Always SKIP |
| Policy | 21 | 21 | 21 | 구현 완료 | Actual S3 run pending |
| Post | 36 | 36 | 36 | 구현 완료 | Actual S3 run pending |
| PutObject | 48 | 48 | 48 | 구현 완료 | Actual S3 run pending |
| Replication | 6 | 6 | 6 | 구현 완료 | Actual S3 run pending |
| SelectObjectContent | 7 | 7 | 7 | SKIP 구현 완료 | Always SKIP |
| SSE_C | 20 | 20 | 20 | 구현 완료 | Actual S3 run pending |
| SSE_S3 | 25 | 25 | 25 | 구현 완료 | Actual S3 run pending |
| Taggings | 13 | 13 | 13 | 구현 완료 | Actual S3 run pending |
| Versioning | 33 | 33 | 33 | 구현 완료 | 3 collision-prefixed names |
| Website | 3 | 3 | 3 | 구현 완료 | Actual S3 run pending |

## Mapping convention

```text
Java  org.example.testV2.{Class}.test{Method}
Go    {snake_class}_test.go → func Test{Method}(t *testing.T)
```

Examples:

| Java | Go target |
|---|---|
| `testBucketDeleteNotExist` | `TestBucketDeleteNotExist` |
| `testBucketAclDefault` | `TestBucketAclDefault` |
| `testVersioningObjListMarker` (Versioning) | `TestVersioning_ObjListMarker` |
| `testVersioningObjListMarker` (ListObjectsVersions) | `TestVersioningObjListMarker` |

`resolve-test <Class> <method>` returns the Go function name. JUnit classname is `s3tests.{Class}` with case name equal to the Go function.
