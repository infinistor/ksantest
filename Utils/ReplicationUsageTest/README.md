# 복제 usage 테스트

원본 버킷 → 대상 버킷 복제를 걸고, 버전·내용과 bucket usage 가 일치하는지 확인한다.
cron 으로 주기 실행하는 것을 전제로 한다. 테스트 케이스는 `cases/` 에 하나씩 추가한다.
예전 C# `Utils/ReplicationTest` 를 대신한다.

## 대상 (local · remote)

같은 케이스를 대상마다 한 번씩 돌린다.

- **local**: 원본과 같은 시스템의 버킷. ARN `arn:aws:s3:::<버킷>`
- **remote**: 포털에 등록된 다른 리전의 버킷. ARN `arn:aws:s3:<리전>::<버킷>`
  - 케이스 ID 뒤에 리전 이름이 붙는다 (예: `TC03@kr-ksan-1`)
  - 대상 버킷은 그 리전에 등록된 키로 만들고 확인한다. KSAN 복제기도 이 키로 대상에 쓴다
  - 리전 주소·포트가 원본과 같으면(같은 시스템) 대상 usage 도 비교하고, 다르면 원본 usage 만 비교한다
    (같은 주소라도 포트가 다르면 앞단 haproxy 가 다른 시스템으로 보낼 수 있어 다른 시스템으로 본다)

원본 S3 주소와 리전 정보는 포털에서 가져온다.

- 원본 = `GET /api/v2/Regions/Default` (내 리전)의 Address·Port (https 면 SSLPort)
  - 리전 주소가 실행 PC 에서 풀리지 않는 이름(예: `system-vip`)이면 `[s3] url` 로 원본 주소를 직접 준다. 그 값이 포털 주소보다 우선한다
- remote 대상 = `GET /api/v2/Regions` 의 나머지 리전

포털은 KSAN 과 InfiniStor(IFS) 둘 다 쓸 수 있다. `[portal] type` 으로 고른다(비우면 ksan).

| | ksan | ifs |
|---|---|---|
| 인증 | `Authorization: Bearer <키>` | `Authorization: <키>` |
| 리전 목록 | `GET /api/v2/Regions` | `GET /api/v1/Region` (SSL 포트 필드는 HttpsPort) |
| 원본 리전 | `GET /api/v2/Regions/Default` | API 가 없다. `[portal] region` 이름, 없으면 `[s3] url` 주소·포트와 같은 리전(하나일 때) |
| bucket usage | `GET /api/v2/S3Api/Buckets/<버킷>` | `GET /api/v1/ExternalApi/Buckets/<버킷>` |

- 두 포털 모두 usage 는 버킷 주인의 키를 헤더(AccessKey·SecretKey)로 줘서 FileCount·UsedSize 를 읽는다
- ifs 는 실패해도 HTTP 200 이고 응답 Result 가 1 이 아니다. 그때는 실패로 본다
- `[portal] region` 은 ksan 에서도 쓸 수 있다. 값이 있으면 기본 리전 API 대신 그 이름의 리전을 원본으로 본다

KSAN 은 `arn:aws:s3:<주소>:<AK>-<SK>:<버킷>` 형식도 받지만, 주소로 포털 리전을 찾아 리전으로 바꾸고 키는 버린다.
그래서 어느 형식이든 원본 포털에 대상 리전이 등록돼 있어야 한다.

## 케이스

- **TC01 기본 복제 테스트**
  1. 신규 버킷 생성 (src, dst)
  2. 버저닝 설정 (둘 다 Enabled)
  3. 룰 설정 (src → dst)
  4. 파일 업로드 (개수·크기는 설정값)
  5. 복제 완료까지 대기 (dst 가 기대와 같아질 때까지)
  6. 버전 비교
  7. 원본 복제 상태 비교 (원본 HeadObject 의 `x-amz-replication-status`)
  8. bucket usage 비교 (포털 API 의 FileCount·UsedSize. ksan `GET /api/v2/S3Api/Buckets/<버킷>`, ifs `GET /api/v1/ExternalApi/Buckets/<버킷>`)

모든 케이스는 이 8단계 틀(`cases/base.py` 의 `ReplicationCase`)을 쓰고 4단계만 다르다.
통과하면 버킷을 지우고, 실패하면 원인 확인용으로 남긴다.
남은 버킷은 `--cleanup-days N` 으로 다음 실행 때 지운다(아래 실행 참고).

비교는 **버전 단위**(`list_object_versions`)다.

- 대상 버전 목록 = 원본 파일 버전 중 룰에 맞는 것, versionId 까지 같아야 함(빠짐·남음 모두 실패)
- 버전마다 크기·ETag·사용자 메타데이터(`x-amz-meta-test`)·태그를 원본·대상 모두 기대값과 비교
- 삭제 마커는 DeleteMarker 복제를 켠 케이스에서만 대상에 있어야 함
- 원본 파일 버전의 `x-amz-replication-status`(boto3 `ReplicationStatus`): 룰에 맞아 복제된 버전은 `COMPLETED`,
  룰에 맞지 않는 버전은 헤더가 없어야 함(AWS 와 같음). 복제기가 대상 쓰기 뒤에 원본 상태를 갱신하므로 최대 60초 기다린다
  - KSAN 은 Replication 설정 `ReplicationStatusUpdate: true` 와, 복제 상태 API 가 있는 GW 가 있어야 갱신된다
- usage 기대값 = 파일 버전 수·크기 합. **삭제 마커는 세지 않는다**
- 대상에 "없어야 할 것"이 있는 케이스는 복제 완료 뒤 20초 더 기다렸다 다시 확인한다

| TC | 축 | 확인 |
|---|---|---|
| TC01 | 기본 | 파일 업로드 후 그대로 복제 |
| TC02 | 여러 버전 | 같은 키 3번 PUT → 3버전 모두 같은 versionId 로 |
| TC03 | Prefix 필터 | `in/` 만 복제, `out/` 은 안 옴 |
| TC04 | Tag 필터 | `Replication=True` 태그가 있는 것만 |
| TC05 | Prefix + Tag | 둘 다 맞는 것만 |
| TC06 | 삭제 마커 복제 끔 | 대상에 마커 없음 |
| TC07 | 삭제 마커 복제 켬 | 마커가 같은 versionId 로 |
| TC08 | 버전 지정 삭제 | 복제 안 됨 → usage 원본 1버전·대상 2버전 |
| TC09 | CopyObject | 같은 버킷·다른 버킷(복제 없는 aux)에서 Copy |
| TC10 | Multipart | 5M+5M+1M 파트. 원본 ETag `-3` 확인, 대상은 ETag 대신 내려받은 내용 MD5 비교(복제기가 PartSize 단위로 다시 잘라 올려 ETag 가 달라짐) |
| TC11 | 체크섬 | CRC32·CRC32C·CRC64NVME·SHA1·SHA256 값 일치(값은 직접 계산해 보냄, awscrt 불필요) |
| TC12 | 태그 변경 | 복제 후 PutObjectTagging·DeleteObjectTagging 반영, usage 이중 집계 없음 |
| TC13 | SSE-S3 원본·대상 | 두 버킷 기본 암호화 AES256, 대상도 암호화 |
| TC14 | Prefix + 삭제 마커 복제 | `in/` 키의 삭제 마커만 대상에, `out/` 마커는 안 옴 |
| TC15 | SSE-S3 원본만 | 원본 객체가 암호화돼 있으니 복제본도 AES256 |
| TC16 | SSE-S3 대상만 | 평문 원본이 대상 버킷 기본 암호화로 AES256 |

버킷 이름은 `rtest-<케이스>-src-<시각>` 처럼 케이스 ID 가 들어간다. remote 는 `rtest-tc03r1-src-<시각>` 처럼 리전 순번이 붙는다.

## 준비

```bash
pip3 install -r requirements.txt
cp config.ini.example config.ini   # 값 채우기
```

설정 항목은 `config.ini.example` 의 주석 참고.

- `[portal]` `url`, `api_key`, `type` (선택, ksan·ifs), `region` (선택, 원본 리전 이름)
- `[s3]` `access_key`, `secret_key` (원본 버킷을 만들 기존 사용자 키), `https` (선택), `url` (선택, 원본 S3 주소 직접 지정)
- `[test]` `file_count`, `file_sizes` (선택)

대기 시간, 조회 간격, 버킷 접두사 같은 나머지 값은 `common.py` 맨 위 상수에 있다.

먼저 점검부터 한다. 버킷은 만들지 않고 포털 접속, 원본 리전, 원본 사용자 키, remote 리전 키를 확인한다.
원본 S3 주소 줄에는 어디서 온 값인지(`config` 또는 `포털 기본 리전`)가 붙는다.

```bash
python3 run.py --check
```

리전 키가 `InvalidAccessKeyId` 로 실패하면 KSAN 복제기도 그 리전으로 복제하지 못한다.
포털의 리전 설정에서 키를 실제 사용자 키로 고쳐야 remote 케이스가 통과한다.

## 실행

```bash
python3 run.py                      # local + remote 전부
python3 run.py --target local       # 같은 시스템만
python3 run.py --target remote      # 다른 리전만
python3 run.py --region kr-ksan-1   # remote 를 이 리전으로만 (여러 번 줄 수 있음)
python3 run.py --tc TC01            # 지정 케이스만 (여러 번 줄 수 있음)
python3 run.py --list               # 케이스 목록
python3 run.py --cleanup-days 3     # 실행 전에 3일보다 오래된 시험 버킷을 지운다
```

`--cleanup-days N` 은 기본 꺼져 있다. 켜면 실행 전에 원본 키(원본·local 대상)와 이번 실행의 remote 대상 리전 키로
버킷 목록을 보고, 이름이 `rtest-*-<yyyyMMddHHmmss>` 이고 그 시각이 N일보다 오래된 버킷을 지운다.
복제 룰 → 모든 버전·삭제 마커 → 버킷 순서다. 실패는 경고만 남기고 시험은 그대로 돈다.

https 로 확인하려면 `[s3] https = true` 인 설정 파일을 따로 만들어 `--config` 로 준다.

종료 코드: 모두 통과 0, 실패 있음 1, 설정·점검 오류 2

## 주기 실행 (cron)

`run.sh` 는 이전 실행이 끝나지 않았으면 그 회차를 건너뛴다(flock).

```bash
chmod +x run.sh
# 예: 30분마다
*/30 * * * * /root/ReplicationUsageTest/run.sh --cleanup-days 3 > /dev/null 2>&1
```

## 결과

- `results/summary.csv` — 실행·케이스·대상마다 한 줄 (결과, 실패 단계, 복제 소요 초, src·dst usage, 메시지)
  - `case` 칸이 `TC03` 이면 local, `TC03@kr-ksan-1` 이면 remote
- `results/<yyyyMMddHHmmss>.log` — 실행 상세 (단계별 `[TC01][4/7] 파일 업로드 ... OK (2.1초, 10개, 1M)`)

## 케이스 추가

`cases/base.py` 의 `ReplicationCase` 를 상속한다.

1. `cases/tcNN_<이름>.py` 에 하위 클래스를 만들고 `CASE_ID`, `TITLE`, `OPERATION`(4단계 이름)을 정한다
2. 룰이 다르면 `RULE_FILTER`, `DELETE_MARKER_REPLICATION`, 필터 판정 `replicates()` 를 바꾼다
3. `operate()` 에서 `self.put()`·`self.record()`·`self.delete_version()` 으로 작업하면 원본·대상 기대값이 함께 기록된다
   - 중간에 복제 완료를 기다려야 하면 `self.wait_replicated()`
   - 대상 버킷을 직접 다룰 때는 `self.dst_s3`(또는 `self.client_for(버킷)`)를 쓴다. remote 에서는 원본 클라이언트와 다르다
4. 대상에 없어야 할 것이 있으면 `HAS_NEGATIVE = True`
5. `cases/__init__.py` 의 `CASES` 목록에 추가한다
