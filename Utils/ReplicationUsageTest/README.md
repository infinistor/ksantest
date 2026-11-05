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

원본 S3 주소와 리전 정보는 설정하지 않고 포털에서 가져온다.

- 원본 = `GET /api/v2/Regions/Default` (내 리전)의 Address·Port (https 면 SSLPort)
- remote 대상 = `GET /api/v2/Regions` 의 나머지 리전

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
  7. bucket usage 비교 (포털 API `GET /api/v2/S3Api/Buckets/<버킷>` 의 FileCount·UsedSize)

모든 케이스는 이 7단계 틀(`cases/base.py` 의 `ReplicationCase`)을 쓰고 4단계만 다르다.
통과하면 버킷을 지우고, 실패하면 원인 확인용으로 남긴다.

비교는 **버전 단위**(`list_object_versions`)다.

- 대상 버전 목록 = 원본 파일 버전 중 룰에 맞는 것, versionId 까지 같아야 함(빠짐·남음 모두 실패)
- 버전마다 크기·ETag·사용자 메타데이터(`x-amz-meta-test`)·태그를 원본·대상 모두 기대값과 비교
- 삭제 마커는 DeleteMarker 복제를 켠 케이스에서만 대상에 있어야 함
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
| TC10 | Multipart | 5M+5M+1M 파트, ETag `-3` |
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

- `[portal]` `url`, `api_key`
- `[s3]` `access_key`, `secret_key` (원본 버킷을 만들 기존 사용자 키), `https` (선택)
- `[test]` `file_count`, `file_sizes` (선택)

대기 시간, 조회 간격, 버킷 접두사 같은 나머지 값은 `common.py` 맨 위 상수에 있다.

먼저 점검부터 한다. 버킷은 만들지 않고 포털 접속, 원본 리전, 원본 사용자 키, remote 리전 키를 확인한다.

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
```

https 로 확인하려면 `[s3] https = true` 인 설정 파일을 따로 만들어 `--config` 로 준다.

종료 코드: 모두 통과 0, 실패 있음 1, 설정·점검 오류 2

## 주기 실행 (cron)

`run.sh` 는 이전 실행이 끝나지 않았으면 그 회차를 건너뛴다(flock).

```bash
chmod +x run.sh
# 예: 30분마다
*/30 * * * * /root/ReplicationUsageTest/run.sh > /dev/null 2>&1
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
