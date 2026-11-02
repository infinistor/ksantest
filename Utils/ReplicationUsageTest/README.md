# 복제(local → local) usage 주기 테스트

같은 KSAN 시스템 안에서 원본 버킷 → 대상 버킷 복제를 걸고, 파일과 bucket usage 가 일치하는지 확인한다.
cron 으로 주기 실행하는 것을 전제로 한다. 테스트 케이스는 `cases/` 에 하나씩 추가한다.

## 케이스

- **TC01 기본 복제 테스트**
  1. 신규 버킷 생성 (src, dst)
  2. 버저닝 설정 (둘 다 Enabled)
  3. 룰 설정 (src → `arn:aws:s3:::<dst>`, 리전 없음 = local)
  4. 파일 업로드 (개수·크기는 설정값)
  5. 복제 완료까지 대기 (dst 에 모든 키가 보일 때까지)
  6. 파일 비교 (키별 크기·ETag, 객체 수)
  7. bucket usage 비교 (포털 API `GET /api/v2/S3Api/Buckets/<버킷>` 의 FileCount·UsedSize)

1~6 은 S3 API, 7 은 포털 API 를 쓴다. 통과하면 버킷을 지우고, 실패하면 원인 확인용으로 남긴다.

TC02 부터는 `cases/base.py` 의 `ReplicationCase` 틀을 쓴다. 단계는 TC01 과 같고(4단계만 케이스별 작업),
비교는 **버전 단위**(`list_object_versions`)다.

- 대상 버전 목록 = 원본 파일 버전 중 룰에 맞는 것, versionId 까지 같아야 함(빠짐·남음 모두 실패)
- 버전마다 크기·ETag·사용자 메타데이터(`x-amz-meta-test`)·태그를 원본·대상 모두 기대값과 비교
- 삭제 마커는 DeleteMarker 복제를 켠 케이스에서만 대상에 있어야 함
- usage 기대값 = 파일 버전 수·크기 합. **삭제 마커는 세지 않는다**
- 대상에 "없어야 할 것"이 있는 케이스는 복제 완료 뒤 20초 더 기다렸다 다시 확인한다

| TC | 축 | 확인 |
|---|---|---|
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
| TC13 | SSE-S3 | 두 버킷 기본 암호화 AES256, 대상도 암호화 |
| TC14 | Prefix + 삭제 마커 복제 | `in/` 키의 삭제 마커만 대상에, `out/` 마커는 안 옴 |

TC02~ 의 버킷 이름은 `rtest-<케이스>-src-<시각>` 처럼 케이스 ID 가 들어간다.

## 준비

```bash
pip3 install -r requirements.txt
cp config.ini.example config.ini   # 값 채우기
```

설정 항목은 `config.ini.example` 의 주석 참고.

- `[portal]` `url`, `api_key`
- `[s3]` `url`, `access_key`, `secret_key`
- `[test]` `file_count`, `file_sizes` (선택)

대기 시간, 조회 간격, 버킷 접두사 같은 나머지 값은 `common.py` 맨 위 상수에 있다.

## 실행

```bash
python3 run.py              # 등록된 케이스 전부
python3 run.py --tc TC01    # 지정 케이스만
python3 run.py --list       # 케이스 목록
```

종료 코드: 모두 통과 0, 실패 있음 1, 설정 오류 2

## 주기 실행 (cron)

`run.sh` 는 이전 실행이 끝나지 않았으면 그 회차를 건너뛴다(flock).

```bash
chmod +x run.sh
# 예: 30분마다
*/30 * * * * /root/ReplicationUsageTest/run.sh > /dev/null 2>&1
```

## 결과

- `results/summary.csv` — 실행·케이스마다 한 줄 (결과, 실패 단계, 복제 소요 초, src·dst usage, 메시지)
- `results/<yyyyMMddHHmmss>.log` — 실행 상세 (단계별 `[TC01][4/7] 파일 업로드 ... OK (2.1초, 10개, 1M)`)

## 케이스 추가

보통은 `cases/base.py` 의 `ReplicationCase` 를 상속한다.

1. `cases/tcNN_<이름>.py` 에 하위 클래스를 만들고 `CASE_ID`, `TITLE`, `OPERATION`(4단계 이름)을 정한다
2. 룰이 다르면 `RULE_FILTER`, `DELETE_MARKER_REPLICATION`, 필터 판정 `replicates()` 를 바꾼다
3. `operate()` 에서 `self.put()`·`self.record()`·`self.delete_version()` 으로 작업하면 원본·대상 기대값이 함께 기록된다
   - 중간에 복제 완료를 기다려야 하면 `self.wait_replicated()`
4. 대상에 없어야 할 것이 있으면 `HAS_NEGATIVE = True`
5. `cases/__init__.py` 의 `CASES` 목록에 추가한다

틀이 맞지 않으면 TC01 처럼 `common.Case` 를 직접 상속해 `STEPS`·`run()`·`cleanup()` 을 쓴다
(단계는 `with self.step(번호):` 로 감싸고, 판정 실패는 `CaseFailed`).
