# S3 Access IP 테스트

포털에서 Access IP(접근 허용 IP)를 설정했을 때 GW 가 기대대로 허용·차단하는지 확인한다.
예전 Jenkins 테스트 두 개를 합친 것이다.

- `jenkins_test_code/java/s3_accessip_test_ksan` (Java, 버킷 단위)
- `ksan/core/jenkins/access-test/access-ips-check.py` (Python, 사용자 단위)

## GW 판정 규칙 (ksan `GWUtils.checkIPs`)

- 사용자에게 `ALL` 정책이 있으면 ListBuckets 를 포함한 모든 요청이 범위 안이어야 통과
- 버킷 정책만 있으면 그 버킷 요청만 검사하고, ListBuckets·다른 버킷은 영향 없음
- 정책이 없으면 모두 통과

## IP 케이스

클라이언트 IP(S3 주소로 나가는 출발지 IP, 또는 설정값)를 기준으로 원본 `testlist` 와 같은 10개를 만든다.
클라이언트가 `192.168.11.45` 일 때:

- IP01 `192.168.12.1/24` 다른 /24 → 차단
- IP02 `192.168.11.1/24` 자기 /24 → 허용
- IP03 `192.168.11.44` 클라이언트 -1 → 차단
- IP04 `192.168.11.46` 클라이언트 +1 → 차단
- IP05 `192.168.11.45` 클라이언트 → 허용
- IP06 `192.168.11.1-192.168.11.44` 바로 앞까지 → 차단
- IP07 `192.168.11.46-192.168.11.100` 바로 뒤부터 → 차단 (클라이언트가 .100 이상이면 끝은 .254)
- IP08 `192.168.11.44-192.168.11.46` 앞뒤 1 → 허용
- IP09 `192.168.11.40-192.168.11.50` 앞뒤 5 → 허용
- IP10 `192.168.11.1-192.168.11.254` 전체 → 허용

클라이언트 끝자리가 1·254 라서 앞뒤 주소가 없는 케이스는 건너뛰고 로그에 남긴다.

## 시나리오 (IP 케이스마다)

**사용자 단위 (`BucketName=ALL`, ListBuckets 로 확인)**

- main-only: main 만 설정 → main=케이스 기대값, alt=허용
- alt-only: alt 만 설정 → main=허용, alt=기대값
- both: 둘 다 설정 → 둘 다 기대값

**버킷 단위 (버킷 A·B 는 main, C·D 는 alt, ListObjects 로 확인)**

- bucket-A: A 에 설정 → A=기대값, B·C·D=허용, ListBuckets 는 둘 다 허용
- bucket-C: C 에 설정 → C=기대값, 나머지 허용
- bucket-A+C: A·C 에 설정 → A·C=기대값, B·D 허용

모든 시나리오는 설정 전·해제 후에 전부 허용인지도 확인한다.
사용자·버킷은 실행마다 새로 만들고(`accessip-main-<시각>`, `accessip-test-<시각>-a` …), 끝나면 성공·실패와 상관없이
정책 → 버킷 → 사용자 순서로 지운다.

## 준비

```bash
pip3 install -r requirements.txt
cp config.ini.example config.ini   # 값 채우기
```

시스템 파이썬에 설치하지 않으려면 이 폴더에 venv 를 만든다. `run.sh` 는 `.venv` 가 있으면 그것을 쓴다.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

설정 항목은 `config.ini.example` 의 주석을 참고한다.

- `[portal]` `url`, `api_key`
- `[s3]` `url`
- `[test]` `client_ip`, `delay` (선택)

버킷·사용자 접두사, 타임아웃 같은 나머지 값은 `common.py` 맨 위 상수에 있다.

### NAT 뒤에서 실행할 때

GW 는 TCP 접속 출발지 IP 로 판정한다. 중간에 NAT 가 있으면 자동 감지한 IP 와 GW 가 보는 IP 가 달라서,
"허용" 케이스가 모두 차단으로 실패한다. 이때는 GW 가 보는 IP 를 `[test] client_ip` 에 적는다.
GW 가 보는 IP 는 GW 서버의 `/var/log/ksan/gw/access.log` 맨 앞 칸에서 확인할 수 있다.

- 예: 192.168.31.155 → 192.168.11.152 GW 는 `192.168.11.252` 로 보인다

## 실행

```bash
python3 run.py                  # 사용자 단위 + 버킷 단위 전부
python3 run.py --mode user      # 사용자 단위만
python3 run.py --mode bucket    # 버킷 단위만
python3 run.py --case IP05      # 지정 케이스만 (여러 번 줄 수 있음)
python3 run.py --list           # 클라이언트 IP 와 케이스 목록만
```

종료 코드: 모두 통과 0, 실패·중단·정리 실패 1, 설정 오류 2

전체 실행은 60개 시나리오, `delay` 1초 기준 약 2분 걸린다.

## 주기 실행 (cron·Jenkins)

`run.sh` 는 이전 실행이 끝나지 않았으면 그 회차를 건너뛴다(flock).

```bash
chmod +x run.sh
# 예: 매일 05시
0 5 * * * /root/workspace/AccessIpTest/run.sh > /dev/null 2>&1
```

## 결과

- `results/<yyyyMMddHHmmss>.log`: 판정마다 한 줄
  - 예: `[BUCKET IP03 bucket-A] 설정 후: main ListObjects A 허용=False (AccessDenied) 기대=False OK`
- `results/summary.csv`: 실행·단위·케이스·시나리오마다 한 줄(결과, 불일치 내용)
