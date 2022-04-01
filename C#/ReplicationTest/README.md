# S3 Replication test for .Net Framework

이 테스트는 아마존에서 제공하는 S3 Api를 사용하여 S3 Replication 기능에 대한 점검 프로그램입니다.

## 테스트 항목

- 복제 기능의 정상 동작 확인(Filtering, DeleteMarker, versionId)
- 로컬 시스템과 외부 시스템간의 복제 가능 여부
- SSE-S3 설정된 버킷과 일반 버킷간의 복제 가능 여부
- Http, Https 환경에서 정상적인 동작 확인

## 테스트 순서

1. 버킷 생성(source, target)
2. 버킷에 복제설정
3. 다양한 오브젝트 업로드
4. 설정한 시간만큼 대기
5. 원본 버킷과 대상 버킷을 ListVersions 하여 비교

## 구동환경

*  .Net Core : 3.1 이상
*  OS : Window 10 이상

## How to Build

### dotnet 설치

#### Window 10
- 설치파일 링크 : [Link](https://dotnet.microsoft.com/en-us/download/dotnet/3.1)

#### CentOS 7
```
sudo rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
sudo yum install -y dotnet-sdk-3.1
```

### Build
``` shell
dotnet build -c Release
```

## 테스트 방법

### Config 설정
``` ini
[Default]
# replication이 완료될때까지 대기하는 시간 (단위 : sec)
Delay = 30

[Global]
# 기본 설정 버킷 명
MainNormalBucket = bucket-normal-bucket
# SSE-S3 설정 버킷 명
MainEncryptionBucket = bucket-encryption-bucket
# 테스트용 생성 버킷 접두어
TargetBucketPrefix = test-
#Test Option
# 0 = ALL
# 1 = Local Only
# 2 = Another Only
TestOption = 0
# SSL Option
# 0 = ALL
# 1 = Http Only
# 2 = Https Only
SSL = 0

# 원본 유저 및 시스템
[Main User]
URL = 0.0.0.0
AccessKey = accesskey
SecretKey = secretkey

# 대상 유저 및 시스템
[Alt User]
URL = 0.0.0.0
AccessKey = accesskey
SecretKey = secretkey

```

### 테스트 실행
``` ps1
dotnet ReplicationTest.dll
```
