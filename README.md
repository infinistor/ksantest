# KSAN TEST

KSAN 시스템의 기능 및 성능을 검증하는 도구를 제공합니다.

## xunit-to-html (서브모듈)

HTML 리포트 도구 [`xunit-to-html`](https://github.com/Zir0-93/xunit-to-html)은 Git submodule입니다.
일반 `git pull`만으로는 내용이 받아지지 않으므로, 아래처럼 초기화·갱신합니다.

처음 한 번 (클론 후 또는 폴더가 비어 있을 때):

```powershell
git submodule update --init --recursive
```

이후 pull 때 submodule까지 같이 받으려면:

```powershell
git pull --recurse-submodules
```

또는 pull 후 별도로:

```powershell
git submodule update --init --recursive
```

추가로 `xunit-to-html/saxon9he.jar`가 필요합니다.

## [S3 compatibility test for Java](https://github.com/infinistor/ksantest/tree/master/java)

## [S3 compatibility test for Python](https://github.com/infinistor/ksantest/tree/master/python)

> 이전 ceph/s3-tests 기반 코드는 `python-legacy/`에 보관되어 있습니다.

## [S3 compatibility test for .Net Core](https://github.com/infinistor/ksantest/tree/master/C%23/Core)

## [S3 compatibility test for .Net Framework](https://github.com/infinistor/ksantest/tree/master/C%23/Framework)

## [S3 Replication test for .Net Core](https://github.com/infinistor/ksantest/tree/master/C%23/ReplicationTest)