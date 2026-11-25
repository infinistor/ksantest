#!/bin/bash
# 빌드 후 서버 배포용 묶음을 만든다.
#   dist/ReplicationTest-<커밋 수>-<짧은 해시>.tar.gz
#     └── ReplicationTest-<커밋 수>-<짧은 해시>/
#         ├── replication-usage-test.jar
#         ├── run.sh
#         └── config.ini.example
# Linux 와 Windows Git Bash 둘 다에서 돈다. mvn, git, tar 가 PATH 에 있어야 한다.
set -euo pipefail

cd "$(dirname "$0")"

count=$(git rev-list --count HEAD)
hash=$(git rev-parse --short HEAD)
name="ReplicationTest-${count}-${hash}"

# 커밋되지 않은 변경이 있으면 이름의 해시와 내용이 다를 수 있다. 막지는 않고 알린다.
if [ -n "$(git status --porcelain -- .)" ]; then
	echo "경고: 이 폴더에 커밋되지 않은 변경이 있습니다. ${hash} 와 내용이 다를 수 있습니다." >&2
fi

echo "빌드: mvn -q clean package"
if ! mvn -q clean package; then
	echo "빌드 실패. 묶음을 만들지 않습니다." >&2
	exit 1
fi

jar=target/replication-usage-test.jar
if [ ! -f "$jar" ]; then
	echo "빌드 결과 $jar 가 없습니다." >&2
	exit 1
fi

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir "$stage/$name"

cp "$jar" "$stage/$name/"
# Windows 에서 core.autocrlf 로 CRLF 가 됐을 수 있으니 CR 을 지워서 넣는다.
tr -d '\r' < run.sh > "$stage/$name/run.sh"
tr -d '\r' < config.ini.example > "$stage/$name/config.ini.example"
chmod 755 "$stage/$name/run.sh"
chmod 644 "$stage/$name/replication-usage-test.jar" "$stage/$name/config.ini.example"

mkdir -p dist
out="dist/${name}.tar.gz"
tar -C "$stage" --owner=0 --group=0 -czf "$out" "$name"

echo "완료: $out"
tar -tvzf "$out"
