package org.example.replication.cases;

import java.util.ArrayList;
import java.util.List;

/**
 * TC08 버전 지정 삭제: 원본에서 versionId 로 지운 버전은 대상에서 지워지지 않는다(복제 안 함).
 * usage 는 원본만 줄고 대상은 그대로여야 한다.
 */
public class Tc08VersionDelete extends ReplicationCase {
	public Tc08VersionDelete() {
		super("TC08", "버전 지정 삭제", "2번 PUT, 복제 후 이전 버전 삭제");
		deleteMarkerReplication = true; // 마커 복제를 켜도 버전 지정 삭제는 복제되지 않아야 한다
		hasNegative = true;
	}

	@Override
	protected void operate(List<String> detail) throws Exception {
		List<VersionKey> oldVersions = new ArrayList<>();
		for (var k : sizedKeys()) {
			oldVersions.add(new VersionKey(k.key(), put(k.key(), k.size())));
			put(k.key(), k.size());
		}
		double waited = waitReplicated();
		for (var kv : oldVersions)
			deleteVersion(kv.key(), kv.versionId());
		detail.add(String.format("PUT %d개, 1차 복제 %.1f초, 이전 버전 %d개 삭제", oldVersions.size() * 2, waited,
				oldVersions.size()));
	}
}
