package org.example.replication.cases;

import java.util.List;

/** TC02 여러 버전: 같은 키를 여러 번 덮어 PUT 하면 모든 버전이 같은 versionId 로 복제되고 usage 는 버전 수만큼 센다. */
public class Tc02Versions extends ReplicationCase {
	static final int VERSIONS_PER_KEY = 3;

	public Tc02Versions() {
		super("TC02", "여러 버전 복제", "같은 키 " + VERSIONS_PER_KEY + "번 PUT");
	}

	@Override
	protected void operate(List<String> detail) {
		for (int i = 0; i < VERSIONS_PER_KEY; i++)
			for (var k : sizedKeys())
				put(k.key(), k.size());
	}
}
