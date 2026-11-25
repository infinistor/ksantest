package org.example.replication.cases;

import java.util.List;

/**
 * TC18 다중 대상: 빈 Filter 룰 3개(Priority 1·2·3)가 각각 다른 대상 버킷을 가리키면 세 곳 모두에 복제된다.
 *
 * AWS 는 Filter 를 쓰는 룰(V2 스키마)에서만 대상 버킷을 여러 개 둘 수 있다. Filter 가 비어도 되지만
 * Priority·DeleteMarkerReplication 은 있어야 하고, Priority 는 서로 달라야 한다.
 * 원본 복제 상태는 모든 대상에 복제가 끝나야 COMPLETED 가 된다.
 */
public class Tc18MultiDestination extends ReplicationCase {
	public Tc18MultiDestination() {
		super("TC18", "다중 대상 복제(룰 3개)", "파일 업로드");
		ruleFilter = emptyFilter();
		destinations = 3;
	}

	@Override
	protected void operate(List<String> detail) {
		for (var k : sizedKeys())
			put(k.key(), k.size());
		detail.add("원본 " + srcExpected.size() + "개 → 대상 " + dsts.size() + "곳");
	}
}
