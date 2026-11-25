package org.example.replication.cases;

import java.util.List;
import java.util.Map;

/** TC14 Prefix + 삭제 마커 복제: 접두사가 맞는 키의 삭제 마커만 대상에 생기고, 나머지 키의 마커는 오지 않는다. */
public class Tc14PrefixDeleteMarker extends ReplicationCase {
	public Tc14PrefixDeleteMarker() {
		super("TC14", "Prefix 필터 + 삭제 마커 복제",
				Tc03Prefix.PREFIX_IN + "·" + Tc03Prefix.PREFIX_OUT + " PUT 후 삭제(마커 생성)");
		ruleFilter = prefixFilter(Tc03Prefix.PREFIX_IN);
		deleteMarkerReplication = true;
		hasNegative = true;
	}

	@Override
	protected boolean replicates(String key, Map<String, String> tags) {
		return key.startsWith(Tc03Prefix.PREFIX_IN);
	}

	@Override
	protected void operate(List<String> detail) {
		for (String prefix : List.of(Tc03Prefix.PREFIX_IN, Tc03Prefix.PREFIX_OUT))
			Tc06DeleteMarkerOff.putThenDelete(this, detail, prefix);
	}
}
