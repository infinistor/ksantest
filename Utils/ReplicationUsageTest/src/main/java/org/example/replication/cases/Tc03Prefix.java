package org.example.replication.cases;

import java.util.List;
import java.util.Map;

/** TC03 Prefix 필터: 접두사가 맞는 키만 복제되고 나머지는 대상에 오지 않는다. */
public class Tc03Prefix extends ReplicationCase {
	static final String PREFIX_IN = "in/";
	static final String PREFIX_OUT = "out/";

	public Tc03Prefix() {
		super("TC03", "Prefix 필터 복제", PREFIX_IN + "·" + PREFIX_OUT + " 키 PUT");
		ruleFilter = prefixFilter(PREFIX_IN);
		hasNegative = true;
	}

	@Override
	protected boolean replicates(String key, Map<String, String> tags) {
		return key.startsWith(PREFIX_IN);
	}

	@Override
	protected void operate(List<String> detail) {
		for (String prefix : List.of(PREFIX_IN, PREFIX_OUT))
			for (var k : sizedKeys(prefix))
				put(k.key(), k.size());
	}
}
