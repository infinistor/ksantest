package org.example.replication.cases;

import java.util.List;
import java.util.Map;

/** TC05 Prefix + Tag 필터: 접두사와 태그가 모두 맞는 객체만 복제된다. */
public class Tc05PrefixTag extends ReplicationCase {
	public Tc05PrefixTag() {
		super("TC05", "Prefix + Tag 필터 복제", "접두사·태그 4조합 PUT");
		ruleFilter = andFilter(Tc03Prefix.PREFIX_IN, Tc04Tag.FILTER_TAG);
		hasNegative = true;
	}

	@Override
	protected boolean replicates(String key, Map<String, String> tags) {
		return key.startsWith(Tc03Prefix.PREFIX_IN) && Tc04Tag.hasFilterTag(tags);
	}

	@Override
	protected void operate(List<String> detail) {
		for (String prefix : List.of(Tc03Prefix.PREFIX_IN, Tc03Prefix.PREFIX_OUT)) {
			var keys = sizedKeys(prefix);
			for (int i = 0; i < keys.size(); i++)
				put(keys.get(i).key(), keys.get(i).size(), Tc04Tag.filterTags(i));
		}
	}
}
