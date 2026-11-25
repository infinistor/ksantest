package org.example.replication.cases;

import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.s3.model.Tag;

/** TC04 Tag 필터: 필터 태그가 붙은 객체만 복제된다. */
public class Tc04Tag extends ReplicationCase {
	static final Tag FILTER_TAG = Tag.builder().key("Replication").value("True").build();

	static boolean hasFilterTag(Map<String, String> tags) {
		return FILTER_TAG.value().equals(tags.get(FILTER_TAG.key()));
	}

	/** 짝수 번째에만 필터 태그를 붙인다. */
	static Map<String, String> filterTags(int index) {
		return index % 2 == 0 ? Map.of(FILTER_TAG.key(), FILTER_TAG.value()) : Map.of();
	}

	public Tc04Tag() {
		super("TC04", "Tag 필터 복제", "태그 있는·없는 객체 PUT");
		ruleFilter = tagFilter(FILTER_TAG);
		hasNegative = true;
	}

	@Override
	protected boolean replicates(String key, Map<String, String> tags) {
		return hasFilterTag(tags);
	}

	@Override
	protected void operate(List<String> detail) {
		var keys = sizedKeys();
		for (int i = 0; i < keys.size(); i++)
			put(keys.get(i).key(), keys.get(i).size(), filterTags(i));
	}
}
