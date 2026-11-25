package org.example.replication.cases;

import java.util.Comparator;

/** 객체 버전 하나: (키, 버전 ID). */
public record VersionKey(String key, String versionId) implements Comparable<VersionKey> {
	private static final Comparator<VersionKey> ORDER = Comparator.comparing(VersionKey::key)
			.thenComparing(VersionKey::versionId);

	@Override
	public int compareTo(VersionKey o) {
		return ORDER.compare(this, o);
	}

	@Override
	public String toString() {
		return "('" + key + "', '" + versionId + "')";
	}
}
