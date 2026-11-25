package org.example.replication.cases;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.example.replication.CaseFailed;
import org.example.replication.Common;
import org.example.replication.Common.Poll;

import software.amazon.awssdk.services.s3.model.Tag;

/**
 * TC12 태그 변경: 복제가 끝난 뒤 바꾼 태그(PutObjectTagging·DeleteObjectTagging)가 대상에 반영되고,
 * 그 복제가 usage 를 두 번 세지 않는다.
 */
public class Tc12Tagging extends ReplicationCase {
	static final Map<String, String> CHANGED_TAG = Map.of("changed", "true");

	public Tc12Tagging() {
		super("TC12", "태그 변경 복제", "복제 후 태그 변경·삭제");
	}

	@Override
	protected void operate(List<String> detail) throws Exception {
		List<VersionKey> versions = new ArrayList<>();
		for (var k : sizedKeys())
			versions.add(new VersionKey(k.key(), put(k.key(), k.size())));
		double waited = waitReplicated();

		for (int i = 0; i < versions.size(); i++) {
			VersionKey kv = versions.get(i);
			Map<String, String> tags;
			if (i % 2 == 0) {
				tags = sourceTags(kv.key(), CHANGED_TAG);
				List<Tag> tagSet = tags.entrySet().stream()
						.map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build()).toList();
				s3.putObjectTagging(b -> b.bucket(src).key(kv.key()).versionId(kv.versionId())
						.tagging(t -> t.tagSet(tagSet)));
			} else {
				tags = Map.of();
				s3.deleteObjectTagging(b -> b.bucket(src).key(kv.key()).versionId(kv.versionId()));
			}
			srcExpected.get(kv).put(TAGS, tags);
			dstExpected.get(kv).put(TAGS, Map.copyOf(tags));
		}

		var w = Common.waitUntil(() -> {
			List<String> pending = new ArrayList<>();
			for (VersionKey kv : versions)
				if (!getTags(dstS3, dst, kv.key(), kv.versionId()).equals(dstExpected.get(kv).get(TAGS)))
					pending.add(kv.key());
			return new Poll<>(pending.isEmpty(), pending);
		}, Common.REPLICATION_TIMEOUT_SEC, Common.REPLICATION_POLL_SEC);
		if (!w.done())
			throw new CaseFailed(Common.REPLICATION_TIMEOUT_SEC + "초 안에 태그가 대상에 반영되지 않은 키 "
					+ w.state().size() + "개: " + head(w.state()));
		detail.add(String.format("PUT %d개, 1차 복제 %.1f초, 태그 변경 %d개·삭제 %d개, 태그 반영 %.1f초", versions.size(), waited,
				(versions.size() + 1) / 2, versions.size() / 2, w.elapsed()));
	}
}
