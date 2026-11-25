package org.example.replication.cases;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/** TC06 삭제 마커 복제 끔: 원본에 생긴 삭제 마커가 대상으로 가지 않는다. 마커는 usage 에 세지 않는다. */
public class Tc06DeleteMarkerOff extends ReplicationCase {
	public Tc06DeleteMarkerOff() {
		super("TC06", "삭제 마커 복제 끔", "PUT 후 삭제(마커 생성)");
		deleteMarkerReplication = false;
		hasNegative = true;
	}

	/** 전부 PUT 한 뒤 절반을 버전 없이 지운다(그 절반의 앞쪽은 DeleteObject, 뒤쪽은 DeleteObjects). */
	static void putThenDelete(ReplicationCase c, List<String> detail, String prefix) {
		List<String> keys = new ArrayList<>();
		for (var k : c.sizedKeys(prefix)) {
			c.put(k.key(), k.size());
			keys.add(k.key());
		}
		List<String> targets = keys.subList(0, Math.max(1, keys.size() / 2));
		int half = (targets.size() + 1) / 2;
		List<String> singles = targets.subList(0, half);
		List<String> batch = targets.subList(half, targets.size());
		for (String key : singles)
			c.s3.deleteObject(b -> b.bucket(c.src).key(key));
		if (!batch.isEmpty())
			c.s3.deleteObjects(b -> b.bucket(c.src).delete(Delete.builder()
					.objects(batch.stream().map(k -> ObjectIdentifier.builder().key(k).build()).toList())
					.quiet(true).build()));
		detail.add(prefix + "PUT " + keys.size() + "개, 삭제 마커 " + targets.size() + "개(DeleteObject "
				+ singles.size() + ", DeleteObjects " + batch.size() + ")");
	}

	@Override
	protected void operate(List<String> detail) {
		putThenDelete(this, detail, "");
	}
}
