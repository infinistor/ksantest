package org.example.replication.cases;

import java.util.List;

import org.example.replication.Common;

/** TC01 기본 복제 테스트: 새 버킷 쌍에 복제를 걸고 업로드한 파일과 bucket usage 가 같아지는지 본다. */
public class Tc01Basic extends ReplicationCase {
	public Tc01Basic() {
		super("TC01", "기본 복제 테스트", "파일 업로드");
	}

	@Override
	protected void operate(List<String> detail) {
		var keys = sizedKeys();
		long total = 0;
		for (var k : keys) {
			put(k.key(), k.size());
			total += k.size();
		}
		detail.add(keys.size() + "개, " + Common.formatSize(total));
	}
}
